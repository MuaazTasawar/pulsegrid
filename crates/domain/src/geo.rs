use geohash::{decode, encode, neighbors, Coord};
use std::collections::HashSet;

use crate::device::Location;
use crate::errors::AppError;

/// Full geohash precision used for device location lookups. 7 chars is
/// roughly a 153m x 153m cell -- fine-grained enough to identify "is this
/// device near this alert" without storing raw lat/lon everywhere.
pub const GEOHASH_PRECISION: usize = 7;

/// Coarser prefix length used for shard routing. 4 chars is roughly a
/// 39km x 19.5km cell -- coarse enough that a single fanout-worker owns a
/// meaningful chunk of geography instead of thousands of tiny shards,
/// fine enough that one shard doesn't end up owning an entire country.
pub const SHARD_PREFIX_LENGTH: usize = 4;

const METERS_PER_DEGREE_LAT: f64 = 111_320.0;
/// Hard safety cap on ring expansion so a malformed radius can never spin
/// the BFS into an unbounded loop.
const MAX_RING_EXPANSION: usize = 40;

/// Encodes a location into a full-precision geohash string.
pub fn encode_location(location: &Location) -> Result<String, AppError> {
    let coord = Coord {
        x: location.lon,
        y: location.lat,
    };
    encode(coord, GEOHASH_PRECISION).map_err(|e| AppError::GeohashError(e.to_string()))
}

/// Truncates a full-precision geohash down to the shard-routing prefix.
/// This is what determines which fanout-worker instance owns a device.
pub fn shard_prefix_of(geohash: &str) -> String {
    geohash.chars().take(SHARD_PREFIX_LENGTH).collect()
}

/// The core routing algorithm: given an alert's center point and radius,
/// returns every shard prefix whose geographic cell intersects the alert
/// circle. The coordinator publishes the alert once per returned prefix
/// (as NATS subject `alerts.geo.<prefix>`) instead of broadcasting to
/// every connected device -- this is what keeps fanout cost proportional
/// to the alert radius instead of the total user count.
///
/// Algorithm: BFS ring expansion over the geohash neighbor grid, starting
/// from the center cell and expanding outward one ring per iteration
/// until the covered radius exceeds the requested alert radius. Grid
/// expansion over-covers slightly at the diagonal edges of each ring, so
/// a haversine-distance prune pass drops any candidate cell whose decoded
/// center falls outside `radius + one cell diagonal` of the true center --
/// keeping the result tight instead of alerting a needlessly wide area.
pub fn shard_prefixes_for_radius(
    center: &Location,
    radius_meters: f64,
) -> Result<Vec<String>, AppError> {
    if radius_meters <= 0.0 || radius_meters > 500_000.0 {
        return Err(AppError::InvalidRadius(radius_meters));
    }

    let center_hash = {
        let coord = Coord {
            x: center.lon,
            y: center.lat,
        };
        encode(coord, SHARD_PREFIX_LENGTH).map_err(|e| AppError::GeohashError(e.to_string()))?
    };

    let cell_size_m = cell_size_meters(&center_hash, center.lat)?;
    let rings_needed = ((radius_meters / cell_size_m).ceil() as usize)
        .max(1)
        .min(MAX_RING_EXPANSION);

    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = vec![center_hash.clone()];
    visited.insert(center_hash);

    for _ in 0..rings_needed {
        let mut next_frontier = Vec::new();
        for hash in &frontier {
            let n = neighbors(hash).map_err(|e| AppError::GeohashError(e.to_string()))?;
            for candidate in [n.n, n.ne, n.e, n.se, n.s, n.sw, n.w, n.nw] {
                if visited.insert(candidate.clone()) {
                    next_frontier.push(candidate);
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }

    let diagonal_buffer_m = cell_size_m * std::f64::consts::SQRT_2;
    let mut result = Vec::with_capacity(visited.len());
    for hash in visited {
        let (coord, _, _) = decode(&hash).map_err(|e| AppError::GeohashError(e.to_string()))?;
        let distance = haversine_distance_meters(center.lat, center.lon, coord.y, coord.x);
        if distance <= radius_meters + diagonal_buffer_m {
            result.push(hash);
        }
    }

    Ok(result)
}

/// Estimates the width of a geohash cell in meters at the given latitude,
/// using the decoded error margins (half-cell-width in degrees) doubled
/// and converted via the standard meters-per-degree approximation.
fn cell_size_meters(hash: &str, lat: f64) -> Result<f64, AppError> {
    let (_, lon_err, lat_err) =
        decode(hash).map_err(|e| AppError::GeohashError(e.to_string()))?;
    let meters_per_degree_lon = METERS_PER_DEGREE_LAT * lat.to_radians().cos();
    let width_m = 2.0 * lon_err * meters_per_degree_lon;
    let height_m = 2.0 * lat_err * METERS_PER_DEGREE_LAT;
    Ok(width_m.min(height_m).max(1.0))
}

/// Standard haversine great-circle distance in meters between two points.
pub fn haversine_distance_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    let (lat1_r, lat2_r) = (lat1.to_radians(), lat2.to_radians());
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1_r.cos() * lat2_r.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn center_point_included_in_its_own_radius() {
        let center = Location::new(24.8607, 67.0011).unwrap(); // Karachi
        let prefixes = shard_prefixes_for_radius(&center, 5_000.0).unwrap();
        let center_prefix = shard_prefix_of(&encode_location(&center).unwrap());
        assert!(prefixes.contains(&center_prefix));
    }

    #[test]
    fn larger_radius_returns_more_or_equal_prefixes() {
        let center = Location::new(24.8607, 67.0011).unwrap();
        let small = shard_prefixes_for_radius(&center, 5_000.0).unwrap();
        let large = shard_prefixes_for_radius(&center, 50_000.0).unwrap();
        assert!(large.len() >= small.len());
    }

    #[test]
    fn rejects_invalid_radius() {
        let center = Location::new(24.8607, 67.0011).unwrap();
        assert!(shard_prefixes_for_radius(&center, 0.0).is_err());
        assert!(shard_prefixes_for_radius(&center, 1_000_000.0).is_err());
    }

    #[test]
    fn haversine_zero_distance_for_same_point() {
        let d = haversine_distance_meters(24.86, 67.00, 24.86, 67.00);
        assert!(d < 0.001);
    }
}