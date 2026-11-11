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

const EARTH_RADIUS_M: f64 = 6_371_000.0;
/// A cell is targeted when the circle reaches it, with this much slack so
/// floating point error can never drop a cell the circle only just touches.
const EDGE_MARGIN_M: f64 = 1.0;
/// Hard safety cap on ring expansion so a malformed radius can never spin
/// the BFS into an unbounded loop. If a radius needs more rings than this
/// (only possible very close to the poles, where cells get narrow) the
/// request is rejected instead of silently under-covering the circle.
const MAX_RING_EXPANSION: usize = 100;

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
/// until the rings cover the requested radius. The ring count is a safe
/// over-estimate, so a second pass keeps only the cells the circle really
/// reaches: a cell is kept when the distance from the circle's center to
/// the nearest point of the cell is at most the radius (see
/// `distance_to_bounds_meters`). The result is sorted.
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

    // How many cell rings does the circle reach? Use the exact spherical
    // angular extent of the circle (it is wider in longitude than a flat
    // meters-per-degree estimate suggests at high latitude), measured in
    // whole cells. `floor + 1` is the worst case when the center sits on a
    // cell edge.
    let (_, lon_err, lat_err) =
        decode(&center_hash).map_err(|e| AppError::GeohashError(e.to_string()))?;
    let (cell_lon_deg, cell_lat_deg) = (2.0 * lon_err, 2.0 * lat_err);
    let ang = radius_meters / EARTH_RADIUS_M;
    let lat_extent_deg = ang.to_degrees();
    let lon_ratio = ang.sin() / center.lat.to_radians().cos();
    let lon_extent_deg = if lon_ratio >= 1.0 {
        360.0 // the circle reaches a pole
    } else {
        lon_ratio.asin().to_degrees()
    };
    let rings_needed = (((lat_extent_deg / cell_lat_deg).floor() as usize) + 1)
        .max(((lon_extent_deg / cell_lon_deg).floor() as usize) + 1)
        .max(1);
    if rings_needed > MAX_RING_EXPANSION {
        return Err(AppError::Validation(format!(
            "radius {radius_meters} m needs {rings_needed} geohash rings at latitude {:.2} \
             (max {MAX_RING_EXPANSION}); use a smaller radius",
            center.lat
        )));
    }

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

    // Keep only the cells the circle really reaches.
    let mut result = Vec::with_capacity(visited.len());
    for hash in visited {
        let bounds = cell_bounds(&hash)?;
        let nearest = distance_to_bounds_meters(center.lat, center.lon, &bounds);
        if nearest <= radius_meters + EDGE_MARGIN_M {
            result.push(hash);
        }
    }
    result.sort();

    Ok(result)
}

/// Smallest great-circle distance in meters from a point to any point of a
/// cell rectangle (0 when the point is inside it).
///
/// A cell's sides are parallels (constant latitude) and meridians (constant
/// longitude), and on a sphere the nearest point of such a side is not
/// always the one at the point's own latitude or longitude, so each case is
/// worked out exactly:
/// - the point's longitude lies within the cell: the nearest point is on
///   the nearest parallel, at the point's own longitude;
/// - otherwise the nearest point is on the nearer meridian (longitudes wrap
///   at the antimeridian). Along a meridian the distance is smallest at the
///   foot of the perpendicular, whose latitude satisfies
///   `tan(foot) = tan(lat) / cos(dlon)`, clamped to the cell's latitudes.
fn distance_to_bounds_meters(lat: f64, lon: f64, b: &BoundingBox) -> f64 {
    let lat_in = lat >= b.min_lat && lat <= b.max_lat;
    let lon_in = lon >= b.min_lon && lon <= b.max_lon;
    if lat_in && lon_in {
        return 0.0;
    }
    if lon_in {
        let edge_lat = lat.clamp(b.min_lat, b.max_lat);
        return haversine_distance_meters(lat, lon, edge_lat, lon);
    }

    let to_min = wrap_degrees(b.min_lon - lon);
    let to_max = wrap_degrees(b.max_lon - lon);
    let (edge_lon, dlon_deg) = if to_min.abs() <= to_max.abs() {
        (b.min_lon, to_min)
    } else {
        (b.max_lon, to_max)
    };
    let dist_at = |edge_lat: f64| haversine_distance_meters(lat, lon, edge_lat, edge_lon);
    let cos_dlon = dlon_deg.to_radians().cos();
    if cos_dlon > 1e-9 {
        let foot_lat = (lat.to_radians().tan() / cos_dlon).atan().to_degrees();
        dist_at(foot_lat.clamp(b.min_lat, b.max_lat))
    } else {
        // 90 degrees or more away in longitude (only near the poles): the
        // distance changes monotonically along the edge, so an end is nearest.
        dist_at(b.min_lat).min(dist_at(b.max_lat))
    }
}

/// Wraps an angle in degrees into [-180, 180).
fn wrap_degrees(degrees: f64) -> f64 {
    (degrees + 180.0).rem_euclid(360.0) - 180.0
}

/// A latitude/longitude rectangle in degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl BoundingBox {
    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        lat >= self.min_lat && lat <= self.max_lat && lon >= self.min_lon && lon <= self.max_lon
    }
}

/// The rectangle covered by a geohash cell (e.g. a 4-char shard prefix).
/// Used to draw shard cells on the dashboard map.
pub fn cell_bounds(geohash: &str) -> Result<BoundingBox, AppError> {
    let (c, lon_err, lat_err) =
        decode(geohash).map_err(|e| AppError::GeohashError(e.to_string()))?;
    Ok(BoundingBox {
        min_lat: c.y - lat_err,
        max_lat: c.y + lat_err,
        min_lon: c.x - lon_err,
        max_lon: c.x + lon_err,
    })
}

/// Great-circle distance in meters between two locations.
pub fn distance_meters(a: &Location, b: &Location) -> f64 {
    haversine_distance_meters(a.lat, a.lon, b.lat, b.lon)
}

/// One or two rectangles that together contain the whole circle
/// (center, radius). Two rectangles are returned when the circle crosses
/// the antimeridian. Intended as a cheap SQL pre-filter before an exact
/// haversine check; it deliberately over-covers by ~0.1%.
pub fn bounding_boxes(center: &Location, radius_meters: f64) -> Result<Vec<BoundingBox>, AppError> {
    if radius_meters <= 0.0 || radius_meters > 500_000.0 {
        return Err(AppError::InvalidRadius(radius_meters));
    }
    let ang = radius_meters / EARTH_RADIUS_M; // radians
    let dlat = ang.to_degrees() * 1.001;
    let min_lat = center.lat - dlat;
    let max_lat = center.lat + dlat;
    let full_lon = |min_lat: f64, max_lat: f64| BoundingBox {
        min_lat: min_lat.max(-90.0),
        max_lat: max_lat.min(90.0),
        min_lon: -180.0,
        max_lon: 180.0,
    };
    if min_lat <= -90.0 || max_lat >= 90.0 {
        return Ok(vec![full_lon(min_lat, max_lat)]);
    }
    let ratio = ang.sin() / center.lat.to_radians().cos();
    if ratio >= 1.0 {
        return Ok(vec![full_lon(min_lat, max_lat)]);
    }
    let dlon = ratio.asin().to_degrees() * 1.001;
    if dlon >= 180.0 {
        return Ok(vec![full_lon(min_lat, max_lat)]);
    }
    let (lo, hi) = (center.lon - dlon, center.lon + dlon);
    let boxed = |min_lon: f64, max_lon: f64| BoundingBox {
        min_lat,
        max_lat,
        min_lon,
        max_lon,
    };
    if lo < -180.0 {
        Ok(vec![boxed(lo + 360.0, 180.0), boxed(-180.0, hi)])
    } else if hi > 180.0 {
        Ok(vec![boxed(lo, 180.0), boxed(-180.0, hi - 360.0)])
    } else {
        Ok(vec![boxed(lo, hi)])
    }
}

/// Standard haversine great-circle distance in meters between two points.
pub fn haversine_distance_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (lat1_r, lat2_r) = (lat1.to_radians(), lat2.to_radians());
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (d_lon / 2.0).sin().powi(2);
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
    fn a_circle_inside_one_cell_targets_only_that_shard() {
        // The default center is about 8 km from the nearest edge of tkrt.
        let center = Location::new(24.8607, 67.0011).unwrap();
        let prefixes = shard_prefixes_for_radius(&center, 5_000.0).unwrap();
        assert_eq!(prefixes, vec!["tkrt".to_string()]);
    }

    #[test]
    fn a_circle_is_routed_to_a_neighbour_exactly_when_it_reaches_it() {
        // 24.79 is a little over half a kilometre above the south edge of tkrt.
        let center = Location::new(24.79, 67.0011).unwrap();
        let south_edge = cell_bounds("tkrt").unwrap().min_lat;
        let gap = haversine_distance_meters(center.lat, center.lon, south_edge, center.lon);
        assert!(gap > 100.0 && gap < 1_000.0, "unexpected gap {gap}");

        let short = shard_prefixes_for_radius(&center, gap - 5.0).unwrap();
        assert_eq!(short, vec!["tkrt".to_string()]);

        let reaching = shard_prefixes_for_radius(&center, gap + 5.0).unwrap();
        assert_eq!(reaching, vec!["tkrs".to_string(), "tkrt".to_string()]);
    }

    #[test]
    fn a_circle_around_a_cell_corner_targets_four_shards() {
        // Just inside the south-west corner of tkrt.
        let center = Location::new(24.79, 66.80).unwrap();
        let prefixes = shard_prefixes_for_radius(&center, 2_000.0).unwrap();
        assert_eq!(prefixes.len(), 4, "got {prefixes:?}");
        for expected in ["tkrt", "tkrs", "tkrm"] {
            assert!(prefixes.contains(&expected.to_string()), "{prefixes:?}");
        }
    }

    #[test]
    fn nearest_point_distance_is_exact_for_points_around_a_cell() {
        let b = cell_bounds("tkrt").unwrap();
        // inside
        assert_eq!(distance_to_bounds_meters(24.87, 67.0, &b), 0.0);
        // due north, due south: along a meridian
        let north = distance_to_bounds_meters(b.max_lat + 0.01, 67.0, &b);
        let expected = haversine_distance_meters(b.max_lat + 0.01, 67.0, b.max_lat, 67.0);
        assert!((north - expected).abs() < 1e-6);
        let south = distance_to_bounds_meters(b.min_lat - 0.01, 67.0, &b);
        let expected = haversine_distance_meters(b.min_lat - 0.01, 67.0, b.min_lat, 67.0);
        assert!((south - expected).abs() < 1e-6);
        // east and west of the cell: never farther than the same-latitude
        // point on the edge, and never nearer than a fine sample of the edge
        for (lat, lon) in [(24.9, 67.3), (24.9, 66.6), (25.2, 67.5), (24.5, 66.5)] {
            let d = distance_to_bounds_meters(lat, lon, &b);
            let edge_lon = if lon > b.max_lon {
                b.max_lon
            } else {
                b.min_lon
            };
            let mut sampled = f64::MAX;
            let steps = 20_000;
            for i in 0..=steps {
                let edge_lat =
                    b.min_lat + (b.max_lat - b.min_lat) * f64::from(i) / f64::from(steps);
                sampled = sampled.min(haversine_distance_meters(lat, lon, edge_lat, edge_lon));
            }
            assert!(
                d <= sampled + 1e-6,
                "({lat},{lon}): {d} > sampled {sampled}"
            );
            assert!(
                sampled - d < 1.0,
                "({lat},{lon}): {d} far below sampled {sampled}"
            );
        }
    }

    #[test]
    fn haversine_zero_distance_for_same_point() {
        let d = haversine_distance_meters(24.86, 67.00, 24.86, 67.00);
        assert!(d < 0.001);
    }

    /// Point at `dist_m` from (lat, lon) along `bearing_deg` on a sphere.
    fn destination(lat: f64, lon: f64, bearing_deg: f64, dist_m: f64) -> (f64, f64) {
        let ang = dist_m / EARTH_RADIUS_M;
        let (lat1, lon1, brg) = (lat.to_radians(), lon.to_radians(), bearing_deg.to_radians());
        let lat2 = (lat1.sin() * ang.cos() + lat1.cos() * ang.sin() * brg.cos()).asin();
        let lon2 =
            lon1 + (brg.sin() * ang.sin() * lat1.cos()).atan2(ang.cos() - lat1.sin() * lat2.sin());
        let mut lon_deg = lon2.to_degrees();
        while lon_deg > 180.0 {
            lon_deg -= 360.0;
        }
        while lon_deg < -180.0 {
            lon_deg += 360.0;
        }
        (lat2.to_degrees(), lon_deg)
    }

    const CENTERS: [(f64, f64); 8] = [
        (24.8607, 67.0011),   // Karachi
        (0.0, 0.0),           // equator / prime meridian
        (51.5074, -0.1278),   // London
        (-33.8688, 151.2093), // Sydney
        (69.6492, 18.9553),   // Tromso (high latitude)
        (80.0, 10.0),         // very high latitude
        (-17.7, 179.95),      // next to the antimeridian
        (35.0, -179.98),      // other side of the antimeridian
    ];
    const RADII: [f64; 6] = [500.0, 2_000.0, 5_000.0, 30_000.0, 100_000.0, 400_000.0];

    #[test]
    fn every_point_inside_the_circle_lands_in_a_targeted_shard() {
        for (lat, lon) in CENTERS {
            let center = Location::new(lat, lon).unwrap();
            for radius in RADII {
                let prefixes = match shard_prefixes_for_radius(&center, radius) {
                    Ok(p) => p,
                    Err(AppError::Validation(_)) => continue, // too many rings at this latitude
                    Err(e) => panic!("unexpected error at ({lat},{lon}) r={radius}: {e}"),
                };
                for frac in [0.0, 0.5, 0.9, 0.999] {
                    for bearing in (0..360).step_by(10) {
                        let (plat, plon) = destination(lat, lon, bearing as f64, radius * frac);
                        let p = Location::new(plat, plon).unwrap();
                        let prefix = shard_prefix_of(&encode_location(&p).unwrap());
                        assert!(
                            prefixes.contains(&prefix),
                            "point at bearing {bearing}, {:.0}% of r={radius} around ({lat},{lon}) \
                             is in shard {prefix}, which was not targeted",
                            frac * 100.0
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn every_targeted_shard_really_touches_the_circle() {
        for (lat, lon) in CENTERS {
            let center = Location::new(lat, lon).unwrap();
            for radius in RADII {
                let Ok(prefixes) = shard_prefixes_for_radius(&center, radius) else {
                    continue;
                };
                for prefix in prefixes {
                    let b = cell_bounds(&prefix).unwrap();
                    let nearest = distance_to_bounds_meters(lat, lon, &b);
                    assert!(
                        nearest <= radius + EDGE_MARGIN_M,
                        "{prefix} is {nearest:.0} m from ({lat},{lon}) but r={radius}"
                    );
                }
            }
        }
    }

    #[test]
    fn max_radius_works_at_80_degrees_but_is_rejected_near_the_pole() {
        let ok = Location::new(80.0, 10.0).unwrap();
        assert!(shard_prefixes_for_radius(&ok, 500_000.0).is_ok());
        let polar = Location::new(86.0, 10.0).unwrap();
        assert!(matches!(
            shard_prefixes_for_radius(&polar, 500_000.0),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn cell_bounds_contain_their_own_points() {
        let loc = Location::new(24.8607, 67.0011).unwrap();
        let prefix = shard_prefix_of(&encode_location(&loc).unwrap());
        let b = cell_bounds(&prefix).unwrap();
        assert!(b.contains(loc.lat, loc.lon));
        assert!(b.max_lat > b.min_lat && b.max_lon > b.min_lon);
        // 4-char cells are ~39km x ~19.5km
        assert!((b.max_lon - b.min_lon) > 0.3 && (b.max_lat - b.min_lat) > 0.15);
    }

    #[test]
    fn bounding_boxes_cover_the_whole_circle() {
        for (lat, lon) in CENTERS {
            let center = Location::new(lat, lon).unwrap();
            for radius in RADII {
                let boxes = bounding_boxes(&center, radius).unwrap();
                assert!(!boxes.is_empty() && boxes.len() <= 2);
                for bearing in (0..360).step_by(5) {
                    let (plat, plon) = destination(lat, lon, bearing as f64, radius);
                    assert!(
                        boxes.iter().any(|b| b.contains(plat, plon)),
                        "({plat},{plon}) on the circle around ({lat},{lon}) r={radius} is outside all boxes"
                    );
                }
            }
        }
    }

    #[test]
    fn bounding_boxes_split_at_antimeridian_and_widen_at_poles() {
        let near_am = Location::new(-17.7, 179.95).unwrap();
        assert_eq!(bounding_boxes(&near_am, 50_000.0).unwrap().len(), 2);
        let pole = Location::new(89.9, 0.0).unwrap();
        let b = bounding_boxes(&pole, 50_000.0).unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!((b[0].min_lon, b[0].max_lon), (-180.0, 180.0));
        assert!(bounding_boxes(&near_am, 0.0).is_err());
    }
}
