use std::time::Duration;

use chrono::{DateTime, Utc};
use domain::device::Location;
use domain::{geo, Alert, AlertId, AlertSeverity, AppError, Device, DeviceId};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

fn db_err(e: sqlx::Error) -> AppError {
    AppError::Database(e.to_string())
}

pub async fn connect(database_url: &str) -> Result<PgPool, AppError> {
    PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
        .map_err(db_err)
}

pub async fn run_migrations(pool: &PgPool) -> Result<(), AppError> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        .map_err(|e| AppError::Database(e.to_string()))
}

/// Cheap liveness probe used by the readiness endpoints (2 s budget).
pub async fn ping(pool: &PgPool) -> Result<(), AppError> {
    ping_with_timeout(pool, Duration::from_secs(2)).await
}

/// Like `ping`, with a caller-chosen budget (tests on a loaded machine
/// use a generous one).
pub async fn ping_with_timeout(pool: &PgPool, timeout: Duration) -> Result<(), AppError> {
    match tokio::time::timeout(timeout, sqlx::query("SELECT 1").execute(pool)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(db_err(e)),
        Err(_) => Err(AppError::Database("postgres ping timed out".into())),
    }
}

/// SQL predicate "device lies inside the circle". It first applies the
/// cheap latitude/longitude bounding boxes (one or two, the second covers
/// an antimeridian wrap) and only then the exact haversine distance.
///
/// Placeholders: $2 center lat, $3 center lon, $4 min lat, $5 max lat,
/// $6/$7 first lon range, $8/$9 second lon range, $10 radius in meters.
macro_rules! in_circle_sql {
    () => {
        "(lat BETWEEN $4 AND $5 \
          AND ((lon BETWEEN $6 AND $7) OR (lon BETWEEN $8 AND $9)) \
          AND 2 * 6371000.0 * asin(LEAST(1.0, sqrt( \
                power(sin(radians(lat - $2) / 2), 2) \
              + cos(radians($2)) * cos(radians(lat)) * power(sin(radians(lon - $3) / 2), 2) \
          ))) <= $10)"
    };
}

/// The bound parameters for `in_circle_sql!`.
struct CircleParams {
    lat: f64,
    lon: f64,
    min_lat: f64,
    max_lat: f64,
    lon1: (f64, f64),
    lon2: (f64, f64),
    radius: f64,
}

impl CircleParams {
    fn new(center: &Location, radius_meters: f64) -> Result<Self, AppError> {
        let boxes = geo::bounding_boxes(center, radius_meters)?;
        let first = boxes[0];
        // BETWEEN with reversed bounds is always false, so (1, 0) is an
        // empty range for "no second box".
        let lon2 = boxes
            .get(1)
            .map(|b| (b.min_lon, b.max_lon))
            .unwrap_or((1.0, 0.0));
        Ok(Self {
            lat: center.lat,
            lon: center.lon,
            min_lat: first.min_lat,
            max_lat: first.max_lat,
            lon1: (first.min_lon, first.max_lon),
            lon2,
            radius: radius_meters,
        })
    }
}

/// Registered devices in a shard: the whole cell, and the part of it that
/// is inside the alert circle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardCounts {
    pub in_shard: i64,
    pub in_radius: i64,
}

/// A device reduced to what the dashboard map needs.
#[derive(Debug, Clone, PartialEq, Serialize, sqlx::FromRow)]
pub struct DeviceSummary {
    pub id: Uuid,
    pub lat: f64,
    pub lon: f64,
    pub shard_prefix: String,
}

#[derive(Clone)]
pub struct DeviceRepository {
    pool: PgPool,
}

impl DeviceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, device: &Device) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO devices (id, lat, lon, geohash, shard_prefix, registered_at, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(device.id.0)
        .bind(device.location.lat)
        .bind(device.location.lon)
        .bind(&device.geohash)
        .bind(&device.shard_prefix)
        .bind(device.registered_at)
        .bind(device.last_seen)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn update_location(
        &self,
        id: DeviceId,
        location: Location,
        geohash: &str,
        shard_prefix: &str,
    ) -> Result<(), AppError> {
        let result = sqlx::query(
            r#"
            UPDATE devices
            SET lat = $2, lon = $3, geohash = $4, shard_prefix = $5, last_seen = now()
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .bind(location.lat)
        .bind(location.lon)
        .bind(geohash)
        .bind(shard_prefix)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        if result.rows_affected() == 0 {
            return Err(AppError::DeviceNotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn find_by_id(&self, id: DeviceId) -> Result<Device, AppError> {
        let row = sqlx::query_as::<_, DeviceRow>(
            r#"SELECT id, lat, lon, geohash, shard_prefix, registered_at, last_seen
               FROM devices WHERE id = $1"#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?
        .ok_or_else(|| AppError::DeviceNotFound(id.to_string()))?;

        row.try_into()
    }

    /// Counts every device assigned to a shard prefix (the whole cell).
    pub async fn count_in_shard(&self, shard_prefix: &str) -> Result<i64, AppError> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM devices WHERE shard_prefix = $1")
            .bind(shard_prefix)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(count.0)
    }

    /// Counts the devices of one shard, and how many of them lie inside the
    /// alert circle, in a single pass over the shard's index range.
    pub async fn count_in_shard_and_radius(
        &self,
        shard_prefix: &str,
        center: &Location,
        radius_meters: f64,
    ) -> Result<ShardCounts, AppError> {
        let p = CircleParams::new(center, radius_meters)?;
        let (in_shard, in_radius): (i64, i64) = sqlx::query_as(concat!(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE ",
            in_circle_sql!(),
            ") FROM devices WHERE shard_prefix = $1"
        ))
        .bind(shard_prefix)
        .bind(p.lat)
        .bind(p.lon)
        .bind(p.min_lat)
        .bind(p.max_lat)
        .bind(p.lon1.0)
        .bind(p.lon1.1)
        .bind(p.lon2.0)
        .bind(p.lon2.1)
        .bind(p.radius)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(ShardCounts {
            in_shard,
            in_radius,
        })
    }

    /// Devices inside the circle, restricted to the given shard prefixes
    /// (so the shard index does the heavy lifting). Used by the dashboard.
    pub async fn list_in_circle(
        &self,
        shard_prefixes: &[String],
        center: &Location,
        radius_meters: f64,
        limit: i64,
    ) -> Result<Vec<DeviceSummary>, AppError> {
        let p = CircleParams::new(center, radius_meters)?;
        // $1 is the shard list here, so the shared placeholders $2..$10
        // line up with the same binds as the count query.
        sqlx::query_as::<_, DeviceSummary>(concat!(
            "SELECT id, lat, lon, shard_prefix FROM devices \
             WHERE shard_prefix = ANY($1) AND ",
            in_circle_sql!(),
            " ORDER BY id LIMIT $11"
        ))
        .bind(shard_prefixes)
        .bind(p.lat)
        .bind(p.lon)
        .bind(p.min_lat)
        .bind(p.max_lat)
        .bind(p.lon1.0)
        .bind(p.lon1.1)
        .bind(p.lon2.0)
        .bind(p.lon2.1)
        .bind(p.radius)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)
    }

    /// Marks a device as seen now (the client heartbeat).
    pub async fn touch(&self, id: DeviceId) -> Result<(), AppError> {
        let result = sqlx::query("UPDATE devices SET last_seen = now() WHERE id = $1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        if result.rows_affected() == 0 {
            return Err(AppError::DeviceNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Deletes a device. Returns whether a row existed.
    pub async fn delete(&self, id: DeviceId) -> Result<bool, AppError> {
        let result = sqlx::query("DELETE FROM devices WHERE id = $1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(result.rows_affected() > 0)
    }

    /// Deletes up to `batch` devices not seen since `cutoff` (oldest
    /// first) and returns how many were removed. Call repeatedly until it
    /// returns less than `batch`.
    pub async fn purge_inactive(&self, cutoff: DateTime<Utc>, batch: i64) -> Result<u64, AppError> {
        let result = sqlx::query(
            r#"
            DELETE FROM devices WHERE id IN (
                SELECT id FROM devices WHERE last_seen < $1 ORDER BY last_seen LIMIT $2
            )
            "#,
        )
        .bind(cutoff)
        .bind(batch)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(result.rows_affected())
    }

    pub async fn count_all(&self) -> Result<i64, AppError> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM devices")
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(count.0)
    }
}

#[derive(sqlx::FromRow)]
struct DeviceRow {
    id: Uuid,
    lat: f64,
    lon: f64,
    geohash: String,
    shard_prefix: String,
    registered_at: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}

impl TryFrom<DeviceRow> for Device {
    type Error = AppError;

    fn try_from(row: DeviceRow) -> Result<Self, Self::Error> {
        let location = Location::new(row.lat, row.lon)?;
        Ok(Device {
            id: DeviceId(row.id),
            location,
            geohash: row.geohash,
            shard_prefix: row.shard_prefix,
            registered_at: row.registered_at,
            last_seen: row.last_seen,
        })
    }
}

/// One audit-trail row: an alert published to one shard.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeliveryRecord {
    pub shard_prefix: String,
    pub published_at: DateTime<Utc>,
    /// Registered devices in the whole shard cell.
    pub devices_in_shard: i64,
    /// Registered devices inside the alert circle.
    pub devices_in_radius: i64,
}

#[derive(Clone)]
pub struct AlertRepository {
    pool: PgPool,
}

impl AlertRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_alert(&self, alert: &Alert) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO alerts (id, center_lat, center_lon, radius_meters, severity, title, message, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(alert.id.0)
        .bind(alert.center.lat)
        .bind(alert.center.lon)
        .bind(alert.radius_meters)
        .bind(alert.severity.as_str())
        .bind(&alert.title)
        .bind(&alert.message)
        .bind(alert.created_at)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Records that an alert was published to a shard, with both the whole
    /// shard's device count and the in-circle count. Idempotent: a second
    /// call for the same (alert, shard) keeps the first row.
    pub async fn record_shard_delivery(
        &self,
        alert_id: AlertId,
        shard_prefix: &str,
        counts: ShardCounts,
    ) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO alert_deliveries
                (id, alert_id, shard_prefix, published_at, devices_in_shard, devices_in_radius)
            VALUES ($1, $2, $3, $6, $4, $5)
            ON CONFLICT (alert_id, shard_prefix) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(alert_id.0)
        .bind(shard_prefix)
        .bind(counts.in_shard as i32)
        .bind(counts.in_radius as i32)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Legacy form that only knows the whole-shard count (the in-circle
    /// count is stored as 0). Prefer `record_shard_delivery`.
    pub async fn record_delivery(
        &self,
        alert_id: AlertId,
        shard_prefix: &str,
        devices_in_shard: i64,
    ) -> Result<(), AppError> {
        self.record_shard_delivery(
            alert_id,
            shard_prefix,
            ShardCounts {
                in_shard: devices_in_shard,
                in_radius: 0,
            },
        )
        .await
    }

    pub async fn get_alert(&self, id: AlertId) -> Result<Alert, AppError> {
        let row = sqlx::query_as::<_, AlertRow>(
            r#"SELECT id, center_lat, center_lon, radius_meters, severity, title, message, created_at
               FROM alerts WHERE id = $1"#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?
        .ok_or_else(|| AppError::NotFound(format!("alert {}", id.0)))?;
        row.try_into()
    }

    /// Newest alerts first.
    pub async fn recent_alerts(&self, limit: i64) -> Result<Vec<Alert>, AppError> {
        let rows = sqlx::query_as::<_, AlertRow>(
            r#"SELECT id, center_lat, center_lon, radius_meters, severity, title, message, created_at
               FROM alerts ORDER BY created_at DESC LIMIT $1"#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        rows.into_iter().map(Alert::try_from).collect()
    }

    pub async fn list_deliveries(
        &self,
        alert_id: AlertId,
    ) -> Result<Vec<DeliveryRecord>, AppError> {
        let rows = sqlx::query_as::<_, DeliveryRow>(
            r#"SELECT shard_prefix, published_at, devices_in_shard, devices_in_radius
               FROM alert_deliveries WHERE alert_id = $1 ORDER BY shard_prefix"#,
        )
        .bind(alert_id.0)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|r| DeliveryRecord {
                shard_prefix: r.shard_prefix,
                published_at: r.published_at,
                devices_in_shard: r.devices_in_shard as i64,
                devices_in_radius: r.devices_in_radius as i64,
            })
            .collect())
    }

    /// Deletes up to `batch` alerts older than `cutoff` (their delivery
    /// rows go with them via ON DELETE CASCADE) and returns how many.
    pub async fn purge_older_than(
        &self,
        cutoff: DateTime<Utc>,
        batch: i64,
    ) -> Result<u64, AppError> {
        let result = sqlx::query(
            r#"
            DELETE FROM alerts WHERE id IN (
                SELECT id FROM alerts WHERE created_at < $1 ORDER BY created_at LIMIT $2
            )
            "#,
        )
        .bind(cutoff)
        .bind(batch)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(result.rows_affected())
    }
}

#[derive(sqlx::FromRow)]
struct AlertRow {
    id: Uuid,
    center_lat: f64,
    center_lon: f64,
    radius_meters: f64,
    severity: String,
    title: String,
    message: String,
    created_at: DateTime<Utc>,
}

impl TryFrom<AlertRow> for Alert {
    type Error = AppError;

    fn try_from(row: AlertRow) -> Result<Self, Self::Error> {
        let severity = AlertSeverity::parse(&row.severity)
            .ok_or_else(|| AppError::Database(format!("unknown severity '{}'", row.severity)))?;
        Ok(Alert {
            id: AlertId(row.id),
            center: Location::new(row.center_lat, row.center_lon)?,
            radius_meters: row.radius_meters,
            severity,
            title: row.title,
            message: row.message,
            created_at: row.created_at,
        })
    }
}

#[derive(sqlx::FromRow)]
struct DeliveryRow {
    shard_prefix: String,
    published_at: DateTime<Utc>,
    devices_in_shard: i32,
    devices_in_radius: i32,
}
