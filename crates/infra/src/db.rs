use domain::device::Location;
use domain::{AppError, Device, DeviceId};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub async fn connect(database_url: &str) -> Result<PgPool, AppError> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await
        .map_err(|e| AppError::Database(e.to_string()))
}

pub async fn run_migrations(pool: &PgPool) -> Result<(), AppError> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        .map_err(|e| AppError::Database(e.to_string()))
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
        .map_err(|e| AppError::Database(e.to_string()))?;
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
        .map_err(|e| AppError::Database(e.to_string()))?;

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
        .map_err(|e| AppError::Database(e.to_string()))?
        .ok_or_else(|| AppError::DeviceNotFound(id.to_string()))?;

        row.try_into()
    }

    /// Counts devices currently assigned to a shard prefix -- used to
    /// populate `alert_deliveries.devices_in_shard` for the audit trail
    /// and the live "N devices notified" counter in the demo.
    pub async fn count_in_shard(&self, shard_prefix: &str) -> Result<i64, AppError> {
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM devices WHERE shard_prefix = $1")
                .bind(shard_prefix)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(count.0)
    }
}

#[derive(sqlx::FromRow)]
struct DeviceRow {
    id: uuid::Uuid,
    lat: f64,
    lon: f64,
    geohash: String,
    shard_prefix: String,
    registered_at: chrono::DateTime<chrono::Utc>,
    last_seen: chrono::DateTime<chrono::Utc>,
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

#[derive(Clone)]
pub struct AlertRepository {
    pool: PgPool,
}

impl AlertRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_alert(&self, alert: &domain::Alert) -> Result<(), AppError> {
        let severity = match alert.severity {
            domain::AlertSeverity::Info => "info",
            domain::AlertSeverity::Warning => "warning",
            domain::AlertSeverity::Critical => "critical",
        };
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
        .bind(severity)
        .bind(&alert.title)
        .bind(&alert.message)
        .bind(alert.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// Records that an alert was published to a given shard, and how many
    /// devices were in that shard at publish time. Called once per shard
    /// prefix from alert_dispatch_service (Phase 4) right after the NATS
    /// publish succeeds -- this table is the ground truth for the demo's
    /// "N devices notified" counter and latency audit.
    pub async fn record_delivery(
        &self,
        alert_id: domain::AlertId,
        shard_prefix: &str,
        devices_in_shard: i64,
    ) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO alert_deliveries (alert_id, shard_prefix, published_at, devices_in_shard)
            VALUES ($1, $2, now(), $3)
            "#,
        )
        .bind(alert_id.0)
        .bind(shard_prefix)
        .bind(devices_in_shard as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }
}