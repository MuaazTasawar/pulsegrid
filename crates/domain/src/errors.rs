use thiserror::Error;

/// Shared error type across the whole PulseGrid fleet. Each binary crate
/// (api-coordinator, fanout-worker) wraps this into its own edge-specific
/// error type (e.g. an Axum IntoResponse impl) rather than exposing it
/// directly — this stays a pure domain type with zero I/O framework deps.
#[derive(Error, Debug)]
pub enum AppError {
    #[error("device not found: {0}")]
    DeviceNotFound(String),

    #[error("invalid location: lat={lat}, lon={lon}")]
    InvalidLocation { lat: f64, lon: f64 },

    #[error("invalid alert radius: {0} meters (must be > 0 and <= 500000)")]
    InvalidRadius(f64),

    #[error("geohash encoding failed: {0}")]
    GeohashError(String),

    #[error("unauthorized")]
    Unauthorized,

    #[error("validation error: {0}")]
    Validation(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("cache error: {0}")]
    Cache(String),

    #[error("messaging error: {0}")]
    Messaging(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    /// Whether this error represents a client mistake (4xx-shaped) as
    /// opposed to a server/infra failure (5xx-shaped). Edge crates use
    /// this to pick the right HTTP status without duplicating the match.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            AppError::DeviceNotFound(_)
                | AppError::InvalidLocation { .. }
                | AppError::InvalidRadius(_)
                | AppError::Unauthorized
                | AppError::Validation(_)
        )
    }
}