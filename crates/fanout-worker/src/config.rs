use domain::AppError;
use uuid::Uuid;

#[derive(Clone)]
pub struct Config {
    pub redis_url: String,
    pub nats_url: String,
    pub jwt_secret: String,
    pub port: u16,
    /// The shard prefixes this worker instance owns. Only devices whose
    /// own shard_prefix is in this set may connect here -- the ws
    /// handler rejects any other prefix so a misrouted client fails
    /// fast instead of silently missing alerts.
    pub owned_prefixes: Vec<String>,
    /// Unique identifier for this worker instance, used as the value
    /// stored in Redis's presence registry (Phase 2's PresenceRegistry)
    /// so a device's connection can be traced back to which physical
    /// instance is holding it.
    pub instance_id: String,
}

impl Config {
    pub fn from_env() -> Result<Self, AppError> {
        dotenvy::dotenv().ok();

        let redis_url = std::env::var("REDIS_URL")
            .map_err(|_| AppError::Validation("REDIS_URL not set".into()))?;
        let nats_url = std::env::var("NATS_URL")
            .map_err(|_| AppError::Validation("NATS_URL not set".into()))?;
        let jwt_secret = std::env::var("JWT_SECRET")
            .map_err(|_| AppError::Validation("JWT_SECRET not set".into()))?;
        let port = std::env::var("FANOUT_WORKER_PORT")
            .unwrap_or_else(|_| "8081".to_string())
            .parse::<u16>()
            .map_err(|_| AppError::Validation("FANOUT_WORKER_PORT must be a valid port".into()))?;
        let owned_prefixes = std::env::var("FANOUT_WORKER_SHARD_PREFIXES")
            .map_err(|_| AppError::Validation("FANOUT_WORKER_SHARD_PREFIXES not set".into()))?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        if owned_prefixes.is_empty() {
            return Err(AppError::Validation(
                "FANOUT_WORKER_SHARD_PREFIXES must list at least one prefix".into(),
            ));
        }

        Ok(Self {
            redis_url,
            nats_url,
            jwt_secret,
            port,
            owned_prefixes,
            instance_id: Uuid::new_v4().to_string(),
        })
    }
}