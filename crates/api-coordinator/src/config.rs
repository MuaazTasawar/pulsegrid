use domain::AppError;

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub nats_url: String,
    pub jwt_secret: String,
    pub port: u16,
}

impl Config {
    pub fn from_env() -> Result<Self, AppError> {
        dotenvy::dotenv().ok(); // fine if .env is absent, e.g. in prod with real env vars

        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| AppError::Validation("DATABASE_URL not set".into()))?;
        let redis_url = std::env::var("REDIS_URL")
            .map_err(|_| AppError::Validation("REDIS_URL not set".into()))?;
        let nats_url = std::env::var("NATS_URL")
            .map_err(|_| AppError::Validation("NATS_URL not set".into()))?;
        let jwt_secret = std::env::var("JWT_SECRET")
            .map_err(|_| AppError::Validation("JWT_SECRET not set".into()))?;
        let port = std::env::var("COORDINATOR_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .map_err(|_| AppError::Validation("COORDINATOR_PORT must be a valid port number".into()))?;

        Ok(Self {
            database_url,
            redis_url,
            nats_url,
            jwt_secret,
            port,
        })
    }
}