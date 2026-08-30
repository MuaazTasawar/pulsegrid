use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config, Pool, Runtime};
use domain::AppError;

const PRESENCE_TTL_SECONDS: u64 = 60;

pub fn build_pool(redis_url: &str) -> Result<Pool, AppError> {
    let cfg = Config::from_url(redis_url);
    cfg.create_pool(Some(Runtime::Tokio1))
        .map_err(|e| AppError::Cache(e.to_string()))
}

#[derive(Clone)]
pub struct PresenceRegistry {
    pool: Pool,
}

impl PresenceRegistry {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(device_id: &str) -> String {
        format!("presence:{device_id}")
    }

    pub async fn register(&self, device_id: &str, worker_instance_id: &str) -> Result<(), AppError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        conn.set_ex::<_, _, ()>(Self::key(device_id), worker_instance_id, PRESENCE_TTL_SECONDS)
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        Ok(())
    }

    pub async fn heartbeat(&self, device_id: &str) -> Result<(), AppError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        conn.expire::<_, ()>(Self::key(device_id), PRESENCE_TTL_SECONDS as i64)
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        Ok(())
    }

    pub async fn lookup(&self, device_id: &str) -> Result<Option<String>, AppError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        let worker_id: Option<String> = conn
            .get(Self::key(device_id))
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        Ok(worker_id)
    }

    pub async fn deregister(&self, device_id: &str) -> Result<(), AppError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        conn.del::<_, ()>(Self::key(device_id))
            .await
            .map_err(|e| AppError::Cache(e.to_string()))?;
        Ok(())
    }
}