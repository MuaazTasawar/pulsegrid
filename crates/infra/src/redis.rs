use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_redis::redis::{self, AsyncCommands};
use deadpool_redis::{Config, Connection, Runtime};
use domain::device::Location;
use domain::AppError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use deadpool_redis::Pool;

const PRESENCE_TTL_SECONDS: u64 = 60;

fn cache_err<E: std::fmt::Display>(e: E) -> AppError {
    AppError::Cache(e.to_string())
}

async fn conn(pool: &Pool) -> Result<Connection, AppError> {
    pool.get().await.map_err(cache_err)
}

/// Runs a Lua script atomically on the server (plain EVAL, one key).
async fn eval<T: redis::FromRedisValue>(
    pool: &Pool,
    script: &str,
    key: String,
    args: &[String],
) -> Result<T, AppError> {
    let mut c = conn(pool).await?;
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(script).arg(1).arg(key);
    for a in args {
        cmd.arg(a);
    }
    cmd.query_async(&mut c).await.map_err(cache_err)
}

pub fn build_pool(redis_url: &str) -> Result<Pool, AppError> {
    let cfg = Config::from_url(redis_url);
    cfg.create_pool(Some(Runtime::Tokio1)).map_err(cache_err)
}

/// Cheap liveness probe used by the readiness endpoints (2 s budget).
pub async fn ping(pool: &Pool) -> Result<(), AppError> {
    ping_with_timeout(pool, Duration::from_secs(2)).await
}

/// Like `ping`, with a caller-chosen budget.
pub async fn ping_with_timeout(pool: &Pool, timeout: Duration) -> Result<(), AppError> {
    let probe = async {
        let mut c = conn(pool).await?;
        let _: String = redis::cmd("PING")
            .query_async(&mut c)
            .await
            .map_err(cache_err)?;
        Ok::<(), AppError>(())
    };
    tokio::time::timeout(timeout, probe)
        .await
        .map_err(|_| AppError::Cache("redis ping timed out".into()))?
}

// ---------------------------------------------------------------------
// Presence: which worker instance currently holds a device's socket.
// ---------------------------------------------------------------------

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

    pub async fn register(
        &self,
        device_id: &str,
        worker_instance_id: &str,
    ) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.set_ex::<_, _, ()>(
            Self::key(device_id),
            worker_instance_id,
            PRESENCE_TTL_SECONDS,
        )
        .await
        .map_err(cache_err)?;
        Ok(())
    }

    pub async fn heartbeat(&self, device_id: &str) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.expire::<_, ()>(Self::key(device_id), PRESENCE_TTL_SECONDS as i64)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    pub async fn lookup(&self, device_id: &str) -> Result<Option<String>, AppError> {
        let mut conn = conn(&self.pool).await?;
        let worker_id: Option<String> = conn.get(Self::key(device_id)).await.map_err(cache_err)?;
        Ok(worker_id)
    }

    pub async fn deregister(&self, device_id: &str) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.del::<_, ()>(Self::key(device_id))
            .await
            .map_err(cache_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Shard registry: shard prefix -> the worker that owns it (with a lease).
// ---------------------------------------------------------------------

/// Claims the key for `ARGV[1]` (the worker URL) or refreshes its TTL if
/// that worker already holds it. Returns 1 on success, 0 if a different
/// worker holds a live lease. One atomic step, so two workers can never
/// both believe they own a shard.
const CLAIM_LUA: &str = r#"
local cur = redis.call('GET', KEYS[1])
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return 1
elseif cur == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"#;

/// Deletes the key only if it still holds `ARGV[1]`, so a worker that lost
/// its lease can never delete the new owner's entry.
const RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"#;

/// Maps a geohash shard prefix to the base WebSocket URL of the
/// fanout-worker instance that currently owns it. Ownership is a lease:
/// the owner refreshes it periodically (`claim_shard`) and releases it on
/// shutdown, so a crashed worker's entry expires on its own instead of
/// sending clients to a dead host forever.
#[derive(Clone)]
pub struct ShardRegistry {
    pool: Pool,
}

impl ShardRegistry {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(shard_prefix: &str) -> String {
        format!("shard-worker:{shard_prefix}")
    }

    /// Legacy unconditional write without a lease (no TTL, last writer
    /// wins). Prefer `claim_shard`.
    pub async fn register_shard(
        &self,
        shard_prefix: &str,
        worker_ws_url: &str,
    ) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.set::<_, _, ()>(Self::key(shard_prefix), worker_ws_url)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    pub async fn lookup_shard(&self, shard_prefix: &str) -> Result<Option<String>, AppError> {
        let mut conn = conn(&self.pool).await?;
        let url: Option<String> = conn.get(Self::key(shard_prefix)).await.map_err(cache_err)?;
        Ok(url)
    }

    /// Claims (or refreshes) the lease on a shard for `ttl_secs`.
    /// Returns `Ok(true)` when this worker holds the lease afterwards and
    /// `Ok(false)` when another live worker owns it.
    pub async fn claim_shard(
        &self,
        shard_prefix: &str,
        worker_ws_url: &str,
        ttl_secs: u64,
    ) -> Result<bool, AppError> {
        let won: i64 = eval(
            &self.pool,
            CLAIM_LUA,
            Self::key(shard_prefix),
            &[worker_ws_url.to_string(), ttl_secs.to_string()],
        )
        .await?;
        Ok(won == 1)
    }

    /// Releases the lease if (and only if) this worker still holds it.
    pub async fn release_shard(
        &self,
        shard_prefix: &str,
        worker_ws_url: &str,
    ) -> Result<bool, AppError> {
        let deleted: i64 = eval(
            &self.pool,
            RELEASE_LUA,
            Self::key(shard_prefix),
            &[worker_ws_url.to_string()],
        )
        .await?;
        Ok(deleted == 1)
    }
}

// ---------------------------------------------------------------------
// Device location: lets a worker verify where a device really is.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceLocationRecord {
    pub lat: f64,
    pub lon: f64,
    pub shard_prefix: String,
}

impl DeviceLocationRecord {
    pub fn location(&self) -> Result<Location, AppError> {
        Location::new(self.lat, self.lon)
    }
}

/// The coordinator writes a device's location here whenever it registers or
/// moves; workers read it on connect to (a) verify the device really
/// belongs to the shard it claims and (b) deliver only to devices inside
/// the alert circle. Entries expire on their own after `ttl_secs`.
#[derive(Clone)]
pub struct DeviceLocationStore {
    pool: Pool,
}

impl DeviceLocationStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(device_id: Uuid) -> String {
        format!("device-loc:{device_id}")
    }

    pub async fn set(
        &self,
        device_id: Uuid,
        record: &DeviceLocationRecord,
        ttl_secs: u64,
    ) -> Result<(), AppError> {
        let json = serde_json::to_string(record).map_err(cache_err)?;
        let mut conn = conn(&self.pool).await?;
        conn.set_ex::<_, _, ()>(Self::key(device_id), json, ttl_secs)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    pub async fn get(&self, device_id: Uuid) -> Result<Option<DeviceLocationRecord>, AppError> {
        let mut conn = conn(&self.pool).await?;
        let raw: Option<String> = conn.get(Self::key(device_id)).await.map_err(cache_err)?;
        raw.map(|s| serde_json::from_str(&s).map_err(cache_err))
            .transpose()
    }

    pub async fn delete(&self, device_id: Uuid) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.del::<_, ()>(Self::key(device_id))
            .await
            .map_err(cache_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------
// One-time WebSocket tickets.
// ---------------------------------------------------------------------

/// Reads and deletes the key in one atomic step. (GETDEL does the same but
/// needs Redis 6.2+; this works on every Redis that supports scripting.)
const CONSUME_LUA: &str = r#"
local v = redis.call('GET', KEYS[1])
if v then
  redis.call('DEL', KEYS[1])
end
return v
"#;

fn is_ticket_shaped(ticket: &str) -> bool {
    ticket.len() == 64 && ticket.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Short-lived, single-use tickets that replace the long-lived JWT in the
/// WebSocket URL. The coordinator issues one for an authenticated device;
/// the worker consumes it atomically (read + delete in one Lua step), so it
/// can be used once.
#[derive(Clone)]
pub struct WsTicketStore {
    pool: Pool,
}

impl WsTicketStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(ticket: &str) -> String {
        format!("ws-ticket:{ticket}")
    }

    pub async fn issue(
        &self,
        ticket: &str,
        device_id: Uuid,
        ttl_secs: u64,
    ) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.set_ex::<_, _, ()>(Self::key(ticket), device_id.to_string(), ttl_secs)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    /// Returns the device the ticket was issued for and invalidates it.
    /// Malformed, unknown, expired and already-used tickets all give None.
    pub async fn consume(&self, ticket: &str) -> Result<Option<Uuid>, AppError> {
        if !is_ticket_shaped(ticket) {
            return Ok(None);
        }
        let raw: Option<String> = eval(&self.pool, CONSUME_LUA, Self::key(ticket), &[]).await?;
        Ok(raw.and_then(|s| Uuid::parse_str(&s).ok()))
    }
}

// ---------------------------------------------------------------------
// Revocation list.
// ---------------------------------------------------------------------

/// Devices whose tokens must stop working immediately (deregistered or
/// revoked by an operator). Entries only need to outlive the longest-lived
/// token, so they carry a TTL.
#[derive(Clone)]
pub struct RevocationStore {
    pool: Pool,
}

impl RevocationStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(device_id: Uuid) -> String {
        format!("revoked:{device_id}")
    }

    pub async fn revoke(&self, device_id: Uuid, ttl_secs: u64) -> Result<(), AppError> {
        let mut conn = conn(&self.pool).await?;
        conn.set_ex::<_, _, ()>(Self::key(device_id), "1", ttl_secs)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    pub async fn is_revoked(&self, device_id: Uuid) -> Result<bool, AppError> {
        let mut conn = conn(&self.pool).await?;
        let exists: bool = conn.exists(Self::key(device_id)).await.map_err(cache_err)?;
        Ok(exists)
    }
}

// ---------------------------------------------------------------------
// Live per-alert delivery statistics reported by the workers.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ShardStats {
    pub delivered: i64,
    pub dropped: i64,
    pub filtered_out: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AlertStats {
    pub delivered: i64,
    pub dropped: i64,
    pub filtered_out: i64,
    pub per_shard: BTreeMap<String, ShardStats>,
}

/// Workers add what they actually pushed to live sockets; the coordinator
/// reads it back. This is the real delivery count, unlike the registered
/// device counts stored in Postgres.
#[derive(Clone)]
pub struct AlertStatsStore {
    pool: Pool,
}

impl AlertStatsStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(alert_id: Uuid) -> String {
        format!("alert-stats:{alert_id}")
    }

    pub async fn record(
        &self,
        alert_id: Uuid,
        shard_prefix: &str,
        delivered: u64,
        dropped: u64,
        filtered_out: u64,
        ttl_secs: u64,
    ) -> Result<(), AppError> {
        let key = Self::key(alert_id);
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (field, delta) in [
            ("delivered".to_string(), delivered),
            ("dropped".to_string(), dropped),
            ("filtered_out".to_string(), filtered_out),
            (format!("delivered:{shard_prefix}"), delivered),
            (format!("dropped:{shard_prefix}"), dropped),
            (format!("filtered_out:{shard_prefix}"), filtered_out),
        ] {
            pipe.cmd("HINCRBY").arg(&key).arg(field).arg(delta).ignore();
        }
        pipe.cmd("EXPIRE").arg(&key).arg(ttl_secs).ignore();

        let mut conn = conn(&self.pool).await?;
        pipe.query_async::<()>(&mut conn).await.map_err(cache_err)?;
        Ok(())
    }

    pub async fn get(&self, alert_id: Uuid) -> Result<Option<AlertStats>, AppError> {
        let mut conn = conn(&self.pool).await?;
        let fields: HashMap<String, i64> =
            conn.hgetall(Self::key(alert_id)).await.map_err(cache_err)?;
        if fields.is_empty() {
            return Ok(None);
        }
        let mut stats = AlertStats {
            delivered: fields.get("delivered").copied().unwrap_or(0),
            dropped: fields.get("dropped").copied().unwrap_or(0),
            filtered_out: fields.get("filtered_out").copied().unwrap_or(0),
            per_shard: BTreeMap::new(),
        };
        for (field, value) in &fields {
            if let Some((kind, shard)) = field.split_once(':') {
                let entry = stats.per_shard.entry(shard.to_string()).or_default();
                match kind {
                    "delivered" => entry.delivered = *value,
                    "dropped" => entry.dropped = *value,
                    "filtered_out" => entry.filtered_out = *value,
                    _ => {}
                }
            }
        }
        Ok(Some(stats))
    }
}

// ---------------------------------------------------------------------
// Replay cursor: the newest alert a device has acknowledged.
// ---------------------------------------------------------------------

/// Sets the cursor only if the new timestamp is newer (so out-of-order acks
/// can never move it backwards). Timestamps use a fixed-width RFC 3339
/// format, so plain string comparison orders them correctly.
const ADVANCE_LUA: &str = r#"
local cur = redis.call('GET', KEYS[1])
if (not cur) or (ARGV[1] > cur) then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return 1
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 0
"#;

fn format_cursor(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

#[derive(Clone)]
pub struct AckCursorStore {
    pool: Pool,
}

impl AckCursorStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn key(device_id: Uuid) -> String {
        format!("last-ack:{device_id}")
    }

    /// Moves the cursor forward to `ts`. Returns whether it advanced.
    pub async fn advance(
        &self,
        device_id: Uuid,
        ts: DateTime<Utc>,
        ttl_secs: u64,
    ) -> Result<bool, AppError> {
        let moved: i64 = eval(
            &self.pool,
            ADVANCE_LUA,
            Self::key(device_id),
            &[format_cursor(ts), ttl_secs.to_string()],
        )
        .await?;
        Ok(moved == 1)
    }

    pub async fn get(&self, device_id: Uuid) -> Result<Option<DateTime<Utc>>, AppError> {
        let mut conn = conn(&self.pool).await?;
        let raw: Option<String> = conn.get(Self::key(device_id)).await.map_err(cache_err)?;
        match raw {
            None => Ok(None),
            Some(s) => DateTime::parse_from_rfc3339(&s)
                .map(|d| Some(d.with_timezone(&Utc)))
                .map_err(cache_err),
        }
    }
}
