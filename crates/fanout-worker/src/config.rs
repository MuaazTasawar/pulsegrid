use domain::auth::ApiKeys;
use domain::geo::SHARD_PREFIX_LENGTH;
use domain::AppError;
use uuid::Uuid;

/// The characters a geohash may contain.
const GEOHASH_ALPHABET: &str = "0123456789bcdefghjkmnpqrstuvwxyz";

/// A rate limit: `per_sec` requests per second per client with room for
/// `burst` at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub per_sec: u32,
    pub burst: u32,
}

#[derive(Clone)]
pub struct Config {
    pub production: bool,
    pub redis_url: String,
    pub nats_url: String,
    pub port: u16,
    /// The shard prefixes this worker owns (validated, lowercase, no
    /// duplicates). Only devices located in one of these shards may connect.
    pub owned_prefixes: Vec<String>,
    /// The WebSocket address clients use to reach THIS worker. It is stored
    /// in the shard registry so the coordinator can tell clients where to go.
    pub public_url: String,
    /// How long the shard lease lasts without being refreshed.
    pub lease_ttl_secs: u64,
    /// Limit on new WebSocket connections per client address.
    pub connect_limit: Limit,
    pub rate_limit_disabled: bool,
    pub trust_proxy_headers: bool,
    /// Keys for the worker's operator endpoints (`/admin/...`).
    pub operator_keys: ApiKeys,
    pub log_json: bool,
    /// Must match the coordinator's setting (both create the stream).
    pub alert_stream_max_age_secs: u64,
    /// At most this many missed alerts are replayed to a reconnecting device.
    pub replay_max_alerts: usize,
    /// Identifies this process in the presence records and in telemetry.
    pub instance_id: String,
}

fn cfg_err(msg: impl Into<String>) -> AppError {
    AppError::Config(msg.into())
}

fn required(get: &dyn Fn(&str) -> Option<String>, name: &str) -> Result<String, AppError> {
    get(name)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| cfg_err(format!("{name} not set")))
}

fn parse_or<T: std::str::FromStr>(
    get: &dyn Fn(&str) -> Option<String>,
    name: &str,
    default: T,
) -> Result<T, AppError> {
    match get(name).filter(|v| !v.trim().is_empty()) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse::<T>()
            .map_err(|_| cfg_err(format!("{name} has an invalid value: '{raw}'"))),
    }
}

fn parse_bool(
    get: &dyn Fn(&str) -> Option<String>,
    name: &str,
    default: bool,
) -> Result<bool, AppError> {
    match get(name).filter(|v| !v.trim().is_empty()) {
        None => Ok(default),
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(cfg_err(format!(
                "{name} must be true or false, got '{raw}'"
            ))),
        },
    }
}

/// Parses `tkrt,tkrw,...` into validated, deduplicated geohash prefixes.
pub fn parse_prefixes(raw: &str) -> Result<Vec<String>, AppError> {
    let mut out: Vec<String> = Vec::new();
    for item in raw.split(',') {
        let prefix = item.trim().to_ascii_lowercase();
        if prefix.is_empty() {
            continue;
        }
        if prefix.len() != SHARD_PREFIX_LENGTH
            || !prefix.chars().all(|c| GEOHASH_ALPHABET.contains(c))
        {
            return Err(cfg_err(format!(
                "'{item}' is not a valid shard prefix: expected {SHARD_PREFIX_LENGTH} geohash \
                 characters (0-9 and b-z without a, i, l, o)"
            )));
        }
        if !out.contains(&prefix) {
            out.push(prefix);
        }
    }
    if out.is_empty() {
        return Err(cfg_err(
            "FANOUT_WORKER_SHARD_PREFIXES must list at least one prefix",
        ));
    }
    Ok(out)
}

impl Config {
    pub fn from_env() -> Result<Self, AppError> {
        dotenvy::dotenv().ok();
        Self::from_lookup(&|name| std::env::var(name).ok())
    }

    pub fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> Result<Self, AppError> {
        let production = match get("PULSEGRID_ENV")
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("dev") | Some("development") => false,
            Some("prod") | Some("production") => true,
            Some(other) => {
                return Err(cfg_err(format!(
                    "PULSEGRID_ENV must be 'dev' or 'production', got '{other}'"
                )))
            }
        };

        let port = parse_or(get, "FANOUT_WORKER_PORT", 8081u16)?;
        let owned_prefixes = parse_prefixes(&required(get, "FANOUT_WORKER_SHARD_PREFIXES")?)?;

        let public_url = get("WORKER_PUBLIC_URL")
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| format!("ws://localhost:{port}"));
        let public_url = public_url.trim().trim_end_matches('/').to_string();
        if !(public_url.starts_with("ws://") || public_url.starts_with("wss://")) {
            return Err(cfg_err(format!(
                "WORKER_PUBLIC_URL must start with ws:// or wss://, got '{public_url}'"
            )));
        }

        let lease_ttl_secs = parse_or(get, "SHARD_LEASE_TTL_SECS", 30u64)?;
        if lease_ttl_secs < 3 {
            return Err(cfg_err("SHARD_LEASE_TTL_SECS must be at least 3"));
        }

        let per_sec = parse_or(get, "WS_CONNECT_RATE_PER_SEC", 50u32)?;
        let burst = parse_or(get, "WS_CONNECT_BURST", 100u32)?;
        if !(1..=10_000).contains(&per_sec) || burst == 0 {
            return Err(cfg_err(
                "WS_CONNECT_RATE_PER_SEC must be 1-10000 and WS_CONNECT_BURST at least 1",
            ));
        }

        let operator_keys =
            ApiKeys::from_csv(&get("OPERATOR_API_KEYS").unwrap_or_default(), production)?;

        let log_json = match get("LOG_FORMAT")
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("pretty") => false,
            Some("json") => true,
            Some(other) => {
                return Err(cfg_err(format!(
                    "LOG_FORMAT must be 'pretty' or 'json', got '{other}'"
                )))
            }
        };

        let alert_stream_max_age_secs = parse_or(get, "ALERT_STREAM_MAX_AGE_SECS", 3600u64)?;
        if alert_stream_max_age_secs == 0 {
            return Err(cfg_err("ALERT_STREAM_MAX_AGE_SECS must be at least 1"));
        }
        let replay_max_alerts = parse_or(get, "REPLAY_MAX_ALERTS", 50usize)?;
        if !(1..=500).contains(&replay_max_alerts) {
            return Err(cfg_err("REPLAY_MAX_ALERTS must be between 1 and 500"));
        }

        Ok(Self {
            production,
            redis_url: required(get, "REDIS_URL")?,
            nats_url: required(get, "NATS_URL")?,
            port,
            owned_prefixes,
            public_url,
            lease_ttl_secs,
            connect_limit: Limit { per_sec, burst },
            rate_limit_disabled: parse_bool(get, "RATE_LIMIT_DISABLED", false)?,
            trust_proxy_headers: parse_bool(get, "TRUST_PROXY_HEADERS", false)?,
            operator_keys,
            log_json,
            alert_stream_max_age_secs,
            replay_max_alerts,
            instance_id: Uuid::new_v4().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn base() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("REDIS_URL", "redis://h"),
            ("NATS_URL", "nats://h"),
            ("FANOUT_WORKER_SHARD_PREFIXES", "tkrt,tkrw"),
        ])
    }

    fn load(vars: &HashMap<&str, &str>) -> Result<Config, AppError> {
        Config::from_lookup(&|k| vars.get(k).map(|v| v.to_string()))
    }

    #[test]
    fn defaults() {
        let c = load(&base()).unwrap();
        assert_eq!(c.port, 8081);
        assert_eq!(c.owned_prefixes, ["tkrt", "tkrw"]);
        assert_eq!(c.public_url, "ws://localhost:8081");
        assert_eq!(c.lease_ttl_secs, 30);
        assert_eq!(
            c.connect_limit,
            Limit {
                per_sec: 50,
                burst: 100
            }
        );
        assert!(!c.rate_limit_disabled && !c.production && !c.log_json);
        assert_eq!(c.replay_max_alerts, 50);
        assert!(c.operator_keys.is_empty());
        assert!(!c.instance_id.is_empty());
    }

    #[test]
    fn prefixes_are_validated_normalised_and_deduplicated() {
        assert_eq!(
            parse_prefixes(" TKRT , tkrw,tkrt ,, ").unwrap(),
            ["tkrt", "tkrw"]
        );
        for bad in [
            "", " , ", "tkr", "tkrtt", "tkra", "tkr!", "tkri", "tkrl", "tkro",
        ] {
            assert!(parse_prefixes(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn public_url_is_checked_and_trimmed() {
        let mut v = base();
        v.insert("WORKER_PUBLIC_URL", "wss://ws.example.com/");
        assert_eq!(load(&v).unwrap().public_url, "wss://ws.example.com");
        v.insert("WORKER_PUBLIC_URL", "http://nope");
        assert!(load(&v).is_err());
        let mut p = base();
        p.insert("FANOUT_WORKER_PORT", "9100");
        assert_eq!(load(&p).unwrap().public_url, "ws://localhost:9100");
    }

    #[test]
    fn invalid_values_are_rejected() {
        for (key, value) in [
            ("FANOUT_WORKER_PORT", "70000"),
            ("SHARD_LEASE_TTL_SECS", "2"),
            ("WS_CONNECT_RATE_PER_SEC", "0"),
            ("WS_CONNECT_BURST", "0"),
            ("REPLAY_MAX_ALERTS", "0"),
            ("REPLAY_MAX_ALERTS", "501"),
            ("PULSEGRID_ENV", "qa"),
            ("LOG_FORMAT", "xml"),
            ("RATE_LIMIT_DISABLED", "perhaps"),
            ("ALERT_STREAM_MAX_AGE_SECS", "0"),
        ] {
            let mut v = base();
            v.insert(key, value);
            assert!(load(&v).is_err(), "{key}={value} should be rejected");
        }
        for name in ["REDIS_URL", "NATS_URL", "FANOUT_WORKER_SHARD_PREFIXES"] {
            let mut v = base();
            v.remove(name);
            assert!(load(&v).err().unwrap().to_string().contains(name));
        }
    }

    #[test]
    fn production_needs_a_strong_operator_key() {
        let mut v = base();
        v.insert("PULSEGRID_ENV", "production");
        assert!(load(&v).is_err());
        let key = "k".repeat(40);
        v.insert("OPERATOR_API_KEYS", &key);
        let c = load(&v).unwrap();
        assert!(c.production && c.operator_keys.verify(&key));
    }
}
