use std::path::PathBuf;
use std::str::FromStr;

use domain::auth::{validate_secret_strength, ApiKeys, DEFAULT_TOKEN_TTL_HOURS};
use domain::AppError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Development,
    Production,
}

impl Environment {
    pub fn is_production(self) -> bool {
        self == Environment::Production
    }
}

/// How device JWTs are signed and verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtConfig {
    /// Shared secret (HS256).
    Hs256 { secret: String },
    /// Ed25519 key pair loaded from PEM files (EdDSA).
    EdDsa {
        private_key_path: PathBuf,
        public_key_path: PathBuf,
    },
}

/// A rate limit: `per_sec` requests per second per client, with room for
/// `burst` requests at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub per_sec: u32,
    pub burst: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimits {
    /// POST /devices/register
    pub register: Limit,
    /// The authenticated device endpoints (location, heartbeat, ws-ticket, delete).
    pub location: Limit,
    /// POST /alerts
    pub alerts: Limit,
    /// Turns every coordinator rate limit off (load testing only).
    pub disabled: bool,
    /// Key limits by the `X-Forwarded-For` client address instead of the
    /// socket peer. Only enable behind a reverse proxy you control, since
    /// clients can forge that header otherwise.
    pub trust_proxy_headers: bool,
}

#[derive(Clone)]
pub struct Config {
    pub environment: Environment,
    pub database_url: String,
    pub redis_url: String,
    pub nats_url: String,
    pub jwt: JwtConfig,
    pub token_ttl_hours: i64,
    pub ws_ticket_ttl_secs: u64,
    pub operator_keys: ApiKeys,
    pub port: u16,
    pub log_json: bool,
    pub rate_limits: RateLimits,
    pub device_retention_days: i64,
    pub alert_retention_days: i64,
    pub retention_sweep_interval_minutes: u64,
    pub alert_stream_max_age_secs: u64,
}

fn cfg_err(msg: impl Into<String>) -> AppError {
    AppError::Config(msg.into())
}

fn required(get: &dyn Fn(&str) -> Option<String>, name: &str) -> Result<String, AppError> {
    get(name)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| cfg_err(format!("{name} not set")))
}

fn parse_or<T: FromStr>(
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

fn parse_limit(
    get: &dyn Fn(&str) -> Option<String>,
    per_sec_name: &str,
    burst_name: &str,
    default: Limit,
) -> Result<Limit, AppError> {
    let per_sec = parse_or(get, per_sec_name, default.per_sec)?;
    let burst = parse_or(get, burst_name, default.burst)?;
    if !(1..=1000).contains(&per_sec) {
        return Err(cfg_err(format!(
            "{per_sec_name} must be between 1 and 1000"
        )));
    }
    if burst == 0 {
        return Err(cfg_err(format!("{burst_name} must be at least 1")));
    }
    Ok(Limit { per_sec, burst })
}

impl Config {
    /// Reads the configuration from the process environment (loading a
    /// `.env` file first if there is one).
    pub fn from_env() -> Result<Self, AppError> {
        dotenvy::dotenv().ok(); // fine if .env is absent, e.g. in prod with real env vars
        Self::from_lookup(&|name| std::env::var(name).ok())
    }

    /// Builds the configuration from any key lookup (used by tests).
    pub fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> Result<Self, AppError> {
        let environment = match get("PULSEGRID_ENV")
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("dev") | Some("development") => Environment::Development,
            Some("prod") | Some("production") => Environment::Production,
            Some(other) => {
                return Err(cfg_err(format!(
                    "PULSEGRID_ENV must be 'dev' or 'production', got '{other}'"
                )))
            }
        };
        let production = environment.is_production();

        let database_url = required(get, "DATABASE_URL")?;
        let redis_url = required(get, "REDIS_URL")?;
        let nats_url = required(get, "NATS_URL")?;

        let private_key = get("JWT_PRIVATE_KEY_PATH").filter(|v| !v.trim().is_empty());
        let public_key = get("JWT_PUBLIC_KEY_PATH").filter(|v| !v.trim().is_empty());
        let jwt = match (private_key, public_key) {
            (Some(private), Some(public)) => JwtConfig::EdDsa {
                private_key_path: PathBuf::from(private.trim()),
                public_key_path: PathBuf::from(public.trim()),
            },
            (None, None) => {
                let secret = required(get, "JWT_SECRET")?;
                validate_secret_strength("JWT_SECRET", &secret, production)?;
                JwtConfig::Hs256 { secret }
            }
            _ => {
                return Err(cfg_err(
                    "JWT_PRIVATE_KEY_PATH and JWT_PUBLIC_KEY_PATH must be set together",
                ))
            }
        };

        let operator_keys =
            ApiKeys::from_csv(&get("OPERATOR_API_KEYS").unwrap_or_default(), production)?;

        let port = parse_or(get, "COORDINATOR_PORT", 8080u16)?;
        let token_ttl_hours = parse_or(get, "DEVICE_TOKEN_TTL_HOURS", DEFAULT_TOKEN_TTL_HOURS)?;
        if token_ttl_hours < 1 {
            return Err(cfg_err("DEVICE_TOKEN_TTL_HOURS must be at least 1"));
        }
        let ws_ticket_ttl_secs = parse_or(get, "WS_TICKET_TTL_SECS", 30u64)?;
        if ws_ticket_ttl_secs == 0 {
            return Err(cfg_err("WS_TICKET_TTL_SECS must be at least 1"));
        }

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

        let rate_limits = RateLimits {
            register: parse_limit(
                get,
                "RATE_LIMIT_REGISTER_PER_SEC",
                "RATE_LIMIT_REGISTER_BURST",
                Limit {
                    per_sec: 5,
                    burst: 10,
                },
            )?,
            location: parse_limit(
                get,
                "RATE_LIMIT_LOCATION_PER_SEC",
                "RATE_LIMIT_LOCATION_BURST",
                Limit {
                    per_sec: 10,
                    burst: 20,
                },
            )?,
            alerts: parse_limit(
                get,
                "RATE_LIMIT_ALERTS_PER_SEC",
                "RATE_LIMIT_ALERTS_BURST",
                Limit {
                    per_sec: 2,
                    burst: 5,
                },
            )?,
            disabled: parse_bool(get, "RATE_LIMIT_DISABLED", false)?,
            trust_proxy_headers: parse_bool(get, "TRUST_PROXY_HEADERS", false)?,
        };

        let device_retention_days = parse_or(get, "DEVICE_RETENTION_DAYS", 90i64)?;
        let alert_retention_days = parse_or(get, "ALERT_RETENTION_DAYS", 365i64)?;
        if device_retention_days < 1 || alert_retention_days < 1 {
            return Err(cfg_err("retention periods must be at least 1 day"));
        }
        let retention_sweep_interval_minutes =
            parse_or(get, "RETENTION_SWEEP_INTERVAL_MINUTES", 60u64)?;
        if retention_sweep_interval_minutes == 0 {
            return Err(cfg_err(
                "RETENTION_SWEEP_INTERVAL_MINUTES must be at least 1",
            ));
        }
        let alert_stream_max_age_secs = parse_or(get, "ALERT_STREAM_MAX_AGE_SECS", 3600u64)?;
        if alert_stream_max_age_secs == 0 {
            return Err(cfg_err("ALERT_STREAM_MAX_AGE_SECS must be at least 1"));
        }

        Ok(Self {
            environment,
            database_url,
            redis_url,
            nats_url,
            jwt,
            token_ttl_hours,
            ws_ticket_ttl_secs,
            operator_keys,
            port,
            log_json,
            rate_limits,
            device_retention_days,
            alert_retention_days,
            retention_sweep_interval_minutes,
            alert_stream_max_age_secs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn base() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("DATABASE_URL", "postgres://u:p@h/db"),
            ("REDIS_URL", "redis://h"),
            ("NATS_URL", "nats://h"),
            ("JWT_SECRET", "dev-secret"),
        ])
    }

    fn load(vars: &HashMap<&str, &str>) -> Result<Config, AppError> {
        Config::from_lookup(&|k| vars.get(k).map(|v| v.to_string()))
    }

    #[test]
    fn defaults_match_the_documented_values() {
        let c = load(&base()).unwrap();
        assert_eq!(c.environment, Environment::Development);
        assert_eq!(c.port, 8080);
        assert_eq!(c.token_ttl_hours, 720);
        assert_eq!(c.ws_ticket_ttl_secs, 30);
        assert!(!c.log_json);
        assert_eq!(
            c.rate_limits.register,
            Limit {
                per_sec: 5,
                burst: 10
            }
        );
        assert_eq!(
            c.rate_limits.location,
            Limit {
                per_sec: 10,
                burst: 20
            }
        );
        assert_eq!(
            c.rate_limits.alerts,
            Limit {
                per_sec: 2,
                burst: 5
            }
        );
        assert!(!c.rate_limits.disabled && !c.rate_limits.trust_proxy_headers);
        assert_eq!(c.device_retention_days, 90);
        assert_eq!(c.alert_retention_days, 365);
        assert_eq!(c.alert_stream_max_age_secs, 3600);
        assert!(c.operator_keys.is_empty());
        assert_eq!(
            c.jwt,
            JwtConfig::Hs256 {
                secret: "dev-secret".into()
            }
        );
    }

    #[test]
    fn overrides_are_applied() {
        let mut v = base();
        v.extend([
            ("COORDINATOR_PORT", "9000"),
            ("LOG_FORMAT", "JSON"),
            ("RATE_LIMIT_REGISTER_PER_SEC", "50"),
            ("RATE_LIMIT_REGISTER_BURST", "100"),
            ("RATE_LIMIT_DISABLED", "true"),
            ("TRUST_PROXY_HEADERS", "1"),
            ("OPERATOR_API_KEYS", "k1, k2"),
            ("DEVICE_TOKEN_TTL_HOURS", "2"),
        ]);
        let c = load(&v).unwrap();
        assert_eq!(c.port, 9000);
        assert!(c.log_json);
        assert_eq!(
            c.rate_limits.register,
            Limit {
                per_sec: 50,
                burst: 100
            }
        );
        assert!(c.rate_limits.disabled && c.rate_limits.trust_proxy_headers);
        assert!(c.operator_keys.verify("k1") && c.operator_keys.verify("k2"));
        assert_eq!(c.token_ttl_hours, 2);
    }

    #[test]
    fn missing_required_values_are_reported_by_name() {
        for name in ["DATABASE_URL", "REDIS_URL", "NATS_URL", "JWT_SECRET"] {
            let mut v = base();
            v.remove(name);
            let err = load(&v).err().expect("must fail").to_string();
            assert!(err.contains(name), "{err}");
        }
    }

    #[test]
    fn invalid_values_are_rejected() {
        for (key, value) in [
            ("COORDINATOR_PORT", "99999"),
            ("PULSEGRID_ENV", "staging"),
            ("LOG_FORMAT", "xml"),
            ("RATE_LIMIT_ALERTS_PER_SEC", "0"),
            ("RATE_LIMIT_ALERTS_PER_SEC", "5000"),
            ("RATE_LIMIT_ALERTS_BURST", "0"),
            ("RATE_LIMIT_DISABLED", "maybe"),
            ("DEVICE_TOKEN_TTL_HOURS", "0"),
            ("WS_TICKET_TTL_SECS", "0"),
            ("DEVICE_RETENTION_DAYS", "0"),
            ("RETENTION_SWEEP_INTERVAL_MINUTES", "0"),
            ("ALERT_STREAM_MAX_AGE_SECS", "0"),
        ] {
            let mut v = base();
            v.insert(key, value);
            assert!(load(&v).is_err(), "{key}={value} should be rejected");
        }
    }

    #[test]
    fn production_refuses_weak_or_placeholder_secrets() {
        let mut v = base();
        v.insert("PULSEGRID_ENV", "production");
        // dev-secret is too short, and there is no operator key
        assert!(load(&v).is_err());

        let strong = "s".repeat(40);
        let key = "k".repeat(40);
        let mut ok = base();
        ok.insert("PULSEGRID_ENV", "production");
        ok.insert("JWT_SECRET", &strong);
        ok.insert("OPERATOR_API_KEYS", &key);
        assert!(load(&ok).unwrap().environment.is_production());

        let mut placeholder = ok.clone();
        placeholder.insert(
            "JWT_SECRET",
            "change-me-in-production-change-me-in-production",
        );
        assert!(load(&placeholder).is_err());

        let mut no_keys = ok.clone();
        no_keys.remove("OPERATOR_API_KEYS");
        assert!(load(&no_keys).is_err());
    }

    #[test]
    fn eddsa_needs_both_key_paths_and_no_secret() {
        let mut only_private = base();
        only_private.insert("JWT_PRIVATE_KEY_PATH", "k.pem");
        assert!(load(&only_private).is_err());

        let mut both = base();
        both.remove("JWT_SECRET");
        both.insert("JWT_PRIVATE_KEY_PATH", "private.pem");
        both.insert("JWT_PUBLIC_KEY_PATH", "public.pem");
        let c = load(&both).unwrap();
        assert!(matches!(c.jwt, JwtConfig::EdDsa { .. }));
    }
}
