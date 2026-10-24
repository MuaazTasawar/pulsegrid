pub mod config;
pub mod errors;
pub mod extractors;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod routes;
pub mod services;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRef;
use axum::routing::get;
use axum::Router;
use domain::{ApiKeys, AppError, TokenSigner, TokenVerifier};
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultOnResponse, TraceLayer};

pub use config::Config;
use config::{JwtConfig, RateLimits};
use metrics::Metrics;
use services::alert_dispatch_service::AlertDispatchService;
use services::device_service::{DeviceService, DeviceServiceParts};

#[derive(Clone)]
pub struct AppState {
    pub device_service: DeviceService,
    pub alert_dispatch_service: AlertDispatchService,
    pub shard_registry: infra::ShardRegistry,
    pub revocation_store: infra::RevocationStore,
    pub verifier: TokenVerifier,
    pub operator_keys: ApiKeys,
    pub pool: infra::PgPool,
    pub redis: infra::redis::Pool,
    pub nats: infra::nats::Client,
    pub metrics: Arc<Metrics>,
}

impl FromRef<AppState> for ApiKeys {
    fn from_ref(state: &AppState) -> ApiKeys {
        state.operator_keys.clone()
    }
}

/// Installs the TLS crypto provider the NATS client needs. Safe to call more
/// than once.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Sets up logging: `RUST_LOG` decides what is shown (default `info`), and
/// `json` switches to one JSON object per line. Safe to call more than once.
pub fn init_tracing(json: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let result = if json {
        builder.json().try_init()
    } else {
        builder.try_init()
    };
    let _ = result; // already initialised (for example in tests)
}

/// Creates the token signer and verifier the configuration asks for.
pub fn load_jwt(config: &Config) -> Result<(TokenSigner, TokenVerifier), AppError> {
    match &config.jwt {
        JwtConfig::Hs256 { secret } => Ok((
            TokenSigner::hs256(secret, config.token_ttl_hours),
            TokenVerifier::hs256(secret),
        )),
        JwtConfig::EdDsa {
            private_key_path,
            public_key_path,
        } => {
            let read = |path: &std::path::Path| {
                std::fs::read(path).map_err(|e| {
                    AppError::Config(format!("cannot read key file {}: {e}", path.display()))
                })
            };
            let signer =
                TokenSigner::eddsa_from_pem(&read(private_key_path)?, config.token_ttl_hours)?;
            let verifier = TokenVerifier::eddsa_from_pem(&read(public_key_path)?)?;
            Ok((signer, verifier))
        }
    }
}

/// Connects to Postgres (running migrations), Redis and NATS and wires up
/// the services. Fails fast if a dependency is unreachable.
pub async fn build_state(config: &Config) -> Result<AppState, AppError> {
    let pool = infra::db::connect(&config.database_url).await?;
    infra::db::run_migrations(&pool).await?;
    tracing::info!("database connected and migrated");

    let nats = infra::nats::connect(&config.nats_url).await?;
    tracing::info!("nats connected");

    let redis = infra::redis::build_pool(&config.redis_url)?;
    infra::redis::ping_with_timeout(&redis, Duration::from_secs(5)).await?;
    tracing::info!("redis connected");

    let (signer, verifier) = load_jwt(config)?;

    let revocation_store = infra::RevocationStore::new(redis.clone());
    let shard_registry = infra::ShardRegistry::new(redis.clone());

    let device_repo = infra::DeviceRepository::new(pool.clone());
    let alert_repo = infra::AlertRepository::new(pool.clone());

    let device_service = DeviceService::new(DeviceServiceParts {
        repo: device_repo.clone(),
        signer,
        locations: infra::DeviceLocationStore::new(redis.clone()),
        revocations: revocation_store.clone(),
        tickets: infra::WsTicketStore::new(redis.clone()),
        shards: shard_registry.clone(),
        nats: nats.clone(),
        location_ttl_secs: (config.device_retention_days as u64 + 1) * 86_400,
        revocation_ttl_secs: config.token_ttl_hours as u64 * 3_600 + 86_400,
        ws_ticket_ttl_secs: config.ws_ticket_ttl_secs,
    });
    let alert_dispatch_service = AlertDispatchService::new(alert_repo, device_repo, nats.clone());

    Ok(AppState {
        device_service,
        alert_dispatch_service,
        shard_registry,
        revocation_store,
        verifier,
        operator_keys: config.operator_keys.clone(),
        pool,
        redis,
        nats,
        metrics: Metrics::new(),
    })
}

/// Request-id, tracing and metrics layers shared by the real router and the
/// tests. The request id layer is outermost so the trace span can read it.
pub fn apply_http_layers<S>(router: Router<S>, metrics: Arc<Metrics>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(
        ServiceBuilder::new()
            .layer(axum::middleware::from_fn(
                middleware::request_id::request_id_middleware,
            ))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(middleware::request_id::make_span)
                    // One access-log line per request at INFO, carrying the
                    // request id (the default level is DEBUG).
                    .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics,
                metrics::track_http,
            )),
    )
}

/// The complete HTTP application. Serve it with
/// `into_make_service_with_connect_info::<SocketAddr>()` so the rate limiter
/// can see client addresses.
pub fn build_router(state: AppState, limits: &RateLimits) -> Router {
    let router = Router::new()
        .merge(routes::devices::router(limits))
        .merge(routes::alerts::router(&state, limits))
        .route("/health", get(handlers::ops::health))
        .route("/ready", get(handlers::ops::ready))
        .route("/metrics", get(handlers::ops::metrics));

    apply_http_layers(router, state.metrics.clone()).with_state(state)
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, draining connections");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    // TEST-ONLY throwaway Ed25519 keys.
    const TEST_PRIV: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIBdWliWBNxPVRVGNuJcFBEdSpDmmFHDpF3CbIbRJDI/m\n-----END PRIVATE KEY-----\n";
    const TEST_PUB: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAUaSbdJyBNsZkUote1GDMqT99UGSCwto0HHs3sMPEZxY=\n-----END PUBLIC KEY-----\n";

    fn config(extra: &[(&str, &str)]) -> Config {
        let mut vars: HashMap<String, String> = HashMap::from([
            ("DATABASE_URL".into(), "postgres://u:p@h/db".into()),
            ("REDIS_URL".into(), "redis://h".into()),
            ("NATS_URL".into(), "nats://h".into()),
        ]);
        for (k, v) in extra {
            vars.insert(k.to_string(), v.to_string());
        }
        Config::from_lookup(&|k| vars.get(k).cloned()).unwrap()
    }

    #[test]
    fn hs256_config_signs_and_verifies() {
        let c = config(&[("JWT_SECRET", "dev-secret")]);
        let (signer, verifier) = load_jwt(&c).unwrap();
        let id = Uuid::new_v4();
        assert_eq!(verifier.verify(&signer.issue(id).unwrap()).unwrap(), id);
    }

    #[test]
    fn eddsa_config_loads_the_key_files() {
        let dir = std::env::temp_dir().join(format!("pulsegrid-keys-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let (private, public) = (dir.join("private.pem"), dir.join("public.pem"));
        std::fs::write(&private, TEST_PRIV).unwrap();
        std::fs::write(&public, TEST_PUB).unwrap();

        let c = config(&[
            ("JWT_PRIVATE_KEY_PATH", private.to_str().unwrap()),
            ("JWT_PUBLIC_KEY_PATH", public.to_str().unwrap()),
        ]);
        let (signer, verifier) = load_jwt(&c).unwrap();
        let id = Uuid::new_v4();
        assert_eq!(verifier.verify(&signer.issue(id).unwrap()).unwrap(), id);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_missing_key_file_is_a_clear_config_error() {
        let c = config(&[
            ("JWT_PRIVATE_KEY_PATH", "/definitely/not/here/private.pem"),
            ("JWT_PUBLIC_KEY_PATH", "/definitely/not/here/public.pem"),
        ]);
        let err = load_jwt(&c).err().expect("must fail").to_string();
        assert!(err.contains("cannot read key file"), "{err}");
    }
}
