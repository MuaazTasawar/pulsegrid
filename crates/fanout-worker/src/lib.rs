pub mod config;
pub mod connection_registry;
pub mod control;
pub mod lease;
pub mod metrics;
pub mod ops;
pub mod rate_limit;
pub mod subscriber;
pub mod ws;

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use domain::AppError;
use tokio::signal;
use tokio::task::JoinHandle;

pub use config::Config;
use connection_registry::ConnectionRegistry;
use lease::ShardLease;
use metrics::WorkerMetrics;

/// Tracks how many shard subscriptions are live (for `/ready`).
pub struct Health {
    pub subscriptions_up: AtomicUsize,
    pub expected_subscriptions: usize,
}

impl Health {
    pub fn subscription_up(&self) {
        self.subscriptions_up.fetch_add(1, Ordering::Relaxed);
    }

    pub fn subscription_down(&self) {
        self.subscriptions_up.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct WorkerState {
    pub config: Arc<Config>,
    pub registry: ConnectionRegistry,
    pub tickets: infra::WsTicketStore,
    pub locations: infra::DeviceLocationStore,
    pub revocations: infra::RevocationStore,
    pub presence: infra::PresenceRegistry,
    pub stats: infra::AlertStatsStore,
    pub shards: infra::ShardRegistry,
    pub redis: infra::redis::Pool,
    pub nats: infra::nats::Client,
    pub lease: ShardLease,
    pub health: Arc<Health>,
    pub metrics: Arc<WorkerMetrics>,
    /// The shard prefixes this worker owns.
    pub owned: Arc<HashSet<String>>,
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

/// Connects to Redis and NATS, claims the shard leases (failing if another
/// worker already owns one of them) and builds the shared state. Background
/// tasks are started separately by `start_background_tasks`.
pub async fn build_state(config: Config) -> Result<WorkerState, AppError> {
    let redis = infra::redis::build_pool(&config.redis_url)?;
    infra::redis::ping_with_timeout(&redis, Duration::from_secs(5)).await?;
    tracing::info!("redis connected");

    let nats = infra::nats::connect(&config.nats_url).await?;
    infra::nats::ensure_alert_stream(&nats, Duration::from_secs(config.alert_stream_max_age_secs))
        .await?;
    tracing::info!("nats connected, alert stream ready");

    let shards = infra::ShardRegistry::new(redis.clone());
    let lease = ShardLease::new(
        shards.clone(),
        config.owned_prefixes.clone(),
        config.public_url.clone(),
        config.lease_ttl_secs,
    );
    lease.acquire_all().await?;
    tracing::info!(
        shards = ?config.owned_prefixes,
        url = %config.public_url,
        ttl_secs = config.lease_ttl_secs,
        "shard leases acquired"
    );

    let expected_subscriptions = config.owned_prefixes.len();
    let owned: HashSet<String> = config.owned_prefixes.iter().cloned().collect();

    Ok(WorkerState {
        registry: ConnectionRegistry::new(),
        tickets: infra::WsTicketStore::new(redis.clone()),
        locations: infra::DeviceLocationStore::new(redis.clone()),
        revocations: infra::RevocationStore::new(redis.clone()),
        presence: infra::PresenceRegistry::new(redis.clone()),
        stats: infra::AlertStatsStore::new(redis.clone()),
        shards,
        redis,
        nats,
        lease,
        health: Arc::new(Health {
            subscriptions_up: AtomicUsize::new(0),
            expected_subscriptions,
        }),
        metrics: WorkerMetrics::new(),
        owned: Arc::new(owned),
        config: Arc::new(config),
    })
}

/// Starts the lease renewal, the per-shard alert subscribers and the control
/// message subscriber. Abort the handles to stop them.
pub fn start_background_tasks(state: &WorkerState) -> Vec<JoinHandle<()>> {
    let mut tasks = vec![state.lease.spawn_refresher()];
    tasks.extend(subscriber::spawn_shard_subscribers(state));
    tasks.push(control::spawn_control_subscriber(state));
    tasks
}

/// The worker's HTTP application. Serve it with
/// `into_make_service_with_connect_info::<SocketAddr>()` so the connection
/// rate limiter can see client addresses.
pub fn build_router(state: WorkerState) -> Router {
    let connect = rate_limit::with_connect_limit(
        Router::new().route("/connect/{device_id}", get(ws::ws_upgrade_handler)),
        &state.config,
    );
    let admin = Router::new()
        .route("/admin/presence/{device_id}", get(ops::presence))
        .layer(axum::middleware::from_fn_with_state(
            state.config.operator_keys.clone(),
            ops::require_operator,
        ));

    Router::new()
        .merge(connect)
        .merge(admin)
        .route("/health", get(ops::health))
        .route("/ready", get(ops::ready))
        .route("/metrics", get(ops::metrics))
        .with_state(state)
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

    tracing::info!("shutdown signal received");
}
