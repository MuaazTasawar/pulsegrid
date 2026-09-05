mod config;
mod connection_registry;
mod subscriber;
mod ws;

use axum::routing::get;
use axum::Router;
use tokio::signal;

use config::Config;
use connection_registry::ConnectionRegistry;
use infra::PresenceRegistry;

#[derive(Clone)]
pub struct AppState {
    pub registry: ConnectionRegistry,
    pub presence: PresenceRegistry,
    pub owned_prefixes: Vec<String>,
    pub instance_id: String,
    pub jwt_secret: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;
    tracing::info!(instance_id = %config.instance_id, owned_prefixes = ?config.owned_prefixes, "starting fanout-worker");

    let redis_pool = infra::redis::build_pool(&config.redis_url)?;
    let presence = PresenceRegistry::new(redis_pool.clone());
    let shard_registry = infra::ShardRegistry::new(redis_pool);

    let nats = infra::nats::connect(&config.nats_url).await?;
    tracing::info!("nats connected");

    // Advertise which shards this instance owns, so api-coordinator can
    // tell clients where to connect. WORKER_PUBLIC_URL lets a real
    // deployment advertise its actual reachable address (behind a load
    // balancer, a different hostname, etc.) instead of always assuming
    // localhost.
    let public_url = std::env::var("WORKER_PUBLIC_URL")
        .unwrap_or_else(|_| format!("ws://localhost:{}", config.port));
    for prefix in &config.owned_prefixes {
        if let Err(e) = shard_registry.register_shard(prefix, &public_url).await {
            tracing::warn!(shard = %prefix, error = ?e, "failed to register shard in service discovery");
        }
    }

    let registry = ConnectionRegistry::new();
    subscriber::spawn_shard_subscribers(nats, registry.clone(), config.owned_prefixes.clone());

    let state = AppState {
        registry,
        presence,
        owned_prefixes: config.owned_prefixes.clone(),
        instance_id: config.instance_id.clone(),
        jwt_secret: config.jwt_secret.clone(),
    };

    let app = Router::new()
        .route("/connect/{device_id}", get(ws::ws_upgrade_handler))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(|| async { "ready" }))
        .route(
            "/admin/presence/{device_id}",
            get(admin_presence_lookup),
        )
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "fanout-worker listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn admin_presence_lookup(
    axum::extract::Path(device_id): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl axum::response::IntoResponse {
    match state.presence.lookup(&device_id).await {
        Ok(Some(worker_id)) => (axum::http::StatusCode::OK, worker_id),
        Ok(None) => (axum::http::StatusCode::NOT_FOUND, "no presence record".to_string()),
        Err(_) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "lookup failed".to_string()),
    }
}

async fn metrics_handler(axum::extract::State(state): axum::extract::State<AppState>) -> String {
    format!(
        "# HELP pulsegrid_ws_connections_current Current WebSocket connections held by this worker\n\
         # TYPE pulsegrid_ws_connections_current gauge\n\
         pulsegrid_ws_connections_current {}\n",
        state.registry.connection_count()
    )
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
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