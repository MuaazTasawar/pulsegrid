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
    let presence = PresenceRegistry::new(redis_pool);

    let nats = infra::nats::connect(&config.nats_url).await?;
    tracing::info!("nats connected");

    let registry = ConnectionRegistry::new();
    subscriber::spawn_shard_subscribers(nats, registry.clone(), config.owned_prefixes.clone());

    let state = AppState {
        registry,
        presence,
        owned_prefixes: config.owned_prefixes.clone(),
        instance_id: config.instance_id.clone(),
    };

    let app = Router::new()
        .route("/connect/{device_id}", get(ws::ws_upgrade_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "fanout-worker listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
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