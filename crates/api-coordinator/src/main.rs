mod config;
mod errors;
mod extractors;
mod handlers;
mod middleware;
mod routes;
mod services;

use axum::Router;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use config::Config;
use services::alert_dispatch_service::AlertDispatchService;
use services::device_service::DeviceService;

#[derive(Clone)]
pub struct AppState {
    pub device_service: DeviceService,
    pub alert_dispatch_service: AlertDispatchService,
    pub jwt_secret: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;

    let pool = infra::db::connect(&config.database_url).await?;
    infra::db::run_migrations(&pool).await?;
    tracing::info!("database connected and migrated");

    let nats = infra::nats::connect(&config.nats_url).await?;
    tracing::info!("nats connected");

    let device_repo = infra::DeviceRepository::new(pool.clone());
    let alert_repo = infra::AlertRepository::new(pool);

    let device_service = DeviceService::new(device_repo.clone(), config.jwt_secret.clone());
    let alert_dispatch_service =
        AlertDispatchService::new(alert_repo, device_repo, nats);

    let state = AppState {
        device_service,
        alert_dispatch_service,
        jwt_secret: config.jwt_secret.clone(),
    };

    let app = Router::new()
        .merge(routes::devices::router())
        .merge(routes::alerts::router())
        .layer(
            ServiceBuilder::new()
                .layer(axum::middleware::from_fn(middleware::request_id::request_id_middleware))
                .layer(TraceLayer::new_for_http()),
        )
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "api-coordinator listening");

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