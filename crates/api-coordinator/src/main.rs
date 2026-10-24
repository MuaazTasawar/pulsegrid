use api_coordinator::{
    build_router, build_state, init_tracing, install_crypto_provider, shutdown_signal, Config,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    init_tracing(config.log_json);
    install_crypto_provider();

    tracing::info!(environment = ?config.environment, "starting api-coordinator");
    if config.operator_keys.is_empty() {
        tracing::warn!("OPERATOR_API_KEYS is empty: POST /alerts will reject every request");
    }
    if config.rate_limits.disabled {
        tracing::warn!("rate limiting is DISABLED (RATE_LIMIT_DISABLED=true)");
    }

    let state = build_state(&config).await?;
    let app = build_router(state, &config.rate_limits);

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "api-coordinator listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}
