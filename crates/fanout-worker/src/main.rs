use std::future::IntoFuture;
use std::net::SocketAddr;
use std::time::Duration;

use fanout_worker::{
    build_router, build_state, init_tracing, install_crypto_provider, shutdown_signal,
    start_background_tasks, Config,
};

/// After a shutdown signal, open WebSockets are closed and we wait this long
/// for the server to finish before giving up.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    init_tracing(config.log_json);
    install_crypto_provider();

    tracing::info!(
        instance_id = %config.instance_id,
        shards = ?config.owned_prefixes,
        "starting fanout-worker"
    );
    if config.operator_keys.is_empty() {
        tracing::warn!("OPERATOR_API_KEYS is empty: /admin endpoints will reject every request");
    }
    if config.rate_limit_disabled {
        tracing::warn!("rate limiting is DISABLED (RATE_LIMIT_DISABLED=true)");
    }

    // Listen before claiming the shards, so a client that learns about this
    // worker the moment the lease appears can already connect.
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    let state = build_state(config).await?;
    let tasks = start_background_tasks(&state);
    tracing::info!(%addr, "fanout-worker listening");

    let app = build_router(state.clone());
    let closing = state.clone();
    let (signalled_tx, signalled_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        // Open WebSockets would keep a graceful shutdown waiting forever.
        closing.registry.close_all();
        let _ = signalled_tx.send(());
    })
    .into_future();
    tokio::pin!(server);

    // The grace period starts when the shutdown signal arrives, not before.
    let grace = async move {
        let _ = signalled_rx.await;
        tokio::time::sleep(SHUTDOWN_GRACE).await;
    };
    tokio::select! {
        result = &mut server => result?,
        _ = grace => tracing::warn!("shutdown grace period elapsed; exiting anyway"),
    }

    for task in tasks {
        task.abort();
    }
    state.lease.release_all().await;
    tracing::info!("shard leases released; bye");
    Ok(())
}
