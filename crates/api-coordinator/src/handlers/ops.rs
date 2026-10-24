use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use domain::AppError;
use serde_json::json;

use crate::AppState;

/// Liveness: the process is up and serving requests.
pub async fn health() -> &'static str {
    "ok"
}

fn verdict(name: &str, result: Result<(), AppError>) -> (bool, &'static str) {
    match result {
        Ok(()) => (true, "ok"),
        Err(e) => {
            tracing::warn!(dependency = name, error = ?e, "readiness check failed");
            (false, "unavailable")
        }
    }
}

/// Readiness: Postgres, Redis and NATS are all reachable. Returns 503 with
/// the failing dependencies otherwise, so an orchestrator stops routing
/// traffic here instead of sending it to a coordinator that cannot work.
pub async fn ready(State(state): State<AppState>) -> Response {
    let (postgres, redis) = tokio::join!(
        infra::db::ping(&state.pool),
        infra::redis::ping(&state.redis)
    );
    let nats = if infra::nats::is_connected(&state.nats) {
        Ok(())
    } else {
        Err(AppError::Messaging("not connected".into()))
    };

    let (pg_ok, pg) = verdict("postgres", postgres);
    let (redis_ok, rd) = verdict("redis", redis);
    let (nats_ok, nt) = verdict("nats", nats);
    let all_ok = pg_ok && redis_ok && nats_ok;

    let status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "status": if all_ok { "ready" } else { "not_ready" },
            "checks": { "postgres": pg, "redis": rd, "nats": nt },
        })),
    )
        .into_response()
}

/// Prometheus scrape endpoint.
pub async fn metrics(State(state): State<AppState>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
        .into_response()
}
