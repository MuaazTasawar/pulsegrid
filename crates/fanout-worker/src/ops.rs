use std::sync::atomic::Ordering;

use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use domain::ApiKeys;
use serde_json::json;
use uuid::Uuid;

use crate::WorkerState;

/// Liveness: the process is up and serving requests.
pub async fn health() -> &'static str {
    "ok"
}

/// Readiness: the worker holds its shard leases, is subscribed to every shard,
/// and can reach Redis and NATS. A worker that is not ready is not doing its
/// job, so an orchestrator should stop sending it traffic (or restart it).
pub async fn ready(State(state): State<WorkerState>) -> Response {
    let redis_ok = infra::redis::ping(&state.redis).await.is_ok();
    let nats_ok = infra::nats::is_connected(&state.nats);
    let lease_ok = state.lease.is_healthy();
    let subscriptions = state.health.subscriptions_up.load(Ordering::Relaxed);
    let subscriptions_ok = subscriptions >= state.health.expected_subscriptions;
    let all_ok = redis_ok && nats_ok && lease_ok && subscriptions_ok;

    let word = |ok: bool| if ok { "ok" } else { "unavailable" };
    let status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "status": if all_ok { "ready" } else { "not_ready" },
            "checks": {
                "redis": word(redis_ok),
                "nats": word(nats_ok),
                "shard_lease": word(lease_ok),
                "subscriptions": word(subscriptions_ok),
            },
            "connections": state.registry.connection_count(),
        })),
    )
        .into_response()
}

/// Prometheus scrape endpoint.
pub async fn metrics(State(state): State<WorkerState>) -> Response {
    // Gauges that mirror live state are refreshed at scrape time.
    state
        .metrics
        .connections
        .set(state.registry.connection_count() as i64);
    state
        .metrics
        .lease_healthy
        .set(i64::from(state.lease.is_healthy()));
    state
        .metrics
        .subscriptions_up
        .set(state.health.subscriptions_up.load(Ordering::Relaxed) as i64);

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
        .into_response()
}

/// Requires a valid operator key in the `X-API-Key` header.
pub async fn require_operator(
    State(keys): State<ApiKeys>,
    request: Request,
    next: Next,
) -> Response {
    let presented = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if keys.verify(presented) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response()
    }
}

/// `GET /admin/presence/{device_id}` (operators): which worker instance holds
/// the device's socket right now.
pub async fn presence(State(state): State<WorkerState>, Path(device_id): Path<Uuid>) -> Response {
    match state.presence.lookup(&device_id.to_string()).await {
        Ok(Some(instance)) => Json(json!({
            "device_id": device_id,
            "worker_instance_id": instance,
            "this_worker": instance == state.config.instance_id,
        }))
        .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "no presence record" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = ?e, "presence lookup failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "presence store unavailable" })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn protected(keys: ApiKeys) -> Router {
        Router::new()
            .route("/secret", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(keys, require_operator))
    }

    async fn status(app: &Router, key: Option<&str>) -> StatusCode {
        let mut request = axum::http::Request::builder().uri("/secret");
        if let Some(k) = key {
            request = request.header("x-api-key", k);
        }
        app.clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn the_admin_layer_needs_a_configured_key() {
        let app = protected(ApiKeys::from_csv("worker-key", false).unwrap());
        assert_eq!(status(&app, Some("worker-key")).await, StatusCode::OK);
        assert_eq!(status(&app, Some("nope")).await, StatusCode::UNAUTHORIZED);
        assert_eq!(status(&app, None).await, StatusCode::UNAUTHORIZED);

        let closed = protected(ApiKeys::default());
        assert_eq!(
            status(&closed, Some("anything")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(status(&closed, None).await, StatusCode::UNAUTHORIZED);
    }
}
