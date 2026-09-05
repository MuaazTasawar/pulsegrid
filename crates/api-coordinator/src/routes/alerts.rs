use axum::routing::post;
use axum::Router;
use std::sync::Arc;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::GovernorLayer;

use crate::handlers::alerts::dispatch_alert;
use crate::AppState;

pub fn router() -> Router<AppState> {
    // Stricter than device registration: 2/sec, burst 5. Alert dispatch
    // is heavier (NATS publish + Postgres writes per shard) and a
    // legitimate caller has no reason to fire alerts faster than this.
    let governor_config = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(2)
            .burst_size(5)
            .finish()
            .expect("valid governor config"),
    );

    Router::new()
        .route("/alerts", post(dispatch_alert))
        .layer(GovernorLayer::new(governor_config))
}