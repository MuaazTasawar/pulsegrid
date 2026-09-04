use axum::routing::{post, put};
use axum::Router;
use std::sync::Arc;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::GovernorLayer;

use crate::handlers::devices::{register, update_location};
use crate::AppState;

pub fn router() -> Router<AppState> {
    // 5 registrations per second per client IP, burst of 10 -- generous
    // enough for legitimate retry/reconnect behavior, tight enough to
    // blunt a naive registration-spam attack. Only /register is limited;
    // /location updates from already-authenticated devices are not
    // rate-limited here since JWT auth already gates that endpoint.
    let governor_config = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(5)
            .burst_size(10)
            .finish()
            .expect("valid governor config"),
    );

    let register_route = Router::new()
        .route("/devices/register", post(register))
        .layer(GovernorLayer::new(governor_config));

    let location_route = Router::new().route("/devices/location", put(update_location));

    register_route.merge(location_route)
}