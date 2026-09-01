use axum::routing::{post, put};
use axum::Router;

use crate::handlers::devices::{register, update_location};
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/devices/register", post(register))
        .route("/devices/location", put(update_location))
}