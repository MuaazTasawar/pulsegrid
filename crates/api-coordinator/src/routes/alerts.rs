use axum::routing::post;
use axum::Router;

use crate::handlers::alerts::dispatch_alert;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/alerts", post(dispatch_alert))
}