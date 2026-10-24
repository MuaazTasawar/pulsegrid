use axum::extract::{FromRef, FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use domain::{ApiKeys, AppError};

use crate::errors::ApiError;

const API_KEY_HEADER: &str = "x-api-key";

fn check(headers: &HeaderMap, keys: &ApiKeys) -> Result<(), ApiError> {
    let presented = headers
        .get(API_KEY_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // An empty key set (nothing configured) rejects everything, and the
    // answer is identical for "no key", "wrong key" and "not configured".
    if keys.verify(presented) {
        Ok(())
    } else {
        Err(ApiError(AppError::Unauthorized))
    }
}

/// Proof that the caller presented a valid operator API key in the
/// `X-API-Key` header. Use it as a handler argument.
pub struct OperatorAuth;

impl<S> FromRequestParts<S> for OperatorAuth
where
    S: Send + Sync,
    ApiKeys: FromRef<S>,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        check(&parts.headers, &ApiKeys::from_ref(state))?;
        Ok(OperatorAuth)
    }
}

/// The same check as a layer, for protecting a whole router:
/// `axum::middleware::from_fn_with_state(keys, require_operator)`.
pub async fn require_operator(
    State(keys): State<ApiKeys>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    check(request.headers(), &keys)?;
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn protected(keys: ApiKeys) -> Router {
        Router::new()
            .route("/secret", get(|| async { "classified" }))
            .layer(axum::middleware::from_fn_with_state(keys, require_operator))
    }

    async fn status(app: &Router, key: Option<&str>) -> StatusCode {
        let mut builder = axum::http::Request::builder().uri("/secret");
        if let Some(k) = key {
            builder = builder.header("x-api-key", k);
        }
        app.clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn only_a_configured_key_is_accepted() {
        let app = protected(ApiKeys::from_csv("alpha-key,beta-key", false).unwrap());
        assert_eq!(status(&app, Some("alpha-key")).await, StatusCode::OK);
        assert_eq!(status(&app, Some("beta-key")).await, StatusCode::OK);
        assert_eq!(
            status(&app, Some("gamma-key")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(status(&app, Some("alpha")).await, StatusCode::UNAUTHORIZED);
        assert_eq!(status(&app, Some("")).await, StatusCode::UNAUTHORIZED);
        assert_eq!(status(&app, None).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn without_configured_keys_everything_is_rejected() {
        let app = protected(ApiKeys::default());
        assert_eq!(
            status(&app, Some("anything")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(status(&app, Some("")).await, StatusCode::UNAUTHORIZED);
        assert_eq!(status(&app, None).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn works_as_a_handler_extractor_too() {
        #[derive(Clone)]
        struct TestState(ApiKeys);
        impl FromRef<TestState> for ApiKeys {
            fn from_ref(state: &TestState) -> ApiKeys {
                state.0.clone()
            }
        }

        let app: Router = Router::new()
            .route("/secret", get(|_: OperatorAuth| async { "ok" }))
            .with_state(TestState(ApiKeys::from_csv("k-123", false).unwrap()));

        let ok = axum::http::Request::builder()
            .uri("/secret")
            .header("x-api-key", "k-123")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(ok).await.unwrap().status(),
            StatusCode::OK
        );

        let bad = axum::http::Request::builder()
            .uri("/secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(bad).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
