use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Axum middleware that validates Bearer token authentication.
/// Checks the `Authorization: Bearer <token>` header against the configured API key.
pub async fn require_auth(req: Request, next: Next) -> Response {
    let api_key = match req.extensions().get::<ApiKey>() {
        Some(key) => key.0.clone(),
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Auth not configured",
                    "retry": false
                }})),
            )
                .into_response();
        }
    };

    // If no API key is configured, skip auth (dev mode)
    if api_key.is_empty() {
        return next.run(req).await;
    }

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header[7..];
            if token == api_key {
                next.run(req).await
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(json!({"error": {
                        "code": "UNAUTHORIZED",
                        "message": "Invalid API key",
                        "retry": false
                    }})),
                )
                    .into_response()
            }
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": {
                "code": "UNAUTHORIZED",
                "message": "Missing Authorization: Bearer <token> header",
                "retry": false
            }})),
        )
            .into_response(),
    }
}

/// Extension type to inject the API key into the request.
#[derive(Clone)]
pub struct ApiKey(pub String);
