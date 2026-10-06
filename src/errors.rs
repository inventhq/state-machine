use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Unauthorized(String),
    Internal(String),
    /// 409 that retrying cannot fix (identity conflict, uncorrelated child final).
    Rejected { code: &'static str, message: String },
}

/// Same (event_type, timestamp) as a committed row on this entity, different params.
pub const EVENT_IDENTITY_CONFLICT: &str = "EVENT_IDENTITY_CONFLICT";
/// Recovery advance refused: the child's final state is not provably from this parent instance.
pub const UNCORRELATED_CHILD_FINAL: &str = "UNCORRELATED_CHILD_FINAL";

impl AppError {
    /// Machine-readable error code for the plugin-runtime to branch on.
    fn code(&self) -> &'static str {
        match self {
            AppError::NotFound(_) => "NOT_FOUND",
            AppError::BadRequest(_) => "BAD_REQUEST",
            AppError::Conflict(_) => "CONFLICT",
            AppError::Unauthorized(_) => "UNAUTHORIZED",
            AppError::Internal(_) => "INTERNAL_ERROR",
            AppError::Rejected { code, .. } => code,
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Rejected { .. } => StatusCode::CONFLICT,
        }
    }

    fn message(&self) -> &str {
        match self {
            AppError::NotFound(msg)
            | AppError::BadRequest(msg)
            | AppError::Conflict(msg)
            | AppError::Unauthorized(msg)
            | AppError::Internal(msg)
            | AppError::Rejected { message: msg, .. } => msg,
        }
    }

    /// Whether the plugin-runtime should retry this request.
    fn retry(&self) -> bool {
        matches!(self, AppError::Conflict(_) | AppError::Internal(_))
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "code": self.code(),
                "message": self.message(),
                "retry": self.retry()
            }
        });
        (self.status(), axum::Json(body)).into_response()
    }
}

impl From<libsql::Error> for AppError {
    fn from(err: libsql::Error) -> Self {
        AppError::Internal(format!("Database error: {}", err))
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        AppError::Internal(format!("Serialization error: {}", err))
    }
}
