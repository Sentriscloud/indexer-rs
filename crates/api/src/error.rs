//! HTTP error type. Maps DB / decode failures to 4xx/5xx responses with the
//! TS port's `{ "error": "..." }` body shape.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Result alias used by handlers.
pub type ApiResult<T> = std::result::Result<T, ApiError>;

/// API-level error. Maps to a (status, body) pair via [`IntoResponse`].
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Caller's input was invalid (400).
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Resource not found (404). Body shape mirrors TS:
    /// `{ "error": "<resource> not found" }`.
    #[error("{0} not found")]
    NotFound(String),

    /// Underlying database failure (500).
    #[error("db: {0}")]
    Db(#[from] indexer_db::DbError),

    /// Direct sqlx error (500).
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::InvalidQuery(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(what) => (StatusCode::NOT_FOUND, format!("{what} not found")),
            ApiError::Db(e) => {
                tracing::error!(error = %e, "db failure");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
            ApiError::Sqlx(e) => {
                tracing::error!(error = %e, "sqlx failure");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
