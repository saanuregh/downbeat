//! Errors the console returns to its screens, in the control API's shape.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use downbeat_api::types::ApiError;
use downbeat_api::with_defaults;

#[derive(Debug)]
pub(crate) struct Failure {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl Failure {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Failure {
        Failure { status, code, message: message.into() }
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::BAD_GATEWAY, "scheduler_unreachable", message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
}

impl From<sqlx::Error> for Failure {
    fn from(error: sqlx::Error) -> Failure {
        tracing::error!(%error, "a database error in the console");
        Failure::new(StatusCode::SERVICE_UNAVAILABLE, "database", "the console could not reach its database; try again")
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.status, Json(with_defaults!(ApiError { error: self.code.to_owned(), message: self.message })))
            .into_response()
    }
}
