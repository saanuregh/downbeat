//! Errors the API returns: a status, a short code and a sentence that says what to do.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use downbeat_api::types::ApiError;

#[derive(Debug)]
pub(crate) struct Failure {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl Failure {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Failure {
        Failure { status, code, message: message.into() }
    }

    pub(crate) fn not_found(what: impl Into<String>) -> Failure {
        Failure::new(StatusCode::NOT_FOUND, "not_found", what)
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub(crate) fn unprocessable(message: impl Into<String>) -> Failure {
        Failure::new(StatusCode::UNPROCESSABLE_ENTITY, "bad_input", message)
    }
}

impl Failure {
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl From<sqlx::Error> for Failure {
    fn from(error: sqlx::Error) -> Failure {
        tracing::error!(%error, "a database error in the control API");
        Failure::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database",
            "the scheduler could not reach its database; try again",
        )
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.status, Json(ApiError { error: self.code.to_owned(), message: self.message })).into_response()
    }
}

pub(crate) type Answer<T> = Result<Json<T>, Failure>;
