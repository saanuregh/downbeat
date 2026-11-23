//! Errors the API returns: a status, a short code and a sentence that says what to do.

use axum::Json;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use downbeat_api::types::ApiError;
use sqlx::postgres::{PgDatabaseError, PgSeverity};

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
        if is_passing(&error) {
            Failure::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "database",
                "the scheduler's database could not answer just now; try again",
            )
        } else {
            Failure::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", "the scheduler failed to answer this request")
        }
    }
}

/// Whether the same request could succeed later: the database could not be reached, or refused
/// this statement for a passing reason, as opposed to failing it for good.
fn is_passing(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::WorkerCrashed => true,
        // FATAL or PANIC: Postgres refused or dropped the connection (not accepting connections, a
        // shutdown, too many clients). Class 08: connection exceptions; class 40: deadlock and
        // serialisation failures; 57014: the statement timeout; 55P03: a lock not available;
        // 25006: a read-only server, as after a failover.
        sqlx::Error::Database(error) => error.try_downcast_ref::<PgDatabaseError>().is_some_and(|error| {
            let code = error.code();
            matches!(error.severity(), PgSeverity::Fatal | PgSeverity::Panic)
                || code.starts_with("08")
                || code.starts_with("40")
                || matches!(code, "57014" | "55P03" | "25006")
        }),
        _ => false,
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.status, Json(ApiError { error: self.code.to_owned(), message: self.message })).into_response()
    }
}

pub(crate) type Answer<T> = Result<Json<T>, Failure>;

/// Turns an error axum answers by itself (an unknown path, a wrong method, a body that does not
/// parse) into the same JSON shape as every other error, keeping its status and its sentence.
pub(crate) async fn as_json(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = response.status();
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"application/json"));
    if !(status.is_client_error() || status.is_server_error()) || is_json {
        return response;
    }
    let text = axum::body::to_bytes(response.into_body(), 16 * 1024).await.unwrap_or_default();
    let text = String::from_utf8_lossy(&text);
    let message = if text.trim().is_empty() {
        status.canonical_reason().unwrap_or("the request was refused").to_lowercase()
    } else {
        text.trim().to_owned()
    };
    let code = match status {
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::METHOD_NOT_ALLOWED => "method_not_allowed",
        StatusCode::PAYLOAD_TOO_LARGE => "too_large",
        StatusCode::UNSUPPORTED_MEDIA_TYPE | StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::UNPROCESSABLE_ENTITY => "bad_input",
        _ if status.is_server_error() => "internal",
        _ => "refused",
    };
    Failure::new(status, code, message).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_passing_database_failure_answers_503() {
        let io = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::Io(io),
            sqlx::Error::Protocol("cut".into()),
        ] {
            assert_eq!(Failure::from(error).status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        for error in [sqlx::Error::RowNotFound, sqlx::Error::ColumnNotFound("missing".into())] {
            assert_eq!(Failure::from(error).status(), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}
