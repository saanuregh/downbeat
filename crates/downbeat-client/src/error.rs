//! What can go wrong talking to a scheduler.

/// An error from the client, with enough said to act on it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("the client is not set up: {0}")]
    Setup(String),
    #[error("the key could not be read: {0}")]
    Key(#[from] downbeat_api::KeyError),
    #[error("the scheduler could not be reached: {0}")]
    Connection(#[source] ConnectionError),
    #[error("the scheduler speaks API version {spoken}; this client supports {supported:?}")]
    Version { spoken: u32, supported: Vec<u32> },
    /// The scheduler answered with an error.
    #[error("{status}: {message}")]
    Api { status: u16, code: String, message: String },
}

impl Error {
    /// Whether retrying the same request could help.
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Connection(ConnectionError(error)) => error.is_timeout() || error.is_connect() || error.is_request(),
            Error::Api { status, .. } => matches!(status, 429 | 502 | 503 | 504),
            _ => false,
        }
    }

    /// The scheduler could not be reached, or its answer could not be read.
    pub(crate) fn connection(error: reqwest::Error) -> Error {
        Error::Connection(ConnectionError(error))
    }
}

/// Why the scheduler could not be reached, or its answer could not be read. The HTTP library's own
/// error is its [`source`](std::error::Error::source), so this crate can change that library
/// without breaking callers.
#[derive(Debug)]
pub struct ConnectionError(reqwest::Error);

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[tokio::test]
    async fn a_connection_error_keeps_the_http_error_as_its_source() {
        // Nothing listens on port 1, so the connection is refused: a transient failure.
        let refused = reqwest::get("http://127.0.0.1:1/").await.unwrap_err();
        let text = refused.to_string();
        let error = Error::connection(refused);
        assert!(error.is_transient());
        assert_eq!(error.to_string(), format!("the scheduler could not be reached: {text}"));
        let connection = error.source().expect("the connection error is the source");
        let http = connection.source().expect("the HTTP error is its source");
        assert!(http.downcast_ref::<reqwest::Error>().is_some_and(reqwest::Error::is_connect));

        let unbuildable = reqwest::Client::new().get("not a url").build().unwrap_err();
        assert!(!Error::connection(unbuildable).is_transient());
    }
}
