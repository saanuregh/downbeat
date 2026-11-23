//! What can go wrong talking to a scheduler.

/// An error from the client, with enough said to act on it.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the client is not set up: {0}")]
    Setup(String),
    #[error("the key could not be read: {0}")]
    Key(#[from] downbeat_api::KeyError),
    #[error("the scheduler could not be reached: {0}")]
    Connection(#[from] reqwest::Error),
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
            Error::Connection(error) => error.is_timeout() || error.is_connect() || error.is_request(),
            Error::Api { status, .. } => matches!(status, 429 | 502 | 503 | 504),
            _ => false,
        }
    }
}
