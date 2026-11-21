//! Why a downbeat process (a role, a command such as `run-once`, or a child) stopped. Only printed
//! before the process exits, so it stays inside the crate; library calls return [`crate::Error`].

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProcessError {
    #[error(transparent)]
    Engine(#[from] crate::Error),
    #[error("{what}: {source}")]
    Io {
        what: String,
        #[source]
        source: std::io::Error,
    },
    /// A command-line option or the callers file is refused.
    #[error("{0}")]
    Refused(String),
    #[error("{}: {source}", path.display())]
    CallersFile {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error(transparent)]
    Key(#[from] downbeat_api::KeyError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("the worker's socket descriptor {0:?} is not a number")]
    Descriptor(String),
    #[error("the connection to the worker closed")]
    WorkerGone,
    /// A role's task panicked.
    #[error(transparent)]
    Role(#[from] tokio::task::JoinError),
}

impl ProcessError {
    pub(crate) fn io(what: impl Into<String>, source: std::io::Error) -> ProcessError {
        ProcessError::Io { what: what.into(), source }
    }
}

impl From<sqlx::Error> for ProcessError {
    fn from(error: sqlx::Error) -> ProcessError {
        ProcessError::Engine(crate::Error::Database(error))
    }
}
