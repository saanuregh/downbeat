//! What the library's entry points (`migrate`, `InAppClient`) can fail with.

/// Why connecting, migrating or starting a job through [`crate::InAppClient`] failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The definition did not build; `check` lists the same problems.
    #[error("the definition has problems; `check` lists them:\n  {}", problems.join("\n  "))]
    Definition { problems: Vec<String> },
    /// The database could not be reached, or a statement failed.
    #[error(transparent)]
    Database(DatabaseError),
    /// The schema has a breaking migration newer than this engine knows.
    #[error(
        "the database's downbeat schema is at version {found}, which this engine (schema {known}) cannot use; deploy a newer build"
    )]
    NewerSchema { found: i32, known: i32 },
    /// The job is not in this definition.
    #[error("there is no job named {0}")]
    UnknownJob(String),
    /// A dry run of a job not marked `supports_dry_run`.
    #[error("{0} does not support dry runs; only a job marked supports_dry_run can be asked")]
    NoDryRun(String),
    /// The input could not be encoded, or does not fit the job.
    #[error("{0}")]
    Input(String),
}

impl From<crate::starting::StartError> for Error {
    fn from(error: crate::starting::StartError) -> Error {
        match error {
            crate::starting::StartError::UnknownJob(job) => Error::UnknownJob(job),
            crate::starting::StartError::NoDryRun(job) => Error::NoDryRun(job),
            crate::starting::StartError::Database(error) => Error::database(error),
        }
    }
}

impl Error {
    /// The database could not be reached, or a statement failed.
    pub(crate) fn database(error: sqlx::Error) -> Error {
        Error::Database(DatabaseError(error))
    }
}

/// Why the database could not be reached, or a statement failed. The database library's own error
/// is its [`source`](std::error::Error::source), so this crate can change that library without
/// breaking callers.
#[derive(Debug)]
pub struct DatabaseError(sqlx::Error);

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
