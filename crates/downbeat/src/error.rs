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
    Database(#[from] sqlx::Error),
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
            crate::starting::StartError::Database(error) => Error::Database(error),
        }
    }
}
