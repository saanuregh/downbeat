//! How a job function ends: `Ok(output)`, `skip(…)`, `not_ready(…)` or an error.

use std::fmt::{Debug, Display};

/// The result a job function returns. `?` works on any error that can be printed.
pub type JobResult<T> = std::result::Result<T, JobError>;

/// Why a job function did not return an output.
///
/// Built by `?` from any printable error, or by [`skip`] and [`not_ready`]. Its text is
/// [`JobError::message`]. It does not implement `Display` itself: then it would be one of the
/// printable errors `?` converts, and that conversion would clash with the standard library's
/// `From<T> for T` (`anyhow::Error` makes the same trade-off).
#[derive(Debug)]
pub struct JobError(pub(crate) Stop);

impl JobError {
    /// What it says, as a run's reason would put it: the error, or why the run skips or waits.
    pub fn message(&self) -> &str {
        match &self.0 {
            Stop::Skip(text) | Stop::NotReady(text) | Stop::Error(text) | Stop::BadInput(text) => text,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stop {
    Skip(String),
    NotReady(String),
    Error(String),
    /// The input did not decode into the job's type.
    BadInput(String),
}

impl<E: Display + Debug + Send + Sync + 'static> From<E> for JobError {
    fn from(error: E) -> Self {
        // `{:#}` prints the whole chain for error types that support it, such as anyhow's.
        JobError(Stop::Error(format!("{error:#}")))
    }
}

impl JobError {
    /// An error with this message, for when there is no error value to convert.
    pub fn msg(message: impl Into<String>) -> Self {
        JobError(Stop::Error(message.into()))
    }
}

/// End the run as skipped: there is nothing to do. Nothing that follows success runs.
pub fn skip<T>(reason: impl Into<String>) -> JobResult<T> {
    Err(JobError(Stop::Skip(reason.into())))
}

/// Send the run back to the queue: what it needs is not there yet. It is called again after the
/// job's `recheck_every`, without spending an attempt.
pub fn not_ready<T>(reason: impl Into<String>) -> JobResult<T> {
    Err(JobError(Stop::NotReady(reason.into())))
}

/// What a call of a job function produced, as seen by a test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<T> {
    Succeeded(T),
    Skipped(String),
    NotReady(String),
    Failed(String),
}

impl<T> From<JobResult<T>> for Outcome<T> {
    fn from(result: JobResult<T>) -> Self {
        match result {
            Ok(output) => Outcome::Succeeded(output),
            Err(JobError(Stop::Skip(reason))) => Outcome::Skipped(reason),
            Err(JobError(Stop::NotReady(reason))) => Outcome::NotReady(reason),
            Err(JobError(Stop::Error(error) | Stop::BadInput(error))) => Outcome::Failed(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_error_says_what_it_carries() {
        let from_question_mark: JobError = std::io::Error::other("disk full").into();
        assert_eq!(from_question_mark.message(), "disk full");
        assert_eq!(skip::<()>("nothing to do").unwrap_err().message(), "nothing to do");
    }

    #[derive(Debug)]
    struct NotStdError;
    impl Display for NotStdError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("the product's own error")
        }
    }

    fn fails() -> JobResult<()> {
        Err(NotStdError)?;
        Ok(())
    }

    #[test]
    fn question_mark_works_on_any_printable_error() {
        assert_eq!(Outcome::from(fails()), Outcome::Failed("the product's own error".into()));
    }

    #[test]
    fn skip_and_not_ready_carry_their_reason() {
        assert_eq!(Outcome::from(skip::<()>("nothing")), Outcome::Skipped("nothing".into()));
        assert_eq!(Outcome::from(not_ready::<()>("later")), Outcome::NotReady("later".into()));
    }
}
