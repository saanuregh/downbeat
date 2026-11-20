//! downbeat: schedule, chain, retry and observe jobs written as typed Rust functions, with
//! Postgres as the only dependency.
//!
//! See the user guide in `docs/` for a tour. In short: write each job as a function with
//! `#[job(…)]`, say when it runs in a pipeline function, describe the product in a
//! [`Definition`], and start it with [`run`].

mod api;
mod check;
mod child;
mod cli;
mod client;
mod definition;
mod dependencies;
mod describe;
mod events;
mod guard;
mod job;
mod pipeline;
mod protocol;
mod pruning;
mod reaper;
mod reasons;
mod registry;
mod schedule;
mod scheduler;
mod starting;
mod store;
mod units;
mod worker;

pub use client::{InAppClient, InAppStart};
pub use definition::{Definition, QueueTimings, Settings};
pub use dependencies::{Dependencies, Dependency};
pub use events::{Event, EventDelivery, FinishedRun};
pub use job::{
    Backoff, EXIT_NOT_READY, FieldShape, FunctionBody, Job, JobData, JobError, JobSpec, MAX_VALUE_BYTES, Outcome,
    Policy, Program, ProgramSpec, Result, RetryOn, RetryOnSet, Run, Shape, Start, Started, Turn, not_ready, presets,
    skip,
};
pub use pipeline::{
    DoneFor, DoneWithin, Ended, Failure, HasParents, JobBuilder, LinkKind, MapExt, Mapped, Need, NoParents, Parents,
    Pipeline, WhenUnmet,
};
pub use schedule::{Day, Occurrence, Schedule, daily_at, every};
pub use units::{Bytes, BytesExt, DurationExt};

pub use downbeat_macros::{JobData, command, job};

/// Runs the product: parses the command line (`scheduler`, `worker`, `check`, `run-once`,
/// `health`) and starts what it asks for. Call it from `main` and do nothing else there.
///
/// ```ignore
/// fn main() -> std::process::ExitCode {
///     downbeat::run(definition)
/// }
/// ```
pub fn run(definition: fn() -> Definition) -> std::process::ExitCode {
    if let Some(code) = child_process(definition) {
        return code;
    }
    cli::run(std::env::args_os().collect(), definition)
}

/// For a product with its own command line: runs downbeat's command line from these arguments,
/// the first being the program's name. Call [`child_process`] first, before any parsing of your own.
pub fn run_as<I, T>(args: I, definition: fn() -> Definition) -> std::process::ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    if let Some(code) = child_process(definition) {
        return code;
    }
    cli::run(args.into_iter().map(Into::into).collect(), definition)
}

/// When this process is a child a worker started to run job functions, runs it and returns its exit
/// code. Otherwise returns `None`. [`run`] calls this itself.
pub fn child_process(definition: fn() -> Definition) -> Option<std::process::ExitCode> {
    std::env::var(child::CHILD_FD).ok().map(|descriptor| child::main(definition, &descriptor))
}

/// Brings the engine's tables in the `downbeat` schema up to date. Every role does this at start;
/// call it yourself to migrate as a separate deploy step.
pub async fn migrate(database_url: &str) -> anyhow::Result<()> {
    store::Store::connect(database_url, 1).await.map(|_| ())
}

/// The problems `check` would report for a definition, as lines of text. Empty when it is sound.
pub fn check_problems(definition: Definition) -> Vec<String> {
    match registry::Registry::build(definition, None) {
        Ok(_) => Vec::new(),
        Err(problems) => problems.iter().map(ToString::to_string).collect(),
    }
}

/// A job's policy, as its attribute set it.
pub fn policy_of(spec: &JobSpec) -> &Policy {
    &spec.policy
}

/// Everything a product's job and pipeline code uses, in one import.
pub mod prelude {
    pub use crate::{
        BytesExt, Day, Definition, DoneFor, DoneWithin, DurationExt, Ended, Failure, Job, JobData, JobError, MapExt,
        Occurrence, Outcome, Pipeline, Program, Result, RetryOn, Run, Started, command, daily_at, every, job,
        not_ready, presets, skip,
    };
    pub use chrono::{NaiveDate, Weekday};
}

/// What the macros' generated code refers to. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use crate::job::Registered;
    pub use crate::job::{
        ProgramFn, TurnValueFn, decode_input, decode_value, encode_output, field_text, without_fields,
    };
    pub use futures::future::BoxFuture;
    pub use inventory;
    pub use serde_json::{Value, to_value};
    pub use std::sync::Arc;
}
