//! `Run`: what a job function knows about the run it is doing, and how it talks back.

use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{Job, JobData, JobError, Result};

/// The run a job function is doing.
///
/// Cheap to clone. A function receives it as its first argument.
#[derive(Clone)]
#[allow(clippy::struct_field_names)] // `dry_run` is the term the API uses.
pub struct Run {
    pub(crate) id: Uuid,
    pub(crate) attempt: u32,
    pub(crate) due_at: DateTime<Utc>,
    pub(crate) dry_run: bool,
    pub(crate) cancelled: CancellationToken,
    pub(crate) last_checkpoint: Option<Value>,
    pub(crate) link: Arc<dyn RunLink>,
}

impl fmt::Debug for Run {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Run")
            .field("id", &self.id)
            .field("attempt", &self.attempt)
            .field("due_at", &self.due_at)
            .field("dry_run", &self.dry_run)
            .finish_non_exhaustive()
    }
}

/// How a run reports back to the worker that holds it. In a test, to a recorder.
pub(crate) trait RunLink: Send + Sync {
    fn progress(&self, text: String);
    fn checkpoint(&self, state: Value);
    fn checkpoint_now(&self, state: Value) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn start(&self, request: StartRequest) -> Pin<Box<dyn Future<Output = Result<Started>> + Send + '_>>;

    /// What a test run started; empty for a real run.
    fn recorded_starts(&self) -> Vec<(String, Value)> {
        Vec::new()
    }

    /// What a test run reported; empty for a real run.
    fn recorded_reports(&self) -> Vec<String> {
        Vec::new()
    }
}

/// A request to start a job, made by a job, a program or a person.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
pub(crate) struct StartRequest {
    pub(crate) job: String,
    pub(crate) input: Value,
    pub(crate) priority: Option<i16>,
    pub(crate) even_if_queued: bool,
}

/// What starting a job did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "result", content = "run_id", rename_all = "snake_case")]
pub enum Started {
    /// A new run was created.
    New(Uuid),
    /// The same work was already queued; this is its run.
    Joined(Uuid),
}

impl Started {
    pub fn run_id(self) -> Uuid {
        match self {
            Started::New(id) | Started::Joined(id) => id,
        }
    }
}

/// The largest input, output or checkpoint the engine stores, in bytes.
pub const MAX_VALUE_BYTES: usize = 64 * 1024;

impl Run {
    /// True once this run has been asked to stop: a timeout, a cancel, a lost lease or shutdown.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }

    /// Completes when this run is asked to stop, for use in `tokio::select!`.
    pub async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }

    /// A line for people, returned with the run by the API. Each call replaces the last.
    pub fn progress(&self, text: impl Into<String>) {
        self.link.progress(text.into());
    }

    /// State a later call of this run reads with [`Run::last_checkpoint`] to continue where this one
    /// stopped. Saved with the next heartbeat.
    pub fn checkpoint<S: Serialize>(&self, state: &S) -> Result<()> {
        self.link.checkpoint(encode_small(state, "checkpoint")?);
        Ok(())
    }

    /// The same as [`Run::checkpoint`], stored before this returns.
    pub async fn checkpoint_now<S: Serialize>(&self, state: &S) -> Result<()> {
        self.link.checkpoint_now(encode_small(state, "checkpoint")?).await
    }

    /// What an earlier call of this run last saved, if anything.
    pub fn last_checkpoint<S: DeserializeOwned>(&self) -> Option<S> {
        self.last_checkpoint.clone().and_then(|state| serde_json::from_value(state).ok())
    }

    /// Start another job. Calling it again on a later attempt of this run does not start a second
    /// run: the engine keys the request on this run and the position of the call.
    #[allow(clippy::needless_pass_by_value)] // The input is the request's: passing it is handing it over.
    pub fn start<J: Job>(&self, job: J, input: J::Input) -> Start<'_> {
        let _ = job;
        Start {
            run: self,
            request: serde_json::to_value(&input)
                .map_err(|error| JobError::msg(format!("the input to {} could not be encoded: {error}", J::NAME)))
                .map(|input| StartRequest { job: J::NAME.to_owned(), input, priority: None, even_if_queued: false }),
        }
    }

    /// True when a dry run was asked for. Only a job marked `supports_dry_run` is ever asked.
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    /// When the run was due: its schedule's occurrence, or when it was requested.
    pub fn due_at(&self) -> DateTime<Utc> {
        self.due_at
    }

    /// Which attempt this is, counting failed calls: 1 for the first.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The run's id.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// A run for a unit test: not cancelled, attempt 1, due now. Jobs it starts are recorded and
    /// can be read with [`Run::started_jobs`].
    pub fn for_test() -> Self {
        Run {
            id: Uuid::nil(),
            attempt: 1,
            due_at: Utc::now(),
            dry_run: false,
            cancelled: CancellationToken::new(),
            last_checkpoint: None,
            link: Arc::new(TestLink::default()),
        }
    }

    /// The same as [`Run::for_test`], continuing from a checkpoint.
    #[must_use]
    pub fn with_checkpoint<S: Serialize>(mut self, state: &S) -> Self {
        self.last_checkpoint = serde_json::to_value(state).ok();
        self
    }

    /// The same as [`Run::for_test`], due at this instant: a test's clock.
    #[must_use]
    pub fn due(mut self, at: DateTime<Utc>) -> Self {
        self.due_at = at;
        self
    }

    /// The same as [`Run::for_test`], on this attempt (counting failed calls, from 1).
    #[must_use]
    pub fn on_attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    /// The same as [`Run::for_test`], already asked to stop: for a job that must notice.
    #[must_use]
    pub fn cancelled_already(self) -> Self {
        self.cancelled.cancel();
        self
    }

    /// The same as [`Run::for_test`], as a dry run.
    #[must_use]
    pub fn dry(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// In a test: the jobs this run started, as `(job name, input)`.
    pub fn started_jobs(&self) -> Vec<(String, Value)> {
        self.link.recorded_starts()
    }

    /// In a test: the progress lines and checkpoints this run reported, in order.
    pub fn reported(&self) -> Vec<String> {
        self.link.recorded_reports()
    }
}

/// A pending `run.start(…)`. Await it, optionally after `.priority(…)` or `.even_if_queued()`.
#[must_use = "a start request does nothing until it is awaited"]
pub struct Start<'a> {
    run: &'a Run,
    request: Result<StartRequest>,
}

impl fmt::Debug for Start<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Start").field("request", &self.request).finish_non_exhaustive()
    }
}

impl Start<'_> {
    /// Give this one run a priority.
    pub fn priority(mut self, priority: i16) -> Self {
        if let Ok(request) = &mut self.request {
            request.priority = Some(priority);
        }
        self
    }

    /// Create a run even when the same work is already queued.
    pub fn even_if_queued(mut self) -> Self {
        if let Ok(request) = &mut self.request {
            request.even_if_queued = true;
        }
        self
    }
}

impl<'a> IntoFuture for Start<'a> {
    type Output = Result<Started>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<Started>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.run.link.start(self.request?).await })
    }
}

pub(crate) fn encode_small<T: Serialize + ?Sized>(value: &T, what: &str) -> Result<Value> {
    let value = serde_json::to_value(value)
        .map_err(|error| JobError::msg(format!("the {what} could not be encoded: {error}")))?;
    let size = value.to_string().len();
    if size > MAX_VALUE_BYTES {
        return Err(JobError::msg(format!("the {what} is {size} bytes; the limit is {MAX_VALUE_BYTES}")));
    }
    Ok(value)
}

/// Decodes a value a job receives into its type.
pub(crate) fn decode<T: JobData>(value: Value) -> std::result::Result<T, String> {
    serde_json::from_value(value).map_err(|error| error.to_string())
}

/// Records what a test run reports.
#[derive(Default)]
struct TestLink {
    starts: Mutex<Vec<(String, Value)>>,
    reports: Mutex<Vec<String>>,
}

impl RunLink for TestLink {
    fn progress(&self, text: String) {
        self.reports.lock().expect("test recorder").push(format!("progress: {text}"));
    }

    fn checkpoint(&self, state: Value) {
        self.reports.lock().expect("test recorder").push(format!("checkpoint: {state}"));
    }

    fn checkpoint_now(&self, state: Value) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        self.checkpoint(state);
        Box::pin(async { Ok(()) })
    }

    fn start(&self, request: StartRequest) -> Pin<Box<dyn Future<Output = Result<Started>> + Send + '_>> {
        self.starts.lock().expect("test recorder").push((request.job, request.input));
        Box::pin(async { Ok(Started::New(Uuid::new_v4())) })
    }

    fn recorded_starts(&self) -> Vec<(String, Value)> {
        self.starts.lock().expect("test recorder").clone()
    }

    fn recorded_reports(&self) -> Vec<String> {
        self.reports.lock().expect("test recorder").clone()
    }
}
