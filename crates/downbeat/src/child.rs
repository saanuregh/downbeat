//! The child process: where job functions run.
//!
//! A worker starts its own binary with `DOWNBEAT_CHILD_FD` set. The child builds the definition,
//! says it is ready, and then does what the worker asks over the socket on that descriptor: prepare
//! a run, call a function, check a command's output. It never touches the engine's tables.

use std::collections::HashMap;
use std::future::Future;
use std::os::fd::{FromRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::definition::Definition;
use crate::job::{Body, JobError, MAX_VALUE_BYTES, Run, RunLink, StartRequest, Started, Stop};
use crate::pipeline::NeedTest;
use crate::protocol::{self, ChildEnding, ChildStart, FromChild, Prepared, RunInfo, ToChild};
use crate::registry::Registry;

/// The variable that tells a process it is a child, and which descriptor is its socket.
pub(crate) const CHILD_FD: &str = "DOWNBEAT_CHILD_FD";
/// The environment whose schedules apply; children build the definition the same way.
pub(crate) const ENVIRONMENT: &str = "DOWNBEAT_ENVIRONMENT";

/// Answers the running functions wait for, by run and request number.
type Pending = Arc<Mutex<HashMap<(Uuid, u64), oneshot::Sender<Result<Option<Started>, String>>>>>;

/// Runs the child until the worker closes the socket.
pub(crate) fn main(definition: fn() -> Definition, descriptor: &str) -> std::process::ExitCode {
    // A job's log lines go to standard error, each inside a span that names its run.
    crate::cli::init_logging();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a tokio runtime");
    match runtime.block_on(serve(definition, descriptor)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("downbeat child: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn serve(definition: fn() -> Definition, descriptor: &str) -> anyhow::Result<()> {
    let descriptor: i32 = descriptor.parse()?;
    let environment = std::env::var(ENVIRONMENT).ok();
    let registry = Registry::build(definition(), environment.as_deref())
        .map_err(|problems| anyhow::anyhow!("the definition has problems: {problems:?}"))?;
    let registry = Arc::new(registry);

    // SAFETY: the worker started this process with its end of a socket pair on this descriptor,
    // which nothing else in the process owns.
    #[allow(unsafe_code)]
    let owned = unsafe { OwnedFd::from_raw_fd(descriptor) };
    let socket = std::os::unix::net::UnixStream::from(owned);
    socket.set_nonblocking(true)?;
    let (mut reader, writer) = UnixStream::from_std(socket)?.into_split();

    let (outgoing, receiver) = mpsc::unbounded_channel::<FromChild>();
    tokio::spawn(write_messages(writer, receiver));
    outgoing.send(FromChild::Ready)?;

    let pending: Pending = Arc::default();
    let mut current: Option<(Uuid, CancellationToken)> = None;
    while let Some(message) = protocol::read::<_, ToChild>(&mut reader).await? {
        match message {
            ToChild::Prepare { run, input, parents } => {
                let result = prepare(&registry, &run, input, parents);
                outgoing.send(FromChild::Prepared { run_id: run.run_id, result })?;
            }
            ToChild::Go { run, input, checkpoint } => {
                let cancelled = CancellationToken::new();
                current = Some((run.run_id, cancelled.clone()));
                go(&registry, &run, input, checkpoint, cancelled, &outgoing, &pending);
            }
            ToChild::CheckOutput { run_id, job, output } => {
                let result = check_output(&registry, &job, output);
                outgoing.send(FromChild::Checked { run_id, result })?;
            }
            ToChild::Cancel { run_id } => {
                if let Some((current_id, token)) = &current
                    && *current_id == run_id
                {
                    token.cancel();
                }
            }
            ToChild::Answer { run_id, request, result } => {
                if let Some(answer) = pending.lock().expect("pending answers").remove(&(run_id, request)) {
                    let _ = answer.send(result);
                }
            }
        }
    }
    Ok(())
}

async fn write_messages(mut writer: OwnedWriteHalf, mut receiver: mpsc::UnboundedReceiver<FromChild>) {
    while let Some(message) = receiver.recv().await {
        if protocol::write(&mut writer, &message).await.is_err() {
            break;
        }
    }
}

/// Builds the run's input, and evaluates its conditions and needs, where the types are known.
fn prepare(
    registry: &Registry,
    run: &RunInfo,
    input: Option<Value>,
    parents: Option<Vec<Value>>,
) -> Result<Prepared, String> {
    let job = registry.job(&run.job).ok_or_else(|| format!("this build has no job named {}", run.job))?;
    let input = match (parents, &job.link) {
        (Some(handed_over), Some(link)) => {
            (link.adapt)(handed_over).map_err(|error| format!("the link's data does not fit: {error}"))?
        }
        _ => input.unwrap_or(Value::Null),
    };
    let input = (job.spec.input_ops.normalize)(input, false)?;
    let input_hash = (job.spec.input_ops.hash)(&input)?;
    let turn_value = job.spec.input_ops.turn_value.map(|value_of| value_of(&input)).transpose()?;
    let mut failed_condition = None;
    for condition in &job.conditions {
        if !(condition.test)(&input)? {
            failed_condition = Some(condition.name.clone());
            break;
        }
    }
    let need_hashes = job
        .needs
        .iter()
        .map(|need| match &need.test {
            NeedTest::DoneFor(hash_of) => hash_of(&input).map(Some),
            NeedTest::DoneWithin(_) => Ok(None),
        })
        .collect::<Result<_, _>>()?;
    Ok(Prepared { input, input_hash, turn_value, failed_condition, need_hashes })
}

fn check_output(registry: &Registry, job: &str, output: Value) -> Result<Value, String> {
    match registry.job(job).map(|job| &job.spec.body) {
        Some(Body::Command { output: check, .. }) => check(output),
        _ => Err(format!("{job} is not a command in this build")),
    }
}

fn go(
    registry: &Arc<Registry>,
    info: &RunInfo,
    input: Value,
    checkpoint: Option<Value>,
    cancelled: CancellationToken,
    outgoing: &mpsc::UnboundedSender<FromChild>,
    pending: &Pending,
) {
    let run_id = info.run_id;
    let Some(job) = registry.job(&info.job) else {
        let _ = outgoing.send(FromChild::Ended {
            run_id,
            ending: ChildEnding::BadInput(format!("this build has no job named {}", info.job)),
        });
        return;
    };
    match &job.spec.body {
        Body::Command { program, .. } => {
            let result = program(input).and_then(|spec| match spec.smuggled_option() {
                Some(argument) => Err(format!(
                    "the argument '{argument}' came from a value but looks like an option; use .values_may_be_options() if that is intended"
                )),
                None => Ok(spec),
            });
            let _ = outgoing.send(FromChild::Program { run_id, result });
        }
        Body::Function(body) => {
            let link = Arc::new(ChildLink {
                run_id,
                outgoing: outgoing.clone(),
                pending: Arc::clone(pending),
                registry: Arc::clone(registry),
                next: AtomicU64::new(0),
                starts: AtomicU64::new(0),
            });
            let run = Run {
                id: run_id,
                attempt: info.attempt,
                due_at: info.due_at,
                dry_run: info.dry_run,
                cancelled,
                last_checkpoint: checkpoint,
                link,
            };
            let future = body(run, input, Arc::clone(&registry.dependencies));
            let outgoing = outgoing.clone();
            let span = tracing::info_span!("run", run_id = %run_id, job = %info.job);
            tokio::spawn(async move {
                let _ = outgoing.send(FromChild::Started { run_id });
                let ending = match tokio::spawn(future.instrument(span)).await {
                    Ok(result) => ending_of(result),
                    Err(join) => ChildEnding::Panic(panic_message(join)),
                };
                let _ = outgoing.send(FromChild::Ended { run_id, ending });
            });
        }
    }
}

fn ending_of(result: Result<Value, JobError>) -> ChildEnding {
    match result {
        Ok(output) => {
            let size = output.to_string().len();
            if size > MAX_VALUE_BYTES { ChildEnding::OutputTooLarge(size) } else { ChildEnding::Output(output) }
        }
        Err(JobError(Stop::Skip(reason))) => ChildEnding::Skip(reason),
        Err(JobError(Stop::NotReady(reason))) => ChildEnding::NotReady(reason),
        Err(JobError(Stop::BadInput(error))) => ChildEnding::BadInput(error),
        Err(JobError(Stop::Error(error))) => ChildEnding::Error(error),
    }
}

fn panic_message(join: tokio::task::JoinError) -> String {
    match join.try_into_panic() {
        Ok(payload) => payload
            .downcast_ref::<&str>()
            .map(|text| (*text).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a panic with no message".to_owned()),
        Err(join) => join.to_string(),
    }
}

/// A running function's way back to the worker.
struct ChildLink {
    run_id: Uuid,
    outgoing: mpsc::UnboundedSender<FromChild>,
    pending: Pending,
    registry: Arc<Registry>,
    /// Request numbers for answers the function waits for.
    next: AtomicU64,
    /// How many `run.start` calls this call of the function has made: a start's position.
    starts: AtomicU64,
}

impl ChildLink {
    fn ask(&self, message: impl FnOnce(u64) -> FromChild) -> oneshot::Receiver<Result<Option<Started>, String>> {
        let request = self.next.fetch_add(1, Ordering::SeqCst);
        let (answer, receiver) = oneshot::channel();
        self.pending.lock().expect("pending answers").insert((self.run_id, request), answer);
        let _ = self.outgoing.send(message(request));
        receiver
    }
}

impl RunLink for ChildLink {
    fn progress(&self, text: String) {
        let _ = self.outgoing.send(FromChild::Progress { run_id: self.run_id, text });
    }

    fn checkpoint(&self, state: Value) {
        let _ = self.outgoing.send(FromChild::Checkpoint { run_id: self.run_id, state, request: None });
    }

    fn checkpoint_now(&self, state: Value) -> Pin<Box<dyn Future<Output = crate::job::Result<()>> + Send + '_>> {
        let answer = self.ask(|request| FromChild::Checkpoint { run_id: self.run_id, state, request: Some(request) });
        Box::pin(async move {
            match answer.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(error)) => Err(JobError::msg(error)),
                Err(_) => Err(JobError::msg("the worker went away before the checkpoint was stored")),
            }
        })
    }

    fn start(&self, request: StartRequest) -> Pin<Box<dyn Future<Output = crate::job::Result<Started>> + Send + '_>> {
        Box::pin(async move {
            let job = self
                .registry
                .job(&request.job)
                .ok_or_else(|| JobError::msg(format!("there is no job named {}", request.job)))?;
            let ops = &job.spec.input_ops;
            let input = (ops.normalize)(request.input, true).map_err(JobError::msg)?;
            let input_hash = (ops.hash)(&input).map_err(JobError::msg)?;
            let turn_value = ops.turn_value.map(|value_of| value_of(&input)).transpose().map_err(JobError::msg)?;
            let position = self.starts.fetch_add(1, Ordering::SeqCst);
            let start = ChildStart {
                job: request.job,
                input,
                input_hash,
                turn_value,
                priority: request.priority,
                even_if_queued: request.even_if_queued,
                position,
            };
            let answer = self.ask(|request| FromChild::Start { run_id: self.run_id, request, start });
            match answer.await {
                Ok(Ok(Some(started))) => Ok(started),
                Ok(Ok(None)) => Err(JobError::msg("the worker gave no run for this start")),
                Ok(Err(error)) => Err(JobError::msg(error)),
                Err(_) => Err(JobError::msg("the worker went away before the job was started")),
            }
        })
    }
}
