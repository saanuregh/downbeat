//! Calling a run's function in a child, or a command's program, under its timeout, memory limit,
//! cancellation and the worker's shutdown.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use tokio::time::{Instant, MissedTickBehavior, sleep_until};

use super::child::ChildHandle;
use super::lease::{Lease, StopReason};
use super::prepare::run_info;
use super::process::{Launch, Process, describe_exit};
use super::{WorkerContext, start_from_job};
use crate::job::{EXIT_NOT_READY, MAX_VALUE_BYTES, ProgramSpec, env_names};
use crate::protocol::{ChildEnding, FromChild, ToChild};
use crate::registry::RegisteredJob;
use crate::store::runs::{RunRow, Timeout};

/// How often a process's memory is sampled.
const MEMORY_SAMPLE: Duration = Duration::from_secs(2);
/// How much of a failed command's standard error is kept.
const STDERR_TAIL: usize = 16 * 1024;

/// What may cut short a wait for the child's answer.
#[derive(Debug)]
pub(crate) enum Interrupted {
    Crashed { code: Option<i32>, signal: Option<i32> },
    TimedOut,
    Stopped(StopReason),
    Shutdown,
}

impl From<Interrupted> for CallOutcome {
    fn from(interrupted: Interrupted) -> CallOutcome {
        match interrupted {
            Interrupted::Crashed { code, signal } => CallOutcome::Crashed { code, signal },
            Interrupted::TimedOut => CallOutcome::TimedOut,
            Interrupted::Stopped(reason) => CallOutcome::Stopped(reason),
            Interrupted::Shutdown => CallOutcome::Interrupted,
        }
    }
}

/// Whether the worker's shutdown cuts a wait for the child short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtShutdown {
    /// Stop waiting: the run goes back to the queue.
    Stop,
    /// Keep waiting: the work is already done, and only its result is being checked.
    Finish,
}

/// Waits for the child message `pick` accepts, while the run's deadline, its lease and (as
/// `at_shutdown` says) the worker's shutdown can still stop it. A child that is cut short is
/// killed: it may be stuck in app code.
pub(crate) async fn wait_for<T>(
    context: &WorkerContext,
    child: &mut ChildHandle,
    lease: &mut Lease,
    deadline_at: Option<Instant>,
    at_shutdown: AtShutdown,
    mut pick: impl FnMut(FromChild) -> Option<T>,
) -> Result<T, Interrupted> {
    let interrupted = loop {
        tokio::select! {
            message = child.next() => match message {
                Some(message) => if let Some(found) = pick(message) { return Ok(found) },
                None => return Err(match crashed(child).await {
                    CallOutcome::Crashed { code, signal } => Interrupted::Crashed { code, signal },
                    _ => Interrupted::Crashed { code: None, signal: None },
                }),
            },
            () = deadline(deadline_at) => break Interrupted::TimedOut,
            _ = lease.stop.changed() => break Interrupted::Stopped(lease.stopped().unwrap_or(StopReason::Lost)),
            () = context.shutdown.stop_running.cancelled(), if at_shutdown == AtShutdown::Stop => {
                break Interrupted::Shutdown;
            }
        }
    };
    child.process.kill();
    child.spoiled = true;
    Err(interrupted)
}

/// How long a database call made for a running function may take, so the call loop keeps watching
/// its deadline and lease.
const ANSWER_WITHIN: std::time::Duration = std::time::Duration::from_secs(10);

/// How one call ended.
#[derive(Debug)]
pub(crate) enum CallOutcome {
    /// The function (or program) ended by itself.
    Ended(ChildEnding),
    /// The process died without the worker killing it.
    Crashed {
        code: Option<i32>,
        signal: Option<i32>,
    },
    TimedOut,
    OutOfMemory {
        limit_mib: u64,
    },
    /// The lease said stop.
    Stopped(StopReason),
    /// The worker is shutting down.
    Interrupted,
    /// A command's program exited with a failure status.
    ProgramFailed {
        code: Option<i32>,
        signal: Option<i32>,
        stderr_tail: String,
    },
}

impl CallOutcome {
    /// Whether the warm child cannot be trusted with another run.
    pub(crate) fn spoils_child(&self) -> bool {
        matches!(
            self,
            CallOutcome::Ended(ChildEnding::Panic(_))
                | CallOutcome::Crashed { .. }
                | CallOutcome::TimedOut
                | CallOutcome::OutOfMemory { .. }
                | CallOutcome::Stopped(_)
                | CallOutcome::Interrupted
        )
    }
}

/// What may stop a call, and when.
struct Limits {
    deadline: Option<Instant>,
    max_memory: Option<u64>,
}

impl Limits {
    fn of(run: &RunRow, job: &RegisteredJob) -> Limits {
        let timeout = match run.timeout_override() {
            Some(Timeout::Never) => None,
            Some(Timeout::After(timeout)) => Some(timeout),
            None => Some(job.spec.policy.timeout),
        };
        Limits {
            deadline: timeout.map(|timeout| Instant::now() + timeout),
            max_memory: job.spec.policy.max_memory.map(|bytes| bytes.0),
        }
    }

    fn over_memory(&self, process: &Process) -> Option<u64> {
        let limit = self.max_memory?;
        let used = process.resident_bytes()?;
        (used > limit).then_some(limit >> 20)
    }
}

async fn deadline(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Calls a job function in its child.
pub(crate) async fn function(
    context: &WorkerContext,
    child: &mut ChildHandle,
    run: &RunRow,
    job: &RegisteredJob,
    input: Value,
    lease: &mut Lease,
) -> CallOutcome {
    let limits = Limits::of(run, job);
    let go = ToChild::Go { run: run_info(run), input, checkpoint: run.checkpoint.clone() };
    if child.send(&go).await.is_err() {
        return crashed(child).await;
    }
    let mut memory = tokio::time::interval(MEMORY_SAMPLE);
    memory.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let stop = loop {
        tokio::select! {
            message = child.next() => match message {
                None => return crashed(child).await,
                Some(FromChild::Ended { run_id, ending }) if run_id == run.run_id => return CallOutcome::Ended(ending),
                Some(message) => answer(context, child, run, lease, message).await,
            },
            () = deadline(limits.deadline) => break CallOutcome::TimedOut,
            _ = lease.stop.changed() => break CallOutcome::Stopped(lease.stopped().unwrap_or(StopReason::Lost)),
            () = context.shutdown.stop_running.cancelled() => break CallOutcome::Interrupted,
            _ = memory.tick() => if let Some(limit_mib) = limits.over_memory(&child.process) {
                child.process.kill();
                return CallOutcome::OutOfMemory { limit_mib };
            },
        }
    };
    // Ask the function to stop, give it the grace period, then kill its process group.
    let _ = child.send(&ToChild::Cancel { run_id: run.run_id }).await;
    let grace = Instant::now() + context.registry.settings.kill_grace;
    loop {
        tokio::select! {
            message = child.next() => match message {
                Some(FromChild::Ended { run_id, ending }) if run_id == run.run_id => {
                    // Work that finished during a shutdown is kept; a timeout or a cancel stands.
                    return if matches!(stop, CallOutcome::Interrupted) { CallOutcome::Ended(ending) } else { stop };
                }
                Some(message) => answer(context, child, run, lease, message).await,
                None => return stop,
            },
            () = sleep_until(grace) => {
                child.process.kill();
                return stop;
            }
        }
    }
}

/// Handles what a running function sends besides its ending.
async fn answer(context: &WorkerContext, child: &mut ChildHandle, run: &RunRow, lease: &Lease, message: FromChild) {
    match message {
        FromChild::LeftTasks { run_id, count } if run_id == run.run_id => {
            tracing::warn!(%run_id, count, "the function left tasks running; its child process will be replaced");
            child.spoiled = true;
        }
        FromChild::Progress { run_id, text } if run_id == run.run_id => lease.progress(text),
        FromChild::Checkpoint { run_id, state, request } if run_id == run.run_id => match request {
            None => lease.checkpoint(state),
            Some(request) => {
                let result = match tokio::time::timeout(ANSWER_WITHIN, lease.checkpoint_now(state)).await {
                    Ok(Ok(true)) => Ok(None),
                    Ok(Ok(false)) => Err("this worker no longer holds the run".to_owned()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err("storing the checkpoint took too long".to_owned()),
                };
                let _ = child.send(&ToChild::Answer { run_id, request, result }).await;
            }
        },
        FromChild::Start { run_id, request, start } if run_id == run.run_id => {
            let result = match tokio::time::timeout(ANSWER_WITHIN, start_from_job(context, run, start)).await {
                Ok(result) => result.map(Some),
                Err(_) => Err("starting the job took too long".to_owned()),
            };
            let _ = child.send(&ToChild::Answer { run_id, request, result }).await;
        }
        _ => {}
    }
}

async fn crashed(child: &mut ChildHandle) -> CallOutcome {
    match child.process.wait().await {
        Ok(status) => {
            let (code, signal) = describe_exit(status);
            CallOutcome::Crashed { code, signal }
        }
        Err(_) => CallOutcome::Crashed { code: None, signal: None },
    }
}

/// Runs a command: its function (in the child) names the program; the worker starts it.
pub(crate) async fn command(
    context: &WorkerContext,
    child: &mut ChildHandle,
    run: &RunRow,
    job: &RegisteredJob,
    input: Value,
    lease: &mut Lease,
) -> CallOutcome {
    let limits = Limits::of(run, job);
    if child.send(&ToChild::Go { run: run_info(run), input: input.clone(), checkpoint: None }).await.is_err() {
        return crashed(child).await;
    }
    let wanted = run.run_id;
    let spec = match wait_for(context, child, lease, limits.deadline, AtShutdown::Stop, |message| match message {
        FromChild::Program { run_id, result } if run_id == wanted => Some(result),
        _ => None,
    })
    .await
    {
        Ok(spec) => spec,
        Err(interrupted) => return interrupted.into(),
    };
    let spec = match spec {
        Ok(spec) => spec,
        Err(error) => return CallOutcome::Ended(ChildEnding::BadInput(error)),
    };
    let output_file = std::env::temp_dir().join(format!("downbeat-{}-{}.json", run.run_id, run.starts));
    let outcome = run_program(context, run, &spec, &input, &output_file, &limits, lease).await;
    let outcome = match outcome {
        CallOutcome::Ended(ChildEnding::Output(_)) => {
            read_output(context, child, run, &output_file, &limits, lease).await
        }
        other => other,
    };
    let _ = std::fs::remove_file(&output_file);
    outcome
}

async fn run_program(
    context: &WorkerContext,
    run: &RunRow,
    spec: &ProgramSpec,
    input: &Value,
    output_file: &std::path::Path,
    limits: &Limits,
    lease: &mut Lease,
) -> CallOutcome {
    let mut command = Command::new(&spec.program);
    command.args(&spec.args).env_clear().stdin(Stdio::piped()).stdout(Stdio::inherit()).stderr(Stdio::piped());
    for name in &spec.pass_env {
        if let Ok(value) = std::env::var(name) {
            command.env(name, value);
        }
    }
    command
        .envs(&spec.env)
        .env(env_names::RUN_ID, run.run_id.to_string())
        .env(env_names::ATTEMPT, (run.failures + 1).to_string())
        .env(env_names::DRY_RUN, if run.dry_run { "1" } else { "0" })
        .env(env_names::OUTPUT_FILE, output_file);
    if let Some(dir) = &spec.dir {
        command.current_dir(dir);
    }
    let mut process = match context.spawner.spawn(Launch { command, socket: None }).await {
        Ok(process) => process,
        Err(error) => {
            return CallOutcome::ProgramFailed {
                code: None,
                signal: None,
                stderr_tail: format!("the program could not be started: {error}"),
            };
        }
    };
    if let Some(mut stdin) = process.child_pipes.stdin.take() {
        let input = input.to_string();
        std::thread::spawn(move || {
            let _ = stdin.write_all(input.as_bytes());
        });
    }
    let tail = process.child_pipes.stderr.take().map(|stderr| std::thread::spawn(move || keep_tail(stderr)));

    let mut memory = tokio::time::interval(MEMORY_SAMPLE);
    memory.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let stop = loop {
        tokio::select! {
            status = process.wait() => return program_ended(&process, tail, status).await,
            () = deadline(limits.deadline) => break CallOutcome::TimedOut,
            _ = lease.stop.changed() => break CallOutcome::Stopped(lease.stopped().unwrap_or(StopReason::Lost)),
            () = context.shutdown.stop_running.cancelled() => break CallOutcome::Interrupted,
            _ = memory.tick() => if let Some(limit_mib) = limits.over_memory(&process) {
                process.kill();
                return CallOutcome::OutOfMemory { limit_mib };
            },
        }
    };
    process.signal_group(libc::SIGTERM);
    match tokio::time::timeout(context.registry.settings.kill_grace, process.wait()).await {
        // Work that finished during a shutdown is kept, as for a function (spec 11.2). A program
        // that failed because it was asked to stop was interrupted, not failed.
        Ok(status) if matches!(stop, CallOutcome::Interrupted) => {
            let ended = program_ended(&process, tail, status).await;
            if matches!(ended, CallOutcome::Ended(_)) { ended } else { stop }
        }
        Ok(_) => stop,
        Err(_) => {
            process.kill();
            stop
        }
    }
}

/// How a command's program ended, from its exit status and the tail of its standard error.
async fn program_ended(
    process: &Process,
    tail: Option<std::thread::JoinHandle<String>>,
    status: std::io::Result<std::process::ExitStatus>,
) -> CallOutcome {
    // Anything the program left running in the background goes with it, and its standard error
    // closes, so the tail can be read without waiting forever.
    process.kill_group();
    let stderr_tail = match tail {
        Some(tail) => tokio::time::timeout(Duration::from_secs(2), tokio::task::spawn_blocking(move || tail.join()))
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(Result::ok)
            .unwrap_or_default(),
        None => String::new(),
    };
    match status {
        Ok(status) if status.success() => CallOutcome::Ended(ChildEnding::Output(Value::Null)),
        Ok(status) if status.code() == Some(EXIT_NOT_READY) => CallOutcome::Ended(ChildEnding::NotReady(
            last_line(&stderr_tail).unwrap_or("the program said it is not ready").to_owned(),
        )),
        Ok(status) => {
            let (code, signal) = describe_exit(status);
            CallOutcome::ProgramFailed { code, signal, stderr_tail }
        }
        Err(error) => CallOutcome::ProgramFailed { code: None, signal: None, stderr_tail: error.to_string() },
    }
}

/// Reads what a program wrote to its output file, and has the child check it against the command's
/// output type.
async fn read_output(
    context: &WorkerContext,
    child: &mut ChildHandle,
    run: &RunRow,
    output_file: &PathBuf,
    limits: &Limits,
    lease: &mut Lease,
) -> CallOutcome {
    // A program that writes nothing has no output; anything it writes must be JSON, so UTF-8.
    let size = std::fs::metadata(output_file).map_or(0, |metadata| metadata.len());
    if size > MAX_VALUE_BYTES as u64 {
        return CallOutcome::Ended(ChildEnding::OutputTooLarge(usize::try_from(size).unwrap_or(usize::MAX)));
    }
    let bytes = match std::fs::read(output_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return CallOutcome::Ended(ChildEnding::BadInput(format!(
                "the program's output could not be read: {error}"
            )));
        }
    };
    let written: Value = if bytes.trim_ascii().is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                return CallOutcome::Ended(ChildEnding::BadInput(format!("the program's output is not JSON: {error}")));
            }
        }
    };
    if let Some(reason) = written.get("skip").and_then(Value::as_str) {
        return CallOutcome::Ended(ChildEnding::Skip(reason.to_owned()));
    }
    let check = ToChild::CheckOutput { run_id: run.run_id, job: run.job_name.clone(), output: written };
    if child.send(&check).await.is_err() {
        return crashed(child).await;
    }
    let wanted = run.run_id;
    match wait_for(context, child, lease, limits.deadline, AtShutdown::Finish, |message| match message {
        FromChild::Checked { run_id, result } if run_id == wanted => Some(result),
        _ => None,
    })
    .await
    {
        Ok(Ok(output)) => CallOutcome::Ended(ChildEnding::Output(output)),
        Ok(Err(error)) => CallOutcome::Ended(ChildEnding::BadInput(format!(
            "the program's output does not fit the command's output type: {error}"
        ))),
        Err(interrupted) => interrupted.into(),
    }
}

fn keep_tail(mut stderr: std::process::ChildStderr) -> String {
    let mut tail: VecDeque<u8> = VecDeque::with_capacity(STDERR_TAIL);
    let mut buffer = [0_u8; 8192];
    while let Ok(read) = stderr.read(&mut buffer) {
        if read == 0 {
            break;
        }
        let _ = std::io::stderr().write_all(&buffer[..read]);
        tail.extend(&buffer[..read]);
        while tail.len() > STDERR_TAIL {
            tail.pop_front();
        }
    }
    String::from_utf8_lossy(&tail.into_iter().collect::<Vec<_>>()).into_owned()
}

fn last_line(text: &str) -> Option<&str> {
    text.lines().rev().map(str::trim).find(|line| !line.is_empty())
}
