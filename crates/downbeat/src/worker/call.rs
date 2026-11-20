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

/// How one call ended.
#[derive(Debug)]
pub(crate) enum CallOutcome {
    /// The function (or program) ended by itself.
    Ended(ChildEnding),
    /// The process died without the worker killing it.
    Crashed { code: Option<i32>, signal: Option<i32> },
    TimedOut,
    OutOfMemory { limit_mib: u64 },
    /// The lease said stop.
    Stopped(StopReason),
    /// The worker is shutting down.
    Interrupted,
    /// A command's program exited with a failure status.
    ProgramFailed { code: Option<i32>, signal: Option<i32>, stderr_tail: String },
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
        Limits { deadline: timeout.map(|timeout| Instant::now() + timeout), max_memory: job.spec.policy.max_memory.map(|bytes| bytes.0) }
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
pub(crate) async fn function(context: &WorkerContext, child: &mut ChildHandle, run: &RunRow, job: &RegisteredJob, input: Value, lease: &mut Lease) -> CallOutcome {
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
        FromChild::Progress { run_id, text } if run_id == run.run_id => lease.progress(text),
        FromChild::Checkpoint { run_id, state, request } if run_id == run.run_id => match request {
            None => lease.checkpoint(state),
            Some(request) => {
                let result = match lease.checkpoint_now(state).await {
                    Ok(true) => Ok(None),
                    Ok(false) => Err("this worker no longer holds the run".to_owned()),
                    Err(error) => Err(error.to_string()),
                };
                let _ = child.send(&ToChild::Answer { run_id, request, result }).await;
            }
        },
        FromChild::Start { run_id, request, start } if run_id == run.run_id => {
            let result = start_from_job(context, run, start).await.map(Some);
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
pub(crate) async fn command(context: &WorkerContext, child: &mut ChildHandle, run: &RunRow, job: &RegisteredJob, input: Value, lease: &mut Lease) -> CallOutcome {
    let limits = Limits::of(run, job);
    if child.send(&ToChild::Go { run: run_info(run), input: input.clone(), checkpoint: None }).await.is_err() {
        return crashed(child).await;
    }
    let spec = loop {
        match child.next().await {
            Some(FromChild::Program { run_id, result }) if run_id == run.run_id => break result,
            Some(_) => {}
            None => return crashed(child).await,
        }
    };
    let spec = match spec {
        Ok(spec) => spec,
        Err(error) => return CallOutcome::Ended(ChildEnding::BadInput(error)),
    };
    let output_file = std::env::temp_dir().join(format!("downbeat-{}-{}.json", run.run_id, run.starts));
    let outcome = run_program(context, run, &spec, &input, &output_file, &limits, lease).await;
    let outcome = match outcome {
        CallOutcome::Ended(ChildEnding::Output(_)) => read_output(child, run, &output_file).await,
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
            return CallOutcome::ProgramFailed { code: None, signal: None, stderr_tail: format!("the program could not be started: {error}") };
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
            status = process.wait() => {
                let stderr_tail = tail.and_then(|tail| tail.join().ok()).unwrap_or_default();
                return match status {
                    Ok(status) if status.success() => CallOutcome::Ended(ChildEnding::Output(Value::Null)),
                    Ok(status) if status.code() == Some(EXIT_NOT_READY) => {
                        CallOutcome::Ended(ChildEnding::NotReady(last_line(&stderr_tail).unwrap_or("the program said it is not ready").to_owned()))
                    }
                    Ok(status) => {
                        let (code, signal) = describe_exit(status);
                        CallOutcome::ProgramFailed { code, signal, stderr_tail }
                    }
                    Err(error) => CallOutcome::ProgramFailed { code: None, signal: None, stderr_tail: error.to_string() },
                };
            }
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
    if tokio::time::timeout(context.registry.settings.kill_grace, process.wait()).await.is_err() {
        process.kill();
    }
    stop
}

/// Reads what a program wrote to its output file, and has the child check it against the command's
/// output type.
async fn read_output(child: &mut ChildHandle, run: &RunRow, output_file: &PathBuf) -> CallOutcome {
    let text = std::fs::read_to_string(output_file).unwrap_or_default();
    if text.len() > MAX_VALUE_BYTES {
        return CallOutcome::Ended(ChildEnding::OutputTooLarge(text.len()));
    }
    let written: Value = if text.trim().is_empty() {
        Value::Null
    } else {
        match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(error) => return CallOutcome::Ended(ChildEnding::BadInput(format!("the program's output is not JSON: {error}"))),
        }
    };
    if let Some(reason) = written.get("skip").and_then(Value::as_str) {
        return CallOutcome::Ended(ChildEnding::Skip(reason.to_owned()));
    }
    let check = ToChild::CheckOutput { run_id: run.run_id, job: run.job_name.clone(), output: written };
    if child.send(&check).await.is_err() {
        return crashed(child).await;
    }
    loop {
        match child.next().await {
            Some(FromChild::Checked { run_id, result }) if run_id == run.run_id => {
                return CallOutcome::Ended(match result {
                    Ok(output) => ChildEnding::Output(output),
                    Err(error) => ChildEnding::BadInput(format!("the program's output does not fit the command's output type: {error}")),
                });
            }
            Some(_) => {}
            None => return crashed(child).await,
        }
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
