//! The worker: a supervisor that claims runs, keeps their leases, enforces their limits and records
//! how they end. It runs no job code: each slot owns a child process where functions run.

mod call;
mod child;
mod lease;
mod prepare;
pub(crate) mod process;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::events::Event;
use crate::job::{RetryOn, Started};
use crate::protocol::{ChildEnding, ChildStart};
use crate::reasons;
use crate::registry::{RegisteredJob, Registry};
use crate::starting::{self, StartJob, StartOptions};
use crate::store::audit;
use crate::store::ending::{self, Ending, Status};
use crate::store::runs::{self, Call, FunctionStart, Held, Requeue, RunRow, StartedBy};
use call::CallOutcome;
use child::ChildHandle;
use lease::{Lease, StopReason};
use process::Spawner;

/// The two phases of stopping: first no new claims, then stop what is running.
#[derive(Debug, Clone, Default)]
pub(crate) struct Shutdown {
    pub(crate) stop_claiming: CancellationToken,
    pub(crate) stop_running: CancellationToken,
}

/// What every slot of a worker shares.
pub(crate) struct WorkerContext {
    pub(crate) pool: PgPool,
    pub(crate) registry: Arc<Registry>,
    pub(crate) id: String,
    pub(crate) build: String,
    pub(crate) spawner: Spawner,
    pub(crate) shutdown: Shutdown,
    jobs: Vec<String>,
}

/// What to record about a run once its turn with this worker is over.
#[derive(Debug)]
pub(crate) enum Decision {
    End(Ending),
    Requeue(Requeue),
    /// The worker no longer holds the run, or cannot write: record nothing.
    Nothing,
}

impl Decision {
    fn child_failed(reason: &str) -> Decision {
        Decision::Requeue(Requeue {
            delay: Duration::from_secs(10),
            because: "no_free_worker",
            reason: format!("Re-queued: {reason}."),
            failed: false,
            interrupted: false,
            call: None,
        })
    }

    #[allow(clippy::needless_pass_by_value)] // Used as `map_err(Decision::database)`.
    fn database(error: sqlx::Error) -> Decision {
        tracing::error!(%error, "a database error while handling a run; its lease will lapse and it will be retried");
        Decision::Nothing
    }
}

/// Runs the worker role until shutdown: one task per slot, plus the worker's own heartbeat.
#[allow(clippy::needless_pass_by_value)] // Owned, because the role runs as a spawned task.
pub(crate) async fn serve(
    pool: PgPool,
    registry: Arc<Registry>,
    queues: Vec<(String, u32)>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let context = Arc::new(WorkerContext {
        pool: pool.clone(),
        jobs: registry.job_names(),
        id: worker_id(),
        build: build_id(),
        spawner: Spawner::new(),
        shutdown: shutdown.clone(),
        registry,
    });
    register(&context, &queues).await?;
    tracing::info!(worker = %context.id, ?queues, "worker started");

    let refresher = tokio::spawn(refresh(Arc::clone(&context)));
    let mut slots = tokio::task::JoinSet::new();
    for (queue, count) in &queues {
        for _ in 0..*count {
            slots.spawn(serve_slot(Arc::clone(&context), queue.clone()));
        }
    }

    shutdown.stop_claiming.cancelled().await;
    let settings = &context.registry.settings;
    tracing::info!(worker = %context.id, "stopping: no new claims; waiting for running functions");
    let drained = tokio::time::timeout(settings.drain, async { while slots.join_next().await.is_some() {} }).await;
    if drained.is_err() {
        shutdown.stop_running.cancel();
        let _ = tokio::time::timeout(settings.kill_grace + Registry::release_time(), async {
            while slots.join_next().await.is_some() {}
        })
        .await;
    }
    refresher.abort();
    sqlx::query("DELETE FROM downbeat.worker WHERE worker_id = $1").bind(&context.id).execute(&pool).await?;
    tracing::info!(worker = %context.id, "worker stopped");
    Ok(())
}

async fn register(context: &WorkerContext, queues: &[(String, u32)]) -> sqlx::Result<()> {
    let queues: serde_json::Map<String, serde_json::Value> =
        queues.iter().map(|(name, slots)| (name.clone(), (*slots).into())).collect();
    sqlx::query("INSERT INTO downbeat.worker (worker_id, role, queues, jobs, build) VALUES ($1, 'worker', $2, $3, $4)")
        .bind(&context.id)
        .bind(serde_json::Value::Object(queues))
        .bind(&context.jobs)
        .bind(&context.build)
        .execute(&context.pool)
        .await?;
    Ok(())
}

async fn refresh(context: Arc<WorkerContext>) {
    let mut tick = tokio::time::interval(context.registry.settings.heartbeat);
    loop {
        tick.tick().await;
        if let Err(error) = sqlx::query("UPDATE downbeat.worker SET last_seen_at = now() WHERE worker_id = $1")
            .bind(&context.id)
            .execute(&context.pool)
            .await
        {
            tracing::warn!(%error, "the worker's own heartbeat failed");
        }
    }
}

/// One slot: claim a run from its queue, execute it, repeat.
async fn serve_slot(context: Arc<WorkerContext>, queue: String) {
    let settings = &context.registry.settings;
    let mut warm: Option<ChildHandle> =
        ChildHandle::start(&context.spawner, context.registry.environment.as_deref()).await.ok();
    while !context.shutdown.stop_claiming.is_cancelled() {
        match runs::claim(&context.pool, &queue, &context.id, settings.timings(&queue).lease, &context.jobs).await {
            Ok(Some(run)) => execute(&context, &mut warm, run).await,
            Ok(None) => {
                tokio::select! {
                    () = tokio::time::sleep(settings.poll) => {}
                    () = context.shutdown.stop_claiming.cancelled() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, queue, "claiming failed");
                tokio::time::sleep(settings.poll).await;
            }
        }
    }
}

/// Executes one claimed run and records how its turn ended.
async fn execute(context: &WorkerContext, warm: &mut Option<ChildHandle>, run: RunRow) {
    let held = Held { run_id: run.run_id, fence: run.fence, worker: &context.id };
    let settings = &context.registry.settings;
    let timings = settings.timings(&run.queue);
    let mut lease = Lease::keep(&context.pool, held, timings.heartbeat, timings.lease);
    let decision = match context.registry.job(&run.job_name) {
        Some(job) => drive(context, warm, &run, job, &mut lease).await,
        None => Decision::child_failed("this worker's build does not have the job"),
    };
    lease.release();
    record(context, &run, decision).await;
}

/// Prepares the run if it has not begun, takes its turn, calls the function and decides.
async fn drive(
    context: &WorkerContext,
    warm: &mut Option<ChildHandle>,
    run: &RunRow,
    job: &RegisteredJob,
    lease: &mut Lease,
) -> Decision {
    let environment = context.registry.environment.as_deref();
    let fresh = job.spec.policy.fresh_process && !job.spec.is_command();
    let mut own_child = None;
    let child = if fresh {
        match ChildHandle::start(&context.spawner, environment).await {
            Ok(child) => own_child.insert(child),
            Err(error) => {
                return Decision::child_failed(&format!("the worker could not start a child process ({error})"));
            }
        }
    } else {
        if warm.as_ref().is_none_or(|child| child.process.has_exited()) {
            *warm = None;
            match ChildHandle::start(&context.spawner, environment).await {
                Ok(child) => *warm = Some(child),
                Err(error) => {
                    return Decision::child_failed(&format!("the worker could not start a child process ({error})"));
                }
            }
        }
        warm.as_mut().expect("a warm child was just ensured")
    };

    let input = if run.has_begun() {
        run.input.clone().unwrap_or(serde_json::Value::Null)
    } else {
        match prepare::prepare(context, child, run, job, lease).await {
            Ok(input) => input,
            Err(decision) => return decision,
        }
    };

    let held = Held { run_id: run.run_id, fence: run.fence, worker: &context.id };
    match runs::function_started(&context.pool, held, context.registry.settings.timings(&run.queue).lease).await {
        Ok(FunctionStart::Started) => {}
        Ok(FunctionStart::TurnTaken) => return waiting_turn(context, run, job).await,
        Ok(FunctionStart::Lost) => return Decision::Nothing,
        Err(error) => return Decision::database(error),
    }

    let started_at = Utc::now();
    let outcome = if job.spec.is_command() {
        call::command(context, child, run, job, input, lease).await
    } else {
        call::function(context, child, run, job, input, lease).await
    };
    child.runs += 1;
    let spoiled =
        outcome.spoils_child() || child.spoiled || child.runs >= context.registry.settings.recycle_child_after;
    if spoiled && !fresh {
        *warm = None;
    }
    decide(context, run, job, outcome, started_at)
}

async fn waiting_turn(context: &WorkerContext, run: &RunRow, job: &RegisteredJob) -> Decision {
    let holder: Option<uuid::Uuid> = sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE turn_key = (SELECT turn_key FROM downbeat.run WHERE run_id = $1) AND starts > 0")
        .bind(run.run_id)
        .fetch_optional(&context.pool)
        .await
        .ok()
        .flatten();
    let whole_job = !matches!(job.spec.policy.turn, crate::job::Turn::SameWork);
    Decision::Requeue(Requeue {
        delay: Duration::ZERO,
        because: "waiting_turn",
        reason: reasons::waiting_turn(holder, whole_job),
        failed: false,
        interrupted: false,
        call: None,
    })
}

/// Turns how a call ended into what to record.
fn decide(
    context: &WorkerContext,
    run: &RunRow,
    job: &RegisteredJob,
    outcome: CallOutcome,
    started_at: chrono::DateTime<Utc>,
) -> Decision {
    let verdict = Verdict::new(context, job, run, started_at);
    let policy = &job.spec.policy;
    match outcome {
        CallOutcome::Ended(ChildEnding::Output(output)) => {
            let mut ending =
                Ending::new(Status::Succeeded, reasons::succeeded()).with_call(Some(verdict.call("succeeded")));
            ending.output = Some(output);
            Decision::End(ending)
        }
        CallOutcome::Ended(ChildEnding::Skip(reason)) => Decision::End(
            Ending::skipped("job_skipped", reasons::job_skipped(&reason)).with_call(Some(verdict.call("skipped"))),
        ),
        CallOutcome::Ended(ChildEnding::NotReady(reason)) => {
            let next = Utc::now() + chrono::TimeDelta::from_std(policy.recheck_every).unwrap_or_default();
            Decision::Requeue(Requeue {
                delay: policy.recheck_every,
                because: "not_ready",
                reason: reasons::not_ready(&reason, next, context.registry.zone),
                failed: false,
                interrupted: false,
                call: Some(verdict.call("not ready")),
            })
        }
        CallOutcome::Ended(ChildEnding::Error(error)) => {
            let reason = reasons::failed(&error, verdict.attempt, policy.attempts);
            verdict.fail_or_retry(RetryOn::Error, "error", reason, error)
        }
        CallOutcome::Ended(ChildEnding::Panic(message)) => {
            verdict.fail("panic", reasons::panicked(&message), Some(message))
        }
        CallOutcome::Ended(ChildEnding::OutputTooLarge(bytes)) => {
            verdict.fail("output_too_large", reasons::output_too_large(bytes), None)
        }
        CallOutcome::Ended(ChildEnding::BadInput(error)) => {
            verdict.fail("bad_input", reasons::bad_input(&error), Some(error))
        }
        CallOutcome::Crashed { code, signal } => {
            let reason = reasons::crashed(code, signal, verdict.ran_for());
            verdict.fail_or_retry(RetryOn::Crash, "crash", reason.clone(), reason)
        }
        CallOutcome::TimedOut => {
            let timeout = match run.timeout_override() {
                Some(runs::Timeout::After(timeout)) => timeout,
                _ => policy.timeout,
            };
            let reason = reasons::timed_out(timeout, verdict.attempt, policy.attempts);
            verdict.fail_or_retry(
                RetryOn::Timeout,
                "timeout",
                reason,
                format!("timed out after {}", crate::units::format_duration(timeout)),
            )
        }
        CallOutcome::OutOfMemory { limit_mib } => {
            let reason = reasons::out_of_memory(limit_mib);
            verdict.fail_or_retry(RetryOn::Memory, "out_of_memory", reason.clone(), reason)
        }
        CallOutcome::ProgramFailed { code, signal, stderr_tail } => verdict.program_failed(code, signal, &stderr_tail),
        CallOutcome::Stopped(StopReason::CancelRequested(by)) => Decision::End(
            Ending::new(Status::Cancelled, reasons::cancelled(&by)).with_call(Some(verdict.call("cancelled"))),
        ),
        CallOutcome::Stopped(StopReason::Lost) => Decision::Nothing,
        CallOutcome::Stopped(StopReason::Unreachable) => unreachable(context, run),
        CallOutcome::Interrupted => Decision::Requeue(Requeue {
            delay: Duration::ZERO,
            because: "no_free_worker",
            reason: reasons::interrupted(verdict.ran_for(), run.checkpoint.is_some()),
            failed: false,
            interrupted: true,
            call: Some(verdict.call("interrupted")),
        }),
    }
}

/// What deciding a call's ending needs to know.
pub(super) struct Verdict<'a> {
    context: &'a WorkerContext,
    job: &'a RegisteredJob,
    /// This call's attempt number, counting failed calls.
    attempt: u32,
    started_at: chrono::DateTime<Utc>,
}

impl<'a> Verdict<'a> {
    /// For a call of this run that started at `started_at`.
    pub(super) fn new(
        context: &'a WorkerContext,
        job: &'a RegisteredJob,
        run: &RunRow,
        started_at: chrono::DateTime<Utc>,
    ) -> Verdict<'a> {
        Verdict { context, job, attempt: u32::try_from(run.failures).unwrap_or(0) + 1, started_at }
    }

    fn call(&self, ended: &str) -> Call {
        Call {
            worker: self.context.id.clone(),
            build: self.context.build.clone(),
            started_at: self.started_at,
            ended_at: Utc::now(),
            ended: ended.to_owned(),
        }
    }

    fn ran_for(&self) -> Duration {
        (Utc::now() - self.started_at).to_std().unwrap_or_default()
    }

    /// A failed call that never gets another attempt.
    fn fail(&self, because: &'static str, reason: String, detail: Option<String>) -> Decision {
        let mut ending = Ending::failed(because, reason).with_call(Some(self.call(because)));
        ending.failed_call = true;
        ending.error_chain = detail.map(|detail| vec![detail]);
        Decision::End(ending)
    }

    /// A failed call: another attempt if the job's policy allows one, otherwise the end.
    pub(super) fn fail_or_retry(
        &self,
        kind: RetryOn,
        because: &'static str,
        final_reason: String,
        detail: String,
    ) -> Decision {
        let policy = &self.job.spec.policy;
        if !policy.retries(kind, u16::try_from(self.attempt).unwrap_or(u16::MAX)) {
            return self.fail(because, final_reason, Some(detail));
        }
        let delay = policy.backoff_before(self.attempt + 1);
        let next = Utc::now() + chrono::TimeDelta::from_std(delay).unwrap_or_default();
        Decision::Requeue(Requeue {
            delay,
            because: "backing_off",
            reason: reasons::another_attempt(self.attempt, policy.attempts, &detail, next, self.context.registry.zone),
            failed: true,
            interrupted: false,
            call: Some(self.call(because)),
        })
    }

    fn program_failed(&self, code: Option<i32>, signal: Option<i32>, stderr_tail: &str) -> Decision {
        let because = if signal.is_some() { "signal" } else { "exit_code" };
        let reason = reasons::command_failed(code, signal);
        match self.fail_or_retry(RetryOn::Error, because, reason.clone(), reason) {
            Decision::End(mut ending) => {
                let redact = self.context.registry.redact.as_ref();
                ending.stderr_tail = Some(redact.map_or_else(|| stderr_tail.to_owned(), |redact| redact(stderr_tail)));
                Decision::End(ending)
            }
            other => other,
        }
    }
}

/// Writes the decision, fenced. A failed write is tried again a few times; after that the lease
/// lapses and the reaper takes the run back.
async fn record(context: &WorkerContext, run: &RunRow, mut decision: Decision) {
    let held = Held { run_id: run.run_id, fence: run.fence, worker: &context.id };
    let mut requeued = false;
    for attempt in 1..=3 {
        let result = match &decision {
            Decision::Nothing => return,
            Decision::End(ending) => {
                ending::end(&context.pool, &context.registry, run.run_id, run.chain_id, run.fence, ending.clone())
                    .await
                    .map(|_| None)
            }
            Decision::Requeue(requeue) => {
                runs::requeue(&context.pool, held, requeue).await.map(|answer| match answer {
                    // Someone asked for a cancel while the function ran: the run ends instead.
                    runs::Requeued::CancelRequested(by) => {
                        Some(Ending::new(Status::Cancelled, reasons::cancelled(&by)).with_call(requeue.call.clone()))
                    }
                    runs::Requeued::Queued => {
                        requeued = true;
                        None
                    }
                    runs::Requeued::Lost => None,
                })
            }
        };
        match result {
            Ok(Some(cancelled)) => decision = Decision::End(cancelled),
            Ok(None) => break,
            Err(error) if attempt < 3 => {
                tracing::warn!(%error, run_id = %run.run_id, "recording a run's ending failed; trying again");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => {
                tracing::error!(%error, run_id = %run.run_id, "recording a run's ending failed; its lease will lapse");
            }
        }
    }
    if requeued
        && let Decision::Requeue(Requeue { interrupted: true, .. }) = decision
        && run.interruptions + 1 >= 3
        && let Ok(mut connection) = context.pool.acquire().await
    {
        let event = Event::RunInterrupted {
            run_id: run.run_id,
            job: run.job_name.clone(),
            times: i32::from(run.interruptions) + 1,
        };
        let _ = audit::event(&mut connection, &event, Some(&run.job_name), Some(run.run_id), None).await;
    }
}

/// The run was stopped because no heartbeat succeeded for a whole lease. Nothing can be written
/// now; the reaper takes the run back, and `on_event` hears of it once the database is back.
fn unreachable(context: &WorkerContext, run: &RunRow) -> Decision {
    tracing::error!(run_id = %run.run_id, "no heartbeat succeeded for a whole lease; the run was stopped");
    tokio::spawn(report_unreachable(context.pool.clone(), context.id.clone()));
    Decision::Nothing
}

/// Tells `on_event` that this worker lost its database for a whole lease, once it can reach the
/// database again (trying for ten minutes).
async fn report_unreachable(pool: PgPool, worker: String) {
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(10)).await;
        if let Ok(mut connection) = pool.acquire().await
            && audit::event(&mut connection, &Event::DatabaseUnreachable { worker: worker.clone() }, None, None, None)
                .await
                .is_ok()
        {
            return;
        }
    }
}

/// `run.start(…)` from a function: a new chain, keyed on the calling run and the call's position.
async fn start_from_job(context: &WorkerContext, run: &RunRow, start: ChildStart) -> Result<Started, String> {
    let request = StartJob {
        job: start.job,
        input: start.input,
        input_hash: start.input_hash,
        turn_value: start.turn_value,
        options: StartOptions {
            priority: start.priority,
            even_if_queued: start.even_if_queued,
            ..StartOptions::default()
        },
        started_by: StartedBy::Job,
        actor: Some(format!("run {}", reasons::short(run.run_id))),
        idempotency: Some((format!("run:{}", run.run_id), start.position.to_string())),
    };
    starting::start(&context.pool, &context.registry, request)
        .await
        .map(|result| result.started)
        .map_err(|error| error.to_string())
}

pub(crate) fn worker_id() -> String {
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_or_else(|_| "worker".to_owned(), |name| name.trim().to_owned());
    let suffix: u16 = rand_suffix();
    format!("{host}-{}-{suffix:04x}", std::process::id())
}

fn rand_suffix() -> u16 {
    let nanos =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |since| since.subsec_nanos());
    u16::try_from(nanos % 65_536).unwrap_or(0)
}

/// What build this process is: `DOWNBEAT_BUILD` when set, otherwise the binary's name and age.
pub(crate) fn build_id() -> String {
    if let Ok(build) = std::env::var("DOWNBEAT_BUILD") {
        return build;
    }
    let exe = std::env::current_exe().ok();
    let name = exe
        .as_ref()
        .and_then(|path| path.file_name())
        .map_or_else(|| "unknown".to_owned(), |name| name.to_string_lossy().into_owned());
    let modified = exe
        .and_then(|path| std::fs::metadata(path).ok())
        .and_then(|meta| meta.modified().ok())
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |age| age.as_secs());
    format!("{name}@{modified}")
}
