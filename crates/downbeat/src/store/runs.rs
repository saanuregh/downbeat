//! Creating, claiming and holding runs.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::types::PgInterval;
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use super::interval;

/// An unfinished run, as stored. Its flags mirror the table's columns.
#[derive(Debug, Clone, FromRow)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct RunRow {
    pub(crate) run_id: Uuid,
    pub(crate) job_name: String,
    pub(crate) queue: String,
    pub(crate) priority: i16,
    pub(crate) state: String,
    pub(crate) run_at: DateTime<Utc>,
    pub(crate) due_at: DateTime<Utc>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) started_by: String,
    pub(crate) schedule: Option<String>,
    pub(crate) chain_id: Uuid,
    pub(crate) parents: Vec<Uuid>,
    pub(crate) input: Option<Value>,
    pub(crate) input_hash: Option<String>,
    pub(crate) same_work_key: Option<String>,
    pub(crate) turn_key: Option<String>,
    pub(crate) dry_run: bool,
    pub(crate) ignore_conditions: bool,
    pub(crate) without_children: bool,
    pub(crate) timeout_override: Option<PgInterval>,
    pub(crate) starts: i32,
    pub(crate) failures: i16,
    pub(crate) lost_count: i16,
    pub(crate) interruptions: i16,
    pub(crate) fence: i64,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_expires_at: Option<DateTime<Utc>>,
    pub(crate) function_started_at: Option<DateTime<Utc>>,
    pub(crate) first_started_at: Option<DateTime<Utc>>,
    pub(crate) queued_because: Option<String>,
    pub(crate) queued_reason: Option<String>,
    pub(crate) progress: Option<String>,
    pub(crate) checkpoint: Option<Value>,
    pub(crate) cancel_requested_by: Option<String>,
    pub(crate) slow_reported: bool,
    pub(crate) call_log: Value,
    pub(crate) actor: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) claimed_at: Option<DateTime<Utc>>,
}

impl RunRow {
    /// Whether the function has been called at least once: the run has begun its work.
    pub(crate) fn has_begun(&self) -> bool {
        self.starts > 0
    }

    /// The timeout a person gave this run, if they gave one.
    pub(crate) fn timeout_override(&self) -> Option<Timeout> {
        self.timeout_override.as_ref().map(|interval| match u64::try_from(interval.microseconds) {
            Ok(micros) if micros > 0 => Timeout::After(Duration::from_micros(micros)),
            _ => Timeout::Never,
        })
    }
}

/// A timeout chosen for one run, in place of the job's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Timeout {
    After(Duration),
    Never,
}

impl Timeout {
    /// Stored as an interval; zero means no timeout.
    fn as_interval(self) -> PgInterval {
        interval(match self {
            Timeout::After(after) => after,
            Timeout::Never => Duration::ZERO,
        })
    }
}

/// Who or what asked for a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StartedBy {
    Schedule,
    Link,
    Person,
    Program,
    Backfill,
    /// Another job, through `run.start`.
    Job,
}

impl StartedBy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            StartedBy::Schedule => "schedule",
            StartedBy::Link => "link",
            StartedBy::Person => "person",
            StartedBy::Program => "program",
            StartedBy::Backfill => "backfill",
            StartedBy::Job => "job",
        }
    }
}

/// Where a new run belongs.
#[derive(Debug, Clone)]
pub(crate) enum Place {
    /// It begins a new chain.
    NewChain { pipeline: String, backfill_id: Option<i64> },
    /// It joins an existing chain, as a linked child or a retried step.
    Chain { chain_id: Uuid, parents: Vec<Uuid> },
}

/// A run to create.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct NewRun {
    pub(crate) job: String,
    pub(crate) queue: String,
    pub(crate) priority: i16,
    pub(crate) input: Option<Value>,
    pub(crate) input_hash: Option<String>,
    pub(crate) turn_key: Option<String>,
    /// Whether rule 1 applies: a request for work that is queued and has not begun joins it.
    pub(crate) joins_queued_work: bool,
    pub(crate) due_at: Option<DateTime<Utc>>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) not_before: Option<DateTime<Utc>>,
    pub(crate) started_by: StartedBy,
    pub(crate) schedule: Option<String>,
    pub(crate) place: Place,
    pub(crate) dry_run: bool,
    pub(crate) ignore_conditions: bool,
    pub(crate) without_children: bool,
    pub(crate) timeout_override: Option<Timeout>,
    pub(crate) actor: Option<String>,
    pub(crate) queued_reason: Option<String>,
}

/// What creating a run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Created {
    New { run_id: Uuid, chain_id: Uuid },
    Joined { run_id: Uuid, chain_id: Uuid },
}

impl Created {
    pub(crate) fn run_id(self) -> Uuid {
        match self {
            Created::New { run_id, .. } | Created::Joined { run_id, .. } => run_id,
        }
    }
}

/// Creates a run, applying rule 1 of "the same work is done once" in the same statement.
pub(crate) async fn create(connection: &mut PgConnection, run: &NewRun) -> sqlx::Result<Created> {
    let run_id = Uuid::now_v7();
    let (chain_id, parents, new_chain) = match &run.place {
        Place::NewChain { pipeline, backfill_id } => {
            let chain_id = Uuid::now_v7();
            sqlx::query(
                "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by, backfill_id) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(chain_id)
            .bind(pipeline)
            .bind(&run.job)
            .bind(run.started_by.as_str())
            .bind(backfill_id)
            .execute(&mut *connection)
            .await?;
            (chain_id, Vec::new(), true)
        }
        Place::Chain { chain_id, parents } => (*chain_id, parents.clone(), false),
    };
    let same_work_key = run.joins_queued_work.then(|| same_work_key(&run.job, run.input_hash.as_deref()));
    let queued_because = run.not_before.map(|_| "scheduled_later");
    let timeout = run.timeout_override.map(Timeout::as_interval);

    let (returned_id, created, returned_chain): (Uuid, bool, Uuid) = sqlx::query_as(
        "INSERT INTO downbeat.run AS r (
            run_id, job_name, queue, priority, run_at, due_at, expires_at, started_by, schedule, chain_id,
            parents, input, input_hash, same_work_key, turn_key, dry_run, ignore_conditions, without_children,
            timeout_override, actor, queued_because, queued_reason)
         VALUES ($1, $2, $3, $4, COALESCE($5, now()), COALESCE($6, now()), $7, $8, $9, $10,
                 $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
         ON CONFLICT (same_work_key) WHERE starts = 0 AND same_work_key IS NOT NULL
         DO UPDATE SET priority = GREATEST(r.priority, EXCLUDED.priority),
                       due_at = GREATEST(r.due_at, EXCLUDED.due_at),
                       expires_at = CASE WHEN r.expires_at IS NULL OR EXCLUDED.expires_at IS NULL THEN NULL
                                         ELSE GREATEST(r.expires_at, EXCLUDED.expires_at) END
         RETURNING r.run_id, (xmax = 0), r.chain_id",
    )
    .bind(run_id)
    .bind(&run.job)
    .bind(&run.queue)
    .bind(run.priority)
    .bind(run.not_before)
    .bind(run.due_at)
    .bind(run.expires_at)
    .bind(run.started_by.as_str())
    .bind(&run.schedule)
    .bind(chain_id)
    .bind(&parents)
    .bind(&run.input)
    .bind(&run.input_hash)
    .bind(&same_work_key)
    .bind(&run.turn_key)
    .bind(run.dry_run)
    .bind(run.ignore_conditions)
    .bind(run.without_children)
    .bind(timeout)
    .bind(&run.actor)
    .bind(queued_because)
    .bind(&run.queued_reason)
    .fetch_one(&mut *connection)
    .await?;

    if created {
        Ok(Created::New { run_id: returned_id, chain_id: returned_chain })
    } else {
        if new_chain {
            sqlx::query("DELETE FROM downbeat.chain WHERE chain_id = $1").bind(chain_id).execute(&mut *connection).await?;
        }
        Ok(Created::Joined { run_id: returned_id, chain_id: returned_chain })
    }
}

/// The key of "the same work": the job and the hash of its input.
pub(crate) fn same_work_key(job: &str, input_hash: Option<&str>) -> String {
    format!("{job}:{}", input_hash.unwrap_or(""))
}

/// Claims the most urgent due run in a queue that this worker can run, or nothing.
pub(crate) async fn claim(pool: &PgPool, queue: &str, worker: &str, lease: Duration, jobs: &[String]) -> sqlx::Result<Option<RunRow>> {
    sqlx::query_as(
        "UPDATE downbeat.run r
            SET state = 'running', fence = fence + 1, lease_owner = $2, lease_expires_at = now() + $3,
                claimed_at = now(), function_started_at = NULL, queued_because = NULL
          WHERE run_id = (
                SELECT c.run_id FROM downbeat.run c
                 WHERE c.state = 'queued' AND c.queue = $1 AND c.run_at <= now()
                   AND c.job_name = ANY($4)
                   AND NOT EXISTS (SELECT 1 FROM downbeat.pause p
                                    WHERE ((p.kind = 'job' AND p.name = c.job_name) OR (p.kind = 'queue' AND p.name = c.queue))
                                      AND (p.until IS NULL OR p.until > now()))
                   AND (c.turn_key IS NULL OR NOT EXISTS (
                          SELECT 1 FROM downbeat.run x
                           WHERE x.turn_key = c.turn_key AND x.starts > 0 AND x.run_id <> c.run_id))
                 ORDER BY c.priority DESC, c.run_at
                 LIMIT 1 FOR UPDATE SKIP LOCKED)
        RETURNING r.*",
    )
    .bind(queue)
    .bind(worker)
    .bind(interval(lease))
    .bind(jobs)
    .fetch_optional(pool)
    .await
}

/// One call of a run's function, kept in the run's call log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Call {
    pub(crate) worker: String,
    pub(crate) build: String,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) ended_at: DateTime<Utc>,
    /// How it ended, in a few words: `succeeded`, `error`, `not ready`, `interrupted` and so on.
    pub(crate) ended: String,
}

/// The ownership a worker's writes must still hold.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Held<'a> {
    pub(crate) run_id: Uuid,
    pub(crate) fence: i64,
    pub(crate) worker: &'a str,
}

/// Stores what preparing a run produced: its input, input hash and turn.
pub(crate) async fn prepared(pool: &PgPool, held: Held<'_>, input: &Value, input_hash: &str, turn_key: &str) -> sqlx::Result<bool> {
    let done = sqlx::query(
        "UPDATE downbeat.run SET input = $4, input_hash = $5, turn_key = $6
          WHERE run_id = $1 AND fence = $2 AND lease_owner = $3 AND state = 'running'",
    )
    .bind(held.run_id)
    .bind(held.fence)
    .bind(held.worker)
    .bind(input)
    .bind(input_hash)
    .bind(turn_key)
    .execute(pool)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// What recording a function start found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FunctionStart {
    Started,
    /// Another run holds this run's turn; it goes back to the queue.
    TurnTaken,
    /// The worker no longer holds the run.
    Lost,
}

/// Records that the function is being called. On a run's first call this takes its turn.
pub(crate) async fn function_started(pool: &PgPool, held: Held<'_>, lease: Duration) -> sqlx::Result<FunctionStart> {
    let result = sqlx::query(
        "UPDATE downbeat.run
            SET function_started_at = now(), starts = starts + 1, lease_expires_at = now() + $4,
                first_started_at = COALESCE(first_started_at, now())
          WHERE run_id = $1 AND fence = $2 AND lease_owner = $3 AND state = 'running'",
    )
    .bind(held.run_id)
    .bind(held.fence)
    .bind(held.worker)
    .bind(interval(lease))
    .execute(pool)
    .await;
    match result {
        Ok(done) if done.rows_affected() == 1 => Ok(FunctionStart::Started),
        Ok(_) => Ok(FunctionStart::Lost),
        Err(sqlx::Error::Database(error)) if error.constraint() == Some("run_turn") => Ok(FunctionStart::TurnTaken),
        Err(error) => Err(error),
    }
}

/// Extends the lease, saves progress and checkpoint, and reads who asked for a cancel, if anyone.
/// `None` means the worker no longer holds the run.
pub(crate) async fn heartbeat(
    pool: &PgPool,
    held: Held<'_>,
    lease: Duration,
    progress: Option<&str>,
    checkpoint: Option<&Value>,
) -> sqlx::Result<Option<Option<String>>> {
    sqlx::query_scalar(
        "UPDATE downbeat.run
            SET lease_expires_at = now() + $4, progress = COALESCE($5, progress), checkpoint = COALESCE($6, checkpoint)
          WHERE run_id = $1 AND fence = $2 AND lease_owner = $3 AND state = 'running'
        RETURNING cancel_requested_by",
    )
    .bind(held.run_id)
    .bind(held.fence)
    .bind(held.worker)
    .bind(interval(lease))
    .bind(progress)
    .bind(checkpoint)
    .fetch_optional(pool)
    .await
}

/// Why a run goes back to the queue.
#[derive(Debug, Clone)]
pub(crate) struct Requeue {
    pub(crate) delay: Duration,
    pub(crate) because: &'static str,
    pub(crate) reason: String,
    /// A failed call: counts toward the job's attempts.
    pub(crate) failed: bool,
    /// Released on shutdown.
    pub(crate) interrupted: bool,
    pub(crate) call: Option<Call>,
}

/// Sends a run back to the queue. False when the worker no longer holds it.
pub(crate) async fn requeue(pool: &PgPool, held: Held<'_>, requeue: &Requeue) -> sqlx::Result<bool> {
    let call = requeue.call.as_ref().map(|call| serde_json::to_value(call).unwrap_or(Value::Null));
    let done = sqlx::query(
        "UPDATE downbeat.run
            SET state = 'queued', run_at = now() + $4, lease_owner = NULL, lease_expires_at = NULL,
                function_started_at = NULL, queued_because = $5, queued_reason = $6,
                failures = failures + CASE WHEN $7 THEN 1 ELSE 0 END,
                interruptions = interruptions + CASE WHEN $8 THEN 1 ELSE 0 END,
                call_log = CASE WHEN $9::jsonb IS NULL THEN call_log ELSE downbeat.append_call(call_log, $9::jsonb) END
          WHERE run_id = $1 AND fence = $2 AND lease_owner = $3 AND state = 'running'",
    )
    .bind(held.run_id)
    .bind(held.fence)
    .bind(held.worker)
    .bind(interval(requeue.delay))
    .bind(requeue.because)
    .bind(&requeue.reason)
    .bind(requeue.failed)
    .bind(requeue.interrupted)
    .bind(call)
    .execute(pool)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// Marks a run as reported slow, once.
pub(crate) async fn mark_slow(pool: &PgPool, run_id: Uuid) -> sqlx::Result<bool> {
    let done = sqlx::query("UPDATE downbeat.run SET slow_reported = true WHERE run_id = $1 AND NOT slow_reported")
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(done.rows_affected() == 1)
}

/// Asks the worker holding a run to stop it; seen at its next heartbeat.
pub(crate) async fn request_cancel(connection: &mut PgConnection, run_id: Uuid, actor: &str) -> sqlx::Result<bool> {
    let done = sqlx::query("UPDATE downbeat.run SET cancel_requested_by = $2 WHERE run_id = $1")
        .bind(run_id)
        .bind(actor)
        .execute(connection)
        .await?;
    Ok(done.rows_affected() == 1)
}

/// Makes a waiting run due now ("check now").
pub(crate) async fn check_now(pool: &PgPool, run_id: Uuid) -> sqlx::Result<bool> {
    let done = sqlx::query(
        "UPDATE downbeat.run SET run_at = now()
          WHERE run_id = $1 AND state = 'queued' AND queued_because IN ('not_ready', 'need_not_met', 'backing_off')",
    )
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// One unfinished run, by id.
pub(crate) async fn get(connection: &mut PgConnection, run_id: Uuid) -> sqlx::Result<Option<RunRow>> {
    sqlx::query_as("SELECT * FROM downbeat.run WHERE run_id = $1").bind(run_id).fetch_optional(connection).await
}
