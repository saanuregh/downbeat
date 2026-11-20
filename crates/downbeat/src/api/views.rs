//! Turning stored rows into what the API returns, with a reason for every state.

use chrono::{DateTime, Utc};
use downbeat_api::types::{CallView, PauseView, RunDetail, RunSummary, RunView};
use sqlx::PgPool;
use uuid::Uuid;

use crate::reasons;
use crate::registry::Registry;
use crate::store::ending::HistoryRow;
use crate::store::runs::RunRow;
use crate::units::format_duration;

/// A finished run.
pub(crate) fn finished(registry: &Registry, row: HistoryRow, detail: Option<RunDetail>) -> RunView {
    let attempts = registry.job(&row.job_name).map_or(1, |job| i32::from(job.spec.policy.attempts));
    // Failures count failed calls; a run that ended any other way was on the attempt after them.
    let attempt = i32::from(row.failures) + i32::from(row.status != "failed");
    RunView {
        pipeline: registry.job(&row.job_name).map(|job| job.pipeline.clone()),
        log_link: registry.log_link_for(row.run_id),
        run_id: row.run_id,
        job: row.job_name,
        chain_id: row.chain_id,
        status: row.status,
        queued_because: None,
        skipped_because: row.skipped_because,
        failed_because: row.failed_because,
        reason: row.reason,
        started_by: row.started_by,
        actor: row.actor,
        schedule: row.schedule,
        queue: row.queue,
        priority: row.priority,
        due_at: row.due_at,
        created_at: row.created_at,
        started_at: row.started_at,
        finished_at: Some(row.finished_at),
        attempt,
        attempts,
        calls: row.starts,
        lost_count: i32::from(row.lost_count),
        interruptions: i32::from(row.interruptions),
        dry_run: row.dry_run,
        progress: None,
        has_checkpoint: false,
        slow: false,
        expected_seconds: None,
        input: row.input,
        output: row.output,
        marked_by: row.marked_by,
        note: row.note,
        detail,
    }
}

/// A queued or running run, with the reason worked out now for states that are not stored.
pub(crate) async fn unfinished(
    pool: &PgPool,
    registry: &Registry,
    row: RunRow,
    detail: Option<RunDetail>,
) -> sqlx::Result<RunView> {
    let (status, queued_because, reason) = if row.state == "running" {
        let reason = match (&row.function_started_at, &row.lease_owner) {
            (Some(started), Some(worker)) => format!("Running on {worker} for {}.", since(*started)),
            (None, Some(worker)) => format!("Claimed by {worker}; preparing."),
            _ => "Running.".to_owned(),
        };
        ("running".to_owned(), None, reason)
    } else {
        let (because, reason) = queued_reason(pool, &row).await?;
        ("queued".to_owned(), Some(because), reason)
    };
    let job = registry.job(&row.job_name);
    let expected = crate::store::history::expected_duration(pool, &row.job_name).await?;
    Ok(RunView {
        pipeline: job.map(|job| job.pipeline.clone()),
        log_link: registry.log_link_for(row.run_id),
        attempts: job.map_or(1, |job| i32::from(job.spec.policy.attempts)),
        run_id: row.run_id,
        job: row.job_name,
        chain_id: row.chain_id,
        status,
        queued_because,
        skipped_because: None,
        failed_because: None,
        reason,
        started_by: row.started_by,
        actor: row.actor,
        schedule: row.schedule,
        queue: row.queue,
        priority: row.priority,
        due_at: row.due_at,
        created_at: row.created_at,
        started_at: row.first_started_at,
        finished_at: None,
        attempt: i32::from(row.failures) + 1,
        calls: row.starts,
        lost_count: i32::from(row.lost_count),
        interruptions: i32::from(row.interruptions),
        dry_run: row.dry_run,
        progress: row.progress,
        has_checkpoint: row.checkpoint.is_some(),
        slow: row.slow_reported,
        expected_seconds: expected.map(|(median, _)| i64::try_from(median.as_secs()).unwrap_or(i64::MAX)),
        input: row.input,
        output: None,
        marked_by: None,
        note: None,
        detail,
    })
}

/// Why a queued run is not running. `paused`, `waiting_turn` and `no_free_worker` are worked out
/// now; the others were stored by whoever queued it.
async fn queued_reason(pool: &PgPool, row: &RunRow) -> sqlx::Result<(String, String)> {
    let pause = match pause_of(pool, "all", "all").await? {
        Some(everything) => Some(everything),
        None => pause_of(pool, "job", &row.job_name).await?.or(pause_of(pool, "queue", &row.queue).await?),
    };
    if let Some(pause) = pause {
        let until = pause.until.map_or_else(
            || "until resumed".to_owned(),
            |until| format!("until {}", until.format("%Y-%m-%d %H:%M UTC")),
        );
        return Ok(("paused".to_owned(), format!("Held: paused by {} {until} ({}).", pause.by, pause.note)));
    }
    if let Some(turn) = &row.turn_key {
        let holder: Option<Uuid> =
            sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE turn_key = $1 AND starts > 0 AND run_id <> $2")
                .bind(turn)
                .bind(row.run_id)
                .fetch_optional(pool)
                .await?;
        if holder.is_some() {
            return Ok(("waiting_turn".to_owned(), reasons::waiting_turn(holder, turn.ends_with(":*"))));
        }
    }
    let stored =
        row.queued_because.as_deref().filter(|because| *because != "no_free_worker" && *because != "waiting_turn");
    if let (Some(because), true) = (stored, row.run_at > Utc::now()) {
        return Ok((
            because.to_owned(),
            row.queued_reason.clone().unwrap_or_else(|| format!("Queued until {}.", row.run_at.format("%H:%M:%S UTC"))),
        ));
    }
    let serving: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM downbeat.worker WHERE role = 'worker' AND queues ? $1 AND last_seen_at > now() - interval '1 minute'",
    )
    .bind(&row.queue)
    .fetch_one(pool)
    .await?;
    let waited = since(row.run_at.min(Utc::now()));
    let reason = if serving == 0 {
        format!("Queued {waited}: no running worker serves queue {}.", row.queue)
    } else {
        format!("Queued {waited}: every slot on queue {} is busy.", row.queue)
    };
    Ok(("no_free_worker".to_owned(), reason))
}

#[derive(sqlx::FromRow)]
struct PauseRow {
    by: String,
    note: String,
    until: Option<DateTime<Utc>>,
    since: DateTime<Utc>,
}

/// An active pause, if there is one.
pub(crate) async fn pause_of(pool: &PgPool, kind: &str, name: &str) -> sqlx::Result<Option<PauseView>> {
    sqlx::query_as(
        "SELECT paused_by AS by, note, until, created_at AS since FROM downbeat.pause
          WHERE kind = $1 AND name = $2 AND (until IS NULL OR until > now())",
    )
    .bind(kind)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map(|row: Option<PauseRow>| {
        row.map(|row| PauseView { by: row.by, note: row.note, until: row.until, since: row.since })
    })
}

/// The detail only the single-run view returns.
pub(crate) async fn detail(
    pool: &PgPool,
    run_id: Uuid,
    parents: Vec<Uuid>,
    call_log: &serde_json::Value,
    error_chain: Option<Vec<String>>,
    stderr_tail: Option<String>,
) -> sqlx::Result<RunDetail> {
    let children: Vec<Uuid> = sqlx::query_scalar(
        "SELECT run_id FROM downbeat.run WHERE $1 = ANY(parents) UNION ALL SELECT run_id FROM downbeat.run_history WHERE $1 = ANY(parents)",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    let call_log: Vec<CallView> = serde_json::from_value(call_log.clone()).unwrap_or_default();
    Ok(RunDetail { error_chain: error_chain.unwrap_or_default(), stderr_tail, call_log, parents, children })
}

pub(crate) fn summary(row: &HistoryRow) -> RunSummary {
    RunSummary { run_id: row.run_id, status: row.status.clone(), reason: row.reason.clone(), at: row.finished_at }
}

fn since(at: DateTime<Utc>) -> String {
    format_duration((Utc::now() - at).to_std().unwrap_or_default())
}
