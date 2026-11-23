//! Turning stored rows into what the API returns, with a reason for every state.

use std::collections::HashMap;

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
    // Failures count failed calls; a run that ended any other way was on the attempt after them. A
    // run marked as succeeded failed first, so its failures already count its last call.
    let attempt = i32::from(row.failures) + i32::from(row.status != "failed" && row.marked_by.is_none());
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
        marked_at: row.marked_at,
        note: row.note,
        detail,
    }
}

/// Queued or running runs, in the order given, with the reason worked out now for states that are
/// not stored. However many rows there are, this costs at most four queries: one per kind of fact
/// the reasons and expected durations need, for every row at once.
pub(crate) async fn unfinished(pool: &PgPool, registry: &Registry, rows: Vec<RunRow>) -> sqlx::Result<Vec<RunView>> {
    let jobs: Vec<String> = rows.iter().map(|row| row.job_name.clone()).collect();
    let durations = crate::store::history::expected_durations(pool, &jobs).await?;
    let reasons = queued_reasons(pool, registry, &rows).await?;
    let mut views = Vec::with_capacity(rows.len());
    for (row, queued) in rows.into_iter().zip(reasons) {
        let (status, queued_because, reason) = if let Some((because, reason)) = queued {
            ("queued".to_owned(), Some(because), reason)
        } else {
            let reason = match (&row.function_started_at, &row.lease_owner) {
                (Some(started), Some(worker)) => format!("Running on {worker} for {}.", since(*started)),
                (None, Some(worker)) => format!("Claimed by {worker}; preparing."),
                _ => "Running.".to_owned(),
            };
            ("running".to_owned(), None, reason)
        };
        let job = registry.job(&row.job_name);
        let expected = durations.get(&row.job_name);
        views.push(RunView {
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
            marked_at: None,
            note: None,
            detail: None,
        });
    }
    Ok(views)
}

/// Why each queued row is not running, and `None` for a running one, in the order of the rows.
/// `paused`, `waiting_turn` and `no_free_worker` are worked out now, in that order of precedence;
/// the others were stored by whoever queued the run.
async fn queued_reasons(
    pool: &PgPool,
    registry: &Registry,
    rows: &[RunRow],
) -> sqlx::Result<Vec<Option<(String, String)>>> {
    let waiting = |row: &RunRow| row.state != "running";
    let mut reasons: Vec<Option<(String, String)>> = vec![None; rows.len()];
    if !rows.iter().any(waiting) {
        return Ok(reasons);
    }

    let mut asked = vec![("all".to_owned(), "all".to_owned())];
    for row in rows.iter().filter(|row| waiting(row)) {
        asked.push(("job".to_owned(), row.job_name.clone()));
        asked.push(("queue".to_owned(), row.queue.clone()));
    }
    let pauses = active_pauses(pool, &asked).await?;
    let pause_of = |kind: &str, name: &str| pauses.get(&(kind.to_owned(), name.to_owned()));
    for (row, reason) in rows.iter().zip(&mut reasons).filter(|(row, _)| waiting(row)) {
        let pause = pause_of("all", "all").or(pause_of("job", &row.job_name)).or(pause_of("queue", &row.queue));
        *reason = pause.map(|pause| {
            let until = pause.until.map_or_else(
                || "until resumed".to_owned(),
                |until| format!("until {}", until.format("%Y-%m-%d %H:%M UTC")),
            );
            ("paused".to_owned(), format!("Held: paused by {} {until} ({}).", pause.by, pause.note))
        });
    }

    let turns: Vec<&str> = rows
        .iter()
        .zip(&reasons)
        .filter(|(row, reason)| waiting(row) && reason.is_none())
        .filter_map(|(row, _)| row.turn_key.as_deref())
        .collect();
    if !turns.is_empty() {
        // The `run_turn` index lets at most one run hold a turn.
        let holders: HashMap<String, Uuid> =
            sqlx::query_as("SELECT turn_key, run_id FROM downbeat.run WHERE turn_key = ANY($1) AND starts > 0")
                .bind(&turns)
                .fetch_all(pool)
                .await?
                .into_iter()
                .collect();
        for (row, reason) in rows.iter().zip(&mut reasons).filter(|(row, reason)| waiting(row) && reason.is_none()) {
            let holder = row.turn_key.as_ref().and_then(|turn| holders.get(turn)).filter(|id| **id != row.run_id);
            if let Some(holder) = holder {
                let rule = registry.job(&row.job_name).map_or(crate::job::Turn::SameWork, |job| job.spec.policy.turn);
                *reason = Some(("waiting_turn".to_owned(), reasons::waiting_turn(Some(*holder), rule)));
            }
        }
    }

    let queues: Vec<&str> = rows
        .iter()
        .zip(&reasons)
        .filter(|(row, reason)| waiting(row) && reason.is_none())
        .map(|(row, _)| row.queue.as_str())
        .collect();
    if queues.is_empty() {
        return Ok(reasons);
    }
    // Whether a run is due later is judged by the database's clock, read in the same query.
    let counted: Vec<(String, i64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT queue.name,
                (SELECT count(*) FROM downbeat.worker
                  WHERE role = 'worker' AND queues ? queue.name AND last_seen_at > now() - interval '1 minute'),
                now()
           FROM (SELECT DISTINCT name FROM unnest($1::text[]) AS name) queue",
    )
    .bind(&queues)
    .fetch_all(pool)
    .await?;
    let now = counted.first().map(|(_, _, now)| *now);
    let serving: HashMap<String, i64> = counted.into_iter().map(|(queue, count, _)| (queue, count)).collect();
    for (row, reason) in rows.iter().zip(&mut reasons).filter(|(row, reason)| waiting(row) && reason.is_none()) {
        let due_later = now.is_some_and(|now| row.run_at > now);
        *reason = Some(stored_or_no_free_worker(row, serving.get(&row.queue).copied().unwrap_or(0), due_later));
    }
    Ok(reasons)
}

/// A queued run that is neither paused nor waiting its turn: its stored reason while it is due
/// later, otherwise whether any worker serves its queue.
fn stored_or_no_free_worker(row: &RunRow, serving: i64, due_later: bool) -> (String, String) {
    let stored =
        row.queued_because.as_deref().filter(|because| *because != "no_free_worker" && *because != "waiting_turn");
    if let (Some(because), true) = (stored, due_later) {
        return (
            because.to_owned(),
            row.queued_reason.clone().unwrap_or_else(|| format!("Queued until {}.", row.run_at.format("%H:%M:%S UTC"))),
        );
    }
    let waited = since(row.run_at.min(Utc::now()));
    let reason = if serving == 0 {
        format!("Queued {waited}: no running worker serves queue {}.", row.queue)
    } else {
        format!("Queued {waited}: every slot on queue {} is busy.", row.queue)
    };
    ("no_free_worker".to_owned(), reason)
}

#[derive(sqlx::FromRow)]
struct PauseRow {
    kind: String,
    name: String,
    by: String,
    note: String,
    until: Option<DateTime<Utc>>,
    since: DateTime<Utc>,
}

/// An active pause, if there is one.
pub(crate) async fn pause_of(pool: &PgPool, kind: &str, name: &str) -> sqlx::Result<Option<PauseView>> {
    let key = (kind.to_owned(), name.to_owned());
    Ok(active_pauses(pool, std::slice::from_ref(&key)).await?.remove(&key))
}

/// The active pauses among these kinds and names, in one query.
async fn active_pauses(
    pool: &PgPool,
    asked: &[(String, String)],
) -> sqlx::Result<HashMap<(String, String), PauseView>> {
    let (kinds, names): (Vec<&str>, Vec<&str>) =
        asked.iter().map(|(kind, name)| (kind.as_str(), name.as_str())).unzip();
    let rows: Vec<PauseRow> = sqlx::query_as(
        "SELECT kind, name, paused_by AS by, note, until, created_at AS since FROM downbeat.pause
          WHERE (kind, name) IN (SELECT * FROM unnest($1::text[], $2::text[]))
            AND (until IS NULL OR until > now())",
    )
    .bind(&kinds)
    .bind(&names)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| ((row.kind, row.name), PauseView { by: row.by, note: row.note, until: row.until, since: row.since }))
        .collect())
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
