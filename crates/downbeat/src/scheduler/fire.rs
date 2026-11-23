//! Firing schedules: once a second, lock the schedules that are due, create the run for the most
//! recent occurrence, and move each schedule's watermark. Any number of schedulers may do this at
//! once; a row lock on each schedule's state keeps them from firing it twice.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::{Acquire, PgConnection, PgPool};

use super::plan;
use crate::events::Event;
use crate::job::KeyedInput;
use crate::reasons;
use crate::registry::Registry;
use crate::schedule::{CompiledSchedule, Occurrence};
use crate::starting::turn_key;
use crate::store::audit;
use crate::store::runs::{self, Created, NewRun, Place, StartedBy};

/// Missed occurrences older than this are reported as dropped; younger ones were only a tick apart.
const REPORT_DROPPED_AFTER: TimeDelta = TimeDelta::seconds(10);
/// A schedule whose run could not be created is tried again this much later.
const RETRY_FAILED_AFTER: Duration = Duration::from_secs(60);

/// Records that this scheduler has the definition's schedules. A new one starts from now, and so
/// does one whose times changed or that no scheduler has registered for ten minutes.
pub(crate) async fn register(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    for schedule in registry.schedules.values() {
        sqlx::query(
            "INSERT INTO downbeat.schedule_state (schedule, watermark, next_due_at, times_hash) VALUES ($1, now(), now(), $2)
             ON CONFLICT (schedule) DO UPDATE SET
                watermark = CASE WHEN schedule_state.seen_at < now() - interval '10 minutes'
                                 THEN GREATEST(schedule_state.watermark, now())
                                 -- Changed times start from now, less a few seconds, so builds that
                                 -- alternate during a rolling deploy never skip an occurrence just due.
                                 WHEN schedule_state.times_hash <> $2
                                 THEN GREATEST(schedule_state.watermark, now() - interval '5 seconds')
                                 ELSE schedule_state.watermark END,
                next_due_at = CASE WHEN schedule_state.times_hash <> $2 THEN now() ELSE schedule_state.next_due_at END,
                times_hash = $2,
                seen_at = now()",
        )
        .bind(&schedule.name)
        .bind(plan::times_hash(schedule))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Fires every due schedule once.
pub(crate) async fn fire_due(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    let names: Vec<String> = registry.schedules.keys().cloned().collect();
    let mut transaction = pool.begin().await?;
    let due: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT schedule, watermark FROM downbeat.schedule_state
          WHERE schedule = ANY($1) AND next_due_at <= now()
          ORDER BY next_due_at LIMIT 50 FOR UPDATE SKIP LOCKED",
    )
    .bind(&names)
    .fetch_all(&mut *transaction)
    .await?;
    if due.is_empty() {
        return Ok(());
    }
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&mut *transaction).await?;
    for (name, watermark) in due {
        let Some(schedule) = registry.schedules.get(&name) else { continue };
        let mut savepoint = transaction.begin().await?;
        match fire(&mut savepoint, registry, schedule, watermark, now).await {
            Ok(()) => savepoint.commit().await?,
            Err(error) => {
                savepoint.rollback().await?;
                tracing::warn!(schedule = name, %error, "a schedule could not fire; trying again in a minute");
                sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = now() + $2 WHERE schedule = $1")
                    .bind(&name)
                    .bind(crate::store::interval(RETRY_FAILED_AFTER))
                    .execute(&mut *transaction)
                    .await?;
                let event = Event::ScheduleFailed { schedule: name.clone(), error };
                audit::event(&mut transaction, &event, Some(schedule.job), None, Some(&name)).await?;
            }
        }
    }
    transaction.commit().await
}

async fn fire(
    connection: &mut PgConnection,
    registry: &Registry,
    schedule: &CompiledSchedule,
    watermark: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), String> {
    let overrides = plan::overrides_for(&mut *connection, &schedule.name).await.map_err(|error| error.to_string())?;
    let paused: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.pause WHERE kind = 'schedule' AND name = $1 AND (until IS NULL OR until > now()))",
    )
    .bind(&schedule.name)
    .fetch_one(&mut *connection)
    .await
    .map_err(|error| error.to_string())?;

    if !paused {
        let missed = plan::occurrences_between(registry, schedule, &overrides, watermark, now).await?;
        if let Some((latest, earlier)) = missed.split_last() {
            let expired = plan::expires_at(schedule, latest.at).is_some_and(|at| at < now);
            let mut dropped: Vec<&Occurrence> = earlier.iter().filter(|o| now - o.at > REPORT_DROPPED_AFTER).collect();
            if expired {
                dropped.push(latest);
            } else {
                create_run(connection, registry, schedule, latest).await?;
            }
            if let (Some(first), Some(last)) = (dropped.first(), dropped.last()) {
                let event = Event::OccurrencesDropped {
                    schedule: schedule.name.clone(),
                    count: i64::try_from(dropped.len()).unwrap_or(i64::MAX),
                    first: first.at,
                    last: last.at,
                };
                audit::event(connection, &event, Some(schedule.job), None, Some(&schedule.name))
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    let next = plan::next_after(registry, schedule, &overrides, now).await?;
    sqlx::query(
        "UPDATE downbeat.schedule_state SET watermark = GREATEST(watermark, $2), next_due_at = $3 WHERE schedule = $1",
    )
    .bind(&schedule.name)
    .bind(now)
    .bind(next)
    .execute(&mut *connection)
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Creates the run for an occurrence, applying rule 1: a request for work already queued joins it.
pub(crate) async fn create_run(
    connection: &mut PgConnection,
    registry: &Registry,
    schedule: &CompiledSchedule,
    occurrence: &Occurrence,
) -> Result<Created, String> {
    let job = registry.job(schedule.job).ok_or_else(|| format!("there is no job named {}", schedule.job))?;
    let input = match &schedule.input {
        Some(input) => {
            crate::guard::guarded(&format!("the input of schedule {}", schedule.name), || input(occurrence))?
        }
        None => serde_json::Value::Null,
    };
    let KeyedInput { input, hash: input_hash, turn_value } = job.spec.input_ops.keyed(schedule.job, input, false)?;
    let policy = &job.spec.policy;
    let expires_at = plan::expires_at(schedule, occurrence.at);
    let run = NewRun {
        job: schedule.job.to_owned(),
        queue: policy.queue.to_owned(),
        priority: policy.priority,
        turn_key: Some(turn_key(schedule.job, policy, &input_hash, turn_value.as_deref())),
        input: Some(input),
        input_hash: Some(input_hash),
        joins_queued_work: true,
        due_at: Some(occurrence.at),
        expires_at,
        not_before: None,
        started_by: StartedBy::Schedule,
        schedule: Some(schedule.name.clone()),
        place: Place::NewChain { pipeline: job.pipeline.clone(), backfill_id: None },
        dry_run: false,
        ignore_conditions: false,
        without_children: false,
        timeout_override: None,
        actor: None,
        queued_reason: None,
    };
    let created = runs::create(connection, &run).await.map_err(|error| error.to_string())?;
    if let Created::Joined { run_id, chain_id } = created {
        record_joined(connection, &run, run_id, chain_id).await.map_err(|error| error.to_string())?;
    }
    Ok(created)
}

/// A scheduled request that joined a queued run leaves a `skipped (joined)` row, so the calendar
/// can show where the occurrence went.
async fn record_joined(
    connection: &mut PgConnection,
    run: &NewRun,
    joined: uuid::Uuid,
    chain_id: uuid::Uuid,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, schedule, chain_id, input,
                                           input_hash, created_at, status, skipped_because, reason, joined_run)
         VALUES ($1, $2, $3, $4, $5, 'schedule', $6, $7, $8, $9, now(), 'skipped', 'joined', $10, $11)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(&run.job)
    .bind(&run.queue)
    .bind(run.priority)
    .bind(run.due_at.unwrap_or_else(Utc::now))
    .bind(&run.schedule)
    .bind(chain_id)
    .bind(&run.input)
    .bind(&run.input_hash)
    .bind(reasons::joined(joined))
    .bind(joined)
    .execute(connection)
    .await?;
    Ok(())
}
