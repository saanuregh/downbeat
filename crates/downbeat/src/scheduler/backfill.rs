//! Driving backfills: a scheduled job run for each date in a range, one date at a time, in order,
//! each date its own chain.

use chrono::{NaiveDate, TimeDelta, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::plan;
use crate::registry::Registry;
use crate::starting::turn_key;
use crate::store::runs::{self, NewRun, Place, StartedBy};

#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct BackfillRow {
    pub(crate) backfill_id: i64,
    pub(crate) job_name: String,
    pub(crate) schedule: String,
    pub(crate) from_date: NaiveDate,
    pub(crate) to_date: NaiveDate,
    pub(crate) next_date: NaiveDate,
    pub(crate) current_chain: Option<Uuid>,
    pub(crate) continue_on_failure: bool,
    pub(crate) state: String,
    pub(crate) reason: Option<String>,
    pub(crate) created_by: String,
    pub(crate) created_at: chrono::DateTime<Utc>,
}

/// Moves every running backfill one step, when its current date has finished.
pub(crate) async fn drive(pool: &PgPool, registry: &Registry) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    let backfills: Vec<BackfillRow> = sqlx::query_as("SELECT * FROM downbeat.backfill WHERE state = 'running' FOR UPDATE SKIP LOCKED")
        .fetch_all(&mut *transaction)
        .await?;
    for backfill in backfills {
        if let Some(chain) = backfill.current_chain {
            let unfinished: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE chain_id = $1)")
                .bind(chain)
                .fetch_one(&mut *transaction)
                .await?;
            if unfinished {
                continue;
            }
            let failed: Option<String> = sqlx::query_scalar(
                "SELECT job_name || ' ' || status FROM downbeat.run_history
                  WHERE chain_id = $1 AND status IN ('failed', 'expired', 'cancelled') LIMIT 1",
            )
            .bind(chain)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(failed) = failed
                && !backfill.continue_on_failure
            {
                let reason = format!("Stopped at {}: {failed}.", backfill.next_date.pred_opt().unwrap_or(backfill.next_date));
                finish(&mut transaction, backfill.backfill_id, "failed", &reason).await?;
                continue;
            }
        }
        let next = start_next_date(&mut transaction, registry, &backfill).await;
        match next {
            Ok(Some((chain, date))) => {
                sqlx::query("UPDATE downbeat.backfill SET current_chain = $2, next_date = $3 WHERE backfill_id = $1")
                    .bind(backfill.backfill_id)
                    .bind(chain)
                    .bind(date.succ_opt().unwrap_or(date))
                    .execute(&mut *transaction)
                    .await?;
            }
            Ok(None) => finish(&mut transaction, backfill.backfill_id, "done", "Every date has run.").await?,
            Err(error) => finish(&mut transaction, backfill.backfill_id, "failed", &format!("Stopped: {error}.")).await?,
        }
    }
    transaction.commit().await
}

async fn finish(connection: &mut sqlx::PgConnection, backfill_id: i64, state: &str, reason: &str) -> sqlx::Result<()> {
    sqlx::query("UPDATE downbeat.backfill SET state = $2, reason = $3, current_chain = NULL WHERE backfill_id = $1")
        .bind(backfill_id)
        .bind(state)
        .bind(reason)
        .execute(connection)
        .await?;
    Ok(())
}

/// Creates the run for the next date that the schedule does not turn off. `None` once past the end.
async fn start_next_date(connection: &mut sqlx::PgConnection, registry: &Registry, backfill: &BackfillRow) -> Result<Option<(Uuid, NaiveDate)>, String> {
    let schedule = registry.schedules.get(&backfill.schedule).ok_or_else(|| format!("schedule {} is not in this build", backfill.schedule))?;
    let job = registry.job(schedule.job).ok_or_else(|| format!("job {} is not in this build", schedule.job))?;
    let overrides = Vec::new();
    let mut date = backfill.next_date;
    while date <= backfill.to_date {
        let (day_plan, _) = plan::plan_for(registry, schedule, &overrides, date).await?;
        // A date the day rule turns off is passed over; dated overrides do not apply to the past.
        if let Some(first) = schedule.occurrences_on(date, &day_plan).into_iter().next() {
            let input = match &schedule.input {
                Some(input) => input(&first)?,
                None => serde_json::Value::Null,
            };
            let ops = &job.spec.input_ops;
            let input = (ops.normalize)(input, false)?;
            let input_hash = (ops.hash)(&input)?;
            let turn_value = ops.turn_value.map(|value_of| value_of(&input)).transpose()?;
            let policy = &job.spec.policy;
            let expires_at = plan::expiry_of(schedule).map(|expiry| Utc::now() + TimeDelta::from_std(expiry).unwrap_or(TimeDelta::MAX));
            let run = NewRun {
                job: schedule.job.to_owned(),
                queue: policy.queue.to_owned(),
                priority: policy.priority,
                turn_key: Some(turn_key(schedule.job, policy, &input_hash, turn_value.as_deref())),
                input: Some(input),
                input_hash: Some(input_hash),
                joins_queued_work: false,
                due_at: None,
                expires_at,
                not_before: None,
                started_by: StartedBy::Backfill,
                schedule: Some(schedule.name.clone()),
                place: Place::NewChain { pipeline: job.pipeline.clone(), backfill_id: Some(backfill.backfill_id) },
                dry_run: false,
                ignore_conditions: false,
                without_children: false,
                timeout_override: None,
                actor: Some(backfill.created_by.clone()),
                queued_reason: None,
            };
            let created = runs::create(connection, &run).await.map_err(|error| error.to_string())?;
            let chain: Uuid = sqlx::query_scalar("SELECT chain_id FROM downbeat.run WHERE run_id = $1")
                .bind(created.run_id())
                .fetch_one(&mut *connection)
                .await
                .map_err(|error| error.to_string())?;
            return Ok(Some((chain, date)));
        }
        date = date.succ_opt().unwrap_or(date);
        if date == NaiveDate::MAX {
            break;
        }
    }
    Ok(None)
}
