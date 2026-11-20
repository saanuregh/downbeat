//! Starting a job on request: from a person or program through the API, from a job through
//! `run.start`, or from code in the app. One path, so every request follows the same rules.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::job::{Policy, Started, Turn};
use crate::registry::Registry;
use crate::store::runs::{self, Created, NewRun, Place, StartedBy, Timeout};

/// A request to start a job, with its input already checked and hashed.
#[derive(Debug, Clone)]
pub(crate) struct StartJob {
    pub(crate) job: String,
    pub(crate) input: Value,
    pub(crate) input_hash: String,
    pub(crate) turn_value: Option<String>,
    pub(crate) options: StartOptions,
    pub(crate) started_by: StartedBy,
    pub(crate) actor: Option<String>,
    /// A key that makes repeating the request return the run it first produced: `(actor, key)`.
    pub(crate) idempotency: Option<(String, String)>,
}

/// What a person or program may choose for one run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct StartOptions {
    pub(crate) priority: Option<i16>,
    pub(crate) not_before: Option<DateTime<Utc>>,
    pub(crate) dry_run: bool,
    pub(crate) without_children: bool,
    pub(crate) even_if_queued: bool,
    pub(crate) ignore_conditions: bool,
    pub(crate) timeout: Option<Timeout>,
}

impl StartOptions {
    /// Rule 1 of "the same work is done once" does not apply to a deliberate run.
    pub(crate) fn deliberate(&self) -> bool {
        self.even_if_queued
            || self.dry_run
            || self.without_children
            || self.ignore_conditions
            || self.not_before.is_some()
            || self.timeout.is_some()
    }
}

/// What starting did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StartResult {
    pub(crate) started: Started,
    /// The idempotency key had been used before; this is the run it produced then.
    pub(crate) repeated: bool,
}

/// Why a start request was refused.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StartError {
    #[error("there is no job named {0}")]
    UnknownJob(String),
    #[error("{0} does not support dry runs; only a job marked supports_dry_run can be asked")]
    NoDryRun(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// The key a run takes turns on (rule 2), given its job's policy.
pub(crate) fn turn_key(job: &str, policy: &Policy, input_hash: &str, turn_value: Option<&str>) -> String {
    match policy.turn {
        Turn::SameWork => runs::same_work_key(job, Some(input_hash)),
        Turn::OneAtATime => format!("{job}:*"),
        Turn::OnePer(field) => format!("{job}:{field}={}", turn_value.unwrap_or("")),
    }
}

pub(crate) async fn start(pool: &PgPool, registry: &Registry, request: StartJob) -> Result<StartResult, StartError> {
    let job = registry.job(&request.job).ok_or_else(|| StartError::UnknownJob(request.job.clone()))?;
    if request.options.dry_run && !job.spec.policy.supports_dry_run {
        return Err(StartError::NoDryRun(request.job));
    }
    let mut transaction = pool.begin().await?;
    // The key is claimed before the run is created. A second request with the same key waits on the
    // unique index until the first commits, then finds its run, so two retries in flight never
    // both create one.
    if let Some((actor, key)) = &request.idempotency {
        let claimed: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO downbeat.idempotency (actor, job_name, key, run_id) VALUES ($1, $2, $3, $4)
             ON CONFLICT DO NOTHING RETURNING run_id",
        )
        .bind(actor)
        .bind(&request.job)
        .bind(key)
        .bind(Uuid::nil())
        .fetch_optional(&mut *transaction)
        .await?;
        if claimed.is_none() {
            let earlier: Uuid = sqlx::query_scalar(
                "SELECT run_id FROM downbeat.idempotency WHERE actor = $1 AND job_name = $2 AND key = $3",
            )
            .bind(actor)
            .bind(&request.job)
            .bind(key)
            .fetch_one(&mut *transaction)
            .await?;
            return Ok(StartResult { started: Started::New(earlier), repeated: true });
        }
    }
    let policy = &job.spec.policy;
    let options = &request.options;
    let run = NewRun {
        job: request.job.clone(),
        queue: policy.queue.to_owned(),
        priority: options.priority.unwrap_or(policy.priority),
        input: Some(request.input.clone()),
        input_hash: Some(request.input_hash.clone()),
        turn_key: Some(turn_key(&request.job, policy, &request.input_hash, request.turn_value.as_deref())),
        joins_queued_work: !options.deliberate(),
        due_at: None,
        expires_at: None,
        not_before: options.not_before,
        started_by: request.started_by,
        schedule: None,
        place: Place::NewChain { pipeline: job.pipeline.clone(), backfill_id: None },
        dry_run: options.dry_run,
        ignore_conditions: options.ignore_conditions,
        without_children: options.without_children,
        timeout_override: options.timeout,
        actor: request.actor.clone(),
        queued_reason: None,
    };
    let created = runs::create(&mut transaction, &run).await?;
    if let Some((actor, key)) = &request.idempotency {
        sqlx::query("UPDATE downbeat.idempotency SET run_id = $4 WHERE actor = $1 AND job_name = $2 AND key = $3")
            .bind(actor)
            .bind(&request.job)
            .bind(key)
            .bind(created.run_id())
            .execute(&mut *transaction)
            .await?;
    }
    if matches!(request.started_by, StartedBy::Person | StartedBy::Program) {
        let action = if matches!(created, Created::Joined { .. }) { "start_joined" } else { "start" };
        crate::store::audit::record(
            &mut transaction,
            request.actor.as_deref().unwrap_or("unknown"),
            action,
            Some(&request.job),
            Some(created.run_id()),
            None,
            serde_json::json!({ "options": format!("{options:?}") }),
        )
        .await?;
    }
    transaction.commit().await?;
    let started = match created {
        Created::New { run_id, .. } => Started::New(run_id),
        Created::Joined { run_id, .. } => Started::Joined(run_id),
    };
    Ok(StartResult { started, repeated: false })
}

#[cfg(test)]
mod tests {
    use super::turn_key;
    use crate::job::{Policy, Turn};

    #[test]
    fn turn_keys_of_different_kinds_never_overlap() {
        let policy = |turn| Policy { turn, ..Policy::DEFAULT };
        let hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        let keys = [
            turn_key("job", &policy(Turn::SameWork), hash, None),
            turn_key("job", &policy(Turn::SameWork), "", None),
            turn_key("job", &policy(Turn::OneAtATime), hash, None),
            turn_key("job", &policy(Turn::OnePer("account")), hash, Some("*")),
            turn_key("job", &policy(Turn::OnePer("account")), hash, Some(hash)),
            turn_key("job", &policy(Turn::OnePer("account")), hash, Some("")),
            turn_key("job", &policy(Turn::OnePer("other")), hash, Some(hash)),
        ];
        let distinct: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "{keys:#?}");
        // Every run of a job shares its turn key's prefix, and no other job's.
        assert!(keys.iter().all(|key| key.starts_with("job:")));
    }
}
