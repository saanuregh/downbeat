//! Preparing a run, before its function is first called: build the input in the child, store it,
//! and check the run's conditions. A run that has begun its work is never prepared again.

use serde_json::{Value, json};

use super::call::{AtShutdown, Interrupted, wait_for};
use super::child::ChildHandle;
use super::lease::{Lease, StopReason};
use super::{Decision, WorkerContext};
use crate::job::RetryOn;
use crate::pipeline::{LinkKind, NeedTest, WhenUnmet};
use crate::protocol::{FromChild, Prepared, RunInfo, ToChild};
use crate::reasons;
use crate::registry::RegisteredJob;
use crate::starting::turn_key;
use crate::store::ending::{Ending, HistoryRow, Status};
use crate::store::history;
use crate::store::runs::{self, Held, Requeue, RunRow};

/// Prepares the run. `Ok` carries the input to call the function with; `Err` what to do instead.
#[allow(clippy::result_large_err)] // Once per run; boxing the decision would only add noise.
pub(crate) async fn prepare(
    context: &WorkerContext,
    child: &mut ChildHandle,
    run: &RunRow,
    job: &RegisteredJob,
    lease: &mut Lease,
) -> Result<Value, Decision> {
    let parents = match (&job.link, run.started_by.as_str()) {
        (Some(link), "link") => Some(handed_over(context, run, link.kind).await?),
        _ => None,
    };
    let info = run_info(run);
    if child
        .send(&ToChild::Prepare {
            run: info,
            input: run.input.clone(),
            parents,
            ignore_conditions: run.ignore_conditions,
        })
        .await
        .is_err()
    {
        return Err(Decision::child_failed("the child process stopped while the run was being prepared"));
    }
    // Building the input and testing conditions run app code; they get the job's timeout.
    let timeout = match run.timeout_override() {
        Some(crate::store::runs::Timeout::After(timeout)) => Some(timeout),
        Some(crate::store::runs::Timeout::Never) => None,
        None => Some(job.spec.policy.timeout),
    };
    let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    let wanted = run.run_id;
    // A crash or a timeout here is a failed call like any other: `retry_on` and `attempts` apply.
    let verdict = super::Verdict::new(context, job, run, chrono::Utc::now());
    let prepared = wait_for(context, child, lease, deadline, AtShutdown::Stop, |message| match message {
        FromChild::Prepared { run_id, result } if run_id == wanted => Some(result),
        _ => None,
    })
    .await
    .map_err(|interrupted| match interrupted {
        Interrupted::Crashed { .. } => verdict.fail_or_retry(
            RetryOn::Crash,
            "crash",
            "Failed: the process crashed while the run was being prepared.".to_owned(),
            "the process crashed while the run was being prepared".to_owned(),
        ),
        Interrupted::TimedOut => verdict.fail_or_retry(
            RetryOn::Timeout,
            "timeout",
            "Failed: preparing the run (building its input and testing its conditions) took longer than the job's timeout.".to_owned(),
            "preparing the run took longer than the job's timeout".to_owned(),
        ),
        Interrupted::Stopped(StopReason::CancelRequested(by)) => Decision::End(Ending::new(Status::Cancelled, reasons::cancelled(&by))),
        Interrupted::Stopped(StopReason::Unreachable) => super::unreachable(context, run),
        Interrupted::Stopped(_) => Decision::Nothing,
        // Nothing ran yet: the run goes back at once for the next worker, with nothing counted.
        Interrupted::Shutdown => Decision::Requeue(Requeue {
            delay: std::time::Duration::ZERO,
            because: "no_free_worker",
            reason: "Re-queued: the worker shut down while the run was being prepared.".to_owned(),
            failed: false,
            interrupted: false,
            call: None,
        }),
    })?;
    let prepared = prepared.map_err(|error| Decision::End(Ending::failed("bad_input", reasons::bad_input(&error))))?;

    let policy = &job.spec.policy;
    let turn = turn_key(&run.job_name, policy, &prepared.input_hash, prepared.turn_value.as_deref());
    let held = Held { run_id: run.run_id, fence: run.fence, worker: &context.id };
    match runs::prepared(&context.pool, held, &prepared.input, &prepared.input_hash, &turn).await {
        Ok(true) => {}
        Ok(false) => return Err(Decision::Nothing),
        Err(error) => return Err(Decision::database(error)),
    }
    if !run.ignore_conditions {
        check_conditions(context, run, job, &prepared, &turn).await?;
    }
    Ok(prepared.input)
}

pub(crate) fn run_info(run: &RunRow) -> RunInfo {
    RunInfo {
        run_id: run.run_id,
        job: run.job_name.clone(),
        attempt: u32::try_from(run.failures).unwrap_or(0) + 1,
        due_at: run.due_at,
        dry_run: run.dry_run,
    }
}

/// What each parent handed over, in the link's order, in the shape the link's kind gives the child.
#[allow(clippy::result_large_err)]
async fn handed_over(context: &WorkerContext, run: &RunRow, kind: LinkKind) -> Result<Vec<Value>, Decision> {
    let parents = history::by_ids(&context.pool, &run.parents).await.map_err(Decision::database)?;
    Ok(parents.iter().map(|parent| view(parent, kind)).collect())
}

fn view(parent: &HistoryRow, kind: LinkKind) -> Value {
    let failure = || {
        json!({
            "job": parent.job_name,
            "run_id": parent.run_id,
            "kind": parent.failed_because.clone().unwrap_or_default(),
            "reason": parent.reason,
        })
    };
    match kind {
        LinkKind::After => parent.output.clone().unwrap_or(Value::Null),
        LinkKind::AfterFailure => failure(),
        LinkKind::AfterAny => match parent.status.as_str() {
            "succeeded" => json!({ "ended": "succeeded", "value": parent.output.clone().unwrap_or(Value::Null) }),
            "failed" => json!({ "ended": "failed", "value": failure() }),
            status => json!({ "ended": status, "value": parent.reason }),
        },
    }
}

#[allow(clippy::result_large_err)]
async fn check_conditions(
    context: &WorkerContext,
    run: &RunRow,
    job: &RegisteredJob,
    prepared: &Prepared,
    turn: &str,
) -> Result<(), Decision> {
    if let Some(name) = &prepared.failed_condition {
        return Err(Decision::End(Ending::skipped("condition_false", reasons::condition_false(name))));
    }
    let pool = &context.pool;
    for (need, hash) in job.needs.iter().zip(&prepared.need_hashes) {
        let (met, what) = match (&need.test, hash) {
            (NeedTest::DoneFor(_), Some(hash)) => (
                history::succeeded_for(pool, need.job, hash).await.map_err(Decision::database)?,
                "for the input this run needs".to_owned(),
            ),
            (NeedTest::DoneWithin(within), _) => (
                history::succeeded_within(pool, need.job, *within).await.map_err(Decision::database)?,
                format!("within {}", crate::units::format_duration(*within)),
            ),
            (NeedTest::DoneFor(_), None) => (false, "for the input this run needs".to_owned()),
        };
        if met {
            continue;
        }
        let unmet = reasons::need_unmet(need.job, &what);
        return Err(match need.when_unmet {
            WhenUnmet::Skip => Decision::End(Ending::skipped("need_not_met", format!("Skipped: {unmet}."))),
            WhenUnmet::Fail => Decision::End(Ending::failed("need_not_met", format!("Failed: {unmet}."))),
            WhenUnmet::Wait => Decision::Requeue(Requeue {
                delay: job.spec.policy.recheck_every,
                because: "need_not_met",
                reason: format!(
                    "Waiting: {unmet}; checking again every {}.",
                    crate::units::format_duration(job.spec.policy.recheck_every)
                ),
                failed: false,
                interrupted: false,
                call: None,
            }),
        });
    }
    let compared = matches!(run.started_by.as_str(), "schedule" | "link");
    if job.if_changed && compared {
        let per_value = matches!(job.spec.policy.turn, crate::job::Turn::OnePer(_)).then_some(turn);
        if let Some((Some(last_hash), at)) =
            history::last_success(pool, &run.job_name, per_value).await.map_err(Decision::database)?
            && last_hash == prepared.input_hash
        {
            return Err(Decision::End(Ending::skipped(
                "input_unchanged",
                reasons::input_unchanged(at, context.registry.zone),
            )));
        }
    }
    Ok(())
}
