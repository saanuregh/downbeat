//! The reaper's fence and call log (spec 9.5), its `job_not_registered` sweep with no worker row
//! (9.6), which processes deliver callbacks and how many (12.1), and pruning in every role and by
//! count (13.1, 13.2).

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use serde_json::Value;
use sqlx::PgPool;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

async fn queued_echo(client: &InAppClient, text: &str) -> Uuid {
    client.start(echo, Text { text: text.into() }).await.unwrap().run_id()
}

async fn set(pool: &PgPool, sql: &'static str, run_id: Uuid) {
    sqlx::query(sql).bind(run_id).execute(pool).await.unwrap();
}

/// Records a pending event; returns its id.
async fn pending_event(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO downbeat.audit (actor, action, detail, notify_after)
         VALUES ('test', 'event', '{\"event\": \"database_unreachable\", \"worker\": \"w-1\"}', now())
         RETURNING audit_id",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Puts a chain of one or more finished runs of `job` straight into the tables, started and
/// finished this many minutes ago (one run per entry); returns the chain.
async fn finished_chain(pool: &PgPool, pipeline: &str, job: &str, minutes_ago: &[i64], notify: bool) -> Uuid {
    let chain = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by, started_at)
         VALUES ($1, $2, $3, 'person', now() - make_interval(mins => $4))",
    )
    .bind(chain)
    .bind(pipeline)
    .bind(job)
    .bind(i32::try_from(minutes_ago[0]).unwrap())
    .execute(pool)
    .await
    .unwrap();
    for minutes in minutes_ago {
        sqlx::query(
            "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id,
                    created_at, status, reason, finished_at, notify_after)
             VALUES ($1, $2, 'default', 0, now(), 'person', $3, now() - make_interval(mins => $4), 'succeeded',
                     'Succeeded.', now() - make_interval(mins => $4), CASE WHEN $5 THEN now() + interval '1 day' END)",
        )
        .bind(Uuid::new_v4())
        .bind(job)
        .bind(chain)
        .bind(i32::try_from(*minutes).unwrap())
        .bind(notify)
        .execute(pool)
        .await
        .unwrap();
    }
    chain
}

async fn chains(pool: &PgPool) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT chain_id FROM downbeat.chain").fetch_all(pool).await.unwrap()
}

// 9.5: a lost worker's run goes back to the queue with its fence raised and a "worker lost" call.
#[tokio::test]
async fn the_reaper_raises_the_fence_and_logs_a_worker_lost_call() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let run = queued_echo(&client, "its worker is gone").await;
    // As if worker gone-1 had claimed it and started its function, then vanished.
    set(
        pool,
        "UPDATE downbeat.run SET state = 'running', fence = fence + 1, lease_owner = 'gone-1', starts = 1,
                lease_expires_at = now() - interval '1 hour', claimed_at = now() - interval '1 hour',
                function_started_at = now() - interval '1 hour'
          WHERE run_id = $1",
        run,
    )
    .await;
    let fence: i64 =
        sqlx::query_scalar("SELECT fence FROM downbeat.run WHERE run_id = $1").bind(run).fetch_one(pool).await.unwrap();

    // A scheduler alone: the reaper runs, but no worker takes the run again.
    let _app = App::with(&database, &["scheduler"], "test");
    let lost = format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND lost_count = 1)");
    eventually(pool, &lost, SOON).await;

    let (state, after, last_call): (String, i64, Value) = sqlx::query_as(
        "SELECT state, fence, call_log -> (jsonb_array_length(call_log) - 1) FROM downbeat.run WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!((state.as_str(), after), ("queued", fence + 1), "back to the queue, fence raised");
    assert_eq!(last_call["ended"], "worker lost", "{last_call}");
    assert_eq!(last_call["worker"], "gone-1", "{last_call}");
}

// 9.6: with no row of a process in the worker role, old runs are not ended as job_not_registered.
#[tokio::test]
async fn no_job_not_registered_sweep_runs_while_no_worker_row_exists() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let old = queued_echo(&client, "from an older build").await;
    set(
        pool,
        "UPDATE downbeat.run SET job_name = 'retired_job', created_at = now() - interval '2 hours' WHERE run_id = $1",
        old,
    )
    .await;
    let unwanted = queued_echo(&client, "unwanted").await;
    set(pool, "UPDATE downbeat.run SET cancel_requested_by = 'asha' WHERE run_id = $1", unwanted).await;

    let _app = App::with(&database, &["scheduler"], "test");
    // The cancel is ended by the same sweep, before it looks for unregistered jobs.
    assert_eq!(finished(pool, unwanted, SOON).await.status, "cancelled");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let workers: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downbeat.worker WHERE role = 'worker'").fetch_one(pool).await.unwrap();
    assert_eq!(workers, 0);
    let still_queued: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = $1)")
        .bind(old)
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(still_queued, "two sweeps ran and left the run alone");
}

// 12.1: events are delivered twenty a pass, as runs are (tested in retention.rs).
#[tokio::test]
async fn one_delivery_pass_handles_at_most_twenty_events() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    for _ in 0..25 {
        pending_event(pool).await;
    }
    downbeat::measure::deliver_callbacks(pool, definition()).await.unwrap();

    let (handled, untouched): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE notify_after IS NULL OR notify_after > now()),
                count(*) FILTER (WHERE notify_after <= now())
           FROM downbeat.audit WHERE action = 'event'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!((handled, untouched), (20, 5));
}

// 12.1: a row another transaction holds is skipped, without waiting for it.
#[tokio::test]
async fn delivery_skips_rows_another_transaction_holds() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    finished_chain(pool, "basics", "echo", &[2, 1], false).await;
    sqlx::query("UPDATE downbeat.run_history SET notify_after = now()").execute(pool).await.unwrap();
    let runs: Vec<Uuid> = sqlx::query_scalar("SELECT run_id FROM downbeat.run_history ORDER BY finished_at")
        .fetch_all(pool)
        .await
        .unwrap();
    let events = [pending_event(pool).await, pending_event(pool).await];

    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM downbeat.run_history WHERE run_id = $1 FOR UPDATE")
        .bind(runs[0])
        .execute(&mut *holder)
        .await
        .unwrap();
    sqlx::query("SELECT 1 FROM downbeat.audit WHERE audit_id = $1 FOR UPDATE")
        .bind(events[0])
        .execute(&mut *holder)
        .await
        .unwrap();
    tokio::time::timeout(SOON, downbeat::measure::deliver_callbacks(pool, definition()))
        .await
        .expect("delivery does not wait for a held row")
        .unwrap();
    holder.rollback().await.unwrap();

    let run_pending = |run: Uuid| async move {
        sqlx::query_scalar::<_, bool>("SELECT notify_after IS NOT NULL FROM downbeat.run_history WHERE run_id = $1")
            .bind(run)
            .fetch_one(pool)
            .await
            .unwrap()
    };
    let event_pending = |event: i64| async move {
        sqlx::query_scalar::<_, bool>("SELECT notify_after IS NOT NULL FROM downbeat.audit WHERE audit_id = $1")
            .bind(event)
            .fetch_one(pool)
            .await
            .unwrap()
    };
    assert!(run_pending(runs[0]).await, "the held history row is still owed");
    assert!(!run_pending(runs[1]).await, "the other history row is delivered");
    assert!(event_pending(events[0]).await, "the held event is still owed");
    assert!(!event_pending(events[1]).await, "the other event is delivered");
}

// 12.1: only the scheduler role delivers.
#[tokio::test]
async fn an_app_with_only_the_worker_role_delivers_no_callbacks() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    let event = pending_event(pool).await;
    let run = queued_echo(&client, "nobody tells").await;
    finished(pool, run, SOON).await;
    // Fifteen of the scheduler's polls.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (run_pending, event_pending): (bool, bool) = sqlx::query_as(
        "SELECT (SELECT notify_after IS NOT NULL FROM downbeat.run_history WHERE run_id = $1),
                (SELECT notify_after IS NOT NULL FROM downbeat.audit WHERE audit_id = $2)",
    )
    .bind(run)
    .bind(event)
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(run_pending && event_pending, "(run, event) pending: ({run_pending}, {event_pending})");
    assert_eq!(std::fs::read_dir(&app.finished_dir).unwrap().count(), 0, "no callback was called");
}

// 13.1: pruning runs in every role; its first pass runs at start.
#[tokio::test]
async fn an_app_with_only_the_worker_role_prunes() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let old = finished_chain(pool, "basics", "echo", &[60 * 24 * 365], false).await;
    let latest = finished_chain(pool, "basics", "echo", &[1], false).await;

    let _app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    eventually(pool, &format!("SELECT NOT EXISTS (SELECT 1 FROM downbeat.chain WHERE chain_id = '{old}')"), SOON).await;
    assert!(chains(pool).await.contains(&latest));
}

// 13.2: a chain that may not be pruned still counts toward keep_runs_at_most, and a chain with
// two runs of its first job counts once.
#[tokio::test]
async fn keep_runs_at_most_counts_chains_that_must_stay_and_each_chain_once() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    // `keeps_three`, oldest first. The newest holds the job's latest success; the one before it
    // still owes a callback. Neither may be pruned.
    let oldest = finished_chain(pool, "chain_rules", "keeps_three", &[50], false).await;
    let older = finished_chain(pool, "chain_rules", "keeps_three", &[40], false).await;
    let twice = finished_chain(pool, "chain_rules", "keeps_three", &[30, 29], false).await;
    let owes = finished_chain(pool, "chain_rules", "keeps_three", &[20], true).await;
    let latest = finished_chain(pool, "chain_rules", "keeps_three", &[10], false).await;

    downbeat::measure::prune(pool, definition()).await.unwrap();

    let kept = chains(pool).await;
    for (name, chain) in [("latest", latest), ("owes", owes), ("twice", twice)] {
        assert!(kept.contains(&chain), "the newest three chains stay: {name}");
    }
    for (name, chain) in [("older", older), ("oldest", oldest)] {
        assert!(!kept.contains(&chain), "chains beyond the newest three go: {name}");
    }
}
