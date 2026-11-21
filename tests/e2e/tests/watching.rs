//! Watching (spec 16.1–16.5): slow runs and overdue jobs are told once. The health endpoints and
//! command are in `health.rs`.

mod support;

use std::time::Duration;

use serde_json::Value;
use sqlx::PgPool;
use support::{App, Database, SOON};
use uuid::Uuid;

/// The events of this kind recorded for this job, oldest first.
async fn events(pool: &PgPool, event: &str, job: &str) -> Vec<Value> {
    sqlx::query_scalar(
        "SELECT detail FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = $1 AND job_name = $2
          ORDER BY at, audit_id",
    )
    .bind(event)
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn watch(pool: &PgPool) {
    downbeat::measure::watch_once(pool, downbeat_e2e::definition()).await.unwrap();
}

async fn new_chain(pool: &PgPool, job: &str) -> Uuid {
    let chain = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by) VALUES ($1, 'test', $2, 'person')",
    )
    .bind(chain)
    .bind(job)
    .execute(pool)
    .await
    .unwrap();
    chain
}

/// A finished run of `job` that began `ago` seconds ago and took `took` seconds.
async fn finished_run(pool: &PgPool, job: &str, status: &str, ago: f64, took: f64) -> Uuid {
    let chain = new_chain(pool, job).await;
    let run_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id,
                created_at, status, reason, started_at, finished_at)
         VALUES ($1, $2, 'default', 0, now(), 'person', $3, now() - make_interval(secs => $4), $5, 'Test.',
                 now() - make_interval(secs => $4), now() - make_interval(secs => $4) + make_interval(secs => $6))",
    )
    .bind(run_id)
    .bind(job)
    .bind(chain)
    .bind(ago)
    .bind(status)
    .bind(took)
    .execute(pool)
    .await
    .unwrap();
    run_id
}

/// A run of `job` whose function began `ago` seconds ago and is still running.
async fn running_run(pool: &PgPool, job: &str, ago: f64) -> Uuid {
    let chain = new_chain(pool, job).await;
    let run_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.run (run_id, job_name, queue, state, due_at, started_by, chain_id, starts,
                function_started_at, first_started_at)
         VALUES ($1, $2, 'default', 'running', now(), 'person', $3, 1,
                 now() - make_interval(secs => $4), now() - make_interval(secs => $4))",
    )
    .bind(run_id)
    .bind(job)
    .bind(chain)
    .bind(ago)
    .execute(pool)
    .await
    .unwrap();
    run_id
}

/// 16.1: with nothing but a running scheduler, an overdue job is reported within one period.
#[tokio::test]
async fn the_scheduler_watches_by_itself() {
    let database = Database::fresh().await;
    let _app = App::with(&database, &["scheduler", "worker", "--queues", "default=1"], "test");
    support::eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'scheduler')", SOON)
        .await;
    // Past the pass the scheduler makes as it starts, so only a later pass can report this.
    tokio::time::sleep(Duration::from_secs(2)).await;
    finished_run(&database.pool, "expected_often", "failed", 60.0, 0.1).await;
    support::eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = 'job_overdue'
          AND job_name = 'expected_often')",
        Duration::from_secs(40),
    )
    .await;
}

/// 16.2: fewer than five successes give no slow threshold.
#[tokio::test]
async fn a_job_with_fewer_than_five_successes_is_never_slow() {
    let database = Database::fresh().await;
    for n in 0..4 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 1.0).await;
    }
    // Failures do not count towards the five.
    finished_run(&database.pool, "echo", "failed", 90.0, 1.0).await;
    running_run(&database.pool, "echo", 30.0).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "run_slow", "echo").await.is_empty());
}

/// 16.2 and 16.3: past 1.5 × p90 of the last successes, a run is slow, once.
#[tokio::test]
async fn a_run_past_its_slow_threshold_is_told_once() {
    let database = Database::fresh().await;
    for n in 0..5 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 1.0).await;
    }
    // Well within the threshold of 1.5 seconds, even after two slow passes.
    running_run(&database.pool, "echo", 0.1).await;
    let slow = running_run(&database.pool, "echo", 5.0).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "run_slow", "echo").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert_eq!(told[0]["run_id"], slow.to_string());
    assert!(told[0]["running_seconds"].as_i64().unwrap() >= 5, "{told:?}");
    assert_eq!(told[0]["expected_seconds"], 1);
}

/// 16.2: the expected duration is the median and the threshold 1.5 × p90, not the mean.
#[tokio::test]
async fn the_expected_duration_is_the_median_and_the_threshold_follows_p90() {
    let database = Database::fresh().await;
    // Median 1 s, mean 2.8 s, p90 6.4 s, so the threshold is 9.6 s.
    for (ago, took) in [(100.0, 1.0), (101.0, 1.0), (102.0, 1.0), (103.0, 1.0), (104.0, 10.0)] {
        finished_run(&database.pool, "echo", "succeeded", ago, took).await;
    }
    running_run(&database.pool, "echo", 7.0).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "run_slow", "echo").await.is_empty(), "7 s is within 1.5 × p90");
    running_run(&database.pool, "echo", 12.0).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "run_slow", "echo").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert_eq!(told[0]["expected_seconds"], 1, "the median, not the mean");
}

/// 16.2: only the last 20 successes count.
#[tokio::test]
async fn only_the_last_twenty_successes_count() {
    let database = Database::fresh().await;
    for n in 0..10 {
        finished_run(&database.pool, "echo", "succeeded", 1000.0 + f64::from(n), 100.0).await;
    }
    for n in 0..20 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 1.0).await;
    }
    running_run(&database.pool, "echo", 5.0).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "run_slow", "echo").await;
    assert_eq!(told.len(), 1, "the older 100-second runs would put the threshold past 5 s: {told:?}");
    assert_eq!(told[0]["expected_seconds"], 1);
}

/// 16.2: the slow threshold is at least one second, however quick the job usually is.
#[tokio::test]
async fn the_slow_threshold_is_at_least_one_second() {
    let database = Database::fresh().await;
    for n in 0..5 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 0.1).await;
    }
    // Past 1.5 × 0.1 seconds, but within the one-second floor.
    let run_id = running_run(&database.pool, "echo", 0.4).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "run_slow", "echo").await.is_empty());
    sqlx::query("UPDATE downbeat.run SET function_started_at = now() - interval '2 seconds' WHERE run_id = $1")
        .bind(run_id)
        .execute(&database.pool)
        .await
        .unwrap();
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "run_slow", "echo").await.len(), 1);
}

/// 16.4: a job that has never run is never overdue; one that never succeeded is, from its first run.
#[tokio::test]
async fn overdue_counts_from_the_first_run_when_nothing_succeeded() {
    let database = Database::fresh().await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "it never ran");

    finished_run(&database.pool, "expected_often", "failed", 1.0, 0.1).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "its first run is recent");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "job_overdue", "expected_often").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert_eq!(told[0]["last_success"], Value::Null);
}

/// 16.4 and 16.5: overdue since the latest success, once per lapse, and again after a new lapse.
#[tokio::test]
async fn overdue_is_told_once_per_lapse() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    finished_run(&database.pool, "expected_often", "succeeded", 1.0, 0.1).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "it succeeded recently");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "job_overdue", "expected_often").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert!(told[0]["last_success"].is_string(), "{told:?}");

    // Finished now, after the event, as a real success would be.
    finished_run(&database.pool, "expected_often", "succeeded", 0.0, 0.0).await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1, "it just succeeded");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 2, "a new lapse");
}

/// 16.5: a paused job is not told overdue.
#[tokio::test]
async fn a_paused_job_is_not_told_overdue() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    database.hold("expected_often").await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty());
    database.release().await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1);
}

/// 16.5: a pause of the job's queue or of everything quiets it too, as it holds back its runs.
#[tokio::test]
async fn a_job_whose_queue_or_everything_is_paused_is_not_told_overdue() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    for (kind, name) in [("queue", "default"), ("all", "")] {
        sqlx::query("INSERT INTO downbeat.pause (kind, name, paused_by, note) VALUES ($1, $2, 'test', 'held')")
            .bind(kind)
            .bind(name)
            .execute(&database.pool)
            .await
            .unwrap();
        watch(&database.pool).await;
        assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "paused by {kind}");
        database.release().await;
    }
}

/// 16.4: a run still queued counts as the first run, so a job whose runs never finish is overdue.
#[tokio::test]
async fn a_job_whose_only_run_never_finishes_is_overdue_too() {
    let database = Database::fresh().await;
    // No app is running, so the run is never claimed.
    let run = queued_run(&database).await;
    sqlx::query(
        "UPDATE downbeat.run SET created_at = now() - interval '3 seconds', run_at = now() - interval '3 seconds'
          WHERE run_id = $1",
    )
    .bind(run)
    .execute(&database.pool)
    .await
    .unwrap();
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1);
}

/// 16.4: a run started for later counts from when it may first start, not from when it was asked for.
#[tokio::test]
async fn a_run_started_for_later_is_not_overdue_before_it_is_due() {
    let database = Database::fresh().await;
    let run = queued_run(&database).await;
    sqlx::query(
        "UPDATE downbeat.run SET created_at = now() - interval '3 seconds', run_at = now() + interval '1 hour'
          WHERE run_id = $1",
    )
    .bind(run)
    .execute(&database.pool)
    .await
    .unwrap();
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty());
}

/// 16.4: a dry run does no work, so it is not the success the job is waiting for.
#[tokio::test]
async fn a_dry_run_does_not_end_a_lapse() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    let dry = finished_run(&database.pool, "expected_often", "succeeded", 0.2, 0.1).await;
    sqlx::query("UPDATE downbeat.run_history SET dry_run = true WHERE run_id = $1")
        .bind(dry)
        .execute(&database.pool)
        .await
        .unwrap();
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1);
}

/// 16.4 and 16.5: a run marked as succeeded is a success from when it was marked, so the next
/// lapse is told even though the run itself ended before the last event.
#[tokio::test]
async fn a_lapse_after_a_marked_success_is_told() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    let failed = finished_run(&database.pool, "expected_often", "failed", 0.6, 0.1).await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1);

    sqlx::query(
        "UPDATE downbeat.run_history SET status = 'succeeded', marked_by = 'test', marked_at = now() WHERE run_id = $1",
    )
    .bind(failed)
    .execute(&database.pool)
    .await
    .unwrap();
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1, "it was just marked");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 2, "a new lapse");
}

/// 16.5: schedulers watching at the same moment tell a lapse once between them. Repeated, with
/// the watches on separate threads, because a race shows only some of the time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedulers_watching_at_once_tell_a_lapse_once() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    for attempt in 0..5 {
        sqlx::query("DELETE FROM downbeat.audit").execute(&database.pool).await.unwrap();
        let watches: Vec<_> = (0..4)
            .map(|_| {
                let pool = database.pool.clone();
                tokio::spawn(async move { watch(&pool).await })
            })
            .collect();
        for watch in watches {
            watch.await.unwrap();
        }
        assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1, "attempt {attempt}");
    }
}

/// A run of `expected_often`, queued and never claimed.
async fn queued_run(database: &Database) -> Uuid {
    let client = downbeat::InAppClient::connect(downbeat_e2e::definition, &database.url).await.unwrap();
    client.start(downbeat_e2e::runtime_rules::expected_often, ()).await.unwrap().run_id()
}
