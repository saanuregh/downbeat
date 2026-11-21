//! The reaper ends what nothing else will: runs past their expiry or `give_up_after`, runs of a job
//! no worker has, and runs whose worker was lost twice. A worker cut off from its database stops.
//! It also deletes expired used-token records. Callbacks older than a day are dropped.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

async fn queued_echo(client: &InAppClient, text: &str) -> Uuid {
    client.start(echo, Text { text: text.into() }).await.unwrap().run_id()
}

async fn set(database: &Database, sql: &'static str, run_id: Uuid) {
    sqlx::query(sql).bind(run_id).execute(&database.pool).await.unwrap();
}

#[tokio::test]
async fn a_queued_run_past_its_expiry_expires_but_a_started_one_never_does() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    database.hold("echo").await;
    let _app = App::start(&database);

    let never_started = queued_echo(&client, "never started").await;
    let started_once = queued_echo(&client, "started once").await;
    set(&database, "UPDATE downbeat.run SET expires_at = now() - interval '1 minute' WHERE run_id = $1", never_started)
        .await;
    set(
        &database,
        "UPDATE downbeat.run SET expires_at = now() - interval '1 minute', starts = 1 WHERE run_id = $1",
        started_once,
    )
    .await;

    let done = finished(&database.pool, never_started, SOON).await;
    assert_eq!(done.status, "expired", "{}", done.reason);
    let still_queued: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = $1)")
        .bind(started_once)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(still_queued, "a run whose function was called is never expired");
}

#[tokio::test]
async fn a_run_queued_past_give_up_after_fails_with_its_last_reason() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let missing = Text { text: format!("/nonexistent/downbeat-{}", Uuid::new_v4()) };
    let run = client.start(waits_for_file, missing).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND queued_because = 'not_ready')"),
        SOON,
    )
    .await;
    set(&database, "UPDATE downbeat.run SET created_at = now() - interval '25 hours' WHERE run_id = $1", run).await;

    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("gave_up")));
    assert!(done.reason.contains("is not there yet"), "the reason ends with why it was waiting: {}", done.reason);
}

#[tokio::test]
async fn a_run_of_a_job_no_worker_has_ends_after_an_hour() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    database.hold("echo").await;
    let _app = App::start(&database);
    eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'worker')", SOON).await;

    let run = queued_echo(&client, "from an older build").await;
    set(
        &database,
        "UPDATE downbeat.run SET job_name = 'retired_job', created_at = now() - interval '2 hours' WHERE run_id = $1",
        run,
    )
    .await;

    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("job_not_registered")));
}

#[tokio::test]
async fn a_run_whose_worker_is_lost_a_second_time_fails() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let first = App::start(&database);

    let run = client.start(counts, Seconds { seconds: 30 }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND function_started_at IS NOT NULL)"
        ),
        SOON,
    )
    .await;
    first.kill();
    // As if this were already its second lost worker, with the lease long gone.
    set(
        &database,
        "UPDATE downbeat.run SET lost_count = 1, lease_expires_at = now() - interval '1 hour' WHERE run_id = $1",
        run,
    )
    .await;

    let _second = App::start(&database);
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("worker_lost")));
}

#[tokio::test]
async fn a_worker_cut_off_from_its_database_stops_the_run_and_says_so_later() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(counts, Seconds { seconds: 30 }).await.unwrap().run_id();
    let checkpointed =
        format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND checkpoint IS NOT NULL)");
    eventually(&database.pool, &checkpointed, SOON).await;

    // Holding the run's row blocks every heartbeat, as a lost database would.
    let mut blocker = database.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM downbeat.run WHERE run_id = $1 FOR UPDATE")
        .bind(run)
        .execute(&mut *blocker)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    blocker.commit().await.unwrap();

    let reported = "SELECT EXISTS (SELECT 1 FROM downbeat.audit
                     WHERE action = 'event' AND detail->>'event' = 'database_unreachable')";
    eventually(&database.pool, reported, Duration::from_secs(30)).await;
    let ended: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE run_id = $1)")
        .bind(run)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(!ended, "the cut-off worker recorded nothing for the run");
}

#[tokio::test]
async fn a_queued_run_with_a_cancel_requested_is_ended_by_the_reaper() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    database.hold("echo").await;
    let _app = App::start(&database);

    let run = queued_echo(&client, "unwanted").await;
    set(&database, "UPDATE downbeat.run SET cancel_requested_by = 'asha' WHERE run_id = $1", run).await;
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.reason.as_str()), ("cancelled", "Cancelled by asha."));
}

#[tokio::test]
async fn events_reach_on_event_and_one_that_does_not_decode_is_skipped() {
    let database = Database::fresh().await;
    let app = App::start(&database);
    let insert = |detail: serde_json::Value| {
        let pool = database.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "INSERT INTO downbeat.audit (actor, action, detail, notify_after)
                 VALUES ('engine', 'event', $1, now()) RETURNING audit_id",
            )
            .bind(detail)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let known = insert(serde_json::json!({ "event": "database_unreachable", "worker": "w-1" })).await;
    let unknown = insert(serde_json::json!({ "event": "something_from_a_newer_build" })).await;

    let delivered = |id: i64| {
        format!("SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE audit_id = {id} AND notify_after IS NULL)")
    };
    eventually(&database.pool, &delivered(known), SOON).await;
    eventually(&database.pool, &delivered(unknown), SOON).await;
    assert_eq!(
        std::fs::read_to_string(app.finished_dir.join(format!("event-{known}"))).unwrap(),
        "database_unreachable"
    );
    assert!(
        !app.finished_dir.join(format!("event-{unknown}")).exists(),
        "an event this build does not know is skipped"
    );
}

#[tokio::test]
async fn a_run_whose_worker_died_before_its_function_started_is_queued_again_with_nothing_counted() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let run = queued_echo(&client, "claimed by a worker that died").await;
    // As if a worker had claimed it and died while preparing it.
    set(
        &database,
        "UPDATE downbeat.run SET state = 'running', fence = fence + 1, lease_owner = 'gone-1',
                lease_expires_at = now() - interval '1 hour', claimed_at = now() - interval '1 hour'
          WHERE run_id = $1",
        run,
    )
    .await;

    let _app = App::start(&database);
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!((done.lost_count, done.failures, done.starts), (0, 0, 1));
    let reported: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE detail->>'event' = 'worker_lost' AND run_id = $1)",
    )
    .bind(run)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(!reported, "no worker_lost event for a function that never started");
}

#[tokio::test]
async fn a_worker_lost_after_the_function_started_is_reported() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let first = App::start(&database);

    let run = client.start(counts, Seconds { seconds: 3 }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND function_started_at IS NOT NULL)"
        ),
        SOON,
    )
    .await;
    first.kill();
    set(&database, "UPDATE downbeat.run SET lease_expires_at = now() - interval '1 hour' WHERE run_id = $1", run).await;

    let _second = App::start(&database);
    let reported = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event'
          AND detail->>'event' = 'worker_lost' AND run_id = '{run}')"
    );
    eventually(&database.pool, &reported, SOON).await;
    let done = finished(&database.pool, run, Duration::from_secs(30)).await;
    assert_eq!((done.status.as_str(), done.lost_count), ("succeeded", 1), "{}", done.reason);
}

#[tokio::test]
async fn the_reaper_deletes_expired_used_token_records() {
    let database = Database::fresh().await;
    sqlx::query(
        "INSERT INTO downbeat.used_token (caller, jti, expires_at)
         VALUES ('billing', 'expired', now() - interval '1 minute'), ('billing', 'current', now() + interval '1 hour')",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let _app = App::start(&database);

    eventually(&database.pool, "SELECT NOT EXISTS (SELECT 1 FROM downbeat.used_token WHERE jti = 'expired')", SOON)
        .await;
    let kept: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.used_token WHERE jti = 'current')")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(kept, "a record that has not expired stays");
}

#[tokio::test]
async fn an_on_finish_delivery_for_a_run_that_finished_over_a_day_ago_is_dropped() {
    let database = Database::fresh().await;
    let chain = Uuid::new_v4();
    sqlx::query("INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by) VALUES ($1, 'basics', 'echo', 'person')")
        .bind(chain)
        .execute(&database.pool)
        .await
        .unwrap();
    let (stale, recent) = (Uuid::new_v4(), Uuid::new_v4());
    for (run, finished_ago) in [(stale, "25 hours"), (recent, "1 minute")] {
        sqlx::query(
            "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id,
                    created_at, status, reason, finished_at, notify_after)
             VALUES ($1, 'echo', 'default', 0, now(), 'person', $2, now() - $3::interval, 'succeeded', 'Succeeded.',
                     now() - $3::interval, now())",
        )
        .bind(run)
        .bind(chain)
        .bind(finished_ago)
        .execute(&database.pool)
        .await
        .unwrap();
    }
    let app = App::start(&database);

    for run in [stale, recent] {
        let delivered = format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE run_id = '{run}' AND notify_after IS NULL)"
        );
        eventually(&database.pool, &delivered, SOON).await;
    }
    assert!(app.finished_dir.join(recent.to_string()).exists(), "a recent run is delivered");
    assert!(!app.finished_dir.join(stale.to_string()).exists(), "a run that finished over a day ago is dropped");
}
