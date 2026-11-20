//! The reaper ends what nothing else will: runs past their expiry or `give_up_after`, runs of a job
//! no worker has, and runs whose worker was lost twice. A worker cut off from its database stops.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

/// Pauses a job directly in the database, so its runs stay queued for the test to age.
async fn hold(database: &Database, job: &str) {
    sqlx::query("INSERT INTO downbeat.pause (kind, name, paused_by, note) VALUES ('job', $1, 'test', 'held')")
        .bind(job)
        .execute(&database.pool)
        .await
        .unwrap();
}

async fn queued_echo(client: &InAppClient, text: &str) -> Uuid {
    client.start(echo, Text { text: text.into() }).await.unwrap().run_id()
}

async fn set(database: &Database, sql: &str, run_id: Uuid) {
    sqlx::query(sql).bind(run_id).execute(&database.pool).await.unwrap();
}

#[tokio::test]
async fn a_queued_run_past_its_expiry_expires_but_a_started_one_never_does() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    hold(&database, "echo").await;
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
    hold(&database, "echo").await;
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
