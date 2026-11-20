//! Grace periods: a child cut short while preparing is never reused, the reaper waits
//! `kill_grace + 10 s` past an expired lease, and a call that ends by itself while being stopped is
//! recorded as it ended.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

fn running_function(run: Uuid) -> String {
    format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND function_started_at IS NOT NULL)")
}

#[tokio::test]
async fn a_child_killed_while_preparing_is_never_reused() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let pids = std::env::temp_dir().join(format!("children-{}", Uuid::new_v4()));

    let run = client.start(records_its_child, Text { text: pids.display().to_string() }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND queued_because = 'backing_off')"
        ),
        SOON,
    )
    .await;
    // The second attempt need not wait out the 30-second backoff.
    sqlx::query("UPDATE downbeat.run SET run_at = now() WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    let done = finished(&database.pool, run, SOON).await;
    let recorded = std::fs::read_to_string(&pids).unwrap_or_default();
    let _ = std::fs::remove_file(&pids);

    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let children: Vec<&str> = recorded.lines().collect();
    assert_eq!(children.len(), 2, "prepared once per attempt: {children:?}");
    assert_ne!(children[0], children[1], "the second attempt ran in another child");
}

#[tokio::test]
async fn the_reaper_waits_kill_grace_and_ten_seconds_after_a_lease_expires() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let first = App::start(&database);

    let run = client.start(counts, Seconds { seconds: 60 }).await.unwrap().run_id();
    eventually(&database.pool, &running_function(run), SOON).await;
    first.kill();
    let _second = App::start(&database);
    eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'worker')", SOON).await;

    // Five seconds past the lease, held there for longer than one reaper period (five seconds),
    // is inside the margin of kill_grace (1 s) + 10 s.
    for _ in 0..7 {
        sqlx::query("UPDATE downbeat.run SET lease_expires_at = now() - interval '5 seconds' WHERE run_id = $1")
            .bind(run)
            .execute(&database.pool)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (state, lost_count): (String, i16) =
            sqlx::query_as("SELECT state, lost_count FROM downbeat.run WHERE run_id = $1")
                .bind(run)
                .fetch_one(&database.pool)
                .await
                .unwrap();
        assert_eq!((state.as_str(), lost_count), ("running", 0), "taken back inside the margin");
    }

    sqlx::query("UPDATE downbeat.run SET lease_expires_at = now() - interval '20 seconds' WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND lost_count = 1)"),
        SOON,
    )
    .await;
}

#[tokio::test]
async fn a_function_that_ends_during_the_grace_period_is_recorded_as_it_ended() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let run = client.start(naps, Number { n: 1500 }).await.unwrap().run_id();
    eventually(&database.pool, &running_function(run), SOON).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Drain (1 s), then the stop is asked and ignored, then kill_grace (1 s): the nap ends first.
    app.stop();

    let done = finished(&database.pool, run, Duration::from_secs(1)).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!((done.failures, done.interruptions), (0, 0));
}

#[tokio::test]
async fn a_command_that_fails_because_it_was_asked_to_stop_counts_as_stopped() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let run = client.start(fails_when_asked_to_stop, ()).await.unwrap().run_id();
    eventually(&database.pool, &running_function(run), SOON).await;
    app.stop();

    let (state, because, failures, interruptions): (String, Option<String>, i16, i16) =
        sqlx::query_as("SELECT state, queued_because, failures, interruptions FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .fetch_optional(&database.pool)
            .await
            .unwrap()
            .expect("the run is not in the history: exit 3 after SIGTERM must not end it");
    assert_eq!(state, "queued");
    assert_eq!(because.as_deref(), Some("no_free_worker"));
    assert_eq!((failures, interruptions), (0, 1), "a stop, not a failure");
}
