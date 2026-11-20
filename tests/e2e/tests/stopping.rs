//! Stopping a run: a cancel asks the function first, and a shutdown during preparing counts nothing.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished, with_api};
use uuid::Uuid;

#[tokio::test]
async fn a_cancel_asks_the_function_to_stop_before_killing_it() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let marker = std::env::temp_dir().join(format!("asked-{}", Uuid::new_v4()));

    let started = client.start("stops_when_asked", &Text { text: marker.display().to_string() }).send().await.unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts > 0)", started.run_id),
        SOON,
    )
    .await;
    client.cancel(started.run_id).await.unwrap();

    let done = finished(&database.pool, started.run_id, SOON).await;
    let asked = marker.exists();
    let _ = std::fs::remove_file(&marker);
    assert_eq!(done.status, "cancelled", "{}", done.reason);
    assert!(asked, "the function was asked to stop and had time to answer");
}

#[tokio::test]
async fn a_run_being_prepared_at_shutdown_goes_back_to_the_queue_uncounted() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let run = client.start(slow_to_prepare, Text { text: "slowly".into() }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'running')"),
        SOON,
    )
    .await;
    // Its condition takes five seconds, so the run is still being prepared when the stop comes.
    tokio::time::sleep(Duration::from_secs(1)).await;
    app.stop();

    let (state, because, due_now, starts, failures, interruptions): (String, Option<String>, bool, i32, i16, i16) =
        sqlx::query_as(
            "SELECT state, queued_because, run_at <= now(), starts, failures, interruptions
               FROM downbeat.run WHERE run_id = $1",
        )
        .bind(run)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(state, "queued", "the run went back to the queue");
    assert_eq!(because.as_deref(), Some("no_free_worker"), "it was stopped while being prepared");
    assert!(due_now, "the next worker may take it at once");
    assert_eq!((starts, failures, interruptions), (0, 0, 0), "nothing was counted");
}
