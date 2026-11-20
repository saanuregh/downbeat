//! How a worker claims, prepares and decodes a run: urgency order, locked rows, its own jobs, one
//! preparation per run, and an input that no longer decodes.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

async fn queued_echo(client: &InAppClient, database: &Database, text: &str, minutes_ago: i32) -> Uuid {
    let run_id = client.start(echo, Text { text: text.into() }).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET run_at = now() - make_interval(mins => $2) WHERE run_id = $1")
        .bind(run_id)
        .bind(minutes_ago)
        .execute(&database.pool)
        .await
        .unwrap();
    run_id
}

fn echo_only() -> Vec<String> {
    vec!["echo".to_owned()]
}

#[tokio::test]
async fn among_equal_priorities_the_earliest_run_at_is_claimed_first() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let middle = queued_echo(&client, &database, "middle", 2).await;
    let earliest = queued_echo(&client, &database, "earliest", 3).await;
    let latest = queued_echo(&client, &database, "latest", 1).await;

    let mut order = Vec::new();
    for _ in 0..3 {
        order.push(downbeat::measure::claim(&database.pool, "default", &echo_only()).await.unwrap().unwrap().0);
    }
    assert_eq!(order, vec![earliest, middle, latest]);
}

#[tokio::test]
async fn a_row_locked_by_another_transaction_is_skipped_not_waited_on() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let locked = queued_echo(&client, &database, "locked", 2).await;
    let free = queued_echo(&client, &database, "free", 1).await;

    let mut holder = database.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM downbeat.run WHERE run_id = $1 FOR UPDATE")
        .bind(locked)
        .execute(&mut *holder)
        .await
        .unwrap();
    let claimed =
        tokio::time::timeout(Duration::from_secs(5), downbeat::measure::claim(&database.pool, "default", &echo_only()))
            .await
            .expect("the claim does not wait for the locked row")
            .unwrap();
    assert_eq!(claimed.map(|(run_id, _)| run_id), Some(free));
    holder.rollback().await.unwrap();

    let next = downbeat::measure::claim(&database.pool, "default", &echo_only()).await.unwrap();
    assert_eq!(next.map(|(run_id, _)| run_id), Some(locked), "once the lock is gone the row is claimable");
}

#[tokio::test]
async fn a_worker_never_claims_a_run_of_a_job_not_in_its_list() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    queued_echo(&client, &database, "not yours", 1).await;

    let others = vec!["noop".to_owned()];
    assert_eq!(downbeat::measure::claim(&database.pool, "default", &others).await.unwrap(), None);
    assert!(downbeat::measure::claim(&database.pool, "default", &echo_only()).await.unwrap().is_some());
}

#[tokio::test]
async fn a_run_whose_function_started_is_never_prepared_again() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let marker = std::env::temp_dir().join(format!("prepared-{}", Uuid::new_v4()));
    let text = marker.display().to_string();
    let conditions = format!("{text}.conditions");

    let app = App::start(&database);
    let run_id = client.start(prepared_once, Text { text: text.clone() }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run_id}' AND function_started_at IS NOT NULL)"
        ),
        SOON,
    )
    .await;
    app.kill();
    sqlx::query("UPDATE downbeat.run SET lease_expires_at = lease_expires_at - interval '1 hour' WHERE run_id = $1")
        .bind(run_id)
        .execute(&database.pool)
        .await
        .unwrap();

    let _app = App::start(&database);
    let ended = finished(&database.pool, run_id, Duration::from_secs(40)).await;
    let tested = std::fs::read_to_string(&conditions).unwrap_or_default();
    let _ = std::fs::remove_file(&conditions);
    assert_eq!(ended.status, "succeeded", "{ended:?}");
    assert_eq!((ended.starts, ended.lost_count), (2, 1), "the function was called again after the loss: {ended:?}");
    assert_eq!(tested.lines().count(), 1, "the condition was tested once: {tested:?}");
}

#[tokio::test]
async fn an_input_that_no_longer_decodes_fails_the_run_as_bad_input() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    database.hold("echo").await;
    let _app = App::start(&database);
    let run_id = client.start(echo, Text { text: "soon broken".into() }).await.unwrap().run_id();
    sqlx::query(r#"UPDATE downbeat.run SET input = '{"nope": 1}' WHERE run_id = $1 AND starts = 0"#)
        .bind(run_id)
        .execute(&database.pool)
        .await
        .unwrap();
    database.release().await;

    let ended = finished(&database.pool, run_id, SOON).await;
    assert_eq!(ended.status, "failed", "{ended:?}");
    assert_eq!(ended.failed_because.as_deref(), Some("bad_input"));
}
