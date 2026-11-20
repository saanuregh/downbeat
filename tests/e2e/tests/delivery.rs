//! Callback delivery with a callback that is slow or refuses, and with rows too old to deliver.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};

#[tokio::test]
async fn a_row_is_pushed_five_minutes_ahead_while_its_callback_runs() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    std::fs::write(app.finished_dir.join("slow"), "").unwrap();

    let run = client.start(echo, Text { text: "slow".into() }).await.unwrap().run_id();
    finished(&database.pool, run, SOON).await;
    eventually(
        &database.pool,
        &format!("SELECT notify_after > now() + interval '4 minutes' FROM downbeat.run_history WHERE run_id = '{run}'"),
        SOON,
    )
    .await;
    let delivered = app.finished_dir.join(run.to_string());
    assert!(!delivered.exists(), "the callback was still running when the row was pushed ahead");

    eventually(
        &database.pool,
        &format!("SELECT notify_after IS NULL FROM downbeat.run_history WHERE run_id = '{run}'"),
        SOON,
    )
    .await;
    assert_eq!(std::fs::read_to_string(&delivered).unwrap(), "succeeded");
}

#[tokio::test]
async fn a_refused_delivery_comes_round_again_after_five_minutes() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    std::fs::write(app.finished_dir.join("refuse"), "").unwrap();

    let run = client.start(echo, Text { text: "refused".into() }).await.unwrap().run_id();
    finished(&database.pool, run, SOON).await;
    eventually(
        &database.pool,
        &format!(
            "SELECT notify_after BETWEEN now() + interval '4 minutes' AND now() + interval '6 minutes'
             FROM downbeat.run_history WHERE run_id = '{run}'"
        ),
        SOON,
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let still_ahead: bool = sqlx::query_scalar(
        "SELECT notify_after BETWEEN now() + interval '4 minutes' AND now() + interval '6 minutes'
         FROM downbeat.run_history WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(still_ahead, "the refused row stays pending, five minutes ahead");
    assert!(!app.finished_dir.join(run.to_string()).exists(), "nothing was delivered");
}

#[tokio::test]
async fn an_event_recorded_more_than_a_day_ago_is_dropped_without_a_call() {
    let database = Database::fresh().await;
    let app = App::start(&database);
    let event: i64 = sqlx::query_scalar(
        "INSERT INTO downbeat.audit (at, actor, action, detail, notify_after)
         VALUES (now() - interval '2 days', 'test', 'event',
                 '{\"event\": \"database_unreachable\", \"worker\": \"w\"}', now())
         RETURNING audit_id",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();

    eventually(
        &database.pool,
        &format!("SELECT notify_after IS NULL FROM downbeat.audit WHERE audit_id = {event}"),
        SOON,
    )
    .await;
    assert!(!app.finished_dir.join(format!("event-{event}")).exists(), "the old event was not delivered");
}
