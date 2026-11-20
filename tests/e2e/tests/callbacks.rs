//! Callback delivery when the definition has no callbacks.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, finished};

#[tokio::test]
async fn without_on_finish_or_on_event_pending_rows_are_marked_delivered() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    // A worker alone delivers no callbacks, so the history row stays pending.
    let app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    let run = client.start(echo, Text { text: "pending".into() }).await.unwrap().run_id();
    finished(&database.pool, run, SOON).await;
    app.stop();

    sqlx::query("UPDATE downbeat.run_history SET notify_after = now() WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    let event: i64 = sqlx::query_scalar(
        "INSERT INTO downbeat.audit (actor, action, detail, notify_after)
         VALUES ('test', 'event', '{\"event\": \"database_unreachable\", \"worker\": \"w-1\"}', now())
         RETURNING audit_id",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();

    downbeat::measure::deliver_callbacks(&database.pool, definition_without_callbacks()).await.unwrap();

    let history_pending: bool =
        sqlx::query_scalar("SELECT notify_after IS NOT NULL FROM downbeat.run_history WHERE run_id = $1")
            .bind(run)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    let event_pending: bool =
        sqlx::query_scalar("SELECT notify_after IS NOT NULL FROM downbeat.audit WHERE audit_id = $1")
            .bind(event)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert!(!history_pending, "the history row is marked delivered");
    assert!(!event_pending, "the event is marked delivered");
}
