//! "The same work is done once": requests join queued work, and the same work never runs twice at
//! once.

mod support;

use std::time::Duration;

use downbeat::{InAppClient, Started};
use downbeat_e2e::*;
use support::{App, Database, SOON, finished};

#[tokio::test]
async fn a_request_for_work_already_queued_joins_it() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();

    let input = Text { text: "once".into() };
    let first = client.start(echo, input.clone()).await.unwrap();
    let second = client.start(echo, input.clone()).await.unwrap();
    assert!(matches!(first, Started::New(_)));
    assert_eq!(second, Started::Joined(first.run_id()));

    let deliberate = client.start(echo, input).even_if_queued().await.unwrap();
    assert!(matches!(deliberate, Started::New(id) if id != first.run_id()));

    let other = client.start(echo, Text { text: "different".into() }).await.unwrap();
    assert!(matches!(other, Started::New(id) if id != first.run_id()));
}

#[tokio::test]
async fn an_idempotency_key_returns_the_first_run() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();

    let first = client.start(echo, Text { text: "a".into() }).idempotency_key("order-1").await.unwrap();
    let again = client.start(echo, Text { text: "a".into() }).idempotency_key("order-1").even_if_queued().await.unwrap();
    assert_eq!(first.run_id(), again.run_id());
}

#[tokio::test]
async fn a_job_that_runs_one_at_a_time_never_overlaps() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let a = client.start(single, Seconds { seconds: 2 }).await.unwrap();
    let b = client.start(single, Seconds { seconds: 1 }).await.unwrap();
    let a = finished(&database.pool, a.run_id(), SOON).await;
    let b = finished(&database.pool, b.run_id(), SOON).await;
    assert_eq!((a.status.as_str(), b.status.as_str()), ("succeeded", "succeeded"));

    let overlapping: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history x, downbeat.run_history y
          WHERE x.job_name = 'single' AND y.job_name = 'single' AND x.run_id < y.run_id
            AND x.started_at < y.finished_at AND y.started_at < x.finished_at)",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(!overlapping, "two runs of a one-at-a-time job overlapped");
    let _ = Duration::ZERO;
}
