//! "The same work is done once": requests join queued work, and the same work never runs twice at
//! once.

mod support;

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
    let again =
        client.start(echo, Text { text: "a".into() }).idempotency_key("order-1").even_if_queued().await.unwrap();
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

    assert!(!overlapped(&database, "single", "true").await, "two runs of a one-at-a-time job overlapped");
}

/// Whether two finished runs of `job` that match the SQL condition `which` (on `x` and `y`) ran
/// at the same time.
async fn overlapped(database: &Database, job: &str, which: &str) -> bool {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history x, downbeat.run_history y
          WHERE x.job_name = $1 AND y.job_name = $1 AND x.run_id < y.run_id AND ({which})
            AND x.started_at < y.finished_at AND y.started_at < x.finished_at)"
    ))
    .bind(job)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn the_same_work_started_twice_on_purpose_runs_in_turn() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let input = Seconds { seconds: 1 };
    let a = client.start(sleeps, input.clone()).await.unwrap();
    let b = client.start(sleeps, input).even_if_queued().await.unwrap();
    assert_ne!(a.run_id(), b.run_id());
    for run in [a.run_id(), b.run_id()] {
        assert_eq!(finished(&database.pool, run, SOON).await.status, "succeeded");
    }
    assert!(!overlapped(&database, "sleeps", "true").await, "the same input ran twice at once");
}

#[tokio::test]
async fn one_per_keeps_runs_for_one_value_apart_and_lets_others_run() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let work = |account: &str, seconds| AccountWork { account: account.into(), seconds };
    let runs = [
        client.start(per_account, work("a", 2)).await.unwrap(),
        client.start(per_account, work("b", 2)).await.unwrap(),
        client.start(per_account, work("a", 1)).await.unwrap(),
    ];
    for run in &runs {
        assert_eq!(finished(&database.pool, run.run_id(), SOON).await.status, "succeeded");
    }
    let same_account = "x.input->>'account' = y.input->>'account'";
    assert!(!overlapped(&database, "per_account", same_account).await, "two runs for account a overlapped");
    let other_account = "x.input->>'account' <> y.input->>'account'";
    assert!(overlapped(&database, "per_account", other_account).await, "account b waited for account a");
}
