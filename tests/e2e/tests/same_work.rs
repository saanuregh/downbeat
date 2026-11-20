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

#[tokio::test]
async fn a_schedule_that_joins_queued_work_leaves_hidden_rows_that_are_not_delivered_and_go_soon() {
    let database = Database::fresh().await;
    database.hold("ticker").await;
    let (app, client, _) = support::with_api_in(&database, "schedules").await;

    // Each second the schedule asks for the same work again, and joins the one held run.
    let joined =
        "SELECT count(*) >= 2 FROM downbeat.run_history WHERE job_name = 'ticker' AND skipped_because = 'joined'";
    support::eventually(&database.pool, joined, SOON).await;
    let held: uuid::Uuid = sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE job_name = 'ticker'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let rows: Vec<(uuid::Uuid, Option<uuid::Uuid>, bool)> = sqlx::query_as(
        "SELECT run_id, joined_run, notify_after IS NOT NULL FROM downbeat.run_history
          WHERE job_name = 'ticker' AND skipped_because = 'joined'",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    for (run_id, joined_run, to_deliver) in &rows {
        assert_eq!(*joined_run, Some(held), "each row names the run it joined");
        assert!(!to_deliver, "a joined row is not sent to on_finish");
        assert!(!app.finished_dir.join(run_id.to_string()).exists());
    }

    let listed = client.runs("job=ticker").await.unwrap();
    assert!(
        listed.items.iter().all(|run| run.skipped_because.as_deref() != Some("joined")),
        "the runs list hides joined rows"
    );

    sqlx::query(
        "UPDATE downbeat.run_history SET finished_at = now() - interval '3 days' WHERE skipped_because = 'joined'",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    downbeat::measure::prune(&database.pool, definition()).await.unwrap();
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM downbeat.run_history WHERE skipped_because = 'joined' AND finished_at < now() - interval '2 days'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(left, 0, "joined rows go after two days");
}

#[tokio::test]
async fn a_join_keeps_the_later_due_time_and_the_looser_expiry() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let input = Text { text: "merged".into() };
    let first = client.start(echo, input.clone()).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET due_at = now() - interval '1 hour', expires_at = now() + interval '1 hour' WHERE run_id = $1")
        .bind(first)
        .execute(&database.pool)
        .await
        .unwrap();

    let joined = client.start(echo, input).await.unwrap();
    assert_eq!(joined, Started::Joined(first));
    let (later_due, expiry): (bool, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT due_at > now() - interval '1 minute', expires_at FROM downbeat.run WHERE run_id = $1")
            .bind(first)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert!(later_due, "the join's later due time wins");
    assert_eq!(expiry, None, "a request that never expires lifts the expiry");
}

#[tokio::test]
async fn a_linked_run_has_no_turn_until_it_is_prepared() {
    let database = Database::fresh().await;
    database.hold("shout").await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let parent = client.start(echo, Text { text: "quiet".into() }).await.unwrap();
    finished(&database.pool, parent.run_id(), SOON).await;
    support::eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = 'shout')", SOON)
        .await;
    let (turn_key, input): (Option<String>, Option<serde_json::Value>) =
        sqlx::query_as("SELECT turn_key, input FROM downbeat.run WHERE job_name = 'shout'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!((turn_key, input), (None, None), "its input, and so its turn, come from its parents when it runs");
}

#[tokio::test]
async fn a_job_that_starts_work_already_queued_joins_it() {
    let database = Database::fresh().await;
    database.hold("echo").await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let queued = client.start(echo, Text { text: "started by asha".into() }).await.unwrap().run_id();
    let _app = App::start(&database);

    let starter_run = client.start(starter, Text { text: "asha".into() }).await.unwrap().run_id();
    let done = finished(&database.pool, starter_run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.output, Some(serde_json::json!({ "text": queued.to_string() })), "run.start joined the queued run");
    let echo_runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE job_name = 'echo'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(echo_runs, 1);
}

#[tokio::test]
async fn each_deliberate_request_creates_a_run_instead_of_joining() {
    let database = Database::fresh().await;
    for job in ["echo", "rehearses"] {
        database.hold(job).await;
    }
    let (_app, _client, listen) = support::with_api_at(&database).await;
    let local = support::Signer::local(&format!("http://{listen}"));
    let start = |job: &'static str, extra: serde_json::Value| {
        let local = &local;
        async move {
            let mut body = serde_json::json!({ "input": { "text": "same" } });
            body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            local.post::<downbeat_client::types::StartResponse>(&format!("/v1/jobs/{job}/runs"), &body).await
        }
    };

    for job in ["echo", "rehearses"] {
        let first = start(job, serde_json::json!({})).await;
        let again = start(job, serde_json::json!({})).await;
        assert!(again.joined && again.run_id == first.run_id, "a plain request for {job} joins: {again:?}");
    }
    let not_before = (chrono::Utc::now() + chrono::TimeDelta::hours(1)).to_rfc3339();
    let deliberate = [
        ("rehearses", serde_json::json!({ "dry_run": true })),
        ("echo", serde_json::json!({ "without_children": true })),
        ("echo", serde_json::json!({ "ignore_conditions": true })),
        ("echo", serde_json::json!({ "not_before": not_before })),
        ("echo", serde_json::json!({ "timeout_seconds": 30 })),
    ];
    for (job, flag) in deliberate {
        let started = start(job, flag.clone()).await;
        assert!(!started.joined, "{job} with {flag} joined queued work");
    }
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE job_name IN ('echo', 'rehearses')")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(runs, 2 + 5, "each deliberate request made its own run");
}
