//! Conditions and needs: waiting or failing on an unmet need, a need met only by a recent success,
//! nothing tested under `ignore_conditions`, the order conditions are checked in, and `if_changed`
//! per turn key.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_client::types::StartResponse;
use downbeat_e2e::condition_rules::*;
use downbeat_e2e::{Number, Text, definition};
use serde_json::json;
use support::{App, Database, SOON, Signer, eventually, finished, with_api_at};
use uuid::Uuid;

fn unique(prefix: &str) -> String {
    std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4())).display().to_string()
}

#[tokio::test]
async fn an_unmet_need_with_or_wait_requeues_the_run_until_the_need_is_met() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let input = Text { text: "wanted".into() };

    let run = client.start(waits_for_need, input.clone()).await.unwrap().run_id();
    let waiting = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'queued'
           AND queued_because = 'need_not_met' AND failures = 0 AND starts = 0)"
    );
    eventually(&database.pool, &waiting, SOON).await;
    let first_due: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT run_at FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    // Checked again after recheck_every (one second): the due time moves on, and nothing is counted.
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND queued_because = 'need_not_met'
               AND failures = 0 AND starts = 0 AND run_at > '{}')",
            first_due.to_rfc3339()
        ),
        SOON,
    )
    .await;

    client.start(needed_for, input).await.unwrap();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!((done.failures, done.starts), (0, 1), "waiting counted nothing");
}

#[tokio::test]
async fn an_unmet_need_with_or_fail_fails_the_run_as_need_not_met() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(fails_without_need, Text { text: "never done".into() }).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "failed", "{}", done.reason);
    assert_eq!(done.failed_because.as_deref(), Some("need_not_met"));
    assert_eq!(done.starts, 0, "the function was never called");

    let met = Text { text: "done before".into() };
    let needed = client.start(needed_for, met.clone()).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, needed, SOON).await.status, "succeeded");
    let run = client.start(fails_without_need, met).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, run, SOON).await.status, "succeeded");
}

#[tokio::test]
async fn a_done_within_need_is_met_only_by_a_success_inside_the_window() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let outcome = |n| {
        let client = client.clone();
        let pool = database.pool.clone();
        async move {
            let run = client.start(needs_recent_success, Number { n }).await.unwrap().run_id();
            let done = finished(&pool, run, SOON).await;
            (done.status, done.failed_because)
        }
    };
    let unmet = ("failed".to_owned(), Some("need_not_met".to_owned()));

    assert_eq!(outcome(1).await, unmet, "never done");

    let fail_file = app.finished_dir.join("fail-done-recently");
    std::fs::write(&fail_file, "").unwrap();
    let failed = client.start(done_recently, ()).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, failed, SOON).await.status, "failed");
    assert_eq!(outcome(2).await, unmet, "a recent failure does not meet it");

    std::fs::remove_file(&fail_file).unwrap();
    let succeeded = client.start(done_recently, ()).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, succeeded, SOON).await.status, "succeeded");
    assert_eq!(outcome(3).await, ("succeeded".to_owned(), None), "a success within four seconds meets it");

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(outcome(4).await, unmet, "the success is now older than the window");
}

#[tokio::test]
async fn with_ignore_conditions_neither_only_if_nor_a_done_for_closure_is_called() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let lines = |path: String| std::fs::read_to_string(path).map_or(0, |text| text.lines().count());

    let tested = unique("tested");
    let start = json!({ "input": { "text": tested } });
    let run: StartResponse = local.post("/v1/jobs/counts_its_tests/runs", &start).await;
    let done = finished(&database.pool, run.run_id, SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("need_not_met"), "{}", done.reason);
    assert_eq!((lines(format!("{tested}.only_if")), lines(format!("{tested}.done_for"))), (1, 1));

    let ignored = unique("ignored");
    let start = json!({ "input": { "text": ignored }, "ignore_conditions": true });
    let run: StartResponse = local.post("/v1/jobs/counts_its_tests/runs", &start).await;
    let done = finished(&database.pool, run.run_id, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(
        (lines(format!("{ignored}.only_if")), lines(format!("{ignored}.done_for"))),
        (0, 0),
        "(only_if calls, done_for calls) with ignore_conditions"
    );
    for path in [format!("{tested}.only_if"), format!("{tested}.done_for")] {
        let _ = std::fs::remove_file(path);
    }
}

/// Runs `order_parent` with this case and returns how its `checked_in_order` child ended.
async fn checked(client: &InAppClient, database: &Database, passes: bool) -> (String, Option<String>, Option<String>) {
    let parent = client.start(order_parent, OrderCase { tag: "same".into(), passes }).await.unwrap().run_id();
    let parent = finished(&database.pool, parent, SOON).await;
    assert_eq!(parent.status, "succeeded");
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        let child: Option<support::Finished> =
            sqlx::query_as("SELECT * FROM downbeat.run_history WHERE job_name = 'checked_in_order' AND chain_id = $1")
                .bind(parent.chain_id)
                .fetch_optional(&database.pool)
                .await
                .unwrap();
        if let Some(child) = child {
            return (child.status, child.skipped_because, child.failed_because);
        }
        assert!(tokio::time::Instant::now() < deadline, "the child of {} did not finish", parent.run_id);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn the_first_condition_that_stops_a_run_decides_only_if_then_needs_then_if_changed() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let some = |text: &str| Some(text.to_owned());

    let recent = client.start(done_recently, ()).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, recent, SOON).await.status, "succeeded");
    let window_ends = tokio::time::Instant::now() + Duration::from_secs(10);

    assert_eq!(checked(&client, &database, true).await, ("succeeded".into(), None, None));
    assert_eq!(
        checked(&client, &database, true).await,
        ("skipped".into(), some("input_unchanged"), None),
        "the need is met, so if_changed decides"
    );
    assert_eq!(
        checked(&client, &database, false).await,
        ("skipped".into(), some("condition_false"), None),
        "only_if comes before if_changed"
    );
    assert!(tokio::time::Instant::now() < window_ends, "the cases above ran within the need's window");

    tokio::time::sleep_until(window_ends + Duration::from_secs(1)).await;
    assert_eq!(
        checked(&client, &database, false).await,
        ("skipped".into(), some("condition_false"), None),
        "only_if comes before an unmet need and if_changed"
    );
    assert_eq!(
        checked(&client, &database, true).await,
        ("failed".into(), None, some("need_not_met")),
        "an unmet need comes before if_changed"
    );
}

#[tokio::test]
async fn if_changed_with_one_per_compares_only_succeeded_rows_with_the_same_turn_key() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let child_of = |key: &'static str, value: &'static str, fail: bool| {
        let client = client.clone();
        let pool = database.pool.clone();
        async move {
            let parent = client
                .start(keyed_parent, Keyed { key: key.into(), value: value.into(), fail })
                .await
                .unwrap()
                .run_id();
            let chain = finished(&pool, parent, SOON).await.chain_id;
            support::eventually(
                &pool,
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'changed_per_key' AND chain_id = '{chain}')"
                ),
                SOON,
            )
            .await;
            let row: (String, Option<String>) = sqlx::query_as(
                "SELECT status, skipped_because FROM downbeat.run_history WHERE job_name = 'changed_per_key' AND chain_id = $1",
            )
            .bind(chain)
            .fetch_one(&pool)
            .await
            .unwrap();
            row
        }
    };
    let ran = ("succeeded".to_owned(), None);
    let unchanged = ("skipped".to_owned(), Some("input_unchanged".to_owned()));

    assert_eq!(child_of("a", "one", false).await, ran);
    assert_eq!(child_of("b", "two", false).await, ran);
    assert_eq!(child_of("a", "one", false).await, unchanged, "compared with key a's last success, not b's");
    assert_eq!(child_of("b", "two", false).await, unchanged);

    assert_eq!(child_of("a", "three", true).await.0, "failed");
    assert_eq!(child_of("a", "three", false).await, ran, "a failed row with the same input does not count");
    assert_eq!(child_of("a", "three", false).await, unchanged);
}
