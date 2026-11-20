//! Chain actions on queued and failed runs, and a start that joins the same work already queued.

mod support;

use downbeat_client::types::{Done, PauseRequest, StartRequest, StartResponse};
use downbeat_client::{Client, Error};
use downbeat_e2e::*;
use serde_json::json;
use support::{Database, SOON, Signer, finished, finished_job, with_api, with_api_at};

/// Pauses a job until it is resumed, so the runs started after stay queued.
async fn hold(client: &Client, job: &str) {
    let pause = PauseRequest { note: "held by the test".into(), until: None, until_resumed: true };
    client.as_caller().pause("job", job, &pause).await.unwrap();
}

#[tokio::test]
async fn cancelling_a_queued_run_ends_it_as_cancelled() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    hold(&client, "echo").await;
    let started = client.start("echo", &Text { text: "never".into() }).send().await.unwrap();

    client.cancel(started.run_id).await.unwrap();
    let done = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(done.status, "cancelled");
    assert_eq!(done.starts, 0, "a queued run is never claimed");
}

#[tokio::test]
async fn a_chain_is_cancelled_once() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    hold(&client, "echo").await;
    let started = client.start("echo", &Text { text: "held".into() }).send().await.unwrap();
    let chain = client.run(started.run_id).await.unwrap().chain_id;
    let path = format!("/v1/chains/{chain}/cancel");

    let _: Done = client.as_caller().post(&path, None::<&()>, None).await.unwrap();
    assert_eq!(finished(&database.pool, started.run_id, SOON).await.status, "cancelled");
    let again = client.as_caller().post::<(), Done>(&path, None, None).await.unwrap_err();
    assert!(matches!(again, Error::Api { status: 409, .. }), "{again}");
}

#[tokio::test]
async fn a_failed_run_in_a_cancelled_chain_cannot_be_retried() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let failed = client.start("panics", &()).send().await.unwrap();
    let failed = finished(&database.pool, failed.run_id, SOON).await;
    assert_eq!(failed.status, "failed");
    finished_job(&database.pool, "on_failure", SOON).await;

    let _: Done = admin.post(&format!("/v1/chains/{}/cancel", failed.chain_id), None::<&()>, None).await.unwrap();
    let retry = admin.post::<(), Done>(&format!("/v1/runs/{}/retry", failed.run_id), None, None).await.unwrap_err();
    assert!(matches!(retry, Error::Api { status: 409, .. }), "{retry}");
}

#[tokio::test]
async fn starting_queued_work_again_joins_it_and_raises_its_priority() {
    let database = Database::fresh().await;
    let (_app, client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    hold(&client, "echo").await;
    let input = json!({ "text": "same" });

    let plain = serde_json::to_vec(&json!({ "input": input })).unwrap();
    let first = Signer::local(&base).send("POST", "/v1/jobs/echo/runs", &plain).await;
    assert_eq!(first.status(), 201);
    let first: StartResponse = first.json().await.unwrap();
    let before = client.run(first.run_id).await.unwrap().priority;

    let urgent = StartRequest { input, priority: Some(before + 10), ..StartRequest::default() };
    let again = Signer::local(&base).send("POST", "/v1/jobs/echo/runs", &serde_json::to_vec(&urgent).unwrap()).await;
    assert_eq!(again.status(), 200);
    let again: StartResponse = again.json().await.unwrap();
    assert!(again.joined);
    assert_eq!(again.run_id, first.run_id);

    assert_eq!(client.run(first.run_id).await.unwrap().priority, before + 10);
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE job_name = 'echo'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(queued, 1, "joining creates no run");
    let audited: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downbeat.audit WHERE action = 'start_joined' AND run_id = $1")
            .bind(first.run_id)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(audited, 1);
}

#[tokio::test]
async fn a_linked_run_cancelled_before_it_ran_is_retried_with_its_parents_output() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    hold(&client, "shout").await;
    let parent = client.start("echo", &Text { text: "quiet".into() }).send().await.unwrap();
    finished(&database.pool, parent.run_id, SOON).await;
    let queued: uuid::Uuid = loop {
        let found: Option<uuid::Uuid> = sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE job_name = 'shout'")
            .fetch_optional(&database.pool)
            .await
            .unwrap();
        if let Some(run_id) = found {
            break run_id;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    client.cancel(queued).await.unwrap();
    assert_eq!(finished(&database.pool, queued, SOON).await.status, "cancelled");

    let _: Done = client.as_caller().post(&format!("/v1/runs/{queued}/retry"), None::<&()>, None).await.unwrap();
    client.as_caller().resume("job", "shout").await.unwrap();
    let succeeded =
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'shout' AND status = 'succeeded')";
    support::eventually(&database.pool, succeeded, SOON).await;
    let output: serde_json::Value =
        sqlx::query_scalar("SELECT output FROM downbeat.run_history WHERE job_name = 'shout' AND status = 'succeeded'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(output, json!({ "text": "QUIET" }), "built from what echo handed over");
}

#[tokio::test]
async fn an_after_any_child_follows_its_parent_however_it_ended() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let told = |parent: uuid::Uuid| {
        let pool = database.pool.clone();
        async move {
            let sql = format!(
                "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'tells_how_empty_ended'
                  AND '{parent}' = ANY(parents) AND status = 'succeeded')"
            );
            support::eventually(&pool, &sql, SOON).await;
            sqlx::query_scalar::<_, serde_json::Value>(
                "SELECT output FROM downbeat.run_history WHERE job_name = 'tells_how_empty_ended' AND $1 = ANY(parents)",
            )
            .bind(parent)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };

    let succeeded = client.start("empty", &Text { text: "words".into() }).send().await.unwrap();
    assert_eq!(told(succeeded.run_id).await, json!({ "text": "succeeded with words" }));
    let skipped = client.start("empty", &Text { text: String::new() }).send().await.unwrap();
    assert_eq!(told(skipped.run_id).await, json!({ "text": "skipped: Skipped: there is nothing to do." }));
}

#[tokio::test]
async fn a_cancelled_chain_starts_nothing_more() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    hold(&client, "empty").await;
    let started = client.start("empty", &Text { text: "held".into() }).send().await.unwrap();
    let chain = client.run(started.run_id).await.unwrap().chain_id;

    let _: Done = client.as_caller().post(&format!("/v1/chains/{chain}/cancel"), None::<&()>, None).await.unwrap();
    assert_eq!(finished(&database.pool, started.run_id, SOON).await.status, "cancelled");
    let children: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE chain_id = $1)
              + (SELECT count(*) FROM downbeat.run_history WHERE chain_id = $1 AND job_name <> 'empty')",
    )
    .bind(chain)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(children, 0, "even an .after_any child, which a cancelled parent would otherwise start");
}

#[tokio::test]
async fn a_run_that_would_go_back_to_the_queue_with_a_cancel_pending_ends_cancelled() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let signal = std::env::temp_dir().join(format!("not-ready-{}", uuid::Uuid::new_v4()));
    let started =
        client.start("not_ready_when_told", &Text { text: signal.display().to_string() }).send().await.unwrap();
    let running = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND function_started_at IS NOT NULL)",
        started.run_id
    );
    support::eventually(&database.pool, &running, SOON).await;

    // Hold the run's row with a cancel already written, so the function's "not ready" reaches
    // the re-queue before any heartbeat can see the cancel.
    let mut holder = database.pool.begin().await.unwrap();
    sqlx::query("UPDATE downbeat.run SET cancel_requested_by = 'asha' WHERE run_id = $1")
        .bind(started.run_id)
        .execute(&mut *holder)
        .await
        .unwrap();
    std::fs::write(&signal, "").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    holder.commit().await.unwrap();
    let _ = std::fs::remove_file(&signal);

    let done = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(done.status, "cancelled", "{}", done.reason);
    assert_eq!(done.reason, "Cancelled by asha.");
    let last_call: Option<String> = sqlx::query_scalar(
        "SELECT call_log -> (jsonb_array_length(call_log) - 1) ->> 'ended' FROM downbeat.run_history WHERE run_id = $1",
    )
    .bind(started.run_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(last_call.as_deref(), Some("not ready"), "the call ended as not ready; the re-queue saw the cancel");
}

#[tokio::test]
async fn an_after_any_child_follows_a_cancelled_parent_too() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    hold(&client, "empty").await;
    let started = client.start("empty", &Text { text: "held".into() }).send().await.unwrap();
    client.cancel(started.run_id).await.unwrap();

    let told = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'tells_how_empty_ended'
          AND '{}' = ANY(parents) AND status = 'succeeded')",
        started.run_id
    );
    support::eventually(&database.pool, &told, SOON).await;
    let output: serde_json::Value =
        sqlx::query_scalar("SELECT output FROM downbeat.run_history WHERE job_name = 'tells_how_empty_ended'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(output, json!({ "text": "ended: Cancelled by local." }));
}

#[tokio::test]
async fn a_run_started_without_children_starts_none_when_it_ends() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let alone = StartRequest { input: json!({ "text": "alone" }), without_children: true, ..StartRequest::default() };
    let alone: StartResponse = client.as_caller().post("/v1/jobs/echo/runs", Some(&alone), None).await.unwrap();
    assert_eq!(finished(&database.pool, alone.run_id, SOON).await.status, "succeeded");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let followers: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE $1 = ANY(parents))
              + (SELECT count(*) FROM downbeat.run_history WHERE $1 = ANY(parents))",
    )
    .bind(alone.run_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(followers, 0, "echo is followed by shout, but not this time");
}

#[tokio::test]
async fn restarting_a_run_starts_a_new_chain_with_the_same_input() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let first = client.start("echo", &Text { text: "again".into() }).send().await.unwrap();
    let first = finished(&database.pool, first.run_id, SOON).await;

    let restarted: Done =
        client.as_caller().post(&format!("/v1/runs/{}/restart", first.run_id), None::<&()>, None).await.unwrap();
    let new_run = restarted.run_id.expect("a restart names the run it started");
    let second = finished(&database.pool, new_run, SOON).await;
    assert_eq!(second.status, "succeeded");
    assert_ne!(second.chain_id, first.chain_id, "a restart is a new chain");
    assert_eq!(second.output, first.output, "with the same input");
}

#[tokio::test]
async fn a_child_inherits_a_dry_run_but_not_ignore_conditions_or_without_children() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));

    let start = json!({ "input": { "text": "rehearsal" }, "dry_run": true, "ignore_conditions": true });
    let parent: StartResponse = local.post("/v1/jobs/rehearses/runs", &start).await;
    let parent = finished(&database.pool, parent.run_id, SOON).await;
    assert_eq!(parent.status, "succeeded", "{}", parent.reason);
    let child = finished_job(&database.pool, "after_rehearses", SOON).await;
    assert_eq!(child.chain_id, parent.chain_id);
    let flags: (bool, bool, bool) = sqlx::query_as(
        "SELECT dry_run, ignore_conditions, without_children FROM downbeat.run_history WHERE run_id = $1",
    )
    .bind(child.run_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(flags, (true, false, false), "(dry_run, ignore_conditions, without_children) of the child");

    // A parent with without_children starts no child at all, so the flag never reaches one.
    let alone = json!({ "input": { "text": "alone" }, "without_children": true });
    let alone: StartResponse = local.post("/v1/jobs/rehearses/runs", &alone).await;
    let alone = finished(&database.pool, alone.run_id, SOON).await;
    let children: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE chain_id = $1)
              + (SELECT count(*) FROM downbeat.run_history WHERE chain_id = $1 AND run_id <> $2)",
    )
    .bind(alone.chain_id)
    .bind(alone.run_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(children, 0);
}
