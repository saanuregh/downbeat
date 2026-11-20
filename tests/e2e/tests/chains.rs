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
