//! The control API's rules for single requests: a token is good once, every error is JSON, inputs
//! are checked strictly and for size, previews tell the truth, and "mark as succeeded" keeps the
//! run's own choices.

mod support;

use downbeat_client::Error;
use downbeat_client::types::{ApiError, MarkSucceededRequest, Preview, StartRequest, StartResponse};
use downbeat_e2e::*;
use serde_json::{Value, json};
use support::{Database, SOON, Signer, finished, finished_job, with_api, with_api_at};

async fn json_error(response: reqwest::Response) -> (u16, ApiError) {
    let status = response.status().as_u16();
    let content_type = response.headers()["content-type"].to_str().unwrap().to_owned();
    assert!(content_type.starts_with("application/json"), "{status} answered {content_type}");
    (status, response.json().await.unwrap())
}

#[tokio::test]
async fn a_token_for_a_change_is_good_once() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    let path = "/v1/jobs/echo/runs";
    let body = serde_json::to_vec(&json!({ "input": { "text": "once" } })).unwrap();
    let local = Signer::local(&base);
    let token = local.token("POST", path, &body);

    let first = local.send_with("POST", path, &body, &token).await;
    assert_eq!(first.status(), 201);
    let (status, replayed) = json_error(local.send_with("POST", path, &body, &token).await).await;
    assert_eq!(status, 401, "{replayed:?}");
    // The token verified before it was found used, so the report names its caller.
    support::eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event'
          AND detail->>'event' = 'request_refused' AND detail->>'caller' = 'local')",
        SOON,
    )
    .await;
}

#[tokio::test]
async fn every_error_is_json() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");

    let (status, unknown) = json_error(Signer::local(&base).send("GET", "/v1/nothing-here", b"").await).await;
    assert_eq!((status, unknown.error.as_str()), (404, "not_found"));
    // Without a token, an unknown path is refused like any other: the token check comes first.
    let (status, unsigned) = json_error(reqwest::get(format!("{base}/v1/nothing-here")).await.unwrap()).await;
    assert_eq!((status, unsigned.error.as_str()), (401, "unauthorized"));

    let (status, wrong_method) = json_error(Signer::local(&base).send("DELETE", "/v1/jobs", b"").await).await;
    assert_eq!((status, wrong_method.error.as_str()), (405, "method_not_allowed"));

    let (status, not_json) =
        json_error(Signer::local(&base).send("POST", "/v1/jobs/echo/runs", b"{not json").await).await;
    assert!(status == 400 || status == 422, "{status} {not_json:?}");
    assert!(!not_json.message.is_empty());
}

#[tokio::test]
async fn inputs_are_checked_strictly_and_for_size() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;

    // A field sent as null is not an unknown field.
    let with_null = client.start("echo", &json!({ "text": "x", "note": null })).send().await.unwrap();
    assert_eq!(finished(&database.pool, with_null.run_id, SOON).await.status, "succeeded");

    // An unknown field inside a nested value is found and named.
    let nested = json!([{ "text": "a" }, { "text": "b", "typo": 1 }]);
    let refused = client.start("both", &nested).send().await.unwrap_err();
    match refused {
        Error::Api { status: 422, message, .. } => assert!(message.contains("1.typo"), "{message}"),
        other => panic!("expected 422, got {other}"),
    }

    let huge = Text { text: "x".repeat(70 * 1024) };
    let too_large = client.start("echo", &huge).send().await.unwrap_err();
    assert!(matches!(too_large, Error::Api { status: 422, .. }), "{too_large}");
}

#[tokio::test]
async fn a_preview_lists_the_conditions_a_start_would_ignore() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let request = StartRequest { input: json!({ "n": -1 }), ignore_conditions: true, ..StartRequest::default() };

    let preview: Preview = admin.post("/v1/jobs/guarded/runs?preview=true", Some(&request), None).await.unwrap();
    assert_eq!(preview.false_conditions, vec!["only if 'positive'".to_owned()]);

    let started: StartResponse = admin.post("/v1/jobs/guarded/runs", Some(&request), None).await.unwrap();
    assert_eq!(finished(&database.pool, started.run_id, SOON).await.status, "succeeded");
}

#[tokio::test]
async fn resuming_a_pause_that_has_ended_is_a_conflict() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    sqlx::query(
        "INSERT INTO downbeat.pause (kind, name, until, paused_by, note)
         VALUES ('job', 'echo', now() - interval '1 minute', 'someone', 'over')",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let refused = client.as_caller().resume("job", "echo").await.unwrap_err();
    assert!(matches!(refused, Error::Api { status: 409, .. }), "{refused}");
}

#[tokio::test]
async fn marking_as_succeeded_starts_children_unless_the_run_was_started_without_them() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let mark = MarkSucceededRequest { output: Value::Null, note: "fixed by hand".into() };

    let alone = StartRequest { without_children: true, ..StartRequest::default() };
    let alone: StartResponse = admin.post("/v1/jobs/panics/runs", Some(&alone), None).await.unwrap();
    assert_eq!(finished(&database.pool, alone.run_id, SOON).await.status, "failed");
    admin.mark_succeeded(alone.run_id, &mark).await.unwrap();
    let marked = client.run(alone.run_id).await.unwrap();
    assert_eq!(marked.marked_by.as_deref(), Some("local"));
    assert!(marked.marked_at.is_some(), "the run says when it was marked");

    let linked = client.start("panics", &()).even_if_queued().send().await.unwrap();
    assert_eq!(finished(&database.pool, linked.run_id, SOON).await.status, "failed");
    admin.mark_succeeded(linked.run_id, &mark).await.unwrap();

    let child = finished_job(&database.pool, "after_panics", SOON).await;
    let parents: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT unnest(parents) FROM downbeat.run_history WHERE run_id = $1")
            .bind(child.run_id)
            .fetch_all(&database.pool)
            .await
            .unwrap();
    assert_eq!(parents, vec![linked.run_id]);
    let children: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE job_name = 'after_panics'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(children, 1, "only the run started with children gets one");
}

#[tokio::test]
async fn on_finish_hears_of_every_finished_run() {
    let database = Database::fresh().await;
    let (app, client) = with_api(&database).await;
    let succeeded = client.start("echo", &Text { text: "told".into() }).send().await.unwrap();
    let failed = client.start("panics", &()).send().await.unwrap();
    for (run, status) in [(succeeded.run_id, "succeeded"), (failed.run_id, "failed")] {
        finished(&database.pool, run, SOON).await;
        let file = app.finished_dir.join(run.to_string());
        let deadline = tokio::time::Instant::now() + SOON;
        while !file.exists() {
            assert!(tokio::time::Instant::now() < deadline, "on_finish was not called for {run}");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        assert_eq!(std::fs::read_to_string(file).unwrap(), status);
    }
    support::eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)",
        SOON,
    )
    .await;
}

#[tokio::test]
async fn on_finish_hears_of_a_run_marked_as_succeeded_long_after_it_failed() {
    let database = Database::fresh().await;
    let (app, client) = with_api(&database).await;
    let failed = client.start("panics", &()).send().await.unwrap();
    finished(&database.pool, failed.run_id, SOON).await;
    let file = app.finished_dir.join(failed.run_id.to_string());
    let read_when = |wanted: &'static str| {
        let file = file.clone();
        async move {
            let deadline = tokio::time::Instant::now() + SOON;
            while std::fs::read_to_string(&file).ok().as_deref() != Some(wanted) {
                assert!(tokio::time::Instant::now() < deadline, "on_finish never said {wanted}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    };
    read_when("failed").await;

    sqlx::query("UPDATE downbeat.run_history SET finished_at = now() - interval '2 days' WHERE run_id = $1")
        .bind(failed.run_id)
        .execute(&database.pool)
        .await
        .unwrap();
    let mark = MarkSucceededRequest { output: Value::Null, note: "fixed by hand".into() };
    client.as_caller().mark_succeeded(failed.run_id, &mark).await.unwrap();
    read_when("succeeded").await;
}
