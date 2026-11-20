//! Requests an endpoint refuses: actions above the token's scope, starts it cannot accept, and a mark
//! as succeeded that does not apply.

mod support;

use downbeat_api::Scope;
use downbeat_client::types::StartResponse;
use serde_json::{Value, json};
use support::{Database, SOON, Signer, finished, with_api_at};

async fn status_of(signer: &Signer, path: &str, body: &Value) -> u16 {
    signer.send("POST", path, &serde_json::to_vec(body).unwrap()).await.status().as_u16()
}

#[tokio::test]
async fn an_operator_is_refused_every_administer_action() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    let admin = Signer::local(&base);
    let failed: StartResponse = admin.post("/v1/jobs/panics/runs", &json!({ "input": null })).await;
    assert_eq!(finished(&database.pool, failed.run_id, SOON).await.status, "failed");

    let operator = Signer { scope: Scope::Operate, ..Signer::local(&base) };
    let actions = [
        ("/v1/jobs/echo/pause".to_owned(), json!({ "note": "maintenance", "until_resumed": true })),
        (
            "/v1/overrides".to_owned(),
            json!({ "schedules": ["report"], "date": "2030-01-07", "change": "off", "times": [], "note": "holiday" }),
        ),
        ("/v1/backfills".to_owned(), json!({ "job": "dated", "from": "2030-01-04", "to": "2030-01-07" })),
        (format!("/v1/runs/{}/mark-succeeded", failed.run_id), json!({ "output": null, "note": "fixed by hand" })),
        ("/v1/jobs/echo/runs".to_owned(), json!({ "input": { "text": "x" }, "ignore_conditions": true })),
    ];
    for (path, body) in &actions {
        assert_eq!(status_of(&operator, path, body).await, 403, "POST {path} as an operator");
    }
    for (path, body) in &actions {
        assert!(
            status_of(&admin, &format!("{path}?preview=true"), body).await < 300,
            "the same request is fine for an administrator: {path}"
        );
    }
}

#[tokio::test]
async fn a_dry_run_of_a_job_without_dry_run_support_or_a_start_of_an_unknown_job_is_refused() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let dry = json!({ "input": { "text": "x" }, "dry_run": true });
    assert_eq!(status_of(&local, "/v1/jobs/echo/runs", &dry).await, 422);
    let plain = json!({ "input": { "text": "x" } });
    assert_eq!(status_of(&local, "/v1/jobs/no_such_job/runs", &plain).await, 404);
}

#[tokio::test]
async fn mark_as_succeeded_needs_a_failed_run_and_an_output_of_the_jobs_type() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));

    let succeeded: StartResponse = local.post("/v1/jobs/echo/runs", &json!({ "input": { "text": "fine" } })).await;
    assert_eq!(finished(&database.pool, succeeded.run_id, SOON).await.status, "succeeded");
    let note = json!({ "output": { "text": "fine" }, "note": "by hand" });
    let path = format!("/v1/runs/{}/mark-succeeded", succeeded.run_id);
    assert_eq!(status_of(&local, &path, &note).await, 409, "a run that did not fail");

    let plan = json!({ "input": { "code": 1, "output": "" } });
    let failed: StartResponse = local.post("/v1/jobs/exits/runs", &plan).await;
    assert_eq!(finished(&database.pool, failed.run_id, SOON).await.status, "failed");
    let path = format!("/v1/runs/{}/mark-succeeded", failed.run_id);
    let wrong = json!({ "output": 5, "note": "by hand" });
    assert_eq!(status_of(&local, &path, &wrong).await, 422, "an output that is not a Text");
    let still_failed: String = sqlx::query_scalar("SELECT status FROM downbeat.run_history WHERE run_id = $1")
        .bind(failed.run_id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(still_failed, "failed");
}
