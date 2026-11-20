//! Rule 14.17: a preview makes the same input checks as the real request, so each refuses the
//! same input with the same 422.

mod support;

use serde_json::{Value, json};
use support::{Database, SOON, Signer, finished, with_api_at};
use uuid::Uuid;

/// The status of the request, and of its preview, with this body.
async fn both_answers(signer: &Signer, path: &str, body: &Value) -> (u16, u16) {
    let body = serde_json::to_vec(body).unwrap();
    let preview = signer.send("POST", &format!("{path}?preview=true"), &body).await.status().as_u16();
    let real = signer.send("POST", path, &body).await.status().as_u16();
    (preview, real)
}

async fn started(signer: &Signer, job: &str, text: &str) -> Uuid {
    let answer: Value = signer.post(&format!("/v1/jobs/{job}/runs"), &json!({ "input": { "text": text } })).await;
    answer["run_id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_start_preview_refuses_an_input_that_does_not_decode() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let signer = Signer::local(&format!("http://{listen}"));

    let answers = both_answers(&signer, "/v1/jobs/echo/runs", &json!({ "input": { "text": 5 } })).await;
    assert_eq!(answers, (422, 422), "(preview, real request)");
}

#[tokio::test]
async fn a_restart_preview_refuses_a_run_whose_stored_input_no_longer_decodes() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let signer = Signer::local(&format!("http://{listen}"));
    let run = started(&signer, "echo", "stored").await;
    finished(&database.pool, run, SOON).await;
    sqlx::query("UPDATE downbeat.run_history SET input = '{\"text\": 5}' WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();

    let answers = both_answers(&signer, &format!("/v1/runs/{run}/restart"), &json!({})).await;
    assert_eq!(answers, (422, 422), "(preview, real request)");
}

#[tokio::test]
async fn a_mark_as_succeeded_preview_refuses_an_output_that_does_not_fit() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let signer = Signer::local(&format!("http://{listen}"));
    let run = started(&signer, "may_fail", "fail on purpose").await;
    assert_eq!(finished(&database.pool, run, SOON).await.status, "failed");

    let body = json!({ "output": { "text": 5 }, "note": "fixed by hand" });
    let answers = both_answers(&signer, &format!("/v1/runs/{run}/mark-succeeded"), &body).await;
    assert_eq!(answers, (422, 422), "(preview, real request)");
}
