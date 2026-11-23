//! The control API's endpoint rules: which scope each action needs, what a token limited to
//! jobs may pause, what a pause must say, and the version header on every answer.

mod support;

use downbeat_api::{PrivateKey, Scope};
use downbeat_client::types::{BackfillRequest, BackfillView, OverrideRequest, OverrideView, StartResponse};
use serde_json::{Value, json};
use support::{Database, SOON, Signer, finished, with_api_at, with_callers};
use uuid::Uuid;

async fn status_of(signer: &Signer, method: &str, path: &str, body: &Value) -> u16 {
    let body = if body.is_null() { Vec::new() } else { serde_json::to_vec(body).unwrap() };
    signer.send(method, path, &body).await.status().as_u16()
}

async fn changes(database: &Database) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM downbeat.audit WHERE action <> 'event'")
        .fetch_one(&database.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_read_token_is_refused_every_operate_action() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    let admin = Signer::local(&base);
    let failed: StartResponse = admin.post("/v1/jobs/panics/runs", &json!({ "input": null })).await;
    let run = finished(&database.pool, failed.run_id, SOON).await;
    let before = changes(&database).await;

    let reader = Signer { scope: Scope::Read, ..Signer::local(&base) };
    let id = failed.run_id;
    let actions = [
        format!("/v1/runs/{id}/cancel"),
        format!("/v1/runs/{id}/retry"),
        format!("/v1/runs/{id}/restart"),
        format!("/v1/runs/{id}/check-now"),
        format!("/v1/chains/{}/cancel", run.chain_id),
    ];
    for path in &actions {
        assert_eq!(status_of(&reader, "POST", path, &Value::Null).await, 403, "POST {path} with a read token");
    }
    assert_eq!(changes(&database).await, before, "a refused action changed something");
}

#[tokio::test]
async fn an_operate_token_is_refused_resumes_override_removal_and_backfill_cancel() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    let admin = Signer::local(&base);
    let pause = json!({ "note": "maintenance", "until_resumed": true });
    for path in ["/v1/jobs/files_dated/pause", "/v1/queues/default/pause", "/v1/schedules/report/pause", "/v1/pause"] {
        let _: Value = admin.post(path, &pause).await;
    }
    let day = chrono::NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();
    let off = OverrideRequest::new(["report"], day, "off", "holiday");
    let _: Value = admin.post("/v1/overrides", &serde_json::to_value(off).unwrap()).await;
    let overrides: Vec<OverrideView> = admin.get("/v1/overrides").await;
    let backfill = BackfillRequest::new("dated", day, day);
    let _: Value = admin.post("/v1/backfills", &serde_json::to_value(backfill).unwrap()).await;
    let backfills: Vec<BackfillView> = admin.get("/v1/backfills").await;
    let before = changes(&database).await;

    let operator = Signer { scope: Scope::Operate, ..Signer::local(&base) };
    let actions = [
        ("POST", "/v1/jobs/files_dated/resume".to_owned()),
        ("POST", "/v1/queues/default/resume".to_owned()),
        ("POST", "/v1/schedules/report/resume".to_owned()),
        ("POST", "/v1/resume".to_owned()),
        ("DELETE", format!("/v1/overrides/{}", overrides[0].override_id)),
        ("POST", format!("/v1/backfills/{}/cancel", backfills[0].backfill_id)),
    ];
    for (method, path) in &actions {
        assert_eq!(
            status_of(&operator, method, path, &Value::Null).await,
            403,
            "{method} {path} with an operate token"
        );
    }
    assert_eq!(changes(&database).await, before, "a refused action changed something");
}

#[tokio::test]
async fn a_token_limited_to_some_jobs_cannot_pause_or_resume_everything() {
    let database = Database::fresh().await;
    let key = PrivateKey::generate();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"billing\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\njobs = [\"echo\"]\n",
        key.public().to_base64()
    );
    let (_app, base) = with_callers(&database, &callers).await;
    let billing = Signer {
        base,
        caller: "billing".into(),
        audience: "test".into(),
        key,
        scope: Scope::Administer,
        jobs: Some(vec!["echo".into()]),
    };

    let pause = json!({ "note": "maintenance", "until_resumed": true });
    assert_eq!(status_of(&billing, "POST", "/v1/pause", &pause).await, 403);
    assert_eq!(status_of(&billing, "POST", "/v1/pause?preview=true", &pause).await, 403, "its preview too");
    assert_eq!(status_of(&billing, "POST", "/v1/resume", &Value::Null).await, 403);
    let pauses: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downbeat.pause").fetch_one(&database.pool).await.unwrap();
    assert_eq!(pauses, 0);
    assert_eq!(status_of(&billing, "POST", "/v1/jobs/echo/pause", &pause).await, 200, "its own job it may pause");
}

#[tokio::test]
async fn a_pause_with_an_empty_note_or_an_end_in_the_past_is_refused() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let admin = Signer::local(&format!("http://{listen}"));
    let past = (chrono::Utc::now() - chrono::TimeDelta::minutes(5)).to_rfc3339();
    let refused = [
        json!({ "note": "", "until_resumed": true }),
        json!({ "note": "   ", "until_resumed": true }),
        json!({ "note": "maintenance", "until": past }),
    ];
    for path in ["/v1/jobs/echo/pause", "/v1/queues/default/pause", "/v1/schedules/report/pause", "/v1/pause"] {
        for body in &refused {
            let status = status_of(&admin, "POST", path, body).await;
            assert!((400..500).contains(&status) && status != 403, "POST {path} {body} answered {status}");
        }
    }
    let pauses: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downbeat.pause").fetch_one(&database.pool).await.unwrap();
    assert_eq!(pauses, 0);
}

fn version_of(response: &reqwest::Response) -> Option<String> {
    response.headers().get("downbeat-api-version").map(|value| value.to_str().unwrap().to_owned())
}

#[tokio::test]
async fn every_answer_under_v1_and_every_error_carries_the_api_version() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    let local = Signer::local(&base);
    let expected = downbeat_api::API_VERSION.to_string();

    let mut answers = vec![
        ("read", local.send("GET", "/v1/jobs", b"").await),
        ("start", local.send("POST", "/v1/jobs/echo/runs", br#"{"input":{"text":"x"}}"#).await),
        ("unknown /v1 path", local.send("GET", "/v1/nothing-here", b"").await),
        ("unknown run", local.send("GET", &format!("/v1/runs/{}", Uuid::new_v4()), b"").await),
        ("wrong method", local.send("DELETE", "/v1/jobs", b"").await),
        ("bad input", local.send("POST", "/v1/jobs/echo/runs", br#"{"input":{"typo":1}}"#).await),
        ("body that does not parse", local.send("POST", "/v1/jobs/echo/runs", b"{not json").await),
        ("scope", Signer { scope: Scope::Read, ..Signer::local(&base) }.send("POST", "/v1/resume", b"").await),
        ("conflict", local.send("POST", "/v1/resume", b"").await),
    ];
    let http = reqwest::Client::new();
    answers.push(("no token", http.get(format!("{base}/v1/jobs")).send().await.unwrap()));
    answers.push(("unknown path outside /v1", http.get(format!("{base}/nothing-here")).send().await.unwrap()));
    let impostor = Signer { key: PrivateKey::generate(), ..Signer::local(&base) };
    answers.push(("bad signature", impostor.send("GET", "/v1/jobs", b"").await));

    let mut statuses = Vec::new();
    for (what, response) in &answers {
        statuses.push(response.status().as_u16());
        assert_eq!(version_of(response).as_deref(), Some(expected.as_str()), "{what} ({})", response.status());
    }
    assert!(statuses.iter().filter(|status| **status >= 400).count() >= 9, "{statuses:?}");
}
