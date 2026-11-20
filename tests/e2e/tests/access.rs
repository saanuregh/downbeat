//! The control API's limits on who may ask for what: body size, a caller's highest scope, the rate limit, and jobs.

mod support;

use downbeat_api::{PrivateKey, Scope};
use downbeat_client::types::{
    ApiError, AuditEntry, Page, PauseRequest, QueueView, RunView, StartRequest, StartResponse,
};
use serde_json::json;
use support::{Database, Signer, with_api_at, with_callers};

/// A callers file with one caller of this name, `max_scope` and job list, and its key.
fn caller(name: &str, max_scope: &str, jobs: Option<&str>) -> (String, PrivateKey) {
    let key = PrivateKey::generate();
    let jobs = jobs.map(|jobs| format!("jobs = {jobs}\n")).unwrap_or_default();
    let entry = format!(
        "[[caller]]\nname = \"{name}\"\nkeys = [\"{}\"]\nmax_scope = \"{max_scope}\"\n{jobs}",
        key.public().to_base64()
    );
    (entry, key)
}

#[tokio::test]
async fn a_body_over_256_kib_is_too_large() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let body = serde_json::to_vec(&json!({ "input": { "text": "x".repeat(300 * 1024) } })).unwrap();

    let response = Signer::local(&format!("http://{listen}")).send("POST", "/v1/jobs/echo/runs", &body).await;
    assert_eq!(response.status(), 413);
    let error: ApiError = response.json().await.unwrap();
    assert_eq!(error.error, "too_large");
}

#[tokio::test]
async fn a_token_asking_for_more_than_its_callers_highest_scope_is_forbidden() {
    let database = Database::fresh().await;
    let (entry, key) = caller("dashboard", "read", None);
    let (_app, base) = with_callers(&database, &format!("audience = \"test\"\n{entry}")).await;
    let mut dashboard =
        Signer { base, caller: "dashboard".into(), audience: "test".into(), key, scope: Scope::Read, jobs: None };

    assert_eq!(dashboard.send("GET", "/v1/jobs", b"").await.status(), 200);
    dashboard.scope = Scope::Operate;
    assert_eq!(dashboard.send("GET", "/v1/jobs", b"").await.status(), 403);
}

#[tokio::test]
async fn a_caller_over_its_burst_is_rate_limited() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));

    let requests = (0..150).map(|_| local.send("GET", "/v1/jobs", b""));
    let statuses: Vec<u16> =
        futures::future::join_all(requests).await.iter().map(|response| response.status().as_u16()).collect();
    let limited = statuses.iter().filter(|status| **status == 429).count();
    let answered = statuses.iter().filter(|status| **status == 200).count();
    assert_eq!(limited + answered, 150, "{statuses:?}");
    assert!(answered >= 100, "the burst of 100 is answered: {answered}");
    assert!(limited > 0, "some of 150 requests at once are refused");
}

#[tokio::test]
async fn a_caller_limited_to_some_jobs_sees_no_runs_of_other_jobs() {
    let database = Database::fresh().await;
    let (admin_entry, admin_key) = caller("operator", "administer", None);
    let (billing_entry, billing_key) = caller("billing", "administer", Some("[\"echo\"]"));
    let callers = format!("audience = \"test\"\n{admin_entry}{billing_entry}");
    let (_app, base) = with_callers(&database, &callers).await;
    let admin = Signer {
        base: base.clone(),
        caller: "operator".into(),
        audience: "test".into(),
        key: admin_key,
        scope: Scope::Administer,
        jobs: None,
    };
    let billing = Signer {
        base,
        caller: "billing".into(),
        audience: "test".into(),
        key: billing_key,
        scope: Scope::Read,
        jobs: Some(vec!["echo".into()]),
    };

    let pause = serde_json::to_value(PauseRequest { note: "hold".into(), until: None, until_resumed: true }).unwrap();
    for job in ["echo", "panics"] {
        let _: serde_json::Value = admin.post(&format!("/v1/jobs/{job}/pause"), &pause).await;
    }
    let echo = serde_json::to_value(StartRequest { input: json!({ "text": "mine" }), ..StartRequest::default() });
    let echo: StartResponse = admin.post("/v1/jobs/echo/runs", &echo.unwrap()).await;
    let other: StartResponse = admin.post("/v1/jobs/panics/runs", &json!({})).await;

    assert_eq!(billing.send("GET", &format!("/v1/runs/{}", other.run_id), b"").await.status(), 403);
    let own: RunView = billing.get(&format!("/v1/runs/{}", echo.run_id)).await;
    assert_eq!(own.job, "echo");

    let runs: Page<RunView> = billing.get("/v1/runs").await;
    let jobs: Vec<&str> = runs.items.iter().map(|run| run.job.as_str()).collect();
    assert_eq!(jobs, ["echo"]);

    let audit: Page<AuditEntry> = billing.get("/v1/audit").await;
    assert!(!audit.items.is_empty(), "the pause and start of echo are audited");
    for entry in &audit.items {
        assert_eq!(entry.job.as_deref(), Some("echo"), "{entry:?}");
    }
    let everything: Page<AuditEntry> = admin.get("/v1/audit").await;
    assert!(everything.items.iter().any(|entry| entry.job.as_deref() == Some("panics")));

    let queues: Vec<QueueView> = billing.get("/v1/queues").await;
    let queued_count: i64 = queues.iter().map(|queue| queue.queued).sum();
    assert_eq!(queued_count, 1, "only the run of echo is counted: {queues:?}");
    let all: Vec<QueueView> = admin.get("/v1/queues").await;
    assert_eq!(all.iter().map(|queue| queue.queued).sum::<i64>(), 2);
}
