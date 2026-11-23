//! The control API's limits on who may ask for what: body size, a caller's highest scope, the rate limit, and jobs.

mod support;

use downbeat_api::{PrivateKey, Scope};
use downbeat_client::types::{
    ApiError, AuditEntry, BackfillRequest, BackfillView, CalendarDay, ChainView, OverrideRequest, OverrideView, Page,
    PauseRequest, QueueView, RunView, StartRequest, StartResponse,
};
use serde_json::json;
use support::{Database, Signer, with_api_at, with_callers};

/// A callers file with one caller of this name, `max_scope` and job list, and its key.
fn caller(name: &str, max_scope: &str, jobs: Option<&str>) -> (String, PrivateKey) {
    let key = PrivateKey::generate();
    let jobs = jobs.map_or_default(|jobs| format!("jobs = {jobs}\n"));
    let entry = format!(
        "[[caller]]\nname = \"{name}\"\nkeys = [\"{}\"]\nmax_scope = \"{max_scope}\"\n{jobs}",
        key.public().to_base64()
    );
    (entry, key)
}

/// The test product trusting two callers: `operator`, who may do anything, and `billing`, who
/// administers only the job `echo`.
async fn operator_and_billing(database: &Database) -> (support::App, Signer, Signer) {
    let (admin_entry, admin_key) = caller("operator", "administer", None);
    let (billing_entry, billing_key) = caller("billing", "administer", Some("[\"echo\"]"));
    let callers = format!("audience = \"test\"\n{admin_entry}{billing_entry}");
    let (app, base) = with_callers(database, &callers).await;
    let signer = |caller: &str, key, jobs| Signer {
        base: base.clone(),
        caller: caller.into(),
        audience: "test".into(),
        key,
        scope: Scope::Administer,
        jobs,
    };
    let admin = signer("operator", admin_key, None);
    let billing = signer("billing", billing_key, Some(vec!["echo".into()]));
    (app, admin, billing)
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
    let (_app, admin, billing) = operator_and_billing(&database).await;

    let pause = serde_json::to_value(PauseRequest::new("hold").until_resumed()).unwrap();
    for job in ["echo", "panics"] {
        let _: serde_json::Value = admin.post(&format!("/v1/jobs/{job}/pause"), &pause).await;
    }
    let echo = serde_json::to_value(StartRequest::new(json!({ "text": "mine" })));
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

#[tokio::test]
async fn refused_requests_are_reported_at_most_once_a_minute() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    // A token that does not verify names no caller the scheduler can trust, so none is reported.
    let impostor = Signer { key: PrivateKey::generate(), ..Signer::local(&format!("http://{listen}")) };
    for _ in 0..3 {
        assert_eq!(impostor.send("GET", "/v1/jobs", b"").await.status(), 401);
    }
    support::eventually(
        &database.pool,
        "SELECT count(*) = 1 FROM downbeat.audit
          WHERE action = 'event' AND detail->>'event' = 'request_refused'
            AND detail->>'caller' IS NULL AND (detail->>'status')::int = 401",
        support::SOON,
    )
    .await;
}

#[tokio::test]
async fn a_caller_limited_to_some_jobs_cannot_cancel_a_chain_that_reaches_others() {
    let database = Database::fresh().await;
    let (_app, admin, billing) = operator_and_billing(&database).await;
    let pause = serde_json::to_value(PauseRequest::new("hold").until_resumed()).unwrap();
    let _: serde_json::Value = admin.post("/v1/jobs/shout/pause", &pause).await;

    let start = json!({ "input": { "text": "mine" } });
    let echo: StartResponse = billing.post("/v1/jobs/echo/runs", &start).await;
    support::eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = 'shout')",
        support::SOON,
    )
    .await;
    let run: RunView = billing.get(&format!("/v1/runs/{}", echo.run_id)).await;

    let cancel = billing.send("POST", &format!("/v1/chains/{}/cancel", run.chain_id), b"").await;
    assert_eq!(cancel.status(), 403, "the chain now holds shout, which billing does not cover");
    let still: ChainView = admin.get(&format!("/v1/chains/{}", run.chain_id)).await;
    assert_eq!(still.cancelled_at, None);
}

#[tokio::test]
async fn a_caller_limited_to_some_jobs_sees_no_overrides_backfills_or_calendar_of_others() {
    let database = Database::fresh().await;
    let (_app, admin, billing) = operator_and_billing(&database).await;
    let day = chrono::NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();
    let off = OverrideRequest::new(["report"], day, "off", "holiday");
    let _: serde_json::Value = admin.post("/v1/overrides", &serde_json::to_value(off).unwrap()).await;
    let backfill = BackfillRequest::new("dated", day, day);
    let _: serde_json::Value = admin.post("/v1/backfills", &serde_json::to_value(backfill).unwrap()).await;

    let overrides: Vec<OverrideView> = admin.get("/v1/overrides").await;
    assert_eq!(overrides.len(), 1);
    let overrides: Vec<OverrideView> = billing.get("/v1/overrides").await;
    assert!(overrides.is_empty(), "{overrides:?}");

    let backfills: Vec<BackfillView> = admin.get("/v1/backfills").await;
    assert_eq!(backfills.len(), 1);
    let backfills: Vec<BackfillView> = billing.get("/v1/backfills").await;
    assert!(backfills.is_empty(), "{backfills:?}");

    let next_day = "/v1/calendar?date=2030-01-08";
    let calendar: CalendarDay = admin.get(next_day).await;
    assert!(calendar.entries.iter().any(|entry| entry.job == "report"));
    let calendar: CalendarDay = billing.get(next_day).await;
    assert!(calendar.entries.is_empty() && calendar.days_off.is_empty(), "{calendar:?}");
}

#[tokio::test]
async fn a_caller_limited_to_some_jobs_cannot_cancel_a_backfill_whose_chain_reaches_others() {
    let database = Database::fresh().await;
    let (admin_entry, admin_key) = caller("operator", "administer", None);
    let (scheduling_entry, scheduling_key) = caller("scheduling", "administer", Some("[\"dated\"]"));
    let callers = format!("audience = \"test\"\n{admin_entry}{scheduling_entry}");
    let (_app, base) = with_callers(&database, &callers).await;
    let signer = |caller: &str, key, jobs| Signer {
        base: base.clone(),
        caller: caller.into(),
        audience: "test".into(),
        key,
        scope: Scope::Administer,
        jobs,
    };
    let admin = signer("operator", admin_key, None);
    let scheduling = signer("scheduling", scheduling_key, Some(vec!["dated".into()]));

    // The backfill's date runs `dated`, then `files_dated`, which is held.
    let pause = serde_json::to_value(PauseRequest::new("hold").until_resumed()).unwrap();
    let _: serde_json::Value = admin.post("/v1/jobs/files_dated/pause", &pause).await;
    let day = chrono::NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();
    let backfill = BackfillRequest::new("dated", day, day);
    let _: serde_json::Value = scheduling.post("/v1/backfills", &serde_json::to_value(backfill).unwrap()).await;
    support::eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = 'files_dated')",
        support::SOON,
    )
    .await;
    let backfills: Vec<BackfillView> = scheduling.get("/v1/backfills").await;
    let id = backfills[0].backfill_id;

    let refused = scheduling.send("POST", &format!("/v1/backfills/{id}/cancel"), b"").await;
    assert_eq!(refused.status(), 403, "its chain now holds files_dated, which this caller does not cover");
    let still: Vec<BackfillView> = admin.get("/v1/backfills").await;
    assert_eq!(still[0].state, "running", "a refused cancel changes nothing");

    let cancelled = admin.send("POST", &format!("/v1/backfills/{id}/cancel"), b"").await;
    assert_eq!(cancelled.status(), 200);
}

#[tokio::test]
async fn a_caller_limited_to_some_jobs_is_refused_a_token_that_names_no_jobs() {
    let database = Database::fresh().await;
    let (_app, _admin, mut billing) = operator_and_billing(&database).await;

    assert_eq!(billing.send("GET", "/v1/jobs", b"").await.status(), 200);
    billing.jobs = None;
    assert_eq!(billing.send("GET", "/v1/jobs", b"").await.status(), 403);
    billing.jobs = Some(vec!["echo".into(), "panics".into()]);
    assert_eq!(billing.send("GET", "/v1/jobs", b"").await.status(), 403, "a job outside the caller's list");
}

#[tokio::test]
async fn a_used_changing_token_is_kept_until_six_seconds_after_it_expires() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let body = serde_json::to_vec(&json!({ "input": { "text": "once" } })).unwrap();
    let binding = downbeat_api::RequestBinding { method: "POST", path_and_query: "/v1/jobs/echo/runs", body: &body };
    let claims = downbeat_api::Claims::for_request(
        &local.caller,
        &local.caller,
        &local.audience,
        local.scope,
        None,
        binding,
        chrono::Utc::now().timestamp(),
    );
    let token = downbeat_api::sign(&local.key, &claims);
    let response = local.send_with("POST", "/v1/jobs/echo/runs", &body, &token).await;
    assert!(response.status().is_success(), "{}", response.status());

    let expires_at: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM expires_at)::bigint FROM downbeat.used_token WHERE caller = $1 AND jti = $2",
    )
    .bind(&claims.iss)
    .bind(&claims.jti)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!((expires_at - (claims.exp + 6)).abs() <= 1, "expires_at {expires_at}, exp {}", claims.exp);
}

// Rule 14.16: an Idempotency-Key belongs to one caller, actor and job.
#[tokio::test]
async fn an_idempotency_key_is_scoped_to_its_caller_actor_and_job() {
    let database = Database::fresh().await;
    database.hold("echo").await;
    database.hold("shout").await;
    let (console_entry, console_key) = caller("console", "operate", None);
    let (portal_entry, portal_key) = caller("portal", "operate", None);
    let people = "acts_for_people = true\n";
    let callers = format!("audience = \"test\"\n{console_entry}{people}{portal_entry}{people}");
    let (_app, base) = with_callers(&database, &callers).await;
    let client =
        |name: &str, key| downbeat_client::Client::builder(&base).caller(name).audience("test").key(key).connect();
    let console = client("console", console_key).await.unwrap();
    let portal = client("portal", portal_key).await.unwrap();
    let start = |client: &downbeat_client::Client, actor: &'static str, job: &'static str| {
        let client = client.clone();
        async move {
            let person = client.acting_for(actor, Scope::Operate, None);
            person.start(job, &json!({ "text": "x" })).idempotency_key("shared").even_if_queued().send().await.unwrap()
        }
    };

    let first = start(&console, "alice", "echo").await;
    let other_actor = start(&console, "bob", "echo").await;
    let other_job = start(&console, "alice", "shout").await;
    let other_caller = start(&portal, "alice", "echo").await;
    assert!(!first.repeated && !other_actor.repeated && !other_job.repeated && !other_caller.repeated);
    assert_ne!(first.run_id, other_actor.run_id, "another actor's key starts its own run");
    assert_ne!(first.run_id, other_job.run_id, "the same key for another job starts its own run");
    assert_ne!(first.run_id, other_caller.run_id, "the same actor name under another caller starts its own run");

    let again = start(&console, "alice", "echo").await;
    assert!(again.repeated);
    assert_eq!(again.run_id, first.run_id);
}
