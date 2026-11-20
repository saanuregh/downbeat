//! The control API, through the Rust client: every request is signed and checked, and every action
//! does what its preview says.

mod support;

use std::time::Duration;

use chrono::NaiveDate;
use downbeat_client::types::{BackfillRequest, MarkSucceededRequest, OverrideRequest, PauseRequest};
use downbeat_client::{Error, PrivateKey, Scope};
use downbeat_e2e::*;
use serde_json::json;
use support::{Database, SOON, eventually, finished, with_api};

#[tokio::test]
async fn requests_without_a_valid_token_are_refused() {
    let database = Database::fresh().await;
    let (_app, client, listen) = support::with_api_at(&database).await;
    let base = format!("http://{listen}");

    let unsigned = reqwest::get(format!("{base}/v1/jobs")).await.unwrap();
    assert_eq!(unsigned.status(), 401);

    let stranger = downbeat_client::Client::builder(&base)
        .caller("local")
        .audience("local")
        .key(PrivateKey::generate())
        .connect()
        .await
        .unwrap();
    let refused = stranger.jobs().await.unwrap_err();
    assert!(matches!(refused, Error::Api { status: 401, .. }), "{refused}");

    let reader = client.acting_for("viewer", Scope::Read, None);
    let forbidden = reader.start("echo", &Text { text: "x".into() }).send().await.unwrap_err();
    assert!(matches!(forbidden, Error::Api { status: 403, .. }), "{forbidden}");

    let health = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.headers()["downbeat-api-version"], "1");
}

#[tokio::test]
async fn a_start_with_an_idempotency_key_creates_one_run() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let input = Text { text: "once".into() };
    let first = client.start("echo", &input).idempotency_key("order-7").send().await.unwrap();
    let again = client.start("echo", &input).idempotency_key("order-7").even_if_queued().send().await.unwrap();
    assert_eq!(first.run_id, again.run_id);
    assert!(again.repeated);

    let wrong = client.start("echo", &json!({ "txt": "typo" })).send().await.unwrap_err();
    assert!(matches!(wrong, Error::Api { status: 422, .. }), "{wrong}");
}

#[tokio::test]
async fn runs_are_listed_with_their_reasons() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let started = client.start("panics", &()).send().await.unwrap();
    finished(&database.pool, started.run_id, SOON).await;

    let failed = client.runs("status=failed").await.unwrap();
    assert_eq!(failed.items.len(), 1);
    assert_eq!(failed.items[0].failed_because.as_deref(), Some("panic"));

    let run = client.run(started.run_id).await.unwrap();
    assert!(run.reason.starts_with("Failed: the function panicked"), "{}", run.reason);
    assert_eq!(run.detail.unwrap().call_log.len(), 1);
}

#[tokio::test]
async fn a_running_job_can_be_cancelled() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let started = client.start("counts", &Seconds { seconds: 30 }).send().await.unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts > 0)", started.run_id),
        SOON,
    )
    .await;
    client.cancel(started.run_id).await.unwrap();
    let done = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(done.status, "cancelled");
    assert_eq!(done.reason, "Cancelled by local.");
}

#[tokio::test]
async fn a_paused_job_holds_its_runs_until_resumed() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let until = chrono::Utc::now() + chrono::TimeDelta::hours(1);
    admin
        .pause(
            "job",
            "echo",
            &PauseRequest { note: "bad upstream data".into(), until: Some(until), until_resumed: false },
        )
        .await
        .unwrap();

    let started = client.start("echo", &Text { text: "held".into() }).send().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let held = client.run(started.run_id).await.unwrap();
    assert_eq!(held.queued_because.as_deref(), Some("paused"));
    assert!(held.reason.contains("bad upstream data"), "{}", held.reason);

    admin.resume("job", "echo").await.unwrap();
    assert_eq!(finished(&database.pool, started.run_id, SOON).await.status, "succeeded");
}

#[tokio::test]
async fn a_pause_needs_an_end_or_an_explicit_until_resumed() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let refused = client
        .as_caller()
        .pause("job", "echo", &PauseRequest { note: "why".into(), until: None, until_resumed: false })
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::Api { status: 400, .. }), "{refused}");
}

#[tokio::test]
async fn a_failed_step_can_be_retried_in_its_chain_or_marked_as_succeeded() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();

    let failed = client.start("panics", &()).send().await.unwrap();
    let failed_run = finished(&database.pool, failed.run_id, SOON).await;
    let retried: downbeat_client::types::Done =
        admin.post(&format!("/v1/runs/{}/retry", failed.run_id), None::<&()>, None).await.unwrap();
    let retry = finished(&database.pool, retried.run_id.unwrap(), SOON).await;
    assert_eq!(retry.chain_id, failed_run.chain_id);

    let note = MarkSucceededRequest { output: json!(null), note: "fixed by hand".into() };
    admin.mark_succeeded(failed.run_id, &note).await.unwrap();
    let marked = client.run(failed.run_id).await.unwrap();
    assert_eq!(marked.status, "succeeded");
    assert_eq!(marked.marked_by.as_deref(), Some("local"));
}

#[tokio::test]
async fn an_override_turns_a_day_off_and_the_calendar_shows_it() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let day = NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();

    let normal = client.calendar(day).await.unwrap();
    assert_eq!(normal.entries.iter().filter(|entry| entry.schedule == "report").count(), 2);

    let request = OverrideRequest {
        schedules: vec!["report".into()],
        date: day,
        change: "off".into(),
        times: vec![],
        note: "exchange holiday".into(),
    };
    let preview = admin.preview_override(&request).await.unwrap();
    assert_eq!(preview.effects, ["report on 2030-01-07: 0 occurrence(s) instead of 2."]);
    admin.add_override(&request).await.unwrap();

    let off = client.calendar(day).await.unwrap();
    assert!(off.entries.iter().all(|entry| entry.schedule != "report"));
    assert!(off.days_off.iter().any(|gap| gap.schedule == "report" && gap.reason.contains("exchange holiday")));
}

#[tokio::test]
async fn a_backfill_runs_each_weekday_in_the_range_in_order() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    // 2030-01-04 is a Friday; the 5th and 6th are a weekend the schedule skips.
    let request = BackfillRequest {
        job: "dated".into(),
        schedule: None,
        from: NaiveDate::from_ymd_opt(2030, 1, 4).unwrap(),
        to: NaiveDate::from_ymd_opt(2030, 1, 7).unwrap(),
        continue_on_failure: false,
    };
    client.as_caller().start_backfill(&request).await.unwrap();
    eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.backfill WHERE state = 'done')",
        Duration::from_secs(30),
    )
    .await;
    let days: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT output FROM downbeat.run_history WHERE job_name = 'dated' ORDER BY finished_at")
            .fetch_all(&database.pool)
            .await
            .unwrap();
    assert_eq!(days, [json!("2030-01-04"), json!("2030-01-07")]);
}

#[tokio::test]
async fn a_chain_shows_every_job_it_could_reach() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let started = client.start("echo", &Text { text: "chain".into() }).send().await.unwrap();
    let first = finished(&database.pool, started.run_id, SOON).await;
    support::finished_job(&database.pool, "both", SOON).await;
    let chain = client.chain(first.chain_id).await.unwrap();
    let jobs: Vec<&str> = chain.nodes.iter().map(|node| node.job.as_str()).collect();
    assert_eq!(jobs[0], "echo");
    for job in ["shout", "both", "after_change"] {
        assert!(jobs.contains(&job), "{jobs:?}");
    }
}

/// A service limited to one job, which may act only as itself.
async fn limited_service(database: &Database) -> (support::App, downbeat_client::Client) {
    let key = PrivateKey::generate();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"billing\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\njobs = [\"echo\"]\n",
        key.public().to_base64()
    );
    let (app, base) = support::with_callers(database, &callers).await;
    let client = downbeat_client::Client::builder(&base)
        .caller("billing")
        .audience("test")
        .key(key)
        .jobs(["echo"])
        .scope(Scope::Administer)
        .connect()
        .await
        .unwrap();
    (app, client)
}

#[tokio::test]
async fn a_service_may_only_act_as_itself() {
    let database = Database::fresh().await;
    let (_app, service) = limited_service(&database).await;
    let impersonating = service.acting_for("alice", Scope::Operate, Some(vec!["echo".into()]));
    let refused = impersonating.start("echo", &Text { text: "x".into() }).send().await.unwrap_err();
    assert!(matches!(refused, Error::Api { status: 403, .. }), "{refused}");
    service.start("echo", &Text { text: "as myself".into() }).send().await.unwrap();
}

#[tokio::test]
async fn a_caller_limited_to_some_jobs_sees_and_does_nothing_else() {
    let database = Database::fresh().await;
    let (_app, service) = limited_service(&database).await;
    let other = service.as_caller().start("panics", &()).send().await.unwrap_err();
    assert!(matches!(other, Error::Api { status: 403, .. }), "{other}");

    let pause = PauseRequest { note: "maintenance".into(), until: None, until_resumed: true };
    let queue = service.as_caller().pause("queue", "default", &pause).await.unwrap_err();
    assert!(matches!(queue, Error::Api { status: 403, .. }), "{queue}");

    let pipelines = service.pipelines().await.unwrap();
    let visible: Vec<&str> =
        pipelines.iter().flat_map(|pipeline| pipeline.jobs.iter().map(|node| node.job.as_str())).collect();
    assert_eq!(visible, ["echo"]);
}

#[tokio::test]
async fn concurrent_retries_with_one_idempotency_key_create_one_run() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let input = Text { text: "once".into() };
    let attempts = (0..8).map(|_| client.start("echo", &input).idempotency_key("payment-9").even_if_queued().send());
    let results = futures::future::join_all(attempts).await;
    let ids: std::collections::BTreeSet<_> = results.into_iter().map(|result| result.unwrap().run_id).collect();
    assert_eq!(ids.len(), 1, "{ids:?}");
}
