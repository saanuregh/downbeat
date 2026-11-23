//! Rules of the control API: who a run was started by, why a queued run waits, what "check now"
//! accepts, that a preview changes nothing, and that every real change is audited by its actor.

mod support;

use chrono::{NaiveDate, Utc};
use downbeat_client::types::{BackfillRequest, Done, MarkSucceededRequest, OverrideRequest, PauseRequest, Preview};
use downbeat_client::types::{StartRequest, StartResponse};
use downbeat_client::{ActingClient, Error, Scope};
use downbeat_e2e::*;
use serde_json::json;
use support::{Database, SOON, eventually, finished, with_api, without_worker};
use uuid::Uuid;

/// Starts a job with a `not_before` an hour from now.
async fn start_later(client: &ActingClient<'_>, job: &str, input: serde_json::Value) -> Uuid {
    let request = StartRequest::new(input).not_before(Utc::now() + chrono::TimeDelta::hours(1));
    let started: StartResponse = client.post(&format!("/v1/jobs/{job}/runs"), Some(&request), None).await.unwrap();
    started.run_id
}

async fn queued_because(client: &downbeat_client::Client, run: Uuid) -> String {
    client.run(run).await.unwrap().queued_because.unwrap_or_default()
}

async fn sql(database: &Database, statement: &'static str, run: Uuid) {
    sqlx::query(statement).bind(run).execute(&database.pool).await.unwrap();
}

fn until_resumed() -> PauseRequest {
    PauseRequest::new("testing").until_resumed()
}

fn check_now_path(run: Uuid) -> String {
    format!("/v1/runs/{run}/check-now")
}

#[tokio::test]
async fn a_caller_acting_as_itself_starts_programs_and_one_acting_for_someone_starts_people() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let own = client.start("echo", &Text { text: "itself".into() }).send().await.unwrap();
    let alice = client.acting_for("alice", Scope::Operate, None);
    let theirs = alice.start("echo", &Text { text: "for alice".into() }).send().await.unwrap();

    assert_eq!(finished(&database.pool, own.run_id, SOON).await.started_by, "program");
    assert_eq!(finished(&database.pool, theirs.run_id, SOON).await.started_by, "person");
}

#[tokio::test]
async fn a_queued_run_reports_a_pause_first_then_its_stored_reason_while_due_later_then_no_free_worker() {
    let database = Database::fresh().await;
    let (_app, client) = without_worker(&database).await;
    let admin = client.as_caller();

    let later = start_later(&admin, "echo", json!({ "text": "later" })).await;
    assert_eq!(queued_because(&client, later).await, "scheduled_later");

    admin.pause("job", "echo", &until_resumed()).await.unwrap();
    assert_eq!(queued_because(&client, later).await, "paused", "a pause comes before a stored reason");
    admin.pause_everything(&until_resumed()).await.unwrap();
    admin.resume("job", "echo").await.unwrap();
    assert_eq!(queued_because(&client, later).await, "paused", "a pause of everything holds it too");
    admin.resume_everything().await.unwrap();
    assert_eq!(queued_because(&client, later).await, "scheduled_later");

    let now = client.start("echo", &Text { text: "now".into() }).send().await.unwrap();
    assert_eq!(queued_because(&client, now.run_id).await, "no_free_worker");

    sql(
        &database,
        "UPDATE downbeat.run SET queued_because = 'not_ready', queued_reason = 'told', run_at = now() + interval '1 hour'
          WHERE run_id = $1",
        now.run_id,
    )
    .await;
    assert_eq!(queued_because(&client, now.run_id).await, "not_ready");
    sql(&database, "UPDATE downbeat.run SET run_at = now() - interval '1 minute' WHERE run_id = $1", now.run_id).await;
    assert_eq!(
        queued_because(&client, now.run_id).await,
        "no_free_worker",
        "a stored reason stands only while the run is due later"
    );
}

#[tokio::test]
async fn a_run_waiting_for_its_turn_says_so_before_its_stored_reason_and_after_a_pause() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();

    let holder = client.start("single", &Seconds { seconds: 30 }).send().await.unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts > 0)", holder.run_id),
        SOON,
    )
    .await;
    let waiting = start_later(&admin, "single", json!({ "seconds": 1 })).await;
    assert_eq!(queued_because(&client, waiting).await, "waiting_turn");

    admin.pause("job", "single", &until_resumed()).await.unwrap();
    assert_eq!(queued_because(&client, waiting).await, "paused");
}

#[tokio::test]
async fn check_now_calls_a_run_that_is_not_ready_again_at_once() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let file = std::env::temp_dir().join(format!("told-{}", Uuid::new_v4()));
    std::fs::write(&file, "go").unwrap();
    let run = client.start("not_ready_when_told", &Text { text: file.display().to_string() }).send().await.unwrap();
    let waiting = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND state = 'queued' AND queued_because = 'not_ready')",
        run.run_id
    );
    eventually(&database.pool, &waiting, SOON).await;

    // Its `recheck_every` is a minute, so only "check now" calls it again this soon.
    let _: Done = client.as_caller().post(&check_now_path(run.run_id), None::<&()>, None).await.unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts >= 2)", run.run_id),
        SOON,
    )
    .await;
    let _ = std::fs::remove_file(&file);
}

#[tokio::test]
async fn check_now_makes_a_run_waiting_on_a_need_due_and_refuses_any_other_run() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    database.hold("echo").await;

    let waiting = client.start("echo", &Text { text: "needs".into() }).send().await.unwrap().run_id;
    sql(
        &database,
        "UPDATE downbeat.run SET queued_because = 'need_not_met', run_at = now() + interval '1 hour' WHERE run_id = $1",
        waiting,
    )
    .await;
    let _: Done = admin.post(&check_now_path(waiting), None::<&()>, None).await.unwrap();
    let due: bool = sqlx::query_scalar("SELECT run_at <= now() FROM downbeat.run WHERE run_id = $1")
        .bind(waiting)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(due, "the run is due at once");

    let later = start_later(&admin, "echo", json!({ "text": "later" })).await;
    let refused = admin.post::<(), Done>(&check_now_path(later), None, None).await.unwrap_err();
    assert!(matches!(refused, Error::Api { status: 409, .. }), "a run scheduled later: {refused}");
    let preview = format!("{}?preview=true", check_now_path(later));
    let refused = admin.post::<(), Preview>(&preview, None, None).await.unwrap_err();
    assert!(matches!(refused, Error::Api { status: 409, .. }), "its preview refuses it too: {refused}");

    let done = client.start("shout", &Text { text: "done".into() }).send().await.unwrap();
    finished(&database.pool, done.run_id, SOON).await;
    let refused = admin.post::<(), Done>(&check_now_path(done.run_id), None, None).await.unwrap_err();
    assert!(matches!(refused, Error::Api { status: 409, .. }), "a finished run: {refused}");
}

/// What a preview must not change: each table's rows, by the columns an action would change.
async fn snapshot(database: &Database) -> Vec<String> {
    let queries = [
        "SELECT count(*) || ':' || coalesce(md5(string_agg(run_id || state || run_at || coalesce(queued_because, '') || starts, ',' ORDER BY run_id)), '') FROM downbeat.run",
        "SELECT count(*) || ':' || coalesce(md5(string_agg(run_id || status, ',' ORDER BY run_id)), '') FROM downbeat.run_history",
        "SELECT count(*) || ':' || coalesce(md5(string_agg(chain_id || coalesce(cancelled_at::text, ''), ',' ORDER BY chain_id)), '') FROM downbeat.chain",
        "SELECT count(*) || ':' || coalesce(md5(string_agg(kind || name || coalesce(until::text, '') || paused_by, ',' ORDER BY kind, name)), '') FROM downbeat.pause",
        "SELECT count(*) || ':' || coalesce(md5(string_agg(override_id || change || on_date, ',' ORDER BY override_id)), '') FROM downbeat.schedule_override",
        "SELECT count(*) || ':' || coalesce(md5(string_agg(backfill_id || state || next_date || coalesce(current_chain::text, ''), ',' ORDER BY backfill_id)), '') FROM downbeat.backfill",
        "SELECT count(*)::text FROM downbeat.audit WHERE action <> 'event'",
    ];
    let mut rows = Vec::new();
    for query in queries {
        rows.push(sqlx::query_scalar(query).fetch_one(&database.pool).await.unwrap());
    }
    rows
}

#[tokio::test]
async fn every_preview_changes_nothing() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    database.hold("echo").await;
    database.hold("dated").await;

    let failed = client.start("panics", &()).send().await.unwrap().run_id;
    finished(&database.pool, failed, SOON).await;
    let queued = client.start("echo", &Text { text: "held".into() }).send().await.unwrap().run_id;
    let chain = client.run(queued).await.unwrap().chain_id;
    // As if it had said "not ready": "check now" applies to it, and the pause keeps it queued.
    sqlx::query("UPDATE downbeat.run SET queued_because = 'not_ready' WHERE run_id = $1")
        .bind(queued)
        .execute(&database.pool)
        .await
        .unwrap();
    let day = NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();
    let off = |date| OverrideRequest::new(["report"], date, "off", "holiday");
    let override_id = admin.add_override(&off(day)).await.unwrap().id.unwrap();
    let backfill = BackfillRequest::new("dated", day, day.succ_opt().unwrap());
    let backfill_id = admin.start_backfill(&backfill).await.unwrap().id.unwrap();
    eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.backfill WHERE current_chain IS NOT NULL)", SOON)
        .await;
    eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name NOT IN ('echo', 'dated'))",
        SOON,
    )
    .await;

    let before = snapshot(&database).await;
    let mut previews: Vec<(String, Result<Preview, Error>)> = Vec::new();
    let mut post = async |path: String, body: Option<serde_json::Value>| {
        let result = admin.post(&format!("{path}?preview=true"), body.as_ref(), None).await;
        previews.push((path, result));
    };
    post("/v1/jobs/echo/runs".into(), Some(json!({ "input": { "text": "new" } }))).await;
    post("/v1/jobs/echo/runs".into(), Some(json!({ "input": { "text": "held" } }))).await;
    post(format!("/v1/runs/{queued}/cancel"), None).await;
    post(format!("/v1/runs/{failed}/retry"), None).await;
    post(format!("/v1/runs/{failed}/restart"), None).await;
    post(format!("/v1/runs/{failed}/mark-succeeded"), Some(json!({ "output": null, "note": "fixed" }))).await;
    post(format!("/v1/runs/{queued}/check-now"), None).await;
    post(format!("/v1/chains/{chain}/cancel"), None).await;
    let pause = Some(json!({ "note": "testing", "until_resumed": true }));
    post("/v1/jobs/shout/pause".into(), pause.clone()).await;
    post("/v1/jobs/echo/resume".into(), None).await;
    post("/v1/queues/default/pause".into(), pause.clone()).await;
    post("/v1/queues/default/resume".into(), None).await;
    post("/v1/schedules/report/pause".into(), pause.clone()).await;
    post("/v1/schedules/report/resume".into(), None).await;
    post("/v1/pause".into(), pause.clone()).await;
    post("/v1/resume".into(), None).await;
    post("/v1/overrides".into(), Some(serde_json::to_value(off(day.succ_opt().unwrap())).unwrap())).await;
    let backfill = BackfillRequest::new("dated", day + chrono::TimeDelta::days(7), day + chrono::TimeDelta::days(8));
    post("/v1/backfills".into(), Some(serde_json::to_value(&backfill).unwrap())).await;
    post(format!("/v1/backfills/{backfill_id}/cancel"), None).await;
    let removal = admin.delete::<Preview>(&format!("/v1/overrides/{override_id}?preview=true")).await;
    previews.push((format!("DELETE /v1/overrides/{override_id}"), removal));

    for (path, result) in &previews {
        let preview = result.as_ref().unwrap_or_else(|error| panic!("{path}: {error}"));
        assert!(!preview.effects.is_empty(), "{path} says what it would do");
    }
    assert_eq!(snapshot(&database).await, before, "a preview changed the database");
}

/// Does every kind of real change through `client`, checking after each that it wrote an audit row
/// for that action naming `actor`, and no row naming anyone else.
async fn audits_every_change(database: &Database, client: &ActingClient<'_>, actor: &str) {
    let mut seen: i64 = sqlx::query_scalar("SELECT coalesce(max(audit_id), 0) FROM downbeat.audit")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let mut audited = async |action: &str| {
        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT audit_id, actor, action FROM downbeat.audit WHERE audit_id > $1 AND action <> 'event' ORDER BY audit_id",
        )
        .bind(seen)
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert!(rows.iter().any(|(_, _, done)| done == action), "no audit row for {action}: {rows:?}");
        assert!(rows.iter().all(|(_, by, _)| by == actor), "{action} was audited as someone else: {rows:?}");
        seen = rows.last().map_or(seen, |row| row.0);
    };

    let echoed = client.start("echo", &Text { text: format!("by {actor}") }).send().await.unwrap();
    audited("start").await;
    finished(&database.pool, echoed.run_id, SOON).await;

    let later = start_later(client, "echo", json!({ "text": format!("later by {actor}") })).await;
    audited("start").await;
    let _: Done = client.post(&format!("/v1/runs/{later}/cancel"), None::<&()>, None).await.unwrap();
    audited("cancel_run").await;

    let failed = client.start("panics", &()).send().await.unwrap();
    audited("start").await;
    finished(&database.pool, failed.run_id, SOON).await;
    let retried: Done = client.post(&format!("/v1/runs/{}/retry", failed.run_id), None::<&()>, None).await.unwrap();
    audited("retry").await;
    finished(&database.pool, retried.run_id.unwrap(), SOON).await;
    let restarted: Done = client.post(&format!("/v1/runs/{}/restart", failed.run_id), None::<&()>, None).await.unwrap();
    // A restart is recorded as the start it makes.
    audited("start").await;
    finished(&database.pool, restarted.run_id.unwrap(), SOON).await;
    let note = MarkSucceededRequest::new("fixed by hand");
    client.mark_succeeded(failed.run_id, &note).await.unwrap();
    audited("mark_succeeded").await;

    let file = std::env::temp_dir().join(format!("told-{}", Uuid::new_v4()));
    std::fs::write(&file, "go").unwrap();
    let waiting = client.start("not_ready_when_told", &Text { text: file.display().to_string() }).send().await.unwrap();
    audited("start").await;
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND queued_because = 'not_ready')",
            waiting.run_id
        ),
        SOON,
    )
    .await;
    let _: Done = client.post(&check_now_path(waiting.run_id), None::<&()>, None).await.unwrap();
    audited("check_now").await;
    let _ = std::fs::remove_file(&file);

    let later = start_later(client, "echo", json!({ "text": format!("chain by {actor}") })).await;
    audited("start").await;
    let chain: Uuid = sqlx::query_scalar("SELECT chain_id FROM downbeat.run WHERE run_id = $1")
        .bind(later)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let _: Done = client.post(&format!("/v1/chains/{chain}/cancel"), None::<&()>, None).await.unwrap();
    audited("cancel_chain").await;

    client.pause("job", "shout", &until_resumed()).await.unwrap();
    audited("pause_job").await;
    client.resume("job", "shout").await.unwrap();
    audited("resume_job").await;
    client.pause_everything(&until_resumed()).await.unwrap();
    audited("pause_all").await;
    client.resume_everything().await.unwrap();
    audited("resume_all").await;

    let request = OverrideRequest::new(["report"], NaiveDate::from_ymd_opt(2030, 1, 7).unwrap(), "off", "holiday");
    let added = client.add_override(&request).await.unwrap();
    audited("add_override").await;
    let _: Done = client.delete(&format!("/v1/overrides/{}", added.id.unwrap())).await.unwrap();
    audited("remove_override").await;

    // Held, so the backfill is still running when it is cancelled.
    database.hold("dated").await;
    let backfill = BackfillRequest::new(
        "dated",
        NaiveDate::from_ymd_opt(2030, 1, 7).unwrap(),
        NaiveDate::from_ymd_opt(2030, 1, 8).unwrap(),
    );
    let started = client.start_backfill(&backfill).await.unwrap();
    audited("start_backfill").await;
    let _: Done =
        client.post(&format!("/v1/backfills/{}/cancel", started.id.unwrap()), None::<&()>, None).await.unwrap();
    audited("cancel_backfill").await;
}

#[tokio::test]
async fn a_change_by_someone_a_caller_acts_for_is_audited_as_them_via_the_caller() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let alice = client.acting_for("alice", Scope::Administer, None);
    audits_every_change(&database, &alice, "alice (via local)").await;
}

#[tokio::test]
async fn a_change_by_a_caller_acting_as_itself_is_audited_with_its_name_alone() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    audits_every_change(&database, &client.as_caller(), "local").await;
}

#[tokio::test]
async fn a_queued_because_filter_pages_past_the_runs_it_drops() {
    let database = Database::fresh().await;
    let (_app, client) = without_worker(&database).await;
    let admin = client.as_caller();

    let later = start_later(&admin, "echo", json!({ "text": "later" })).await;
    for n in 0..3 {
        client.start("echo", &Text { text: format!("now {n}") }).send().await.unwrap();
    }

    let mut found = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let query = match &cursor {
            Some(cursor) => format!("queued_because=scheduled_later&limit=2&cursor={cursor}"),
            None => "queued_because=scheduled_later&limit=2".to_owned(),
        };
        let page = client.runs(&query).await.unwrap();
        found.extend(page.items.into_iter().map(|run| run.run_id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found, vec![later]);
}

#[tokio::test]
async fn a_name_with_a_space_or_a_slash_is_signed_and_sent_as_one_path_segment() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    for name in ["no such job", "no/such?job"] {
        match client.job(name).await {
            Err(Error::Api { status: 404, message, .. }) => assert!(message.contains(name), "{message}"),
            other => panic!("{name}: {other:?}"),
        }
    }
}
