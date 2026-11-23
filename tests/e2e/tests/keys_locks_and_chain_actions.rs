//! Same-work keys, conditions, chain actions and the control API: the exact same-work and
//! turn keys, a join's chain, `if_changed` on a scheduled run, the chain lock every chain action
//! takes, which `.after` children are not created, a retry's dry run, idempotency keys per actor,
//! a preview's false conditions, and audit rows for pausing queues and schedules.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_client::Scope;
use downbeat_client::types::{Done, MarkSucceededRequest, PauseRequest, Preview, StartResponse};
use downbeat_e2e::runtime_rules::*;
use downbeat_e2e::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use support::{App, Database, SOON, finished, with_api};
use uuid::Uuid;

fn sha256(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

async fn keys(database: &Database, run: Uuid) -> (Option<String>, Option<String>) {
    sqlx::query_as("SELECT same_work_key, turn_key FROM downbeat.run WHERE run_id = $1")
        .bind(run)
        .fetch_one(&database.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn the_same_work_key_is_the_job_and_the_input_hash() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();

    let run = client.start(echo, Text { text: "hashed".into() }).await.unwrap().run_id();
    let (same_work_key, _) = keys(&database, run).await;
    assert_eq!(same_work_key, Some(format!("echo:{}", sha256(r#"{"text":"hashed"}"#))));
}

#[tokio::test]
async fn a_join_leaves_no_new_chain() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();

    let first = client.start(echo, Text { text: "once".into() }).await.unwrap().run_id();
    let second = client.start(echo, Text { text: "once".into() }).await.unwrap().run_id();
    assert_eq!(second, first, "the second start joined the first");
    let (runs, chains): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM downbeat.run), (SELECT count(*) FROM downbeat.chain)")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!((runs, chains), (1, 1));
}

#[tokio::test]
async fn the_turn_key_of_each_policy_is_exact() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();

    let default = client.start(echo, Text { text: "turn".into() }).await.unwrap().run_id();
    let one_at_a_time = client.start(single, Seconds { seconds: 1 }).await.unwrap().run_id();
    let account = AccountWork { account: "acme".into(), seconds: 1 };
    let one_per = client.start(per_account, account).await.unwrap().run_id();

    assert_eq!(keys(&database, default).await.1, Some(format!("echo:{}", sha256(r#"{"text":"turn"}"#))));
    assert_eq!(keys(&database, one_at_a_time).await.1.as_deref(), Some("single:*"));
    assert_eq!(keys(&database, one_per).await.1.as_deref(), Some(r#"per_account:account="acme""#));
}

#[tokio::test]
async fn if_changed_skips_a_scheduled_run_whose_input_already_succeeded() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let earlier = client.start(ticks_unless_unchanged, Text { text: "same".into() }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, earlier, SOON).await.status, "succeeded");

    downbeat::measure::register_schedules(&database.pool, definition(), "rules").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // The `rules` environment has more due schedules than one pass fires.
    for _ in 0..4 {
        downbeat::measure::fire_due_schedules(&database.pool, definition(), "rules").await.unwrap();
    }
    let scheduled: Uuid = sqlx::query_scalar(
        "SELECT run_id FROM downbeat.run WHERE job_name = 'ticks_unless_unchanged' AND started_by = 'schedule'
         UNION ALL SELECT run_id FROM downbeat.run_history
          WHERE job_name = 'ticks_unless_unchanged' AND started_by = 'schedule'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let done = finished(&database.pool, scheduled, SOON).await;
    assert_eq!(done.skipped_because.as_deref(), Some("input_unchanged"), "{}", done.reason);
}

async fn chain_of(database: &Database, run: Uuid) -> Uuid {
    sqlx::query_scalar(
        "SELECT chain_id FROM downbeat.run WHERE run_id = $1
         UNION ALL SELECT chain_id FROM downbeat.run_history WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

/// Holds the chain's lock while `request` is sent, checks it is still waiting after two seconds,
/// then lets the lock go and returns what the request answered.
async fn waits_for_the_chain_lock<T>(
    database: &Database,
    chain: Uuid,
    request: impl Future<Output = Result<T, downbeat_client::Error>>,
) -> T {
    let mut transaction = database.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
        .bind(chain)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let mut request = std::pin::pin!(request);
    let early = tokio::time::timeout(Duration::from_secs(2), &mut request).await;
    assert!(early.is_err(), "the request was answered while the chain was locked");
    transaction.rollback().await.unwrap();
    request.await.unwrap()
}

#[tokio::test]
async fn cancel_retry_and_mark_as_succeeded_wait_for_the_chain_lock() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();

    database.hold("echo").await;
    let queued = client.start("echo", &Text { text: "to cancel".into() }).send().await.unwrap().run_id;
    let chain = chain_of(&database, queued).await;
    let path = format!("/v1/runs/{queued}/cancel");
    let cancel = admin.post::<(), Done>(&path, None, None);
    waits_for_the_chain_lock(&database, chain, cancel).await;
    assert_eq!(finished(&database.pool, queued, SOON).await.status, "cancelled");

    let failed = client.start("panics", &()).send().await.unwrap().run_id;
    assert_eq!(finished(&database.pool, failed, SOON).await.status, "failed");
    let chain = chain_of(&database, failed).await;
    let path = format!("/v1/runs/{failed}/retry");
    let retry = admin.post::<(), Done>(&path, None, None);
    let retried = waits_for_the_chain_lock(&database, chain, retry).await;
    finished(&database.pool, retried.run_id.unwrap(), SOON).await;

    let note = MarkSucceededRequest::new("fixed by hand");
    let mark = admin.mark_succeeded(failed, &note);
    waits_for_the_chain_lock(&database, chain, mark).await;
    let status: String = sqlx::query_scalar("SELECT status FROM downbeat.run_history WHERE run_id = $1")
        .bind(failed)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(status, "succeeded");
}

async fn has_child(database: &Database, parent: Uuid) -> bool {
    let chain = chain_of(database, parent).await;
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE chain_id = $1 AND job_name = 'after_may_skip')
             OR EXISTS (SELECT 1 FROM downbeat.run_history WHERE chain_id = $1 AND job_name = 'after_may_skip')",
    )
    .bind(chain)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn an_after_child_is_not_created_after_a_parent_that_was_skipped_cancelled_or_expired() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;

    let skipped = client.start("may_skip", &Number { n: 0 }).send().await.unwrap().run_id;
    assert_eq!(finished(&database.pool, skipped, SOON).await.status, "skipped");

    database.hold("may_skip").await;
    let cancelled = client.start("may_skip", &Number { n: 2 }).send().await.unwrap().run_id;
    let expired = client.start("may_skip", &Number { n: 3 }).send().await.unwrap().run_id;
    client.cancel(cancelled).await.unwrap();
    sqlx::query("UPDATE downbeat.run SET expires_at = now() - interval '1 second' WHERE run_id = $1")
        .bind(expired)
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(finished(&database.pool, cancelled, SOON).await.status, "cancelled");
    assert_eq!(finished(&database.pool, expired, SOON).await.status, "expired");
    database.release().await;

    let succeeded = client.start("may_skip", &Number { n: 1 }).send().await.unwrap().run_id;
    assert_eq!(finished(&database.pool, succeeded, SOON).await.status, "succeeded");
    assert!(has_child(&database, succeeded).await, "a succeeded parent has its child");
    for (parent, ended) in [(skipped, "skipped"), (cancelled, "cancelled"), (expired, "expired")] {
        assert!(!has_child(&database, parent).await, "a child was created after a parent that {ended}");
    }
}

#[tokio::test]
async fn a_retry_keeps_the_dry_run() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();

    let body = json!({ "input": { "text": "rehearsal" }, "dry_run": true });
    let started: StartResponse = admin.post("/v1/jobs/rehearses/runs", Some(&body), None).await.unwrap();
    finished(&database.pool, started.run_id, SOON).await;
    let retried: Done = admin.post(&format!("/v1/runs/{}/retry", started.run_id), None::<&()>, None).await.unwrap();
    let retried = retried.run_id.unwrap();
    finished(&database.pool, retried, SOON).await;
    let dry_run: bool = sqlx::query_scalar("SELECT dry_run FROM downbeat.run_history WHERE run_id = $1")
        .bind(retried)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert!(dry_run, "the retry lost the dry run");
}

#[tokio::test]
async fn one_idempotency_key_used_by_two_actors_starts_two_runs() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    database.hold("echo").await;
    let alice = client.acting_for("alice", Scope::Operate, None);
    let bob = client.acting_for("bob", Scope::Operate, None);

    let by_alice = alice.start("echo", &Text { text: "alice".into() }).idempotency_key("shared").send().await.unwrap();
    let by_bob = bob.start("echo", &Text { text: "bob".into() }).idempotency_key("shared").send().await.unwrap();
    assert!(!by_alice.repeated && !by_bob.repeated, "{by_alice:?} {by_bob:?}");
    assert_ne!(by_alice.run_id, by_bob.run_id);

    let again = alice.start("echo", &Text { text: "alice".into() }).idempotency_key("shared").send().await.unwrap();
    assert!(again.repeated, "the same actor's key is still claimed");
    assert_eq!(again.run_id, by_alice.run_id);
}

#[tokio::test]
async fn a_preview_lists_false_conditions_only_with_ignore_conditions() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let path = "/v1/jobs/guarded/runs?preview=true";

    let plain: Preview = admin.post(path, Some(&json!({ "input": { "n": 0 } })), None).await.unwrap();
    assert!(plain.false_conditions.is_empty(), "{:?}", plain.false_conditions);
    let ignoring: Preview =
        admin.post(path, Some(&json!({ "input": { "n": 0 }, "ignore_conditions": true })), None).await.unwrap();
    assert!(
        ignoring.false_conditions.len() == 1 && ignoring.false_conditions[0].contains("positive"),
        "{:?}",
        ignoring.false_conditions
    );
}

#[tokio::test]
async fn pausing_and_resuming_a_queue_and_a_schedule_is_audited() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let admin = client.as_caller();
    let pause = PauseRequest::new("testing").until_resumed();

    admin.pause("queue", "default", &pause).await.unwrap();
    admin.resume("queue", "default").await.unwrap();
    admin.pause("schedule", "report", &pause).await.unwrap();
    admin.resume("schedule", "report").await.unwrap();

    let rows: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT actor, action, schedule FROM downbeat.audit WHERE action <> 'event' ORDER BY audit_id")
            .fetch_all(&database.pool)
            .await
            .unwrap();
    let scheduled = Some("report".to_owned());
    assert_eq!(
        rows,
        vec![
            ("local".to_owned(), "pause_queue".to_owned(), None),
            ("local".to_owned(), "resume_queue".to_owned(), None),
            ("local".to_owned(), "pause_schedule".to_owned(), scheduled.clone()),
            ("local".to_owned(), "resume_schedule".to_owned(), scheduled),
        ]
    );
}
