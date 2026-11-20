//! Links and chains (spec section 7): which children an ending creates, what an `.after_any` or
//! `.after_failure` child is handed, and what marking a failed run as succeeded records.

mod support;

use downbeat_client::types::MarkSucceededRequest;
use downbeat_e2e::Text;
use serde_json::{Value, json};
use sqlx::PgPool;
use support::{Database, Finished, SOON, eventually, finished, with_api};
use uuid::Uuid;

/// The output of the run of `job` whose parent is `parent`, once it has succeeded.
async fn output_of_child(pool: &PgPool, job: &str, parent: Uuid) -> Value {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = '{job}'
          AND '{parent}' = ANY(parents) AND status = 'succeeded')"
    );
    eventually(pool, &sql, SOON).await;
    sqlx::query_scalar("SELECT output FROM downbeat.run_history WHERE job_name = $1 AND $2 = ANY(parents)")
        .bind(job)
        .bind(parent)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// How many runs of `job`, finished or not, are in the chain.
async fn runs_in_chain(pool: &PgPool, job: &str, chain: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = $1 AND chain_id = $2)
              + (SELECT count(*) FROM downbeat.run_history WHERE job_name = $1 AND chain_id = $2)",
    )
    .bind(job)
    .bind(chain)
    .fetch_one(pool)
    .await
    .unwrap()
}

fn failure_of(parent: &Finished) -> Value {
    json!({
        "job": "may_fail",
        "run_id": parent.run_id,
        "kind": parent.failed_because.as_deref().unwrap(),
        "reason": parent.reason,
    })
}

// Child creation (`.after_any` after `failed`) and child input (`Ended::Failed` and `Failure`).
#[tokio::test]
async fn children_of_a_failed_parent_are_handed_its_failure() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let started = client.start("may_fail", &Text { text: "fail now".into() }).send().await.unwrap();
    let parent = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!((parent.status.as_str(), parent.failed_because.as_deref()), ("failed", Some("error")));

    let ended = output_of_child(&database.pool, "tells_how_it_ended", parent.run_id).await;
    assert_eq!(ended, json!({ "ended": "failed", "value": failure_of(&parent) }));
    let failure = output_of_child(&database.pool, "takes_the_failure", parent.run_id).await;
    assert_eq!(failure, failure_of(&parent));
}

// Child creation: `.after_any` after `expired`, and no `.after_failure` child for it.
#[tokio::test]
async fn an_after_any_child_follows_an_expired_parent() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    database.hold("may_fail").await;
    let started = client.start("may_fail", &Text { text: "too late".into() }).send().await.unwrap();
    sqlx::query("UPDATE downbeat.run SET expires_at = now() - interval '1 minute' WHERE run_id = $1")
        .bind(started.run_id)
        .execute(&database.pool)
        .await
        .unwrap();

    let parent = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(parent.status, "expired", "{}", parent.reason);
    let ended = output_of_child(&database.pool, "tells_how_it_ended", parent.run_id).await;
    assert_eq!(ended, json!({ "ended": "expired", "value": parent.reason }));
    assert_eq!(runs_in_chain(&database.pool, "takes_the_failure", parent.chain_id).await, 0);
}

// Child creation: `.after_failure` is not created when the parent succeeds.
#[tokio::test]
async fn an_after_failure_child_is_not_created_when_its_parent_succeeds() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let started = client.start("may_fail", &Text { text: "fine".into() }).send().await.unwrap();
    let parent = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(parent.status, "succeeded", "{}", parent.reason);

    // Children are created in the transaction that ends the parent, so both answers are final now.
    assert_eq!(runs_in_chain(&database.pool, "tells_how_it_ended", parent.chain_id).await, 1);
    assert_eq!(runs_in_chain(&database.pool, "takes_the_failure", parent.chain_id).await, 0);
}

// Chain action 3: marking as succeeded clears `failed_because` and stores the note and output.
#[tokio::test]
async fn marking_a_failed_run_as_succeeded_clears_why_it_failed_and_keeps_the_note() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let started = client.start("may_fail", &Text { text: "fail, then be fixed".into() }).send().await.unwrap();
    let failed = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(failed.failed_because.as_deref(), Some("error"));

    let mark = MarkSucceededRequest { output: json!({ "text": "fixed" }), note: "repaired the rows by hand".into() };
    client.as_caller().mark_succeeded(started.run_id, &mark).await.unwrap();

    let marked: (String, Option<String>, Option<String>, Option<Value>) =
        sqlx::query_as("SELECT status, failed_because, note, output FROM downbeat.run_history WHERE run_id = $1")
            .bind(started.run_id)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(
        marked,
        ("succeeded".to_owned(), None, Some("repaired the rows by hand".to_owned()), Some(json!({ "text": "fixed" })))
    );
}
