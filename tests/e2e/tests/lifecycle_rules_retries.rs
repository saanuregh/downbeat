//! Which failures get another attempt: memory when `retry_on` lists it, a failing command as an
//! error, and never a panic, a bad input or an output that is too large; and "check now" on a run
//! that is backing off.

mod support;

use downbeat::InAppClient;
use downbeat_client::types::Done;
use downbeat_e2e::runtime_rules::*;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished, with_api};
use uuid::Uuid;

#[tokio::test]
async fn running_out_of_memory_gets_another_attempt_when_retry_on_lists_memory() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(eats_memory, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("out_of_memory"), "{}", done.reason);
    assert_eq!(done.starts, 2, "the first out-of-memory failure was tried again");
}

#[tokio::test]
async fn a_command_that_exits_with_a_failure_is_tried_again_as_an_error() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(exits_with_three, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("exit_code"), "{}", done.reason);
    assert_eq!(done.starts, 2);
}

#[tokio::test]
async fn a_panic_a_bad_input_and_a_too_large_output_never_get_another_attempt() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    // Stored as a run would be, with an input its type cannot decode.
    let chain = Uuid::new_v4();
    let bad = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by)
         VALUES ($1, 'runtime_rules', 'bad_input_with_attempts', 'program')",
    )
    .bind(chain)
    .execute(&database.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO downbeat.run (run_id, job_name, queue, priority, due_at, started_by, chain_id, input, input_hash, turn_key)
         VALUES ($1, 'bad_input_with_attempts', 'default', 0, now(), 'program', $2, '{\"n\": \"not a number\"}', 'h', 'bad_input_with_attempts:h')",
    )
    .bind(bad)
    .bind(chain)
    .execute(&database.pool)
    .await
    .unwrap();
    let panicked = client.start(panics_with_attempts, ()).await.unwrap().run_id();
    let oversized = client.start(too_big_with_attempts, ()).await.unwrap().run_id();
    let _app = App::start(&database);

    for (run, kind) in [(panicked, "panic"), (bad, "bad_input"), (oversized, "output_too_large")] {
        let done = finished(&database.pool, run, SOON).await;
        assert_eq!(done.failed_because.as_deref(), Some(kind), "{}", done.reason);
        assert!(done.starts <= 1, "{kind} was called {} times", done.starts);
        assert!(done.failures <= 1, "{kind} counted {} failures", done.failures);
    }
}

#[tokio::test]
async fn check_now_makes_a_run_backing_off_due_at_once() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;

    let run = client.start("fails_then_waits_a_minute", &()).send().await.unwrap().run_id;
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND queued_because = 'backing_off')"
        ),
        SOON,
    )
    .await;

    // Its backoff is a minute, so only "check now" calls it again this soon.
    let _: Done = client.as_caller().post(&format!("/v1/runs/{run}/check-now"), None::<&()>, None).await.unwrap();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "failed", "{}", done.reason);
    assert_eq!(done.starts, 2, "the second attempt ran at once");
}
