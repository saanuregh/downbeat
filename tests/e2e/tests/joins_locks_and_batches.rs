//! Links, schedules and pruning: no join child when a parent never ran in the chain (spec 7),
//! "mark as succeeded" checks its output strictly (7, chain action 3), a schedule locked by
//! another transaction is skipped (8.7), and pruning deletes at most 500 chains a statement (13.1).

mod support;

use std::time::Duration;

use downbeat::{InAppClient, measure};
use downbeat_client::types::StartResponse;
use downbeat_e2e::*;
use serde_json::json;
use support::{App, Database, SOON, Signer, finished, with_api_at};

// 7, child creation: "a parent has no run in this chain". `both` follows `echo` and `shout`.
#[tokio::test]
async fn a_join_child_is_not_created_when_one_parent_never_ran_in_the_chain() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(shout, Text { text: "alone".into() }).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    // Children are created in the ending's own transaction, so they would exist by now.
    let jobs: Vec<String> = sqlx::query_scalar(
        "SELECT job_name FROM downbeat.run WHERE chain_id = $1
         UNION ALL SELECT job_name FROM downbeat.run_history WHERE chain_id = $1",
    )
    .bind(done.chain_id)
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(jobs, ["shout"], "no `both`, since `echo` has no run in this chain");
}

// 7, chain action 3: the output is checked strictly against the output type.
#[tokio::test]
async fn marking_as_succeeded_refuses_an_output_with_an_unknown_field() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let failed: StartResponse =
        local.post("/v1/jobs/exits/runs", &json!({ "input": { "code": 1, "output": "" } })).await;
    assert_eq!(finished(&database.pool, failed.run_id, SOON).await.status, "failed");
    let path = format!("/v1/runs/{}/mark-succeeded", failed.run_id);

    let body = json!({ "output": { "text": "fine", "surplus": 1 }, "note": "by hand" });
    let refused = local.send("POST", &path, &serde_json::to_vec(&body).unwrap()).await;
    let status = refused.status();
    let text = refused.text().await.unwrap();
    assert_eq!(status, 422, "{text}");
    assert!(text.contains("surplus"), "the refusal names the unknown field: {text}");
    let still: String = sqlx::query_scalar("SELECT status FROM downbeat.run_history WHERE run_id = $1")
        .bind(failed.run_id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(still, "failed");
}

// 8.7: firing skips a schedule row another transaction holds, rather than waiting for it.
#[tokio::test]
async fn a_schedule_locked_by_another_transaction_is_skipped_not_waited_on() {
    let database = Database::fresh().await;
    measure::register_schedules(&database.pool, definition(), "rules").await.unwrap();
    for statement in [
        "UPDATE downbeat.schedule_state SET next_due_at = NULL WHERE schedule <> 'ticks_unless_unchanged'",
        "UPDATE downbeat.schedule_state SET next_due_at = now(), watermark = now() - interval '3 seconds'
          WHERE schedule = 'ticks_unless_unchanged'",
    ] {
        sqlx::query(statement).execute(&database.pool).await.unwrap();
    }
    let state = async || -> (i64, Option<chrono::DateTime<chrono::Utc>>) {
        sqlx::query_as(
            "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = 'ticks_unless_unchanged'),
                    (SELECT watermark FROM downbeat.schedule_state WHERE schedule = 'ticks_unless_unchanged')",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap()
    };
    let before = state().await;

    let mut holder = database.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM downbeat.schedule_state WHERE schedule = 'ticks_unless_unchanged' FOR UPDATE")
        .execute(&mut *holder)
        .await
        .unwrap();
    let fired = tokio::time::timeout(
        Duration::from_secs(5),
        measure::fire_due_schedules(&database.pool, definition(), "rules"),
    )
    .await;
    assert!(fired.is_ok(), "firing returned without waiting for the lock");
    fired.unwrap().unwrap();
    assert_eq!(state().await, before, "the locked schedule was left alone");

    holder.rollback().await.unwrap();
    measure::fire_due_schedules(&database.pool, definition(), "rules").await.unwrap();
    assert_eq!(state().await.0, 1, "once free, the schedule fires");
}

// 13.1: pruning removes chains in statements of at most 500.
#[tokio::test]
async fn pruning_deletes_at_most_five_hundred_chains_per_statement() {
    let database = Database::fresh().await;
    for statement in [
        "CREATE TABLE public.chain_deletes (removed bigint)",
        "CREATE FUNCTION public.count_chain_deletes() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO public.chain_deletes SELECT count(*) FROM gone;
             RETURN NULL;
         END $$",
        "CREATE TRIGGER count_chain_deletes AFTER DELETE ON downbeat.chain
             REFERENCING OLD TABLE AS gone FOR EACH STATEMENT EXECUTE FUNCTION public.count_chain_deletes()",
        // Chains of `echo` started ten years ago, with nothing in them: every one may go.
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_at, started_by)
         SELECT gen_random_uuid(), 'basics', 'echo', now() - interval '10 years', 'person'
           FROM generate_series(1, 1201)",
    ] {
        sqlx::query(statement).execute(&database.pool).await.unwrap();
    }

    measure::prune(&database.pool, definition()).await.unwrap();

    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.chain").fetch_one(&database.pool).await.unwrap();
    assert_eq!(left, 0, "every old chain went");
    let mut removed: Vec<i64> = sqlx::query_scalar("SELECT removed FROM public.chain_deletes WHERE removed > 0")
        .fetch_all(&database.pool)
        .await
        .unwrap();
    removed.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(removed, [500, 500, 201], "chains removed by each statement");
}
