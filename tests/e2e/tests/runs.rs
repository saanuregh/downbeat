//! How runs end: success, links and joins, conditions, retries, failures of every kind.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, finished, finished_job};

#[tokio::test]
async fn a_chain_hands_each_output_to_the_next_job_and_joins_branches() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let started = client.start(echo, Text { text: "hello".into() }).await.unwrap();
    let first = finished(&database.pool, started.run_id(), SOON).await;
    assert_eq!(first.status, "succeeded");
    assert_eq!(first.reason, "Succeeded.");

    let loud = finished_job(&database.pool, "shout", SOON).await;
    assert_eq!(loud.output.unwrap()["text"], "HELLO");
    assert_eq!(loud.started_by, "link");
    assert_eq!(loud.chain_id, first.chain_id);

    let joined = finished_job(&database.pool, "both", SOON).await;
    assert_eq!(joined.output.unwrap()["text"], "hello HELLO");
    assert_eq!(joined.chain_id, first.chain_id);
}

#[tokio::test]
async fn conditions_skip_with_a_reason() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let guarded_run = client.start(guarded, Number { n: -1 }).await.unwrap();
    let skipped = finished(&database.pool, guarded_run.run_id(), SOON).await;
    assert_eq!((skipped.status.as_str(), skipped.skipped_because.as_deref()), ("skipped", Some("condition_false")));
    assert_eq!(skipped.reason, "Skipped: condition 'positive' was false.");

    let need = client.start(needs_echo, Text { text: "never echoed".into() }).await.unwrap();
    let unmet = finished(&database.pool, need.run_id(), SOON).await;
    assert_eq!(unmet.skipped_because.as_deref(), Some("need_not_met"));

    let nothing = client.start(empty, Text { text: String::new() }).await.unwrap();
    let skipped = finished(&database.pool, nothing.run_id(), SOON).await;
    assert_eq!(skipped.skipped_because.as_deref(), Some("job_skipped"));
    assert_eq!(skipped.reason, "Skipped: there is nothing to do.");
}

#[tokio::test]
async fn a_need_is_met_once_the_needed_work_has_succeeded() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let input = Text { text: "day 1".into() };
    let echoed = client.start(echo, input.clone()).await.unwrap();
    finished(&database.pool, echoed.run_id(), SOON).await;
    let need = client.start(needs_echo, input).await.unwrap();
    assert_eq!(finished(&database.pool, need.run_id(), SOON).await.status, "succeeded");
}

#[tokio::test]
async fn if_changed_skips_a_child_whose_input_did_not_change() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let input = Text { text: "same".into() };
    let first = client.start(echo, input.clone()).await.unwrap();
    let first = finished(&database.pool, first.run_id(), SOON).await;
    support::eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'after_change' AND chain_id = '{}')",
            first.chain_id
        ),
        SOON,
    )
    .await;

    let second = client.start(echo, input).await.unwrap();
    let second = finished(&database.pool, second.run_id(), SOON).await;
    support::eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'after_change' AND chain_id = '{}')",
            second.chain_id
        ),
        SOON,
    )
    .await;
    let (because,): (Option<String>,) = sqlx::query_as(
        "SELECT skipped_because FROM downbeat.run_history WHERE job_name = 'after_change' AND chain_id = $1",
    )
    .bind(second.chain_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(because.as_deref(), Some("input_unchanged"));
}

#[tokio::test]
async fn a_failing_job_is_attempted_again_until_it_succeeds() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let counter = std::env::temp_dir().join(format!("flaky-{}", uuid::Uuid::new_v4()));

    let run = client.start(flaky, Flaky { counter: counter.display().to_string(), fail_times: 2 }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "succeeded");
    assert_eq!(done.failures, 2);
    assert_eq!(done.output, Some(serde_json::json!(3)));
    let _ = std::fs::remove_file(counter);
}

#[tokio::test]
async fn a_job_that_keeps_failing_fails_after_its_attempts() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let counter = std::env::temp_dir().join(format!("flaky-{}", uuid::Uuid::new_v4()));

    let run = client.start(flaky, Flaky { counter: counter.display().to_string(), fail_times: 10 }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("error")));
    assert_eq!(done.failures, 3);
    assert_eq!(done.reason, "Failed on attempt 3 of 3: call 3 fails on purpose.");
    let _ = std::fs::remove_file(counter);
}

#[tokio::test]
async fn a_panic_fails_the_run_and_starts_its_failure_child() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(panics, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("panic"));
    assert!(done.reason.contains("this job panics on purpose"), "{}", done.reason);

    let child = finished_job(&database.pool, "on_failure", SOON).await;
    assert_eq!(child.output.unwrap()["text"], "panics failed: panic");

    // The slot's child process was replaced; the next run works.
    let next = client.start(echo, Text { text: "after a panic".into() }).await.unwrap();
    assert_eq!(finished(&database.pool, next.run_id(), SOON).await.status, "succeeded");
}

#[tokio::test]
async fn a_crash_is_recorded_and_harms_nothing_else() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let crash = client.start(aborts, ()).await.unwrap();
    let alongside = client.start(echo, Text { text: "alongside".into() }).await.unwrap();
    let crashed = finished(&database.pool, crash.run_id(), SOON).await;
    assert_eq!(crashed.failed_because.as_deref(), Some("crash"));
    assert!(crashed.reason.contains("signal 6"), "{}", crashed.reason);
    assert_eq!(finished(&database.pool, alongside.run_id(), SOON).await.status, "succeeded");
}

#[tokio::test]
async fn a_function_past_its_timeout_is_stopped() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(sleeps, Seconds { seconds: 60 }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("timeout"));
    assert_eq!(done.reason, "Failed: timed out after 2s on attempt 1 of 1.");
}

#[tokio::test]
async fn a_function_over_its_memory_limit_is_killed() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(hungry, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("out_of_memory"));
}

#[tokio::test]
async fn a_run_that_is_not_ready_waits_without_spending_an_attempt() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let path = std::env::temp_dir().join(format!("ready-{}", uuid::Uuid::new_v4()));

    let run = client.start(waits_for_file, Text { text: path.display().to_string() }).await.unwrap();
    support::eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND queued_because = 'not_ready')",
            run.run_id()
        ),
        SOON,
    )
    .await;
    std::fs::write(&path, "here").unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "succeeded");
    assert_eq!(done.failures, 0);
    assert!(done.starts >= 2);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn a_command_runs_its_program_and_reads_its_output() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let ok = client.start(exits, ExitPlan { code: 0, output: r#"{"text": "from the program"}"#.into() }).await.unwrap();
    let done = finished(&database.pool, ok.run_id(), SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.output.unwrap()["text"], "from the program");

    let waiting = client.start(exits, ExitPlan { code: 75, output: String::new() }).await.unwrap();
    support::eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND queued_because = 'not_ready')",
            waiting.run_id()
        ),
        SOON,
    )
    .await;

    let broken = client.start(exits, ExitPlan { code: 3, output: String::new() }).await.unwrap();
    let failed = finished(&database.pool, broken.run_id(), SOON).await;
    assert_eq!(failed.failed_because.as_deref(), Some("exit_code"));
    assert_eq!(failed.reason, "Failed: the program exited with status 3.");
    assert!(failed.stderr_tail.unwrap().contains("a line on stderr"));
}

#[tokio::test]
async fn a_job_can_start_another_job() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(starter, Text { text: "a job".into() }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    let started: uuid::Uuid = done.output.unwrap()["text"].as_str().unwrap().parse().unwrap();
    let echoed = finished(&database.pool, started, SOON).await;
    assert_eq!(echoed.output.unwrap()["text"], "started by a job");
    assert_eq!(echoed.started_by, "job");
}

#[tokio::test]
async fn a_command_that_leaves_a_background_process_still_finishes() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let run = client.start(leaves_a_process_behind, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), std::time::Duration::from_secs(10)).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
}

#[tokio::test]
async fn a_child_is_replaced_after_a_function_leaves_a_task_running() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    let pid = |n| {
        let client = client.clone();
        let pool = database.pool.clone();
        async move {
            let run = client.start(child_pid, Number { n }).await.unwrap();
            finished(&pool, run.run_id(), SOON).await.output.unwrap()
        }
    };
    let first = pid(1).await;
    assert_eq!(pid(2).await, first, "ordinary runs share the slot's child");
    let leaving = client.start(leaves_a_task, ()).await.unwrap();
    assert_eq!(finished(&database.pool, leaving.run_id(), SOON).await.status, "succeeded");
    assert_ne!(pid(3).await, first, "the child that held a leftover task was replaced");
}
