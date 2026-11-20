//! Commands and child processes: what a program receives, how its output file is read, its
//! standard error redacted, when a reused child is replaced, and a person's timeout override.

mod support;

use std::collections::HashSet;
use std::time::Duration;

use downbeat::InAppClient;
use downbeat_client::types::StartResponse;
use downbeat_e2e::condition_rules::*;
use downbeat_e2e::{Number, Text, child_pid, definition, echo, guarded};
use serde_json::{Value, json};
use support::{App, Database, SOON, Signer, finished, with_api_at};
use uuid::Uuid;

#[tokio::test]
async fn a_program_gets_a_cleared_environment_with_its_variables_and_its_input_on_stdin() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with_env(
        &database,
        &["scheduler", "worker", "--queues", "default=2"],
        "test",
        &[("E2E_PASSED", "passed on"), ("E2E_NOT_PASSED", "kept back")],
    );
    let dir = std::env::temp_dir().join(format!("program-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let input = Text { text: dir.display().to_string() };

    let run = client.start(shows_what_it_gets, input.clone()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();

    assert_eq!(read("E2E_PASSED"), "passed on");
    assert_eq!(read("JOB_RUN_ID"), run.to_string());
    assert_eq!(read("JOB_ATTEMPT"), "1");
    assert_eq!(read("JOB_DRY_RUN"), "0");
    assert!(!read("JOB_OUTPUT_FILE").is_empty());
    let stdin: Value = serde_json::from_str(&read("stdin")).unwrap();
    assert_eq!(stdin, json!({ "text": input.text }));

    let env = read("env");
    for absent in ["E2E_NOT_PASSED", "E2E_NOT_SET", "DOWNBEAT_DATABASE_URL", "RUST_LOG", "E2E_FINISHED_DIR"] {
        assert!(!env.contains(absent), "{absent} reached the program:\n{env}");
    }
    for present in ["E2E_PASSED", "DIR", "JOB_RUN_ID", "JOB_ATTEMPT", "JOB_DRY_RUN", "JOB_OUTPUT_FILE"] {
        assert!(env.contains(present), "{present} did not reach the program:\n{env}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_value_that_looks_like_an_option_fails_as_bad_input_unless_the_command_allows_options() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let option = Text { text: "--delete-everything".into() };

    let refused = client.start(takes_a_value, option.clone()).await.unwrap().run_id();
    let refused = finished(&database.pool, refused, SOON).await;
    assert_eq!(refused.status, "failed", "{}", refused.reason);
    assert_eq!(refused.failed_because.as_deref(), Some("bad_input"));

    let plain = client.start(takes_a_value, Text { text: "a value; not a shell".into() }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, plain, SOON).await.status, "succeeded");

    let allowed = client.start(takes_an_option, option).await.unwrap().run_id();
    let allowed = finished(&database.pool, allowed, SOON).await;
    assert_eq!(allowed.status, "succeeded", "{}", allowed.reason);
}

#[tokio::test]
async fn a_program_output_file_is_read_as_its_output() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let ended = |kind: &'static str| {
        let client = client.clone();
        let pool = database.pool.clone();
        async move {
            let run = client.start(writes_its_output, OutputPlan { kind: kind.into() }).await.unwrap().run_id();
            finished(&pool, run, SOON).await
        }
    };

    let empty = ended("empty").await;
    assert_eq!(empty.status, "succeeded", "{}", empty.reason);
    assert!(matches!(empty.output, None | Some(Value::Null)), "an empty file is null: {:?}", empty.output);

    let skipped = ended("skip").await;
    assert_eq!((skipped.status.as_str(), skipped.skipped_because.as_deref()), ("skipped", Some("job_skipped")));
    assert!(skipped.reason.contains("nothing new"), "{}", skipped.reason);

    for (kind, because) in [("huge", "output_too_large"), ("not_json", "bad_input"), ("not_utf8", "bad_input")] {
        let done = ended(kind).await;
        assert_eq!(
            (done.status.as_str(), done.failed_because.as_deref()),
            ("failed", Some(because)),
            "{kind}: {}",
            done.reason
        );
    }
}

#[tokio::test]
async fn a_failed_command_keeps_its_stderr_tail_after_redact() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(fails_with_a_secret, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("exit_code"), "{}", done.reason);
    let tail = done.stderr_tail.unwrap_or_default();
    assert!(tail.contains("the password is [redacted]"), "{tail}");
    assert!(!tail.contains("hunter2"), "{tail}");
}

#[tokio::test]
async fn a_redact_hook_that_panics_withholds_the_tail_and_costs_nothing_else() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["scheduler", "worker", "--queues", "default=1"], "test");

    let run = client.start(breaks_redact, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("exit_code"), "{}", done.reason);
    let tail = done.stderr_tail.unwrap_or_default();
    assert!(tail.starts_with("[standard error withheld"), "{tail}");
    assert!(!tail.contains("breaks redact"), "nothing the hook should have seen is kept: {tail}");

    // The only worker slot still works.
    let after = client.start(echo, Text { text: "still here".into() }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, after, SOON).await.status, "succeeded");
}

#[tokio::test]
async fn a_reused_child_is_replaced_after_recycle_child_after_called_runs() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with_env(
        &database,
        &["scheduler", "worker", "--queues", "default=1"],
        "test",
        &[("E2E_RECYCLE_CHILD_AFTER", "2")],
    );
    let pid = |n| {
        let client = client.clone();
        let pool = database.pool.clone();
        async move {
            let run = client.start(child_pid, Number { n }).await.unwrap();
            finished(&pool, run.run_id(), SOON).await.output.unwrap()
        }
    };

    let first = pid(1).await;
    // Skipped while prepared: its function is never called, so it does not count.
    let skipped = client.start(guarded, Number { n: -1 }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, skipped, SOON).await.status, "skipped");
    assert_eq!(pid(2).await, first, "the second called run shares the child");
    let second = pid(3).await;
    assert_ne!(second, first, "the child was replaced after two called runs");
    assert_eq!(pid(4).await, second);
    assert_ne!(pid(5).await, second);
}

#[tokio::test]
async fn a_fresh_process_job_gets_a_new_child_for_each_run() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["scheduler", "worker", "--queues", "default=1"], "test");

    let mut pids = HashSet::new();
    for n in 0..3 {
        let run = client.start(fresh_child_pid, Number { n }).await.unwrap().run_id();
        let done = finished(&database.pool, run, SOON).await;
        assert_eq!(done.status, "succeeded", "{}", done.reason);
        pids.insert(done.output.unwrap());
    }
    assert_eq!(pids.len(), 3, "each run had a child of its own: {pids:?}");
}

#[tokio::test]
async fn a_timeout_override_changes_the_deadline_and_zero_means_none() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let ended = |job: &'static str, body: Value| {
        let local = &local;
        let pool = database.pool.clone();
        async move {
            let started: StartResponse = local.post(&format!("/v1/jobs/{job}/runs"), &body).await;
            let done = finished(&pool, started.run_id, Duration::from_secs(30)).await;
            (done.status, done.failed_because)
        }
    };

    // `sleeps` has a two-second timeout; `counts` one minute.
    assert_eq!(
        ended("sleeps", json!({ "input": { "seconds": 4 }, "timeout_seconds": 8 })).await,
        ("succeeded".to_owned(), None),
        "a longer override"
    );
    assert_eq!(
        ended("sleeps", json!({ "input": { "seconds": 4 }, "timeout_seconds": 0 })).await,
        ("succeeded".to_owned(), None),
        "no timeout"
    );
    assert_eq!(
        ended("counts", json!({ "input": { "seconds": 5 }, "timeout_seconds": 1 })).await,
        ("failed".to_owned(), Some("timeout".to_owned())),
        "a shorter override"
    );
    assert_eq!(
        ended("sleeps", json!({ "input": { "seconds": 4 } })).await,
        ("failed".to_owned(), Some("timeout".to_owned())),
        "without an override, the job's own timeout"
    );
}
