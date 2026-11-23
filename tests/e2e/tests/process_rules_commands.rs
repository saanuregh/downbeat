//! Rules of a command's program (spec 10.6, 10.7, 10.8, 11.2): its memory limit, its process group
//! killed when it exits, how it ends, and a "not ready" during the shutdown grace period.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::process_rules::*;
use downbeat_e2e::{ExitPlan, Text, definition, exits};
use support::{App, Database, SOON, eventually, finished};

#[tokio::test]
async fn a_program_over_max_memory_is_killed() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(hungry_program, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, Duration::from_secs(30)).await;
    assert_eq!(
        (done.status.as_str(), done.failed_because.as_deref()),
        ("failed", Some("out_of_memory")),
        "{}",
        done.reason
    );
}

#[tokio::test]
async fn a_program_that_exits_takes_its_background_process_with_it() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let pid_file = app.finished_dir.join("helper.pid");

    let run = client.start(leaves_a_helper, Text { text: pid_file.display().to_string() }).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);

    let pid = std::fs::read_to_string(&pid_file).unwrap().trim().to_owned();
    let proc_entry = std::path::PathBuf::from(format!("/proc/{pid}"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while proc_entry.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let survived = proc_entry.exists();
    if survived {
        let _ = std::process::Command::new("kill").args(["-KILL", &pid]).status();
    }
    assert!(!survived, "the helper {pid} outlived its program");
}

#[tokio::test]
async fn a_program_finds_its_output_file_made_and_private_to_its_user() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(reports_its_output_file_mode, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.output, Some(serde_json::json!({ "text": "600" })));
}

#[tokio::test]
async fn an_output_that_does_not_fit_the_type_fails_as_bad_input() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let plan = ExitPlan { code: 0, output: r#"{"nope": 1}"#.into() };
    let run = client.start(exits, plan).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(
        (done.status.as_str(), done.failed_because.as_deref()),
        ("failed", Some("bad_input")),
        "{}",
        done.reason
    );
}

#[tokio::test]
async fn a_program_ended_by_a_signal_fails_as_signal() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(killed_by_a_signal, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("signal")), "{}", done.reason);
}

#[tokio::test]
async fn only_the_last_16_kib_of_standard_error_are_kept() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(long_stderr, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("exit_code"), "{}", done.reason);
    let tail = done.stderr_tail.unwrap_or_default();
    assert_eq!(tail.len(), 16 * 1024, "the program wrote over 40 000 bytes");
    assert!(
        tail.starts_with('a') && tail.ends_with("the end\n"),
        "the tail is the last part: {:?}",
        &tail[tail.len() - 20..]
    );
}

#[tokio::test]
async fn a_command_that_says_not_ready_during_the_shutdown_grace_is_recorded_as_not_ready() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let run = client.start(not_ready_when_asked_to_stop, ()).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND function_started_at IS NOT NULL)"
        ),
        SOON,
    )
    .await;
    // The shell sets its trap before it waits.
    tokio::time::sleep(Duration::from_millis(500)).await;
    app.stop();

    let (state, because, failures, interruptions): (String, Option<String>, i16, i16) =
        sqlx::query_as("SELECT state, queued_because, failures, interruptions FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(state, "queued");
    assert_eq!(because.as_deref(), Some("not_ready"), "recorded as not ready, not interrupted");
    assert_eq!((failures, interruptions), (0, 0));
}
