//! Rules of child processes (spec 9.4, 10.1, 10.2): a child cut short while it names a command's
//! program or checks its output is killed at once and not reused; product code guarded in the API
//! process; and every process started from one dedicated thread.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use downbeat::InAppClient;
use downbeat_client::types::{Preview, StartResponse};
use downbeat_e2e::process_rules::*;
use downbeat_e2e::{Number, Text, child_pid, definition};
use serde_json::json;
use support::{App, Database, SOON, Signer, eventually, finished, with_api_at};

/// Kills the processes it holds when dropped, so a failing test leaves none running.
struct Reaper(Vec<u32>);

impl Drop for Reaper {
    fn drop(&mut self) {
        for pid in &self.0 {
            if alive(*pid) {
                let _ = std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status();
            }
        }
    }
}

/// Whether the process runs: it exists and is not a zombie waiting to be reaped.
fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .is_ok_and(|stat| stat.rsplit_once(')').is_some_and(|(_, rest)| !rest.trim_start().starts_with('Z')))
}

/// Waits up to `within` for the process to stop running; returns how long that took.
async fn stopped_within(pid: u32, within: Duration) -> Option<Duration> {
    let started = tokio::time::Instant::now();
    while alive(pid) {
        if started.elapsed() > within {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Some(started.elapsed())
}

/// Waits for a file holding a process id, and returns the id.
async fn pid_in(file: &std::path::Path) -> u32 {
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        if let Some(pid) = std::fs::read_to_string(file).ok().and_then(|text| text.trim().parse().ok()) {
            return pid;
        }
        assert!(tokio::time::Instant::now() < deadline, "{} was not written", file.display());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn child_of_next_run(client: &InAppClient, database: &Database) -> u32 {
    let run = client.start(child_pid, Number { n: 1 }).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    u32::try_from(done.output.unwrap().as_u64().unwrap()).unwrap()
}

fn one_slot(database: &Database) -> App {
    App::with(database, &["scheduler", "worker", "--queues", "default=1"], "test")
}

#[tokio::test]
async fn a_child_cut_short_while_naming_a_program_is_killed_at_once_and_not_reused() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = one_slot(&database);
    let pid_file = app.finished_dir.join("naming.pid");

    let run = client.start(names_its_program_slowly, Text { text: pid_file.display().to_string() }).await.unwrap();
    let namer = pid_in(&pid_file).await;
    let _reaper = Reaper(vec![namer]);
    // The one-second timeout counts from the call's start, just before the pid is written. Asking
    // first would add kill_grace (1 s); naming the program would take five.
    let took = stopped_within(namer, Duration::from_secs(5)).await;
    assert!(took.is_some_and(|took| took < Duration::from_millis(1700)), "the child stopped after {took:?}");
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("timeout")), "{}", done.reason);

    assert_ne!(child_of_next_run(&client, &database).await, namer, "the next run got another child");
}

#[tokio::test]
async fn a_child_cut_short_while_checking_a_command_output_is_killed_at_once_and_not_reused() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = one_slot(&database);
    let pid_file = app.finished_dir.join("checking.pid");

    let run = client.start(touchy_output, Text { text: "slow".into() }).await.unwrap();
    let checker = pid_in(&pid_file).await;
    let _reaper = Reaper(vec![checker]);
    // The two-second timeout counts from the call's start, before the program ran; the check takes
    // five seconds, and asking first would add kill_grace (1 s).
    let took = stopped_within(checker, Duration::from_secs(6)).await;
    assert!(took.is_some_and(|took| took < Duration::from_millis(2500)), "the child stopped after {took:?}");
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("timeout")), "{}", done.reason);

    assert_ne!(child_of_next_run(&client, &database).await, checker, "the next run got another child");
}

#[tokio::test]
async fn a_condition_that_panics_in_a_start_preview_counts_as_false() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let path = "/v1/jobs/panics_when_tested/runs?preview=true";

    let panicked: Preview = local.post(path, &json!({ "input": { "text": "panic" }, "ignore_conditions": true })).await;
    assert_eq!(panicked.false_conditions, ["only if 'calm'"], "{panicked:?}");
    let calm: Preview = local.post(path, &json!({ "input": { "text": "fine" }, "ignore_conditions": true })).await;
    assert!(calm.false_conditions.is_empty(), "the API still answers after the panic: {calm:?}");
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run").fetch_one(&database.pool).await.unwrap();
    assert_eq!(runs, 0, "a preview creates nothing");
}

#[tokio::test]
async fn an_output_that_panics_when_marked_as_succeeded_is_refused_cleanly() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));

    let failed: StartResponse =
        local.post("/v1/jobs/touchy_output/runs", &json!({ "input": { "text": "fail" } })).await;
    assert_eq!(finished(&database.pool, failed.run_id, SOON).await.status, "failed");
    let path = format!("/v1/runs/{}/mark-succeeded", failed.run_id);

    let body = serde_json::to_vec(&json!({ "output": { "text": "panic" }, "note": "by hand" })).unwrap();
    let refused = local.send("POST", &path, &body).await;
    let status = refused.status();
    let text = refused.text().await.unwrap();
    assert_eq!(status, 422, "{text}");
    assert!(text.contains("panicked"), "the refusal says the decoding panicked: {text}");
    let still: String = sqlx::query_scalar("SELECT status FROM downbeat.run_history WHERE run_id = $1")
        .bind(failed.run_id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(still, "failed");

    let body = serde_json::to_vec(&json!({ "output": { "text": "fine" }, "note": "by hand" })).unwrap();
    let marked = local.send("POST", &path, &body).await;
    assert!(marked.status().is_success(), "the API still works after the panic: {}", marked.status());
}

/// For each thread of `pid`, the processes it started that are still running.
fn children_by_thread(pid: u32) -> BTreeMap<u32, Vec<u32>> {
    let mut by_thread = BTreeMap::new();
    for entry in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap().flatten() {
        let Ok(thread) = entry.file_name().to_string_lossy().parse::<u32>() else { continue };
        let children: Vec<u32> = std::fs::read_to_string(entry.path().join("children"))
            .unwrap_or_default()
            .split_whitespace()
            .filter_map(|child| child.parse().ok())
            .collect();
        if !children.is_empty() {
            by_thread.insert(thread, children);
        }
    }
    by_thread
}

#[tokio::test]
async fn every_process_is_started_from_one_dedicated_thread() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let run = client.start(sleeps_a_minute, ()).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND function_started_at IS NOT NULL)"
        ),
        SOON,
    )
    .await;
    // Two warm children (one per slot) and the command's program.
    let deadline = tokio::time::Instant::now() + SOON;
    let by_thread = loop {
        let by_thread = children_by_thread(app.pid());
        if by_thread.values().map(Vec::len).sum::<usize>() >= 3 {
            break by_thread;
        }
        assert!(tokio::time::Instant::now() < deadline, "fewer than three processes: {by_thread:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let _reaper = Reaper(by_thread.values().flatten().copied().collect());

    assert_eq!(by_thread.len(), 1, "the processes were started from more than one thread: {by_thread:?}");
    let thread = by_thread.keys().next().unwrap();
    let name = std::fs::read_to_string(format!("/proc/{}/task/{thread}/comm", app.pid())).unwrap();
    assert!(name.starts_with("downbeat-spawn"), "they were started from thread '{}'", name.trim());
}

#[tokio::test]
async fn a_message_the_worker_cannot_read_ends_the_run_as_a_crash() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = one_slot(&database);

    let run = client.start(names_a_huge_program, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("crash")), "{}", done.reason);

    let next = client.start(child_pid, Number { n: 1 }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, next, SOON).await.status, "succeeded", "the slot is free again");
}

#[tokio::test]
async fn long_progress_and_error_text_is_cut_to_fit() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = one_slot(&database);

    let quiet = client.start(says_too_much, Text { text: "succeed".into() }).await.unwrap().run_id();
    let done = finished(&database.pool, quiet, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);

    let failing = client.start(says_too_much, Text { text: "fail".into() }).await.unwrap().run_id();
    let done = finished(&database.pool, failing, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("error")), "{}", done.reason);
    let error: Vec<String> = sqlx::query_scalar("SELECT error_chain FROM downbeat.run_history WHERE run_id = $1")
        .bind(failing)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let kept = error.concat();
    assert!(kept.starts_with("eee") && kept.len() <= downbeat::MAX_VALUE_BYTES + 16, "{} bytes kept", kept.len());
}
