//! More process and shutdown rules (spec 10.1, 10.6, 11.1 and 11.3).

mod support;

use std::process::Command;
use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::process_rules::hungry_program;
use downbeat_e2e::*;
use sqlx::PgPool;
use support::{App, Database, SOON, eventually, finished, finished_job, with_api_in};
use uuid::Uuid;

/// The parent of a live process, from `/proc`.
fn parent_of(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?.1.split_whitespace().nth(1)?.parse().ok()
}

/// Whether a process has exited (gone, or a zombie its parent has not reaped).
fn exited(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_or(true, |stat| stat.rsplit_once(')').is_some_and(|(_, rest)| rest.trim_start().starts_with('Z')))
}

async fn wait_started(pool: &PgPool, run: Uuid, interruptions: i16) {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'running'
           AND function_started_at IS NOT NULL AND interruptions = {interruptions})"
    );
    eventually(pool, &sql, SOON).await;
}

// 10.1: a function runs in a child of the worker, never in the worker's own process.
#[tokio::test]
async fn a_function_runs_in_a_child_of_the_worker() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::with(&database, &["worker", "--queues", "default=1"], "test");

    let run = client.start(child_pid, Number { n: 1 }).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    let pid = u32::try_from(done.output.unwrap().as_u64().unwrap()).unwrap();
    assert_ne!(pid, app.pid(), "the function did not run in the worker");
    // The child is kept for the next run, so it is still alive.
    assert_eq!(parent_of(pid), Some(app.pid()), "the function ran in the worker's child");
}

// 10.6: memory is sampled every two seconds, so a program over its limit goes within a few.
#[tokio::test]
async fn a_program_over_its_memory_limit_is_killed_within_seconds() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(hungry_program, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, Duration::from_secs(30)).await;
    assert_eq!(done.failed_because.as_deref(), Some("out_of_memory"), "{}", done.reason);
    let ran_for: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM finished_at - started_at)::float8 FROM downbeat.run_history WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    // Two seconds between samples, plus the time to fill the memory, on a busy machine.
    assert!(ran_for < 8.0, "killed {ran_for} s after it started, against a 30 s timeout");
}

// 11.1: SIGINT stops the process as SIGTERM does: the run goes back to the queue, the API stops
// answering and the scheduler fires nothing more.
#[tokio::test]
async fn sigint_stops_the_worker_scheduler_and_api_as_sigterm_does() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    // `schedules` turns on a schedule that fires every second.
    let (app, _api, listen) = with_api_in(&database, "schedules").await;
    finished_job(pool, "ticker", SOON).await;
    let run = client.start(counts, Seconds { seconds: 30 }).await.unwrap().run_id();
    wait_started(pool, run, 0).await;

    Command::new("kill").args(["-INT", &app.pid().to_string()]).status().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !exited(app.pid()) {
        assert!(tokio::time::Instant::now() < deadline, "the process did not stop on SIGINT");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (state, because, interruptions): (String, Option<String>, i16) =
        sqlx::query_as("SELECT state, queued_because, interruptions FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!((state.as_str(), because.as_deref(), interruptions), ("queued", Some("no_free_worker"), 1));
    let workers: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.worker").fetch_one(pool).await.unwrap();
    assert_eq!(workers, 0);
    assert!(reqwest::get(format!("http://{listen}/healthz")).await.is_err(), "the API stopped answering");
    let ticks = "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = 'ticker')
                      + (SELECT count(*) FROM downbeat.run_history WHERE job_name = 'ticker')";
    let before: i64 = sqlx::query_scalar(ticks).fetch_one(pool).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after: i64 = sqlx::query_scalar(ticks).fetch_one(pool).await.unwrap();
    assert_eq!(after, before, "nothing fired after the stop");
}

async fn interrupted_event(pool: &PgPool, run: Uuid, times: i32) {
    let reported = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event'
          AND detail->>'event' = 'run_interrupted' AND run_id = '{run}' AND (detail->>'times')::int = {times})"
    );
    eventually(pool, &reported, Duration::from_secs(10)).await;
}

// 11.3: RunInterrupted is recorded at the fourth interruption and the ones after it too.
#[tokio::test]
async fn run_interrupted_is_recorded_at_the_fourth_and_fifth_interruptions() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    database.hold("counts").await;
    let run = client.start(counts, Seconds { seconds: 30 }).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET interruptions = 3 WHERE run_id = $1").bind(run).execute(pool).await.unwrap();
    database.release().await;

    for times in [4, 5] {
        let app = App::start(&database);
        wait_started(pool, run, i16::try_from(times - 1).unwrap()).await;
        app.stop();
        interrupted_event(pool, run, times).await;
    }
}
