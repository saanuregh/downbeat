//! Product code outside a child is guarded (spec 10.1): a panic while an input is decoded, hashed
//! or has its `one_per` field read at start, in a start preview's `done_for` closure, or in
//! `on_finish` and `on_event`, costs only that call. A child runs one function at a time and never
//! connects to the database (spec 10.3).

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use downbeat::InAppClient;
use downbeat_client::types::{Preview, StartResponse};
use downbeat_e2e::runtime_rules::records_its_span;
use downbeat_e2e::*;
use serde_json::json;
use support::{App, Database, SOON, Signer, eventually, finished, with_api_at};
use uuid::Uuid;

// 10.1: decoding, hashing and reading `one_per` of an input at start.
#[tokio::test]
async fn a_panic_while_an_input_is_started_is_an_error_and_the_scheduler_keeps_working() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let path = "/v1/jobs/takes_fragile/runs";

    for stage in ["decode", "identity", "one_per"] {
        let body = serde_json::to_vec(&json!({ "input": { "stage": stage, "account": stage } })).unwrap();
        let answer = local.send("POST", path, &body).await;
        let status = answer.status();
        let text = answer.text().await.unwrap();
        assert_eq!(status, 422, "{stage}: {text}");
        assert!(text.contains("panicked"), "{stage}: the answer says the input's code panicked: {text}");
    }
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run").fetch_one(&database.pool).await.unwrap();
    assert_eq!(runs, 0, "no run was created for a panicking input");

    let fine: StartResponse = local.post(path, &json!({ "input": { "stage": "fine", "account": "a" } })).await;
    let done = finished(&database.pool, fine.run_id, SOON).await;
    assert_eq!(done.status, "succeeded", "the scheduler still starts and runs work: {}", done.reason);
}

// 10.1: a start preview's `done_for` closure.
#[tokio::test]
async fn a_need_closure_that_panics_in_a_start_preview_counts_as_unmet() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let echoed: StartResponse = local.post("/v1/jobs/echo/runs", &json!({ "input": { "text": "fine" } })).await;
    assert_eq!(finished(&database.pool, echoed.run_id, SOON).await.status, "succeeded");
    let path = "/v1/jobs/has_a_fragile_need/runs?preview=true";

    let panicked: Preview = local.post(path, &json!({ "input": { "text": "panic" }, "ignore_conditions": true })).await;
    assert_eq!(panicked.false_conditions, ["needs echo"], "{panicked:?}");
    let met: Preview = local.post(path, &json!({ "input": { "text": "fine" }, "ignore_conditions": true })).await;
    assert!(met.false_conditions.is_empty(), "the API still answers, and the need is met: {met:?}");
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run").fetch_one(&database.pool).await.unwrap();
    assert_eq!(runs, 0, "a preview creates nothing");
}

// 10.1: `on_finish` and `on_event`. The test product's own callbacks cannot panic, so this hands
// delivery a definition with the same pipelines and callbacks that do.
#[tokio::test]
async fn a_callback_that_panics_costs_only_that_delivery() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    // No scheduler role, so nothing delivers the row before the test does.
    let app = App::with(&database, &["worker", "--queues", "default=2"], "test");
    let run = client.start(echo, Text { text: "delivered".into() }).await.unwrap().run_id();
    finished(&database.pool, run, SOON).await;
    app.stop();
    let event: i64 = sqlx::query_scalar(
        "INSERT INTO downbeat.audit (at, actor, action, detail, notify_after)
         VALUES (now(), 'test', 'event', '{\"event\": \"database_unreachable\", \"worker\": \"w\"}', now())
         RETURNING audit_id",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let pending = async || -> (f64, f64) {
        sqlx::query_as(
            "SELECT extract(epoch FROM (SELECT notify_after FROM downbeat.run_history WHERE run_id = $1) - now())::float8,
                    extract(epoch FROM (SELECT notify_after FROM downbeat.audit WHERE audit_id = $2) - now())::float8",
        )
        .bind(run)
        .bind(event)
        .fetch_one(&database.pool)
        .await
        .unwrap()
    };

    let mut panicking = definition_without_callbacks();
    panicking.on_finish(|_: downbeat::FinishedRun| async move {
        assert!(std::hint::black_box(false), "on_finish panics on purpose");
        Ok::<(), std::io::Error>(())
    });
    panicking.on_event(|_: downbeat::EventDelivery| async move {
        assert!(std::hint::black_box(false), "on_event panics on purpose");
        Ok::<(), std::io::Error>(())
    });
    downbeat::measure::deliver_callbacks(&database.pool, panicking).await.expect("delivery survives the panics");
    let (finish_ahead, event_ahead) = pending().await;
    assert!((240.0..=360.0).contains(&finish_ahead), "the run comes round again in five minutes: {finish_ahead}");
    assert!((240.0..=360.0).contains(&event_ahead), "the event comes round again in five minutes: {event_ahead}");
}

// 10.3: one function at a time per child.
#[tokio::test]
async fn a_child_never_runs_two_functions_at_once() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["worker", "--queues", "default=2"], "test");
    let base = std::env::temp_dir().join(format!("spans-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&base).unwrap();

    let mut runs = Vec::new();
    for n in 0..8 {
        let file = base.join(n.to_string()).display().to_string();
        runs.push(client.start(records_its_span, Text { text: file }).await.unwrap().run_id());
    }
    for run in runs {
        let done = finished(&database.pool, run, SOON).await;
        assert_eq!(done.status, "succeeded", "{}", done.reason);
    }
    let mut by_child: BTreeMap<u32, Vec<(u128, u128)>> = BTreeMap::new();
    for n in 0..8 {
        let line = std::fs::read_to_string(base.join(n.to_string())).unwrap();
        let parts: Vec<&str> = line.split(' ').collect();
        by_child
            .entry(parts[0].parse().unwrap())
            .or_default()
            .push((parts[1].parse().unwrap(), parts[2].parse().unwrap()));
    }
    std::fs::remove_dir_all(&base).unwrap();

    assert!(by_child.values().any(|spans| spans.len() >= 2), "some child ran several functions: {by_child:?}");
    for (child, spans) in &mut by_child {
        spans.sort_unstable();
        for pair in spans.windows(2) {
            assert!(pair[0].1 <= pair[1].0, "child {child} ran two functions at once: {spans:?}");
        }
    }
}

/// The ids of processes holding a connection to the test cluster, read with `ss`: the processes
/// with an established TCP connection to the cluster's port.
fn processes_connected_to_postgres() -> Vec<u32> {
    let url = std::env::var("DOWNBEAT_TEST_DATABASE_URL").unwrap();
    let port = url.rsplit_once(':').and_then(|(_, rest)| rest.split('/').next()).expect("the URL names a port");
    let listing = std::process::Command::new("ss")
        .args(["-tnpH", "state", "established", "dport", "=", &format!(":{port}")])
        .output()
        .expect("ss runs");
    String::from_utf8_lossy(&listing.stdout)
        .lines()
        .flat_map(|line| line.split("pid=").skip(1).filter_map(|rest| rest.split(',').next()?.parse().ok()))
        .collect()
}

// 10.3: a child never touches the engine's tables, shown at the operating-system level: while a
// child runs a function, it holds no connection to Postgres, though the worker that started it
// does (which shows the check sees connections).
#[tokio::test]
async fn a_child_holds_no_database_connection_while_its_function_runs() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    let run = client.start(naps, Number { n: 4000 }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND starts > 0)"),
        SOON,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let children = std::process::Command::new("pgrep").args(["-P", &app.pid().to_string()]).output().unwrap();
    let children: Vec<u32> =
        String::from_utf8_lossy(&children.stdout).lines().filter_map(|line| line.trim().parse().ok()).collect();
    assert!(!children.is_empty(), "the worker has a child");
    let connected = processes_connected_to_postgres();
    assert!(connected.contains(&app.pid()), "the worker's own connections are seen: {connected:?}");
    for child in &children {
        assert!(!connected.contains(child), "child {child} holds a database connection: {connected:?}");
    }
    assert_eq!(finished(&database.pool, run, SOON).await.status, "succeeded");
}
