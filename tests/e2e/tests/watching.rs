//! Watching and health (spec 16): slow runs and overdue jobs are told once, and the health
//! endpoints and `health` command say whether the scheduler and workers can do their work.

mod support;

use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use sqlx::PgPool;
use support::{App, Database, SOON, Signer, free_port, with_api_at};
use uuid::Uuid;

/// The events of this kind recorded for this job, oldest first.
async fn events(pool: &PgPool, event: &str, job: &str) -> Vec<Value> {
    sqlx::query_scalar(
        "SELECT detail FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = $1 AND job_name = $2
          ORDER BY at, audit_id",
    )
    .bind(event)
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn watch(pool: &PgPool) {
    downbeat::measure::watch_once(pool, downbeat_e2e::definition()).await.unwrap();
}

async fn new_chain(pool: &PgPool, job: &str) -> Uuid {
    let chain = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by) VALUES ($1, 'test', $2, 'person')",
    )
    .bind(chain)
    .bind(job)
    .execute(pool)
    .await
    .unwrap();
    chain
}

/// A finished run of `job` that began `ago` seconds ago and took `took` seconds.
async fn finished_run(pool: &PgPool, job: &str, status: &str, ago: f64, took: f64) {
    let chain = new_chain(pool, job).await;
    sqlx::query(
        "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id,
                created_at, status, reason, started_at, finished_at)
         VALUES ($1, $2, 'default', 0, now(), 'person', $3, now() - make_interval(secs => $4), $5, 'Test.',
                 now() - make_interval(secs => $4), now() - make_interval(secs => $4) + make_interval(secs => $6))",
    )
    .bind(Uuid::new_v4())
    .bind(job)
    .bind(chain)
    .bind(ago)
    .bind(status)
    .bind(took)
    .execute(pool)
    .await
    .unwrap();
}

/// A run of `job` whose function began `ago` seconds ago and is still running.
async fn running_run(pool: &PgPool, job: &str, ago: f64) -> Uuid {
    let chain = new_chain(pool, job).await;
    let run_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.run (run_id, job_name, queue, state, due_at, started_by, chain_id, starts,
                function_started_at, first_started_at)
         VALUES ($1, $2, 'default', 'running', now(), 'person', $3, 1,
                 now() - make_interval(secs => $4), now() - make_interval(secs => $4))",
    )
    .bind(run_id)
    .bind(job)
    .bind(chain)
    .bind(ago)
    .execute(pool)
    .await
    .unwrap();
    run_id
}

/// 16.1: with nothing but a running scheduler, an overdue job is reported within one period.
#[tokio::test]
async fn the_scheduler_watches_by_itself() {
    let database = Database::fresh().await;
    let _app = App::with(&database, &["scheduler", "worker", "--queues", "default=1"], "test");
    support::eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'scheduler')", SOON)
        .await;
    // Past the pass the scheduler makes as it starts, so only a later pass can report this.
    tokio::time::sleep(Duration::from_secs(2)).await;
    finished_run(&database.pool, "expected_often", "failed", 60.0, 0.1).await;
    support::eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = 'job_overdue'
          AND job_name = 'expected_often')",
        Duration::from_secs(40),
    )
    .await;
}

/// 16.2: fewer than five successes give no slow threshold.
#[tokio::test]
async fn a_job_with_fewer_than_five_successes_is_never_slow() {
    let database = Database::fresh().await;
    for n in 0..4 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 1.0).await;
    }
    // Failures do not count towards the five.
    finished_run(&database.pool, "echo", "failed", 90.0, 1.0).await;
    running_run(&database.pool, "echo", 30.0).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "run_slow", "echo").await.is_empty());
}

/// 16.2 and 16.3: past 1.5 × p90 of the last successes, a run is slow, once.
#[tokio::test]
async fn a_run_past_its_slow_threshold_is_told_once() {
    let database = Database::fresh().await;
    for n in 0..5 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 1.0).await;
    }
    // Within the threshold of 1.5 seconds.
    running_run(&database.pool, "echo", 1.0).await;
    let slow = running_run(&database.pool, "echo", 5.0).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "run_slow", "echo").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert_eq!(told[0]["run_id"], slow.to_string());
    assert!(told[0]["running_seconds"].as_i64().unwrap() >= 5, "{told:?}");
    assert_eq!(told[0]["expected_seconds"], 1);
}

/// 16.2: the slow threshold is at least one second, however quick the job usually is.
#[tokio::test]
async fn the_slow_threshold_is_at_least_one_second() {
    let database = Database::fresh().await;
    for n in 0..5 {
        finished_run(&database.pool, "echo", "succeeded", 100.0 + f64::from(n), 0.1).await;
    }
    let run_id = running_run(&database.pool, "echo", 0.5).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "run_slow", "echo").await.is_empty());
    sqlx::query("UPDATE downbeat.run SET function_started_at = now() - interval '2 seconds' WHERE run_id = $1")
        .bind(run_id)
        .execute(&database.pool)
        .await
        .unwrap();
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "run_slow", "echo").await.len(), 1);
}

/// 16.4: a job that has never run is never overdue; one that never succeeded is, from its first run.
#[tokio::test]
async fn overdue_counts_from_the_first_run_when_nothing_succeeded() {
    let database = Database::fresh().await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "it never ran");

    finished_run(&database.pool, "expected_often", "failed", 1.0, 0.1).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "its first run is recent");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "job_overdue", "expected_often").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert_eq!(told[0]["last_success"], Value::Null);
}

/// 16.4 and 16.5: overdue since the latest success, once per lapse, and again after a new lapse.
#[tokio::test]
async fn overdue_is_told_once_per_lapse() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    finished_run(&database.pool, "expected_often", "succeeded", 1.0, 0.1).await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty(), "it succeeded recently");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    let told = events(&database.pool, "job_overdue", "expected_often").await;
    assert_eq!(told.len(), 1, "{told:?}");
    assert!(told[0]["last_success"].is_string(), "{told:?}");

    // Finished now, after the event, as a real success would be.
    finished_run(&database.pool, "expected_often", "succeeded", 0.0, 0.0).await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1, "it just succeeded");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    watch(&database.pool).await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 2, "a new lapse");
}

/// 16.5: a paused job is not told overdue.
#[tokio::test]
async fn a_paused_job_is_not_told_overdue() {
    let database = Database::fresh().await;
    finished_run(&database.pool, "expected_often", "succeeded", 10.0, 0.1).await;
    database.hold("expected_often").await;
    watch(&database.pool).await;
    assert!(events(&database.pool, "job_overdue", "expected_often").await.is_empty());
    database.release().await;
    watch(&database.pool).await;
    assert_eq!(events(&database.pool, "job_overdue", "expected_often").await.len(), 1);
}

/// 16.6: `/healthz` and `/version` need no token.
#[tokio::test]
async fn health_and_version_answer_without_a_token() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let health = reqwest::get(format!("http://{listen}/healthz")).await.unwrap();
    assert_eq!(health.status(), 200);
    let health: Value = health.json().await.unwrap();
    assert_eq!(health["ok"], true);
    assert_eq!(health["api"], downbeat_api::API_VERSION);

    let version = reqwest::get(format!("http://{listen}/version")).await.unwrap();
    assert_eq!(version.status(), 200);
    let version: Value = version.json().await.unwrap();
    assert_eq!(version["api"], downbeat_api::API_VERSION);
    // The e2e crate shares the workspace's version with downbeat.
    assert_eq!(version["engine"], env!("CARGO_PKG_VERSION"));
}

/// 16.6: `/readyz` answers only while the database does.
#[tokio::test]
async fn ready_only_while_the_database_answers() {
    let database = Database::fresh().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let _app = App::with(&database, &["scheduler", "--local", "--listen", &listen], "test");
    let ready = format!("http://{listen}/readyz");
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(&ready).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "the API did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let answer: Value = reqwest::get(&ready).await.unwrap().json().await.unwrap();
    assert_eq!(answer["ok"], true);
    assert_eq!(answer["api"], downbeat_api::API_VERSION);

    let name: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&database.pool).await.unwrap();
    let admin = PgPool::connect(&std::env::var("DOWNBEAT_TEST_DATABASE_URL").unwrap()).await.unwrap();
    // `ALLOW_CONNECTIONS false` holds back superusers too, unlike a connection limit.
    sqlx::query(&format!("ALTER DATABASE {name} ALLOW_CONNECTIONS false")).execute(&admin).await.unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1")
        .bind(&name)
        .execute(&admin)
        .await
        .unwrap();
    let down = reqwest::Client::new().get(&ready).timeout(Duration::from_secs(30)).send().await.unwrap();
    let status = down.status();
    let body: Value = down.json().await.unwrap();
    sqlx::query(&format!("ALTER DATABASE {name} ALLOW_CONNECTIONS true")).execute(&admin).await.unwrap();
    admin.close().await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"], "database", "{body}");

    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        let answer = reqwest::get(&ready).await.unwrap();
        if answer.status() == 200 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "not ready again after the database came back");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn health(database: &Database) -> bool {
    Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .arg("health")
        .env("DOWNBEAT_DATABASE_URL", &database.url)
        .env("DOWNBEAT_ENVIRONMENT", "test")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .status()
        .expect("the health command runs")
        .success()
}

/// 16.7: `health` succeeds while a worker on this host runs, and fails once none has refreshed its
/// row within two heartbeats (one second each in the test product).
#[tokio::test]
async fn health_follows_the_workers_on_this_host() {
    let database = Database::fresh().await;
    assert!(!health(&database), "no worker has ever run");

    let app = App::start(&database);
    support::eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'worker')", SOON)
        .await;
    assert!(health(&database), "a worker runs");
    app.stop();
    assert!(!health(&database), "the worker stopped");

    let host = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap().trim().to_owned();
    let worker = format!("{host}-1-abcd");
    sqlx::query(
        "INSERT INTO downbeat.worker (worker_id, role, build, last_seen_at) VALUES ($1, 'worker', 'test', now() - interval '3 seconds')",
    )
    .bind(&worker)
    .execute(&database.pool)
    .await
    .unwrap();
    assert!(!health(&database), "its row is older than two heartbeats");
    sqlx::query("UPDATE downbeat.worker SET last_seen_at = now() WHERE worker_id = $1")
        .bind(&worker)
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(health(&database), "its row is fresh");
}

/// 16.7: a scheduler alone is not a worker.
#[tokio::test]
async fn health_fails_with_only_a_scheduler() {
    let database = Database::fresh().await;
    let _app = App::with(&database, &["scheduler"], "test");
    support::eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'scheduler')", SOON)
        .await;
    assert!(!health(&database));
}

/// 14.12: a request that fails on the database (503) is not counted as refused.
#[tokio::test]
async fn a_database_error_is_not_a_refused_request() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    sqlx::query("DROP TABLE downbeat.used_token").execute(&database.pool).await.unwrap();
    let body = serde_json::to_vec(&serde_json::json!({ "input": { "text": "x" } })).unwrap();
    let answer = Signer::local(&format!("http://{listen}")).send("POST", "/v1/jobs/echo/runs", &body).await;
    let status = answer.status();
    let detail: Value = answer.json().await.unwrap();
    assert_eq!(status, 503, "{detail}");
    assert_eq!(detail["error"], "database", "{detail}");
    let refused: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = 'request_refused')",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(!refused);
}

#[tokio::test]
async fn a_job_whose_only_run_never_finishes_is_overdue_too() {
    let database = Database::fresh().await;
    // A run of the job queued three seconds ago, never claimed: no app is running.
    database.hold("expected_often").await;
    let client = downbeat::InAppClient::connect(downbeat_e2e::definition, &database.url).await.unwrap();
    let run = client.start(downbeat_e2e::runtime_rules::expected_often, ()).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET created_at = now() - interval '3 seconds' WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    database.release().await;

    downbeat::measure::watch_once(&database.pool, downbeat_e2e::definition()).await.unwrap();
    let told: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM downbeat.audit WHERE detail->>'event' = 'job_overdue' AND job_name = 'expected_often'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(told, 1, "two seconds without a success since its first run, which never finished");
}
