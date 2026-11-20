//! What every end-to-end test needs: a fresh database, the product running as real processes, and
//! a way to wait for a run to finish.

#![allow(dead_code, unreachable_pub)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

pub const SOON: Duration = Duration::from_secs(20);

/// A database of its own for one test, created in the throwaway cluster `scripts/test.sh` starts.
pub struct Database {
    pub url: String,
    pub pool: PgPool,
}

impl Database {
    pub async fn fresh() -> Database {
        let base = std::env::var("DOWNBEAT_TEST_DATABASE_URL")
            .expect("DOWNBEAT_TEST_DATABASE_URL is not set; run the tests with scripts/test.sh");
        let admin = PgPool::connect(&base).await.expect("the test cluster is reachable");
        let name = format!("test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.expect("a test database");
        admin.close().await;
        let url = base.replacen("/postgres", &format!("/{name}"), 1);
        downbeat::migrate(&url).await.expect("the engine's tables are created");
        let pool = PgPool::connect(&url).await.expect("the test database is reachable");
        Database { url, pool }
    }
}

/// The test product, running as one process with the roles asked for.
pub struct App {
    process: Child,
}

impl App {
    /// `scheduler worker --queues default=2` in environment `test`.
    pub fn start(database: &Database) -> App {
        App::with(database, &["scheduler", "worker", "--queues", "default=2"], "test")
    }

    pub fn with(database: &Database, args: &[&str], environment: &str) -> App {
        let process = Command::new(env!("CARGO_BIN_EXE_e2e-app"))
            .args(args)
            .env("DOWNBEAT_DATABASE_URL", &database.url)
            .env("DOWNBEAT_ENVIRONMENT", environment)
            .env("RUST_LOG", "warn,downbeat=info")
            .stdin(Stdio::null())
            .spawn()
            .expect("the test app starts");
        App { process }
    }

    /// Sends SIGTERM and waits for the process to stop.
    pub fn stop(mut self) {
        let pid = self.process.id().to_string();
        Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        let _ = self.process.wait();
    }

    /// Kills the process outright, as a crash or a lost machine would.
    pub fn kill(mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

impl Drop for App {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// The test product with the control API on a free local port, and a client for it.
pub async fn with_api(database: &Database) -> (App, downbeat_client::Client) {
    let (app, client, _) = with_api_at(database).await;
    (app, client)
}

/// The same, with the API's address.
pub async fn with_api_at(database: &Database) -> (App, downbeat_client::Client, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let listen = format!("127.0.0.1:{port}");
    let app =
        App::with(database, &["scheduler", "worker", "--queues", "default=2", "--local", "--listen", &listen], "test");
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        match downbeat_client::Client::builder(&format!("http://{listen}")).local().connect().await {
            Ok(client) => return (app, client, listen),
            Err(error) => {
                assert!(tokio::time::Instant::now() < deadline, "the API did not come up: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// A finished run, as the tests look at it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Finished {
    pub run_id: Uuid,
    pub job_name: String,
    pub chain_id: Uuid,
    pub status: String,
    pub reason: String,
    pub skipped_because: Option<String>,
    pub failed_because: Option<String>,
    pub output: Option<Value>,
    pub failures: i16,
    pub lost_count: i16,
    pub interruptions: i16,
    pub starts: i32,
    pub started_by: String,
    pub stderr_tail: Option<String>,
}

/// Waits until the run is in the history.
pub async fn finished(pool: &PgPool, run_id: Uuid, within: Duration) -> Finished {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let row: Option<Finished> = sqlx::query_as("SELECT * FROM downbeat.run_history WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(pool)
            .await
            .unwrap();
        if let Some(row) = row {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run {run_id} did not finish within {within:?}: {:?}",
            unfinished(pool, run_id).await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits until a run of this job has finished, and returns the first one.
pub async fn finished_job(pool: &PgPool, job: &str, within: Duration) -> Finished {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let row: Option<Finished> = sqlx::query_as(
            "SELECT * FROM downbeat.run_history WHERE job_name = $1 AND joined_run IS NULL ORDER BY finished_at LIMIT 1",
        )
        .bind(job)
        .fetch_optional(pool)
        .await
        .unwrap();
        if let Some(row) = row {
            return row;
        }
        assert!(tokio::time::Instant::now() < deadline, "no run of {job} finished within {within:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits until a condition on the database holds.
pub async fn eventually(pool: &PgPool, sql: &str, within: Duration) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let holds: bool = sqlx::query_scalar(sql).fetch_one(pool).await.unwrap();
        if holds {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "`{sql}` did not hold within {within:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn unfinished(pool: &PgPool, run_id: Uuid) -> Option<(String, Option<String>, Option<String>)> {
    sqlx::query_as("SELECT state, queued_because, queued_reason FROM downbeat.run WHERE run_id = $1")
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .unwrap()
}
