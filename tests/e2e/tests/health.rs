//! Health (spec 16.6 and 16.7): the health endpoints and the `health` command say whether the
//! scheduler and the workers on this host can do their work.

mod support;

use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use sqlx::PgPool;
use support::{App, Database, SOON, free_port, with_api_at};

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

/// 16.7: the host is matched whole and only worker rows count, so a worker on `<host>-2` or a
/// scheduler on this host does not make this host healthy.
#[tokio::test]
async fn health_counts_only_workers_on_exactly_this_host() {
    let database = Database::fresh().await;
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap().trim().to_owned();
    let insert = |worker_id: String, role: &'static str| {
        let pool = database.pool.clone();
        async move {
            sqlx::query("INSERT INTO downbeat.worker (worker_id, role, build) VALUES ($1, $2, 'test')")
                .bind(worker_id)
                .bind(role)
                .execute(&pool)
                .await
                .unwrap();
        }
    };
    insert(format!("{host}-2-123-abcd"), "worker").await;
    insert(format!("{host}-124-beef"), "scheduler").await;
    assert!(!health(&database), "a worker on another host and a scheduler on this one");
    insert(format!("{host}-125-cafe"), "worker").await;
    assert!(health(&database), "a worker on this host");
}
