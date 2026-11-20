//! The console in `--local` mode, in front of a scheduler: it forwards the screens' requests,
//! signed for the stand-in person, and refuses changes without the session's CSRF token.

mod support;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use support::{Database, SOON, finished, with_api_at};

/// The console binary, built next to this test's own binary by `scripts/test.sh`.
fn console_binary() -> std::path::PathBuf {
    let test = std::env::current_exe().unwrap();
    test.parent().and_then(std::path::Path::parent).unwrap().join("downbeat-console")
}

struct Console(Child);

impl Drop for Console {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn console(database: &Database, scheduler: &str) -> (Console, String) {
    let port = support::free_port();
    let listen = format!("127.0.0.1:{port}");
    let process = Command::new(console_binary())
        .args(["--local", "--scheduler", &format!("http://{scheduler}"), "--listen", &listen])
        .env("DOWNBEAT_CONSOLE_DATABASE_URL", &database.url)
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .spawn()
        .expect("the console starts");
    let base = format!("http://{listen}");
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(format!("{base}/api/me")).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "the console did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    (Console(process), base)
}

#[tokio::test]
async fn the_console_forwards_signed_requests_for_the_signed_in_person() {
    let database = Database::fresh().await;
    let (_app, _client, scheduler) = with_api_at(&database).await;
    let (_console, base) = console(&database, &scheduler).await;
    let http = reqwest::Client::new();

    let me: Value = http.get(format!("{base}/api/me")).send().await.unwrap().json().await.unwrap();
    assert_eq!(me["schedulers"][0]["scope"], "administer");
    assert_eq!(me["schedulers"][0]["reachable"], true);
    let csrf = me["csrf_token"].as_str().unwrap().to_owned();

    let response = http.get(format!("{base}/api/schedulers/local/v1/jobs")).send().await.unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    let text = response.text().await.unwrap();
    let jobs: Value = serde_json::from_str(&text).unwrap_or_else(|_| panic!("{status} {headers:?}: {text}"));
    assert!(jobs.as_array().unwrap().iter().any(|job| job["name"] == "echo"));

    let start = format!("{base}/api/schedulers/local/v1/jobs/echo/runs");
    let refused = http.post(&start).json(&json!({ "input": { "text": "x" } })).send().await.unwrap();
    assert_eq!(refused.status(), 403, "a change without the CSRF token is refused");

    let response = http
        .post(&start)
        .header("x-csrf-token", &csrf)
        .json(&json!({ "input": { "text": "from the console" } }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    let started: Value = serde_json::from_str(&text).unwrap_or_else(|_| panic!("{status}: {text}"));
    assert!(started["run_id"].is_string(), "{started}");
    let run_id = started["run_id"].as_str().unwrap().parse().unwrap();
    let done = finished(&database.pool, run_id, SOON).await;
    assert_eq!(done.status, "succeeded");

    let audit: Value =
        http.get(format!("{base}/api/schedulers/local/v1/audit")).send().await.unwrap().json().await.unwrap();
    assert!(
        audit["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["action"] == "start" && entry["actor"] == "local@localhost (via local)")
    );
}

#[tokio::test]
async fn the_console_serves_its_screens_with_a_strict_content_policy() {
    let database = Database::fresh().await;
    let (_app, _client, scheduler) = with_api_at(&database).await;
    let (_console, base) = console(&database, &scheduler).await;
    let page = reqwest::get(format!("{base}/runs")).await.unwrap();
    assert!(page.headers()["content-security-policy"].to_str().unwrap().starts_with("default-src 'self'"));
    assert_eq!(page.headers()["x-content-type-options"], "nosniff");
}

#[tokio::test]
async fn in_local_mode_only_this_machines_own_names_are_answered() {
    let database = Database::fresh().await;
    let (_app, _client, scheduler) = with_api_at(&database).await;
    let (_console, base) = console(&database, &scheduler).await;
    let http = reqwest::Client::new();

    for path in ["/api/me", "/auth/login", "/"] {
        let rebound = http.get(format!("{base}{path}")).header("host", "attacker.example").send().await.unwrap();
        assert_eq!(rebound.status(), 403, "{path} answered a page that pointed its own name here");
    }
    let ours = http.get(format!("{base}/api/me")).send().await.unwrap();
    assert_eq!(ours.status(), 200);
}

#[tokio::test]
async fn an_action_that_needs_administer_is_signed_again_for_someone_who_holds_it() {
    let database = Database::fresh().await;
    let (_app, _client, scheduler) = with_api_at(&database).await;
    let (_console, base) = console(&database, &scheduler).await;
    let http = reqwest::Client::new();
    let me: serde_json::Value = http.get(format!("{base}/api/me")).send().await.unwrap().json().await.unwrap();
    let csrf = me["csrf_token"].as_str().unwrap();

    // A start with a timeout of its own needs administer; the console first signs for operate.
    let started = http
        .post(format!("{base}/api/schedulers/local/v1/jobs/echo/runs"))
        .header("x-csrf-token", csrf)
        .json(&serde_json::json!({ "input": { "text": "x" }, "timeout_seconds": 30 }))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), 201, "{}", started.text().await.unwrap());
}

#[tokio::test]
async fn a_start_repeated_with_the_same_idempotency_key_returns_the_first_run() {
    let database = Database::fresh().await;
    let (_app, _client, scheduler) = with_api_at(&database).await;
    let (_console, base) = console(&database, &scheduler).await;
    let http = reqwest::Client::new();
    let me: Value = http.get(format!("{base}/api/me")).send().await.unwrap().json().await.unwrap();
    let csrf = me["csrf_token"].as_str().unwrap().to_owned();
    let start = || {
        http.post(format!("{base}/api/schedulers/local/v1/jobs/echo/runs"))
            .header("x-csrf-token", &csrf)
            .header("idempotency-key", "once-only")
            .json(&json!({ "input": { "text": "once" } }))
            .send()
    };

    let first = start().await.unwrap();
    assert_eq!(first.status(), 201);
    let first: Value = first.json().await.unwrap();
    let second = start().await.unwrap();
    assert_eq!(second.status(), 200);
    let second: Value = second.json().await.unwrap();
    assert_eq!(second["run_id"], first["run_id"], "{second}");
    assert_eq!(second["repeated"], true, "{second}");
}

#[tokio::test]
async fn in_local_mode_the_console_refuses_to_listen_beyond_this_machine() {
    let database = Database::fresh().await;
    let listen = format!("0.0.0.0:{}", support::free_port());
    let mut process = Command::new(console_binary())
        .args(["--local", "--scheduler", "http://127.0.0.1:7400", "--listen", &listen])
        .env("DOWNBEAT_CONSOLE_DATABASE_URL", &database.url)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the console starts");
    let deadline = std::time::Instant::now() + SOON;
    let status = loop {
        if let Some(status) = process.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() > deadline {
            let _ = process.kill();
            panic!("the console kept running on {listen}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut process.stderr.take().unwrap(), &mut stderr).unwrap();
    assert!(!status.success());
    assert!(stderr.contains("not a loopback address"), "{stderr}");
}
