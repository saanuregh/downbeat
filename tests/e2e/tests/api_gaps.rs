//! The control API's rules not checked elsewhere: the body hash's presence, the log line for every
//! refused request, and reading a chain with a token limited to some jobs.

mod support;

use std::process::{Command, Stdio};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use downbeat_api::{Claims, PrivateKey, RequestBinding, Scope, sign};
use downbeat_e2e::FINISHED_DIR;
use serde_json::json;
use sha2::{Digest, Sha256};
use support::{Database, SOON, Signer, finished_job, with_api_at, with_callers};
use uuid::Uuid;

/// A token for this request whose `body_sha256` is replaced by `body_sha256`.
fn token_with_hash(signer: &Signer, method: &str, path: &str, body: &[u8], body_sha256: Option<String>) -> String {
    let binding = RequestBinding { method, path_and_query: path, body };
    let now = chrono::Utc::now().timestamp();
    let claims =
        Claims::for_request(&signer.caller, &signer.caller, &signer.audience, signer.scope, None, binding, now);
    sign(&signer.key, &Claims { body_sha256, ..claims })
}

#[tokio::test]
async fn a_body_hash_must_be_absent_for_an_empty_body_and_present_for_any_other() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));

    let hash_of_nothing = Some(URL_SAFE_NO_PAD.encode(Sha256::digest(b"")));
    let token = token_with_hash(&local, "GET", "/v1/jobs", b"", hash_of_nothing);
    assert_eq!(local.send_with("GET", "/v1/jobs", b"", &token).await.status(), 401, "a hash for an empty body");
    assert_eq!(local.send("GET", "/v1/jobs", b"").await.status(), 200);

    let path = "/v1/jobs/echo/runs";
    let body = serde_json::to_vec(&json!({ "input": { "text": "hashed" } })).unwrap();
    let token = token_with_hash(&local, "POST", path, &body, None);
    assert_eq!(local.send_with("POST", path, &body, &token).await.status(), 401, "no hash for a body");
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run").fetch_one(&database.pool).await.unwrap();
    assert_eq!(runs, 0, "the refused start created nothing");
    assert_eq!(local.send("POST", path, &body).await.status(), 201);
}

#[tokio::test]
async fn every_refused_request_is_logged() {
    let database = Database::fresh().await;
    let key = PrivateKey::generate();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"reader\"\nkeys = [\"{}\"]\nmax_scope = \"read\"\n",
        key.public().to_base64()
    );
    let dir = std::env::temp_dir().join(format!("refused-log-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let callers_file = dir.join("callers.toml");
    std::fs::write(&callers_file, callers).unwrap();
    let log = dir.join("stderr.log");
    let listen = format!("127.0.0.1:{}", support::free_port());
    // Started here rather than with `App`, to keep its standard error.
    let mut process = Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .args(["scheduler", "--listen", &listen, "--callers", &callers_file.display().to_string()])
        .env(FINISHED_DIR, &dir)
        .env("DOWNBEAT_DATABASE_URL", &database.url)
        .env("DOWNBEAT_ENVIRONMENT", "test")
        .env("RUST_LOG", "warn,downbeat=info")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let base = format!("http://{listen}");
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(format!("{base}/healthz")).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "the API did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let reader = Signer { base, caller: "reader".into(), audience: "test".into(), key, scope: Scope::Read, jobs: None };
    let impostor = Signer { key: PrivateKey::generate(), ..like(&reader) };
    assert_eq!(impostor.send("GET", "/v1/jobs", b"").await.status(), 401);
    let greedy = Signer { scope: Scope::Administer, ..like(&reader) };
    assert_eq!(greedy.send("GET", "/v1/jobs", b"").await.status(), 403);
    assert_eq!(reader.send("GET", "/v1/jobs", b"").await.status(), 200);

    let refused_with = |text: &str, status: &str| {
        text.lines().any(|line| line.contains("WARN") && line.contains("refused") && line.contains(status))
    };
    let deadline = tokio::time::Instant::now() + SOON;
    let text = loop {
        let text = std::fs::read_to_string(&log).unwrap();
        if (refused_with(&text, "401") && refused_with(&text, "403")) || tokio::time::Instant::now() > deadline {
            break text;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let _ = process.kill();
    let _ = process.wait();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(refused_with(&text, "401"), "a warning names the 401 refusal: {text}");
    assert!(refused_with(&text, "403"), "a warning names the 403 refusal: {text}");
    let refusals = text.lines().filter(|line| line.contains("refused")).count();
    assert_eq!(refusals, 2, "one line per refused request, none for the accepted one: {text}");
}

/// The same caller and key, with other fields as given.
fn like(signer: &Signer) -> Signer {
    Signer {
        base: signer.base.clone(),
        caller: signer.caller.clone(),
        audience: signer.audience.clone(),
        key: signer.key.clone(),
        scope: signer.scope,
        jobs: signer.jobs.clone(),
    }
}

#[tokio::test]
async fn a_chain_is_refused_to_a_token_that_does_not_cover_its_first_job() {
    let database = Database::fresh().await;
    let key = PrivateKey::generate();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"billing\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\njobs = [\"echo\", \"shout\"]\n",
        key.public().to_base64()
    );
    let (_app, base) = with_callers(&database, &callers).await;
    let both = Signer {
        base,
        caller: "billing".into(),
        audience: "test".into(),
        key,
        scope: Scope::Administer,
        jobs: Some(vec!["echo".into(), "shout".into()]),
    };

    let _: serde_json::Value = both.post("/v1/jobs/echo/runs", &json!({ "input": { "text": "chain" } })).await;
    let shout = finished_job(&database.pool, "shout", SOON).await;
    let path = format!("/v1/chains/{}", shout.chain_id);

    let only_shout = Signer { jobs: Some(vec!["shout".into()]), ..like(&both) };
    let refused = only_shout.send("GET", &path, b"").await;
    assert_eq!(refused.status(), 403, "covering a later job, not the first, is not enough");
    let only_echo = Signer { jobs: Some(vec!["echo".into()]), ..like(&both) };
    assert_eq!(only_echo.send("GET", &path, b"").await.status(), 200, "covering the first job is");
}
