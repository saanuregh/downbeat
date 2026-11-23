//! The control API's configuration and authentication rules: when it is on and where
//! `--local` listens, a request with no token, the rate limit's refill and buckets, and which
//! refusals are reported, and a database error is not one.

mod support;

use std::time::Duration;

use downbeat_api::{PrivateKey, RequestBinding, Scope};
use serde_json::json;
use support::{App, Database, SOON, Signer, with_api_at, with_callers};

/// The address `--local` listens on when given none.
const LOCAL_DEFAULT: &str = "127.0.0.1:7400";

fn accepts_connections(address: &str) -> bool {
    std::net::TcpStream::connect_timeout(&address.parse().unwrap(), Duration::from_millis(500)).is_ok()
}

/// Waits until the scheduler role of a process has started, by its row in the worker table.
async fn scheduler_started(database: &Database) {
    support::eventually(&database.pool, "SELECT EXISTS (SELECT 1 FROM downbeat.worker WHERE role = 'scheduler')", SOON)
        .await;
}

#[tokio::test]
async fn the_api_is_off_without_listen_or_local_and_local_defaults_to_port_7400() {
    if std::net::TcpListener::bind(LOCAL_DEFAULT).is_err() {
        eprintln!("skipped: {LOCAL_DEFAULT} is in use by something else, so the default cannot be checked");
        return;
    }

    let database = Database::fresh().await;
    let without = App::with(&database, &["scheduler"], "test");
    scheduler_started(&database).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!accepts_connections(LOCAL_DEFAULT), "a scheduler without --listen or --local listens nowhere");
    without.stop();

    let database = Database::fresh().await;
    let _local = App::with(&database, &["scheduler", "--local"], "test");
    let base = format!("http://{LOCAL_DEFAULT}");
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(format!("{base}/healthz")).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "--local did not listen on {LOCAL_DEFAULT}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let response = Signer::local(&base).send("GET", "/v1/jobs", b"").await;
    assert_eq!(response.status(), 200, "the development key, audience local, is trusted there");
}

/// Sends only the head of a POST that announces a body of `length` bytes, and returns the status
/// of the answer. A server that read the body before checking the token would never answer.
async fn status_before_the_body(listen: &str, length: usize) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(listen).await.unwrap();
    let head = format!(
        "POST /v1/jobs/echo/runs HTTP/1.1\r\nhost: {listen}\r\ncontent-type: application/json\r\ncontent-length: {length}\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !answer.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buffer))
            .await
            .expect("the API answers without waiting for the body")
            .unwrap();
        assert!(read > 0, "the connection closed without an answer");
        answer.extend_from_slice(&buffer[..read]);
    }
    let text = String::from_utf8_lossy(&answer);
    text.split(' ').nth(1).unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_request_with_no_token_is_unauthorized_whatever_the_size_of_its_body() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let http = reqwest::Client::new();
    let small = http.post(format!("http://{listen}/v1/jobs/echo/runs")).body("{}").send().await.unwrap();
    assert_eq!(small.status(), 401);
    for length in [300 * 1024, 64 << 20] {
        assert_eq!(status_before_the_body(&listen, length).await, 401, "a body of {length} bytes announced");
    }
}

/// A token for this caller acting as `actor`.
fn token_as(signer: &Signer, actor: &str, path: &str) -> String {
    let binding = RequestBinding { method: "GET", path_and_query: path, body: b"" };
    let claims = downbeat_api::Claims::for_request(
        &signer.caller,
        actor,
        &signer.audience,
        signer.scope,
        signer.jobs.clone(),
        binding,
        chrono::Utc::now().timestamp(),
    );
    downbeat_api::sign(&signer.key, &claims)
}

/// A path past the rate limit that answers 404 at once, so requests are cheap enough to empty
/// a bucket faster than it refills.
const PROBE: &str = "/v1/rate-limit-probe";

/// Sends this many reads at once as `actor` and returns how many were let through and how many
/// were answered 429.
async fn burst(http: &reqwest::Client, signer: &Signer, actor: &str, count: usize) -> (usize, usize) {
    let tokens: Vec<String> = (0..count).map(|_| token_as(signer, actor, PROBE)).collect();
    let requests = tokens.iter().map(|token| http.get(format!("{}{PROBE}", signer.base)).bearer_auth(token).send());
    let statuses: Vec<u16> = futures::future::join_all(requests)
        .await
        .into_iter()
        .map(|response| response.unwrap().status().as_u16())
        .collect();
    let through = statuses.iter().filter(|status| **status == 404).count();
    let limited = statuses.iter().filter(|status| **status == 429).count();
    assert_eq!(through + limited, count, "{statuses:?}");
    (through, limited)
}

/// Sends waves of reads as `actor` until one is refused with 429; returns how many got through.
/// The bucket then holds less than one request.
async fn drain(http: &reqwest::Client, signer: &Signer, actor: &str) -> usize {
    let mut through = 0;
    for _ in 0..100 {
        let (wave, limited) = burst(http, signer, actor, 10).await;
        through += wave;
        if limited > 0 {
            return through;
        }
    }
    panic!("a thousand reads were never rate limited");
}

#[tokio::test]
async fn an_emptied_bucket_refills_at_about_twenty_a_second() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let local = Signer::local(&format!("http://{listen}"));
    let http = reqwest::Client::new();

    let first = drain(&http, &local, "local").await;
    assert!(first >= 100, "the burst of 100 gets through first: {first}");
    let emptied = std::time::Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let refilled = f64::from(u32::try_from(drain(&http, &local, "local").await).unwrap());
    let expected = 20.0 * emptied.elapsed().as_secs_f64();
    assert!(
        (refilled - expected).abs() <= 4.0 + expected * 0.15,
        "{refilled} got through after the bucket was emptied, against about {expected:.1} at 20 a second"
    );
}

#[tokio::test]
async fn each_actor_and_each_caller_has_a_bucket_of_its_own() {
    let database = Database::fresh().await;
    let key = |name: &str| {
        let key = PrivateKey::generate();
        let entry = format!(
            "[[caller]]\nname = \"{name}\"\nkeys = [\"{}\"]\nmax_scope = \"read\"\nacts_for_people = true\n",
            key.public().to_base64()
        );
        (entry, key)
    };
    let (first_entry, first_key) = key("first");
    let (second_entry, second_key) = key("second");
    let (_app, base) = with_callers(&database, &format!("audience = \"test\"\n{first_entry}{second_entry}")).await;
    let signer = |caller: &str, key| Signer {
        base: base.clone(),
        caller: caller.into(),
        audience: "test".into(),
        key,
        scope: Scope::Read,
        jobs: None,
    };
    let first = signer("first", first_key);
    let second = signer("second", second_key);

    let http = reqwest::Client::new();

    drain(&http, &first, "alice").await;
    let (_, limited) = burst(&http, &first, "alice", 4).await;
    assert!(limited >= 3, "alice via first stays limited: {limited} of 4 refused");
    assert_eq!(burst(&http, &first, "bob", 5).await.0, 5, "bob via the same caller has his own bucket");
    assert_eq!(burst(&http, &second, "alice", 5).await.0, 5, "alice via another caller has her own bucket");
}

async fn refusal_reports(database: &Database, caller: &str) -> Vec<(i32, i64)> {
    sqlx::query_as(
        "SELECT (detail->>'status')::int, (detail->>'count')::bigint FROM downbeat.audit
          WHERE action = 'event' AND detail->>'event' = 'request_refused' AND detail->>'caller' IS NOT DISTINCT FROM $1
          ORDER BY audit_id",
    )
    .bind(caller)
    .fetch_all(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn refusals_by_an_endpoint_are_not_reported() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    let base = format!("http://{listen}");
    let admin = Signer::local(&base);
    let reader = Signer { scope: Scope::Read, ..Signer::local(&base) };
    let send = async |signer: &Signer, path: &str, body: serde_json::Value| {
        signer.send("POST", path, &serde_json::to_vec(&body).unwrap()).await.status().as_u16()
    };

    assert_eq!(send(&reader, "/v1/jobs/echo/runs", json!({ "input": { "text": "x" } })).await, 403, "scope");
    assert_eq!(send(&admin, "/v1/jobs/no_such_job/runs", json!({ "input": null })).await, 404);
    assert_eq!(send(&admin, "/v1/jobs/echo/resume", json!(null)).await, 409, "not paused");
    assert_eq!(send(&admin, "/v1/jobs/echo/runs", json!({ "input": { "typo": 1 } })).await, 422);
    assert!(refusal_reports(&database, "local").await.is_empty(), "an endpoint's answer was reported");

    // The authentication layer's refusals are reported at once, so the ones above would be too.
    let other_audience = Signer { audience: "elsewhere".into(), ..Signer::local(&base) };
    assert_eq!(other_audience.send("GET", "/v1/jobs", b"").await.status(), 401);
    support::eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = 'request_refused')",
        SOON,
    )
    .await;
    assert!(refusal_reports(&database, "local").await.is_empty());
}

#[tokio::test]
async fn a_named_callers_refusals_are_reported_once_a_minute_with_their_count() {
    let database = Database::fresh().await;
    let key = PrivateKey::generate();
    let entry = format!(
        "audience = \"test\"\n[[caller]]\nname = \"dashboard\"\nkeys = [\"{}\"]\nmax_scope = \"read\"\n",
        key.public().to_base64()
    );
    let (_app, base) = with_callers(&database, &entry).await;
    let over_scope =
        Signer { base, caller: "dashboard".into(), audience: "test".into(), key, scope: Scope::Operate, jobs: None };

    for _ in 0..3 {
        assert_eq!(over_scope.send("GET", "/v1/jobs", b"").await.status(), 403);
    }
    assert_eq!(refusal_reports(&database, "dashboard").await, [(403, 1)], "the first is reported at once");

    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(refusal_reports(&database, "dashboard").await, [(403, 1)], "nothing is reported without a refusal");
    assert_eq!(over_scope.send("GET", "/v1/jobs", b"").await.status(), 403);
    assert_eq!(
        refusal_reports(&database, "dashboard").await,
        [(403, 1), (403, 3)],
        "a minute later, the next carries every refusal since the last report"
    );
}

/// 14.12: a request that fails on the database (here a missing table, so 500) is not counted as refused.
#[tokio::test]
async fn a_database_error_is_not_a_refused_request() {
    let database = Database::fresh().await;
    let (_app, _client, listen) = with_api_at(&database).await;
    sqlx::query("DROP TABLE downbeat.used_token").execute(&database.pool).await.unwrap();
    let body = serde_json::to_vec(&json!({ "input": { "text": "x" } })).unwrap();
    let answer = Signer::local(&format!("http://{listen}")).send("POST", "/v1/jobs/echo/runs", &body).await;
    let status = answer.status();
    let detail: serde_json::Value = answer.json().await.unwrap();
    assert_eq!(status, 500, "{detail}");
    assert_eq!(detail["error"], "internal", "{detail}");
    let refused: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND detail->>'event' = 'request_refused')",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(!refused);
}
