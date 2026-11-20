//! The console's access rules that need a closer look: which signature algorithms it accepts,
//! what it stores of a session id, and how often it fetches a scheduler's pipelines.

mod fake_provider;
mod support;

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fake_provider::{BY_DOMAIN, Spoil, setup, setup_counting, setup_listing, sign_in};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use support::Database;

async fn sessions(database: &Database) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM console.session").fetch_one(&database.pool).await.unwrap()
}

#[tokio::test]
async fn an_identity_token_signed_with_a_shared_secret_is_refused() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "asha@example.com".into();

    *provider.next_spoil.lock().unwrap() = Some(Spoil::SharedSecret);
    assert_eq!(sign_in(&base).await, Err(401), "HS256 under the client secret");
    assert_eq!(sessions(&database).await, 0);
    assert!(sign_in(&base).await.is_ok(), "the same person with an EdDSA token signs in");
}

#[tokio::test]
async fn an_identity_token_signed_with_an_algorithm_the_provider_does_not_list_is_refused() {
    let database = Database::fresh().await;
    // The provider publishes its Ed25519 key and signs with it, but lists only RS256.
    let (_app, provider, _console, base) = setup_listing(&database, BY_DOMAIN, &["RS256"]).await;
    *provider.next_email.lock().unwrap() = "asha@example.com".into();

    assert_eq!(sign_in(&base).await, Err(401));
    assert_eq!(sessions(&database).await, 0);
}

#[tokio::test]
async fn only_the_sha256_of_a_random_32_byte_session_id_is_stored() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let cookie = sign_in(&base).await.unwrap();
    let session_id = cookie.strip_prefix("downbeat_session=").unwrap();

    let raw = URL_SAFE_NO_PAD.decode(session_id).expect("the cookie is the id in unpadded base64url");
    assert_eq!(raw.len(), 32);
    let stored: Vec<String> =
        sqlx::query_scalar("SELECT session_hash FROM console.session").fetch_all(&database.pool).await.unwrap();
    assert_eq!(stored, [URL_SAFE_NO_PAD.encode(Sha256::digest(session_id.as_bytes()))]);

    let raw_hex = raw.iter().fold(String::new(), |mut hex, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
        hex
    });
    let holding_the_id: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM console.session s WHERE s::text LIKE '%' || $1 || '%' OR s::text LIKE '%' || $2 || '%'",
    )
    .bind(session_id)
    .bind(raw_hex)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(holding_the_id, 0, "no row holds the session id itself");
}

#[tokio::test]
async fn pipelines_are_fetched_once_and_cached_for_a_minute() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base, requests) = setup_counting(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();
    let granted = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", me["csrf_token"].as_str().unwrap())
        .json(&json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": "read", "pipelines": ["links"] }))
        .send()
        .await
        .unwrap();
    assert!(granted.status().is_success(), "{}", granted.status());

    let fetches = || requests.lock().unwrap().iter().filter(|seen| *seen == "GET /v1/pipelines").count();
    for attempt in 0..2 {
        if attempt == 1 {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        let jobs =
            http.get(format!("{base}/api/schedulers/test/v1/jobs")).header("cookie", &asha).send().await.unwrap();
        assert_eq!(jobs.status(), 200, "a read limited to the links pipeline");
    }
    assert_eq!(fetches(), 1, "two requests three seconds apart fetched the pipelines once");
    let forwarded = requests.lock().unwrap().iter().filter(|seen| *seen == "GET /v1/jobs").count();
    assert_eq!(forwarded, 2, "both reads reached the scheduler");
}

#[tokio::test]
async fn pipelines_are_fetched_again_once_the_minute_is_over() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base, requests) = setup_counting(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();
    let granted = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", me["csrf_token"].as_str().unwrap())
        .json(&json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": "read", "pipelines": ["links"] }))
        .send()
        .await
        .unwrap();
    assert!(granted.status().is_success(), "{}", granted.status());

    let fetches = || requests.lock().unwrap().iter().filter(|seen| *seen == "GET /v1/pipelines").count();
    let read = || http.get(format!("{base}/api/schedulers/test/v1/jobs")).header("cookie", &asha).send();
    assert_eq!(read().await.unwrap().status(), 200);
    assert_eq!(fetches(), 1);
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(read().await.unwrap().status(), 200);
    assert_eq!(fetches(), 2, "after sixty seconds the pipelines are fetched again");
}
