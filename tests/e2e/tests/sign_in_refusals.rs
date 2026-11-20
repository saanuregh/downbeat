//! What the console refuses at sign-in and in its own API: spoiled identity tokens, people outside
//! the allowed domains, and people who administer nothing.

mod fake_provider;
mod support;

use fake_provider::{BY_DOMAIN, Spoil, setup, sign_in};
use serde_json::{Value, json};
use support::Database;

async fn sessions(database: &Database) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM console.session").fetch_one(&database.pool).await.unwrap()
}

#[tokio::test]
async fn an_identity_token_that_is_not_valid_starts_no_session() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "asha@example.com".into();

    for spoil in [Spoil::OtherKey, Spoil::OtherIssuer, Spoil::OtherAudience, Spoil::WrongNonce, Spoil::Expired] {
        *provider.next_spoil.lock().unwrap() = Some(spoil);
        assert_eq!(sign_in(&base).await, Err(401), "{spoil:?}");
        assert_eq!(sessions(&database).await, 0, "{spoil:?} started a session");
    }
    assert!(sign_in(&base).await.is_ok(), "an unspoiled token still signs her in");
    assert_eq!(sessions(&database).await, 1);
}

#[tokio::test]
async fn a_refused_sign_in_is_audited() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "mallory@elsewhere.org".into();
    assert_eq!(sign_in(&base).await, Err(403));

    let audited: Vec<(String, String)> =
        sqlx::query_as("SELECT actor, action FROM console.audit").fetch_all(&database.pool).await.unwrap();
    assert_eq!(audited, [("mallory@elsewhere.org".to_owned(), "sign_in_refused".to_owned())]);
    assert_eq!(sessions(&database).await, 0);

    *provider.next_email.lock().unwrap() = "unverified@example.com".into();
    assert_eq!(sign_in(&base).await, Err(403));
    let unverified: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM console.audit WHERE actor = 'unverified@example.com' AND action = 'sign_in_refused'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(unverified, 1, "an unverified address is refused and audited too");
}

#[tokio::test]
async fn a_change_the_person_has_no_scope_for_is_refused_and_audited() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    let me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &asha).send().await.unwrap().json().await.unwrap();

    let start = http
        .post(format!("{base}/api/schedulers/test/v1/jobs/echo/runs"))
        .header("cookie", &asha)
        .header("x-csrf-token", me["csrf_token"].as_str().unwrap())
        .json(&json!({ "input": { "text": "not allowed" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(start.status(), 403, "no grant, no start");
    let refused: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM console.audit WHERE actor = 'asha@example.com' AND action = 'refused'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(refused, 1);
}

#[tokio::test]
async fn someone_who_administers_nothing_cannot_see_grants_people_or_the_audit() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();

    for path in ["/api/grants", "/api/people", "/api/audit"] {
        let refused = http.get(format!("{base}{path}")).header("cookie", &asha).send().await.unwrap();
        assert_eq!(refused.status(), 403, "{path}");
    }
}

#[tokio::test]
async fn a_person_who_has_never_signed_in_cannot_be_granted_access() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();

    let refused = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", me["csrf_token"].as_str().unwrap())
        .json(&json!({ "to_person": "newcomer@example.com", "scheduler": "test", "scope": "read" }))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 404);
    assert!(refused.text().await.unwrap().contains("has not signed in yet"));
    let grants: i64 = sqlx::query_scalar("SELECT count(*) FROM console.grant").fetch_one(&database.pool).await.unwrap();
    assert_eq!(grants, 0);
}
