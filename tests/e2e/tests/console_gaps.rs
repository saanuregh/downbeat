//! The console's access rules not checked elsewhere: an identity token with no email, the
//! pipelines list fetched as the console itself, who may see people and the audit, and the names
//! `--local` answers to.

mod fake_provider;
mod support;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fake_provider::{BY_DOMAIN, Provider, Spoil, setup, setup_counting, sign_in};
use serde_json::{Value, json};
use support::{Database, with_api_at};

async fn sessions(database: &Database) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM console.session").fetch_one(&database.pool).await.unwrap()
}

/// Signs in this person and returns their cookie and CSRF token.
async fn signed_in(provider: &Provider, base: &str, email: &str) -> (String, String) {
    *provider.next_email.lock().unwrap() = email.into();
    let cookie = sign_in(base).await.unwrap();
    let me: Value = reqwest::Client::new()
        .get(format!("{base}/api/me"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (cookie, me["csrf_token"].as_str().unwrap().to_owned())
}

/// Grants as `by` (cookie, CSRF token).
async fn grant(base: &str, by: &(String, String), grant: Value) {
    let response = reqwest::Client::new()
        .post(format!("{base}/api/grants"))
        .header("cookie", &by.0)
        .header("x-csrf-token", &by.1)
        .json(&grant)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{grant}: {}", response.status());
}

/// The claims of a request token, read without checking it.
fn claims_of(token: &str) -> Value {
    let payload = token.split('.').nth(1).expect("a compact JWS");
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap()
}

#[tokio::test]
async fn an_identity_token_with_no_email_is_refused() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "asha@example.com".into();

    *provider.next_spoil.lock().unwrap() = Some(Spoil::NoEmail);
    assert!(sign_in(&base).await.is_err(), "a token with no email claim signed someone in");
    assert_eq!(sessions(&database).await, 0);
    let people: i64 =
        sqlx::query_scalar("SELECT count(*) FROM console.person").fetch_one(&database.pool).await.unwrap();
    assert_eq!(people, 0);
    assert!(sign_in(&base).await.is_ok(), "the same person with an email signs in");
}

#[tokio::test]
async fn the_pipelines_list_is_fetched_as_the_console_itself() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base, tokens) = setup_counting(&database, BY_DOMAIN).await;
    let asha = signed_in(&provider, &base, "asha@example.com").await;
    let boss = signed_in(&provider, &base, "boss@example.com").await;
    let to_asha =
        json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": "read", "pipelines": ["links"] });
    grant(&base, &boss, to_asha).await;

    let jobs = reqwest::Client::new()
        .get(format!("{base}/api/schedulers/test/v1/jobs"))
        .header("cookie", &asha.0)
        .send()
        .await
        .unwrap();
    assert_eq!(jobs.status(), 200);

    let seen = tokens.lock().unwrap().clone();
    let token_for = |request: &str| {
        let (_, token) =
            seen.iter().find(|(line, _)| line == request).unwrap_or_else(|| panic!("no {request}: {seen:?}"));
        claims_of(token)
    };
    let pipelines = token_for("GET /v1/pipelines");
    assert_eq!(pipelines["iss"], "console");
    assert_eq!(pipelines["sub"], "console", "the console fetches the pipelines as itself: {pipelines}");
    assert!(pipelines.get("jobs").is_none(), "unlimited to any job: {pipelines}");
    let forwarded = token_for("GET /v1/jobs");
    assert_eq!(forwarded["sub"], "asha@example.com", "the person's own read is signed for her");
}

#[tokio::test]
async fn someone_who_administers_only_some_pipelines_cannot_see_people_or_the_audit() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let ravi = signed_in(&provider, &base, "ravi@example.com").await;
    let boss = signed_in(&provider, &base, "boss@example.com").await;
    let partial =
        json!({ "to_person": "ravi@example.com", "scheduler": "test", "scope": "administer", "pipelines": ["links"] });
    grant(&base, &boss, partial).await;

    let http = reqwest::Client::new();
    for path in ["/api/people", "/api/audit"] {
        let refused = http.get(format!("{base}{path}")).header("cookie", &ravi.0).send().await.unwrap();
        assert_eq!(refused.status(), 403, "{path} for someone administering one pipeline");
        let allowed = http.get(format!("{base}{path}")).header("cookie", &boss.0).send().await.unwrap();
        assert_eq!(allowed.status(), 200, "{path} for a first administrator");
    }
}

/// The console binary, built next to this test's own binary by `scripts/test.sh`.
#[tokio::test]
async fn in_local_mode_each_own_name_is_answered_only_with_the_consoles_port() {
    let database = Database::fresh().await;
    let (_app, _client, scheduler) = with_api_at(&database).await;
    let console = support::local_console(&database, &scheduler).await;
    let port = console.port;
    let http = reqwest::Client::new();
    let status = |host: String| {
        let http = http.clone();
        async move {
            let response =
                http.get(format!("http://127.0.0.1:{port}/api/me")).header("host", &host).send().await.unwrap();
            (host, response.status().as_u16())
        }
    };

    for host in [format!("127.0.0.1:{port}"), format!("localhost:{port}"), format!("[::1]:{port}")] {
        assert_eq!(status(host.clone()).await, (host, 200));
    }
    let other = port.wrapping_add(1);
    for host in [
        "localhost".to_owned(),
        "[::1]".to_owned(),
        "127.0.0.1".to_owned(),
        format!("localhost:{other}"),
        format!("[::1]:{other}"),
        format!("127.0.0.1:{other}"),
        format!("localhost:{port}0"),
    ] {
        assert_eq!(status(host.clone()).await, (host, 403));
    }
}
