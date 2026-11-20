//! Signing in to the console through OpenID Connect, against a small fake identity provider that
//! checks PKCE and signs identity tokens with Ed25519.

mod fake_provider;
mod support;

use fake_provider::{BY_DOMAIN, setup, sign_in};
use serde_json::{Value, json};
use support::Database;

#[tokio::test]
async fn a_person_signs_in_and_is_given_what_their_grants_allow() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();

    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();
    assert_eq!(me["person"]["email"], "boss@example.com");
    assert_eq!(me["schedulers"][0]["scope"], "administer", "a first administrator administers everything");

    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    let me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &asha).send().await.unwrap().json().await.unwrap();
    assert_eq!(me["schedulers"][0]["scope"], Value::Null, "no grant, no access");
    let refused = http.get(format!("{base}/api/schedulers/test/v1/jobs")).header("cookie", &asha).send().await.unwrap();
    assert_eq!(refused.status(), 403);

    // The boss grants the operators group read access; asha is in it.
    let boss_me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();
    let csrf = boss_me["csrf_token"].as_str().unwrap();
    let granted = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", csrf)
        .json(&json!({ "to_group": "operators", "scheduler": "test", "scope": "read" }))
        .send()
        .await
        .unwrap();
    assert!(granted.status().is_success(), "{}", granted.text().await.unwrap());
    let jobs = http.get(format!("{base}/api/schedulers/test/v1/jobs")).header("cookie", &asha).send().await.unwrap();
    assert_eq!(jobs.status(), 200);
    let start = http
        .post(format!("{base}/api/schedulers/test/v1/jobs/echo/runs"))
        .header("cookie", &asha)
        .header("x-csrf-token", "anything")
        .json(&json!({ "input": { "text": "x" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(start.status(), 403, "read access cannot start a job");

    let asha_me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &asha).send().await.unwrap().json().await.unwrap();
    let without_token = http.post(format!("{base}/auth/logout")).header("cookie", &asha).send().await.unwrap();
    assert_eq!(without_token.status(), 204);
    let still = http.get(format!("{base}/api/me")).header("cookie", &asha).send().await.unwrap();
    assert_eq!(still.status(), 200, "signing out needs the session's CSRF token");
    let logout = http
        .post(format!("{base}/auth/logout"))
        .header("cookie", &asha)
        .header("x-csrf-token", asha_me["csrf_token"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), 204);
    let after = http.get(format!("{base}/api/me")).header("cookie", &asha).send().await.unwrap();
    assert_eq!(after.status(), 401, "a session ends at once on sign-out");
}

#[tokio::test]
async fn someone_outside_the_allowed_domains_cannot_sign_in() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "mallory@elsewhere.org".into();
    assert_eq!(sign_in(&base).await, Err(403));
}

#[tokio::test]
async fn an_address_the_provider_has_not_verified_cannot_sign_in() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "unverified@example.com".into();
    assert_eq!(sign_in(&base).await, Err(403));
}

#[tokio::test]
async fn someone_who_administers_one_pipeline_cannot_grant_the_whole_scheduler() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();

    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let csrf_of = |me: &Value| me["csrf_token"].as_str().unwrap().to_owned();
    let boss_me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();
    let granted = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", csrf_of(&boss_me))
        .json(&json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": "administer", "pipelines": ["basics"] }))
        .send()
        .await
        .unwrap();
    assert!(granted.status().is_success(), "{}", granted.text().await.unwrap());

    let asha_me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &asha).send().await.unwrap().json().await.unwrap();
    let escalate = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &asha)
        .header("x-csrf-token", csrf_of(&asha_me))
        .json(&json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": "administer" }))
        .send()
        .await
        .unwrap();
    assert_eq!(escalate.status(), 403, "administering one pipeline must not let her grant the whole scheduler");
}

#[tokio::test]
async fn a_group_the_provider_reports_lets_people_from_any_domain_sign_in() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, r#"allowed_groups = ["operators"]"#).await;
    *provider.next_email.lock().unwrap() = "contractor@elsewhere.org".into();
    assert!(sign_in(&base).await.is_ok(), "the provider puts everyone in the operators group");

    let (_app, provider, _console, base) = setup(&Database::fresh().await, r#"allowed_groups = ["admins"]"#).await;
    *provider.next_email.lock().unwrap() = "contractor@elsewhere.org".into();
    assert_eq!(sign_in(&base).await, Err(403), "not in an allowed group");
}

#[tokio::test]
async fn a_session_ends_when_idle_too_long_or_too_old() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    let me = |cookie: String| {
        let (http, base) = (http.clone(), base.clone());
        async move { http.get(format!("{base}/api/me")).header("cookie", cookie).send().await.unwrap().status() }
    };
    *provider.next_email.lock().unwrap() = "asha@example.com".into();

    let idle = sign_in(&base).await.unwrap();
    assert_eq!(me(idle.clone()).await, 200);
    sqlx::query("UPDATE console.session SET last_used_at = now() - interval '13 hours'")
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(me(idle).await, 401, "twelve hours without use ends a session");

    let old = sign_in(&base).await.unwrap();
    assert_eq!(me(old.clone()).await, 200);
    sqlx::query("UPDATE console.session SET expires_at = now() - interval '1 minute'")
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(me(old).await, 401, "a session ends at its absolute limit, however busy");
}

#[tokio::test]
async fn someone_who_may_only_operate_cannot_do_what_needs_administer() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();
    let me = |cookie: &str| {
        let (http, base, cookie) = (http.clone(), base.clone(), cookie.to_owned());
        async move {
            http.get(format!("{base}/api/me"))
                .header("cookie", cookie)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };

    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let granted = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", me(&boss).await["csrf_token"].as_str().unwrap())
        .json(&json!({ "to_group": "operators", "scheduler": "test", "scope": "operate" }))
        .send()
        .await
        .unwrap();
    assert!(granted.status().is_success(), "{}", granted.text().await.unwrap());

    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    let csrf = me(&asha).await["csrf_token"].as_str().unwrap().to_owned();
    let start = |body: Value| {
        let (http, base, asha, csrf) = (http.clone(), base.clone(), asha.clone(), csrf.clone());
        async move {
            http.post(format!("{base}/api/schedulers/test/v1/jobs/echo/runs"))
                .header("cookie", asha)
                .header("x-csrf-token", csrf)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(start(json!({ "input": { "text": "plain" } })).await, 201);
    assert_eq!(start(json!({ "input": { "text": "timed" }, "timeout_seconds": 30 })).await, 403);
}

#[tokio::test]
async fn a_grant_limited_to_a_pipeline_shows_only_that_pipelines_jobs() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let http = reqwest::Client::new();

    *provider.next_email.lock().unwrap() = "boss@example.com".into();
    let boss = sign_in(&base).await.unwrap();
    let boss_me: Value =
        http.get(format!("{base}/api/me")).header("cookie", &boss).send().await.unwrap().json().await.unwrap();
    let granted = http
        .post(format!("{base}/api/grants"))
        .header("cookie", &boss)
        .header("x-csrf-token", boss_me["csrf_token"].as_str().unwrap())
        .json(&json!({ "to_group": "operators", "scheduler": "test", "scope": "read", "pipelines": ["clock"] }))
        .send()
        .await
        .unwrap();
    assert!(granted.status().is_success(), "{}", granted.text().await.unwrap());

    *provider.next_email.lock().unwrap() = "asha@example.com".into();
    let asha = sign_in(&base).await.unwrap();
    let jobs: Value = http
        .get(format!("{base}/api/schedulers/test/v1/jobs"))
        .header("cookie", &asha)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut names: Vec<&str> = jobs.as_array().unwrap().iter().map(|job| job["name"].as_str().unwrap()).collect();
    names.sort_unstable();
    assert_eq!(names, ["dated", "files_dated", "report", "ticker"], "only the clock pipeline's jobs");
    let other =
        http.get(format!("{base}/api/schedulers/test/v1/jobs/echo")).header("cookie", &asha).send().await.unwrap();
    assert_eq!(other.status(), 403);
}
