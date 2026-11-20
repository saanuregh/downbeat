//! More of the console's access rules: the return path after sign-in, a change the scheduler
//! refuses at every scope tried, and who may list and remove grants.

mod fake_provider;
mod support;

use fake_provider::{BY_DOMAIN, Provider, setup, setup_counting, sign_in};
use serde_json::{Value, json};
use support::Database;

/// Signs in whoever the provider has next, asking to come back to `return_to`; returns where the
/// console sent the browser once signed in.
async fn sign_in_returning_to(base: &str, return_to: &str) -> String {
    let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let login = reqwest::Url::parse_with_params(&format!("{base}/auth/login"), [("return", return_to)]).unwrap();
    let to_provider = http.get(login).send().await.unwrap();
    let authorize = to_provider.headers()["location"].to_str().unwrap().to_owned();
    let back = http.get(&authorize).send().await.unwrap();
    let callback = back.headers()["location"].to_str().unwrap().to_owned();
    let signed_in = http.get(&callback).send().await.unwrap();
    assert!(signed_in.status().is_redirection(), "{}", signed_in.status());
    signed_in.headers()["location"].to_str().unwrap().to_owned()
}

#[tokio::test]
async fn a_return_path_with_a_backslash_is_replaced_by_the_home_page() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    *provider.next_email.lock().unwrap() = "asha@example.com".into();

    for path in ["/\\evil.example", "/\\\\evil.example/runs", "/runs\\..\\x"] {
        assert_eq!(sign_in_returning_to(&base, path).await, "/", "{path:?}");
    }
    assert_eq!(sign_in_returning_to(&base, "/runs?status=failed").await, "/runs?status=failed");
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

/// Grants as `by` (cookie, CSRF token) and returns the grant's id.
async fn grant(base: &str, by: &(String, String), grant: Value) -> i64 {
    let response = reqwest::Client::new()
        .post(format!("{base}/api/grants"))
        .header("cookie", &by.0)
        .header("x-csrf-token", &by.1)
        .json(&grant)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{grant}: {}", response.status());
    response.json::<Value>().await.unwrap()["grant_id"].as_i64().unwrap()
}

#[tokio::test]
async fn a_change_refused_at_every_scope_tried_is_answered_403_and_audited() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base, requests) = setup_counting(&database, BY_DOMAIN).await;
    let asha = signed_in(&provider, &base, "asha@example.com").await;
    let boss = signed_in(&provider, &base, "boss@example.com").await;
    // Asha operates and administers the links pipeline only; echo is in another.
    for scope in ["operate", "administer"] {
        let to_asha =
            json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": scope, "pipelines": ["links"] });
        grant(&base, &boss, to_asha).await;
    }

    let start = reqwest::Client::new()
        .post(format!("{base}/api/schedulers/test/v1/jobs/echo/runs"))
        .header("cookie", &asha.0)
        .header("x-csrf-token", &asha.1)
        .json(&json!({ "input": { "text": "not hers" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(start.status(), 403);
    let tried = requests.lock().unwrap().iter().filter(|seen| *seen == "POST /v1/jobs/echo/runs").count();
    assert_eq!(tried, 2, "signed for operate, then again for administer");
    let refused: Vec<Value> =
        sqlx::query_scalar("SELECT detail FROM console.audit WHERE actor = 'asha@example.com' AND action = 'refused'")
            .fetch_all(&database.pool)
            .await
            .unwrap();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0]["path"], "/v1/jobs/echo/runs");
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run").fetch_one(&database.pool).await.unwrap();
    assert_eq!(runs, 0);
}

async fn grant_ids(base: &str, person: &(String, String)) -> Result<Vec<i64>, u16> {
    let response =
        reqwest::Client::new().get(format!("{base}/api/grants")).header("cookie", &person.0).send().await.unwrap();
    if !response.status().is_success() {
        return Err(response.status().as_u16());
    }
    let grants: Vec<Value> = response.json().await.unwrap();
    Ok(grants.iter().map(|grant| grant["grant_id"].as_i64().unwrap()).collect())
}

async fn remove(base: &str, person: &(String, String), id: i64) -> u16 {
    reqwest::Client::new()
        .delete(format!("{base}/api/grants/{id}"))
        .header("cookie", &person.0)
        .header("x-csrf-token", &person.1)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test]
async fn only_someone_who_administers_a_scheduler_in_full_lists_or_removes_its_grants() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database, BY_DOMAIN).await;
    let asha = signed_in(&provider, &base, "asha@example.com").await;
    let ravi = signed_in(&provider, &base, "ravi@example.com").await;
    let mina = signed_in(&provider, &base, "mina@example.com").await;
    let boss = signed_in(&provider, &base, "boss@example.com").await;

    let full =
        grant(&base, &boss, json!({ "to_person": "asha@example.com", "scheduler": "test", "scope": "administer" }))
            .await;
    let partial = grant(
        &base,
        &boss,
        json!({ "to_person": "ravi@example.com", "scheduler": "test", "scope": "administer", "pipelines": ["links"] }),
    )
    .await;
    let reader =
        grant(&base, &boss, json!({ "to_person": "mina@example.com", "scheduler": "test", "scope": "read" })).await;
    // A grant on a scheduler this console no longer lists: nobody here administers it.
    let elsewhere: i64 = sqlx::query_scalar(
        "INSERT INTO console.grant (to_group, scheduler, scope, created_by) VALUES ('operators', 'retired', 'read', 'test')
         RETURNING grant_id",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();

    let mut seen = grant_ids(&base, &asha).await.unwrap();
    seen.sort_unstable();
    assert_eq!(seen, [full, partial, reader], "only grants on a scheduler she administers in full");
    assert!(!seen.contains(&elsewhere));
    assert_eq!(grant_ids(&base, &ravi).await, Err(403), "administering one pipeline is not enough to list");
    assert_eq!(grant_ids(&base, &mina).await, Err(403));

    assert_eq!(remove(&base, &ravi, reader).await, 403, "someone administering one pipeline");
    assert_eq!(remove(&base, &mina, reader).await, 403, "a reader");
    assert_eq!(remove(&base, &asha, elsewhere).await, 403, "a grant on a scheduler she does not administer");
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM console.grant").fetch_one(&database.pool).await.unwrap();
    assert_eq!(kept, 4, "a refused removal removed something");

    assert_eq!(remove(&base, &asha, reader).await, 200, "someone administering the scheduler in full");
    let revoked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM console.audit WHERE actor = 'asha@example.com' AND action = 'revoke'")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(revoked, 1);
}
