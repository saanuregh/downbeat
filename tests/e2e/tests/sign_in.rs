//! Signing in to the console through OpenID Connect, against a small fake identity provider that
//! checks PKCE and signs identity tokens with Ed25519.

mod support;

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Form, Query, State};
use axum::response::Redirect;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use support::{Database, SOON};

/// The fake provider's state: who signs in next, and the sign-ins in progress.
#[derive(Clone)]
struct Provider {
    base: String,
    key: SigningKey,
    next_email: Arc<Mutex<String>>,
    pending: Arc<Mutex<HashMap<String, (String, String)>>>,
}

async fn discovery(State(provider): State<Provider>) -> Json<Value> {
    let base = &provider.base;
    Json(json!({
        "issuer": base,
        "authorization_endpoint": format!("{base}/authorize"),
        "token_endpoint": format!("{base}/token"),
        "jwks_uri": format!("{base}/jwks"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["EdDSA"],
    }))
}

async fn jwks(State(provider): State<Provider>) -> Json<Value> {
    let x = URL_SAFE_NO_PAD.encode(provider.key.verifying_key().as_bytes());
    Json(json!({ "keys": [{ "kty": "OKP", "crv": "Ed25519", "x": x, "kid": "k1", "use": "sig", "alg": "EdDSA" }] }))
}

/// Signs in whoever is next at once, and sends the browser back with a code.
async fn authorize(State(provider): State<Provider>, Query(query): Query<HashMap<String, String>>) -> Redirect {
    assert_eq!(query["code_challenge_method"], "S256", "the console must use PKCE with S256");
    let code = format!("code-{}", uuid::Uuid::new_v4());
    provider.pending.lock().unwrap().insert(code.clone(), (query["code_challenge"].clone(), query["nonce"].clone()));
    Redirect::to(&format!("{}?code={code}&state={}", query["redirect_uri"], query["state"]))
}

async fn token(
    State(provider): State<Provider>,
    Form(form): Form<HashMap<String, String>>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let (challenge, nonce) = provider
        .pending
        .lock()
        .unwrap()
        .remove(&form["code"])
        .ok_or((axum::http::StatusCode::BAD_REQUEST, "unknown code".into()))?;
    let derived = URL_SAFE_NO_PAD.encode(Sha256::digest(form["code_verifier"].as_bytes()));
    if derived != challenge {
        return Err((axum::http::StatusCode::BAD_REQUEST, "PKCE verifier does not match".into()));
    }
    let email = provider.next_email.lock().unwrap().clone();
    let now = chrono::Utc::now().timestamp();
    let claims = json!({
        "iss": provider.base, "sub": format!("subject-{email}"), "aud": "console", "iat": now, "exp": now + 300,
        "nonce": nonce, "email": email, "email_verified": true, "name": "Test Person", "groups": ["operators"],
    });
    let header = URL_SAFE_NO_PAD.encode(json!({ "alg": "EdDSA", "kid": "k1", "typ": "JWT" }).to_string());
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signature = provider.key.sign(format!("{header}.{payload}").as_bytes());
    let id_token = format!("{header}.{payload}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()));
    Ok(Json(json!({ "access_token": "access", "token_type": "Bearer", "expires_in": 300, "id_token": id_token })))
}

async fn start_provider() -> Provider {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let provider = Provider {
        base,
        key: SigningKey::from_bytes(&[7; 32]),
        next_email: Arc::new(Mutex::new(String::new())),
        pending: Arc::default(),
    };
    let app = Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/authorize", get(authorize))
        .route("/token", post(token))
        .with_state(provider.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    provider
}

struct Console(Child);

impl Drop for Console {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn console_binary() -> std::path::PathBuf {
    std::env::current_exe().unwrap().parent().and_then(std::path::Path::parent).unwrap().join("downbeat-console")
}

/// The scheduler trusting the console's key, the fake provider, and the console configured for both.
async fn setup(database: &Database) -> (support::App, Provider, Console, String) {
    let key = downbeat_client::PrivateKey::generate();
    let key_file = std::env::temp_dir().join(format!("console-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&key_file, key.to_pem()).unwrap();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"console\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\nacts_for_people = true\n",
        key.public().to_base64()
    );
    let (app, scheduler) = support::with_callers(database, &callers).await;
    let provider = start_provider().await;

    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let base = format!("http://127.0.0.1:{port}");
    let config = format!(
        r#"listen = "127.0.0.1:{port}"
public_url = "{base}"
first_administrators = ["boss@example.com"]

[sign_in]
issuer = "{issuer}"
client_id = "console"
client_secret_env = "TEST_CLIENT_SECRET"
allowed_domains = ["example.com"]

[[scheduler]]
name = "test"
url = "{scheduler}"
audience = "test"
key_file = "{key}"
"#,
        issuer = provider.base,
        key = key_file.display(),
    );
    let config_file = std::env::temp_dir().join(format!("console-{}.toml", uuid::Uuid::new_v4()));
    std::fs::write(&config_file, config).unwrap();
    let process = Command::new(console_binary())
        .args(["--config", &config_file.display().to_string()])
        .env("DOWNBEAT_CONSOLE_DATABASE_URL", &database.url)
        .env("TEST_CLIENT_SECRET", "secret")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(format!("{base}/api/me")).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "the console did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    (app, provider, Console(process), base)
}

/// Follows the sign-in redirects by hand and returns the session cookie, or the refusal's status.
async fn sign_in(base: &str) -> Result<String, u16> {
    let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let to_provider = http.get(format!("{base}/auth/login?return=/runs")).send().await.unwrap();
    let authorize = to_provider.headers()["location"].to_str().unwrap().to_owned();
    let back = http.get(&authorize).send().await.unwrap();
    let callback = back.headers()["location"].to_str().unwrap().to_owned();
    let signed_in = http.get(&callback).send().await.unwrap();
    if !signed_in.status().is_redirection() {
        return Err(signed_in.status().as_u16());
    }
    assert_eq!(signed_in.headers()["location"], "/runs");
    let cookie = signed_in.headers()["set-cookie"].to_str().unwrap();
    assert!(cookie.contains("HttpOnly") && cookie.contains("Secure") && cookie.contains("SameSite=Strict"), "{cookie}");
    Ok(cookie.split(';').next().unwrap().to_owned())
}

#[tokio::test]
async fn a_person_signs_in_and_is_given_what_their_grants_allow() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database).await;
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
    let (_app, provider, _console, base) = setup(&database).await;
    *provider.next_email.lock().unwrap() = "mallory@elsewhere.org".into();
    assert_eq!(sign_in(&base).await, Err(403));
}

#[tokio::test]
async fn someone_who_administers_one_pipeline_cannot_grant_the_whole_scheduler() {
    let database = Database::fresh().await;
    let (_app, provider, _console, base) = setup(&database).await;
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
