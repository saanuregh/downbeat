//! A small fake OpenID Connect provider that checks PKCE and signs identity tokens with Ed25519,
//! and the console configured to sign people in through it.

#![allow(dead_code, unreachable_pub)]

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

use crate::support::{self, Database, SOON};

/// How the provider spoils the next identity token it hands out.
#[derive(Debug, Clone, Copy)]
pub enum Spoil {
    /// Signed by a key the provider does not publish.
    OtherKey,
    OtherIssuer,
    OtherAudience,
    /// A nonce other than the one the console sent.
    WrongNonce,
    Expired,
}

/// The fake provider's state: who signs in next, how their token is spoiled, and the sign-ins in
/// progress.
#[derive(Clone)]
pub struct Provider {
    pub base: String,
    key: SigningKey,
    pub next_email: Arc<Mutex<String>>,
    pub next_spoil: Arc<Mutex<Option<Spoil>>>,
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
    let spoil = provider.next_spoil.lock().unwrap().take();
    // The provider has not checked an address that starts with "unverified".
    let verified = !email.starts_with("unverified");
    let now = chrono::Utc::now().timestamp();
    let mut claims = json!({
        "iss": provider.base, "sub": format!("subject-{email}"), "aud": "console", "iat": now, "exp": now + 300,
        "nonce": nonce, "email": email, "email_verified": verified, "name": "Test Person", "groups": ["operators"],
    });
    let mut key = provider.key.clone();
    match spoil {
        None => {}
        Some(Spoil::OtherKey) => key = SigningKey::from_bytes(&[9; 32]),
        Some(Spoil::OtherIssuer) => claims["iss"] = json!("https://other-issuer.example"),
        Some(Spoil::OtherAudience) => claims["aud"] = json!("another-client"),
        Some(Spoil::WrongNonce) => claims["nonce"] = json!("not-the-nonce-the-console-sent"),
        Some(Spoil::Expired) => {
            claims["iat"] = json!(now - 7200);
            claims["exp"] = json!(now - 3600);
        }
    }
    let header = URL_SAFE_NO_PAD.encode(json!({ "alg": "EdDSA", "kid": "k1", "typ": "JWT" }).to_string());
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signature = key.sign(format!("{header}.{payload}").as_bytes());
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
        next_spoil: Arc::default(),
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

pub struct Console(Child);

impl Drop for Console {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn console_binary() -> std::path::PathBuf {
    std::env::current_exe().unwrap().parent().and_then(std::path::Path::parent).unwrap().join("downbeat-console")
}

/// Who may sign in, unless a test says otherwise.
pub const BY_DOMAIN: &str = r#"allowed_domains = ["example.com"]"#;

/// The scheduler trusting the console's key, the fake provider, and the console configured for both,
/// with `who_may_sign_in` as the `[sign_in]` section's rules.
pub async fn setup(database: &Database, who_may_sign_in: &str) -> (support::App, Provider, Console, String) {
    let key = downbeat_client::PrivateKey::generate();
    let key_file = std::env::temp_dir().join(format!("console-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&key_file, key.to_pem()).unwrap();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"console\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\nacts_for_people = true\n",
        key.public().to_base64()
    );
    let (app, scheduler) = support::with_callers(database, &callers).await;
    let provider = start_provider().await;

    let port = support::free_port();
    let base = format!("http://127.0.0.1:{port}");
    let config = format!(
        r#"listen = "127.0.0.1:{port}"
public_url = "{base}"
first_administrators = ["boss@example.com"]

[sign_in]
issuer = "{issuer}"
client_id = "console"
client_secret_env = "TEST_CLIENT_SECRET"
{who_may_sign_in}

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
pub async fn sign_in(base: &str) -> Result<String, u16> {
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
