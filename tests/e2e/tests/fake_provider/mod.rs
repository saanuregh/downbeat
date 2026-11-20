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
    /// Signed with HMAC-SHA256 under the client secret, which the console also knows.
    SharedSecret,
}

/// The fake provider's state: who signs in next, how their token is spoiled, and the sign-ins in
/// progress.
#[derive(Clone)]
pub struct Provider {
    pub base: String,
    key: SigningKey,
    pub next_email: Arc<Mutex<String>>,
    pub next_spoil: Arc<Mutex<Option<Spoil>>>,
    /// The signature algorithms the discovery document lists.
    listed: Vec<String>,
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
        "id_token_signing_alg_values_supported": provider.listed,
    }))
}

async fn jwks(State(provider): State<Provider>) -> Json<Value> {
    let x = URL_SAFE_NO_PAD.encode(provider.key.verifying_key().as_bytes());
    // No `alg` on the key: which algorithms count is then decided only by the discovery document's
    // list, which is what the console must check.
    Json(json!({ "keys": [{ "kty": "OKP", "crv": "Ed25519", "x": x, "kid": "k1", "use": "sig" }] }))
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
    let mut algorithm = "EdDSA";
    match spoil {
        None => {}
        Some(Spoil::SharedSecret) => algorithm = "HS256",
        Some(Spoil::OtherKey) => key = SigningKey::from_bytes(&[9; 32]),
        Some(Spoil::OtherIssuer) => claims["iss"] = json!("https://other-issuer.example"),
        Some(Spoil::OtherAudience) => claims["aud"] = json!("another-client"),
        Some(Spoil::WrongNonce) => claims["nonce"] = json!("not-the-nonce-the-console-sent"),
        Some(Spoil::Expired) => {
            claims["iat"] = json!(now - 7200);
            claims["exp"] = json!(now - 3600);
        }
    }
    let header = URL_SAFE_NO_PAD.encode(json!({ "alg": algorithm, "kid": "k1", "typ": "JWT" }).to_string());
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signed = format!("{header}.{payload}");
    let signature = match spoil {
        Some(Spoil::SharedSecret) => hmac_sha256(CLIENT_SECRET.as_bytes(), signed.as_bytes()).to_vec(),
        _ => key.sign(signed.as_bytes()).to_bytes().to_vec(),
    };
    let id_token = format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature));
    Ok(Json(json!({ "access_token": "access", "token_type": "Bearer", "expires_in": 300, "id_token": id_token })))
}

/// The client secret the console is given, and so the key a shared-secret token is signed with.
const CLIENT_SECRET: &str = "secret";

/// HMAC-SHA256 (RFC 2104), so a test can sign a token with a shared secret.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0_u8; 64];
    if key.len() > block.len() {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let inner_pad: Vec<u8> = block.iter().map(|byte| byte ^ 0x36).collect();
    let outer_pad: Vec<u8> = block.iter().map(|byte| byte ^ 0x5c).collect();
    let inner = Sha256::new().chain_update(&inner_pad).chain_update(message).finalize();
    Sha256::new().chain_update(&outer_pad).chain_update(inner).finalize().into()
}

async fn start_provider(listed: &[&str]) -> Provider {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let provider = Provider {
        base,
        key: SigningKey::from_bytes(&[7; 32]),
        next_email: Arc::new(Mutex::new(String::new())),
        next_spoil: Arc::default(),
        listed: listed.iter().map(|algorithm| (*algorithm).to_owned()).collect(),
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

/// The paths of the requests the console sent to the scheduler, as `METHOD /path`.
pub type SchedulerRequests = Arc<Mutex<Vec<String>>>;

#[derive(Clone)]
struct Relay {
    target: String,
    http: reqwest::Client,
    seen: SchedulerRequests,
}

/// Passes a request on to the scheduler unchanged, noting its method and path.
async fn relay(State(relay): State<Relay>, request: axum::extract::Request) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 16 << 20).await.unwrap();
    relay.seen.lock().unwrap().push(format!("{} {}", parts.method, parts.uri.path()));
    let path = parts.uri.path_and_query().map(|path| path.as_str().to_owned()).unwrap_or_default();
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes()).unwrap();
    let mut forwarded = relay.http.request(method, format!("{}{path}", relay.target));
    for (name, value) in &parts.headers {
        if !matches!(name.as_str(), "host" | "content-length" | "transfer-encoding") {
            forwarded = forwarded.header(name.as_str(), value.as_bytes());
        }
    }
    let answer = forwarded.body(body.to_vec()).send().await.unwrap();
    let status = axum::http::StatusCode::from_u16(answer.status().as_u16()).unwrap();
    let content_type = answer
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();
    let bytes = answer.bytes().await.unwrap();
    (status, [(axum::http::header::CONTENT_TYPE, content_type)], bytes.to_vec()).into_response()
}

/// A relay in front of the scheduler at `target`; returns its base URL and what passed through it.
async fn start_relay(target: String) -> (String, SchedulerRequests) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let seen = SchedulerRequests::default();
    let app =
        Router::new().fallback(relay).with_state(Relay { target, http: reqwest::Client::new(), seen: seen.clone() });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, seen)
}

/// The scheduler trusting the console's key, the fake provider, and the console configured for both,
/// with `who_may_sign_in` as the `[sign_in]` section's rules.
pub async fn setup(database: &Database, who_may_sign_in: &str) -> (support::App, Provider, Console, String) {
    let (app, provider, console, base, _) = start(database, who_may_sign_in, &["EdDSA"], false).await;
    (app, provider, console, base)
}

/// The same, with a provider whose discovery document lists these signature algorithms (it still
/// signs with EdDSA).
pub async fn setup_listing(
    database: &Database,
    who_may_sign_in: &str,
    listed: &[&str],
) -> (support::App, Provider, Console, String) {
    let (app, provider, console, base, _) = start(database, who_may_sign_in, listed, false).await;
    (app, provider, console, base)
}

/// The same as `setup`, with the console reaching the scheduler through a relay that notes every
/// request it sends.
pub async fn setup_counting(
    database: &Database,
    who_may_sign_in: &str,
) -> (support::App, Provider, Console, String, SchedulerRequests) {
    start(database, who_may_sign_in, &["EdDSA"], true).await
}

async fn start(
    database: &Database,
    who_may_sign_in: &str,
    listed: &[&str],
    counting: bool,
) -> (support::App, Provider, Console, String, SchedulerRequests) {
    let key = downbeat_client::PrivateKey::generate();
    let key_file = std::env::temp_dir().join(format!("console-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&key_file, key.to_pem()).unwrap();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"console\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\nacts_for_people = true\n",
        key.public().to_base64()
    );
    let (app, scheduler) = support::with_callers(database, &callers).await;
    let (scheduler, seen) =
        if counting { start_relay(scheduler).await } else { (scheduler, SchedulerRequests::default()) };
    let provider = start_provider(listed).await;

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
        .env("TEST_CLIENT_SECRET", CLIENT_SECRET)
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(format!("{base}/api/me")).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "the console did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    (app, provider, Console(process), base, seen)
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
