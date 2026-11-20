//! Signing in with OpenID Connect, and sessions.
//!
//! Sign-in is the authorization-code flow with PKCE (`S256`), `state` and `nonce` (RFC 9700). A
//! session is a random id in a cookie scripts cannot read, sent only over HTTPS and never on
//! requests from other sites; only its hash is stored, so it can be ended from the database.

use std::sync::Arc;

use axum::extract::{Query, Request, State as Extract};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreJwsSigningAlgorithm, CoreProviderMetadata};
use openidconnect::{
    AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce, PkceCodeChallenge, PkceCodeVerifier,
    RedirectUrl, Scope, TokenResponse,
};
use rand::RngCore;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::State;
use crate::error::Failure;
use crate::store::{self, Person};

const COOKIE: &str = "downbeat_session";

/// The signed-in person and their session's CSRF token, put on every API request.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub(crate) person: Person,
    pub(crate) csrf_token: String,
}

/// The identity provider, discovered at start.
pub(crate) struct Provider {
    metadata: CoreProviderMetadata,
    /// The signature algorithms the provider says it signs identity tokens with, less any that
    /// are unsigned or use a shared secret.
    algorithms: Vec<CoreJwsSigningAlgorithm>,
    http: openidconnect::reqwest::Client,
    client_id: String,
    client_secret: String,
}

impl Provider {
    pub(crate) async fn discover(sign_in: &crate::config::SignIn) -> anyhow::Result<Provider> {
        let http = openidconnect::reqwest::ClientBuilder::new()
            // Following redirects would open the console up to request forgery.
            .redirect(openidconnect::reqwest::redirect::Policy::none())
            .build()?;
        let issuer = IssuerUrl::new(sign_in.issuer.clone())?;
        let metadata = CoreProviderMetadata::discover_async(issuer, &http)
            .await
            .map_err(|error| anyhow::anyhow!("discovering {}: {error}", sign_in.issuer))?;
        let client_secret = std::env::var(&sign_in.client_secret_env)
            .map_err(|_| anyhow::anyhow!("{} is not set", sign_in.client_secret_env))?;
        let algorithms: Vec<CoreJwsSigningAlgorithm> = metadata
            .id_token_signing_alg_values_supported()
            .iter()
            .filter(|algorithm| {
                !matches!(
                    algorithm,
                    CoreJwsSigningAlgorithm::None
                        | CoreJwsSigningAlgorithm::HmacSha256
                        | CoreJwsSigningAlgorithm::HmacSha384
                        | CoreJwsSigningAlgorithm::HmacSha512
                )
            })
            .cloned()
            .collect();
        anyhow::ensure!(
            !algorithms.is_empty(),
            "{} signs identity tokens with no algorithm the console accepts",
            sign_in.issuer
        );
        Ok(Provider { metadata, algorithms, http, client_id: sign_in.client_id.clone(), client_secret })
    }
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn hash(text: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(text.as_bytes()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct LoginQuery {
    #[serde(rename = "return")]
    return_to: Option<String>,
}

/// `GET /auth/login`: sends the browser to the identity provider.
pub(crate) async fn login(
    Extract(state): Extract<State>,
    Query(query): Query<LoginQuery>,
) -> Result<Response, Failure> {
    let return_to = safe_return(query.return_to.as_deref());
    let Some(provider) = &state.provider else { return Ok(Redirect::to(&return_to).into_response()) };
    let redirect = RedirectUrl::new(format!("{}/auth/callback", state.config.public_url))
        .map_err(|error| Failure::internal(error.to_string()))?;
    let client = CoreClient::from_provider_metadata(
        provider.metadata.clone(),
        ClientId::new(provider.client_id.clone()),
        Some(ClientSecret::new(provider.client_secret.clone())),
    )
    .set_redirect_uri(redirect);
    let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
    let (url, csrf, nonce) = client
        .authorize_url(CoreAuthenticationFlow::AuthorizationCode, CsrfToken::new_random, Nonce::new_random)
        .add_scope(Scope::new("email".to_owned()))
        .add_scope(Scope::new("profile".to_owned()))
        .set_pkce_challenge(challenge)
        .url();
    sqlx::query("INSERT INTO console.sign_in (state, pkce_verifier, nonce, return_to) VALUES ($1, $2, $3, $4)")
        .bind(csrf.secret())
        .bind(verifier.secret())
        .bind(nonce.secret())
        .bind(&return_to)
        .execute(&state.pool)
        .await?;
    Ok(Redirect::to(url.as_str()).into_response())
}

/// Only paths on this console, so the return address cannot send the browser elsewhere.
fn safe_return(path: Option<&str>) -> String {
    match path {
        // Browsers drop tabs and newlines when reading a URL, so "/\t/x" would become "//x".
        Some(path)
            if path.starts_with('/')
                && !path.starts_with("//")
                && !path.contains('\\')
                && !path.chars().any(char::is_control) =>
        {
            path.to_owned()
        }
        _ => "/".to_owned(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CallbackQuery {
    code: String,
    state: String,
}

/// `GET /auth/callback`: checks the provider's answer and starts a session.
pub(crate) async fn callback(
    Extract(state): Extract<State>,
    Query(query): Query<CallbackQuery>,
) -> Result<Response, Failure> {
    let provider = state.provider.as_ref().ok_or_else(|| Failure::not_found("sign-in is not configured"))?;
    let pending: Option<(String, String, String)> =
        sqlx::query_as("DELETE FROM console.sign_in WHERE state = $1 AND created_at > now() - interval '10 minutes' RETURNING pkce_verifier, nonce, return_to")
            .bind(&query.state)
            .fetch_optional(&state.pool)
            .await?;
    let (verifier, nonce, return_to) =
        pending.ok_or_else(|| Failure::unauthorized("this sign-in has expired or was not started here; try again"))?;
    let redirect = RedirectUrl::new(format!("{}/auth/callback", state.config.public_url))
        .map_err(|error| Failure::internal(error.to_string()))?;
    let client = CoreClient::from_provider_metadata(
        provider.metadata.clone(),
        ClientId::new(provider.client_id.clone()),
        Some(ClientSecret::new(provider.client_secret.clone())),
    )
    .set_redirect_uri(redirect);
    let tokens = client
        .exchange_code(AuthorizationCode::new(query.code))
        .map_err(|error| Failure::internal(error.to_string()))?
        .set_pkce_verifier(PkceCodeVerifier::new(verifier))
        .request_async(&provider.http)
        .await
        .map_err(|error| Failure::unauthorized(format!("the identity provider refused the sign-in: {error}")))?;
    let id_token =
        tokens.id_token().ok_or_else(|| Failure::unauthorized("the identity provider returned no identity token"))?;
    // Checks the signature, issuer, audience, expiry and nonce.
    let claims = id_token
        .claims(&client.id_token_verifier().set_allowed_algs(provider.algorithms.clone()), &Nonce::new(nonce))
        .map_err(|error| Failure::unauthorized(format!("the identity token is not valid: {error}")))?;
    let subject = claims.subject().as_str().to_owned();
    let email = claims
        .email()
        .map(|email| email.as_str().to_owned())
        .ok_or_else(|| Failure::unauthorized("the identity provider did not share an email address"))?;
    // An email the provider has not verified could be anyone's, and grants follow emails.
    if claims.email_verified() != Some(true) {
        return Err(Failure::forbidden("the identity provider has not verified this email address"));
    }
    let name =
        claims.name().and_then(|names| names.get(None)).map_or_else(|| email.clone(), |name| name.as_str().to_owned());
    let sign_in = state.config.sign_in.as_ref().expect("a provider implies sign-in settings");
    let groups = groups_of(&id_token.to_string(), &sign_in.groups_claim);

    let domain_ok = email
        .rsplit_once('@')
        .is_some_and(|(_, domain)| sign_in.allowed_domains.iter().any(|allowed| allowed.eq_ignore_ascii_case(domain)));
    let group_ok = groups.iter().any(|group| sign_in.allowed_groups.contains(group));
    if !domain_ok && !group_ok {
        store::audit(&state.pool, &email, "sign_in_refused", json!({ "reason": "not in an allowed domain or group" }))
            .await?;
        return Err(Failure::forbidden("you may not sign in to this console; ask its administrators"));
    }
    let person = store::upsert_person(&state.pool, &subject, &email, &name, &groups).await?;
    let session = random_token();
    sqlx::query(
        "INSERT INTO console.session (session_hash, person_id, csrf_token, expires_at) VALUES ($1, $2, $3, now() + $4)",
    )
    .bind(hash(&session))
    .bind(person.person_id)
    .bind(random_token())
    .bind(interval(state.config.session_absolute))
    .execute(&state.pool)
    .await?;
    store::audit(&state.pool, &email, "sign_in", json!({})).await?;
    let mut response = Redirect::to(&return_to).into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie(&session, state.config.session_absolute.as_secs()));
    Ok(response)
}

/// The groups claim, read from the identity token's payload once its signature has been checked.
fn groups_of(id_token: &str, claim: &str) -> Vec<String> {
    let payload = id_token.split('.').nth(1).and_then(|part| URL_SAFE_NO_PAD.decode(part).ok());
    let claims: serde_json::Value = payload.and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or_default();
    claims
        .get(claim)
        .and_then(serde_json::Value::as_array)
        .map(|groups| groups.iter().filter_map(|group| group.as_str().map(str::to_owned)).collect())
        .unwrap_or_default()
}

fn cookie(value: &str, max_age: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("{COOKIE}={value}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age={max_age}"))
        .expect("a valid cookie")
}

fn interval(duration: std::time::Duration) -> sqlx::postgres::types::PgInterval {
    sqlx::postgres::types::PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
    }
}

/// `POST /auth/logout`: ends the session at once.
pub(crate) async fn logout(Extract(state): Extract<State>, headers: HeaderMap) -> Result<Response, Failure> {
    if let Some(session) = session_cookie(&headers) {
        // Signing out changes something, so it carries the session's CSRF token like everything else.
        let csrf = headers.get("x-csrf-token").and_then(|value| value.to_str().ok()).unwrap_or_default();
        let person: Option<String> = sqlx::query_scalar(
            "DELETE FROM console.session s USING console.person p
              WHERE s.session_hash = $1 AND s.csrf_token = $2 AND p.person_id = s.person_id RETURNING p.email",
        )
        .bind(hash(&session))
        .bind(csrf)
        .fetch_optional(&state.pool)
        .await?;
        if let Some(email) = person {
            store::audit(&state.pool, &email, "sign_out", json!({})).await?;
        }
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie("", 0));
    Ok(response)
}

fn session_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == COOKIE)
        .map(|(_, value)| value.to_owned())
}

/// Puts the signed-in person on every `/api` request, and checks the CSRF token on every request
/// that changes something.
pub(crate) async fn require(Extract(state): Extract<State>, mut request: Request, next: Next) -> Response {
    match authenticate(&state, request.headers()).await {
        Ok(session) => {
            if request.method() != Method::GET {
                let sent = request.headers().get("x-csrf-token").and_then(|value| value.to_str().ok());
                if sent != Some(session.csrf_token.as_str()) {
                    return Failure::forbidden("the request is missing this session's CSRF token").into_response();
                }
            }
            request.extensions_mut().insert(Arc::new(session));
            next.run(request).await
        }
        Err(failure) => failure.into_response(),
    }
}

async fn authenticate(state: &State, headers: &HeaderMap) -> Result<Session, Failure> {
    if state.config.local {
        let person = store::upsert_person(&state.pool, "local", "local@localhost", "Local", &[]).await?;
        return Ok(Session { person, csrf_token: state.local_csrf.clone() });
    }
    let session = session_cookie(headers).ok_or_else(|| Failure::unauthorized("sign in first"))?;
    let found: Option<(i64, String, String, Vec<String>, String)> = sqlx::query_as(
        "UPDATE console.session s SET last_used_at = now()
           FROM console.person p
          WHERE s.session_hash = $1 AND p.person_id = s.person_id AND s.expires_at > now() AND s.last_used_at > now() - $2
        RETURNING p.person_id, p.email, p.name, p.groups, s.csrf_token",
    )
    .bind(hash(&session))
    .bind(interval(state.config.session_idle))
    .fetch_optional(&state.pool)
    .await?;
    let (person_id, email, name, groups, csrf_token) =
        found.ok_or_else(|| Failure::unauthorized("the session has ended; sign in again"))?;
    Ok(Session { person: Person { person_id, email, name, groups }, csrf_token })
}

/// In `--local` mode everyone is an administrator, so a page elsewhere must not reach the console
/// by pointing its own name at this machine (DNS rebinding): every route answers only to this
/// machine's own names.
pub(crate) async fn local_host_only(Extract(state): Extract<State>, request: Request, next: Next) -> Response {
    if state.config.local {
        let port = state.config.listen.port();
        let host = request.headers().get(header::HOST).and_then(|value| value.to_str().ok()).unwrap_or_default();
        let ours = [format!("127.0.0.1:{port}"), format!("localhost:{port}"), format!("[::1]:{port}")];
        if !ours.iter().any(|name| name == host) {
            return Failure::forbidden("in --local mode the console answers only to 127.0.0.1, localhost or [::1]")
                .into_response();
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_return_address_stays_on_this_console() {
        assert_eq!(safe_return(Some("/runs?status=failed")), "/runs?status=failed");
        assert_eq!(safe_return(Some("https://evil.example")), "/");
        assert_eq!(safe_return(Some("//evil.example")), "/");
        assert_eq!(safe_return(Some("/\t/evil.example")), "/");
        assert_eq!(safe_return(Some("/\n/evil.example")), "/");
        assert_eq!(safe_return(None), "/");
    }
}
