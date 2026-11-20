//! Request tokens: a JSON Web Token (RFC 7519) per request, signed with Ed25519 as a compact JWS
//! (RFC 7515), following the JWT best practices of RFC 8725.
//!
//! A token says who issued it (`iss`), who is asking (`sub`), which scheduler it is for (`aud`), what
//! it may do (`scope`, `jobs`), and which request it is for: its method (`htm`), its path and query
//! (`htu`) and the SHA-256 of its body (`body_sha256`). It lives for at most a minute.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Signer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::keys::{PrivateKey, PublicKey};

/// The explicit type every request token carries, so no other JWT can be passed off as one.
pub const TOKEN_TYPE: &str = "downbeat-request+jwt";
/// The longest a token may live.
const LIFETIME_SECONDS: i64 = 60;
/// How far apart the caller's and the scheduler's clocks may be.
const LEEWAY_SECONDS: i64 = 5;

/// What a request may do. Each scope includes the ones before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Every GET.
    Read,
    /// Start, cancel, retry, restart, check now.
    Operate,
    /// Pauses, overrides, backfills, mark as succeeded, and a start that ignores conditions or
    /// changes the timeout.
    Administer,
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Scope::Read => "read",
            Scope::Operate => "operate",
            Scope::Administer => "administer",
        })
    }
}

impl std::str::FromStr for Scope {
    type Err = String;
    fn from_str(text: &str) -> Result<Scope, String> {
        match text {
            "read" => Ok(Scope::Read),
            "operate" => Ok(Scope::Operate),
            "administer" => Ok(Scope::Administer),
            other => Err(format!("'{other}' is not a scope; use read, operate or administer")),
        }
    }
}

/// The request a token is bound to.
#[derive(Debug, Clone, Copy)]
pub struct RequestBinding<'a> {
    pub method: &'a str,
    /// The path and query, such as `/v1/runs?status=failed`.
    pub path_and_query: &'a str,
    pub body: &'a [u8],
}

/// What a token says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// The trusted caller that issued it.
    pub iss: String,
    /// Who is asking: a person the console signed in, or the service itself.
    pub sub: String,
    /// The scheduler it is for.
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
    /// A random id; a token for a changing request is accepted once.
    pub jti: String,
    pub scope: Scope,
    /// The only jobs it covers, when it is limited to some.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jobs: Option<Vec<String>>,
    /// The request's method.
    pub htm: String,
    /// The request's path and query.
    pub htu: String,
    /// The SHA-256 of the request's body, base64url, for a request with a body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_sha256: Option<String>,
}

impl Claims {
    /// Claims for one request, valid from now for a minute.
    pub fn for_request(
        caller: &str,
        actor: &str,
        audience: &str,
        scope: Scope,
        jobs: Option<Vec<String>>,
        request: RequestBinding<'_>,
        now: i64,
    ) -> Claims {
        let mut jti = [0_u8; 16];
        rand::Rng::fill(&mut rand::thread_rng(), &mut jti);
        Claims {
            iss: caller.to_owned(),
            sub: actor.to_owned(),
            aud: audience.to_owned(),
            iat: now,
            exp: now + LIFETIME_SECONDS,
            jti: URL_SAFE_NO_PAD.encode(jti),
            scope,
            jobs,
            htm: request.method.to_ascii_uppercase(),
            htu: request.path_and_query.to_owned(),
            body_sha256: body_hash(request.body),
        }
    }

    /// Whether the token may act on this job.
    pub fn covers_job(&self, job: &str) -> bool {
        self.jobs.as_ref().is_none_or(|jobs| jobs.iter().any(|allowed| allowed == job))
    }
}

fn body_hash(body: &[u8]) -> Option<String> {
    (!body.is_empty()).then(|| URL_SAFE_NO_PAD.encode(Sha256::digest(body)))
}

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    alg: String,
    typ: String,
    kid: String,
}

/// Signs claims with a caller's key, as a compact JWS.
pub fn sign(key: &PrivateKey, claims: &Claims) -> String {
    let header = Header { alg: "Ed25519".to_owned(), typ: TOKEN_TYPE.to_owned(), kid: key.id() };
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).expect("a header encodes"));
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("claims encode"));
    let signing_input = format!("{header}.{payload}");
    let signature: Signature = key.signing().sign(signing_input.as_bytes());
    format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

/// Why a token was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("the token is not a compact JWS")]
    Malformed,
    #[error("the token's type is not {TOKEN_TYPE}")]
    WrongType,
    #[error("the token's algorithm must be Ed25519")]
    WrongAlgorithm,
    #[error("the token comes from '{0}', which is not a trusted caller")]
    UnknownCaller(String),
    #[error("the token's signature does not match any of the caller's keys")]
    BadSignature,
    #[error("the token is for '{0}', not this scheduler")]
    WrongAudience(String),
    #[error("the token has expired; the clocks differ by {0} seconds")]
    Expired(i64),
    #[error("the token lives longer than a minute")]
    TooLong,
    #[error("the token was made for another request")]
    WrongRequest,
}

/// Checks a token: its type and algorithm, its signature against the issuing caller's keys, its
/// audience, its lifetime, and that it was made for this request. Returns what it says.
pub fn verify<'k>(
    token: &str,
    keys_of: impl Fn(&str) -> Option<&'k [PublicKey]>,
    audience: &str,
    request: RequestBinding<'_>,
    now: i64,
) -> Result<Claims, TokenError> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) = (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(TokenError::Malformed);
    };
    let decode = |part: &str| URL_SAFE_NO_PAD.decode(part).map_err(|_| TokenError::Malformed);
    let parsed: Header = serde_json::from_slice(&decode(header)?).map_err(|_| TokenError::Malformed)?;
    if parsed.typ != TOKEN_TYPE {
        return Err(TokenError::WrongType);
    }
    // `Ed25519` is the fully specified name (RFC 9864); `EdDSA` is the older one for the same thing.
    if parsed.alg != "Ed25519" && parsed.alg != "EdDSA" {
        return Err(TokenError::WrongAlgorithm);
    }
    let claims: Claims = serde_json::from_slice(&decode(payload)?).map_err(|_| TokenError::Malformed)?;
    let keys = keys_of(&claims.iss).ok_or_else(|| TokenError::UnknownCaller(claims.iss.clone()))?;
    let signature = Signature::from_slice(&decode(signature)?).map_err(|_| TokenError::Malformed)?;
    let signing_input = format!("{header}.{payload}");
    let signed = keys
        .iter()
        .filter(|key| key.id() == parsed.kid)
        .chain(keys.iter())
        .any(|key| key.verifying().verify_strict(signing_input.as_bytes(), &signature).is_ok());
    if !signed {
        return Err(TokenError::BadSignature);
    }
    if claims.aud != audience {
        return Err(TokenError::WrongAudience(claims.aud));
    }
    if claims.exp - claims.iat > LIFETIME_SECONDS {
        return Err(TokenError::TooLong);
    }
    if now > claims.exp + LEEWAY_SECONDS || claims.iat > now + LEEWAY_SECONDS {
        return Err(TokenError::Expired(now - claims.iat));
    }
    let bound = claims.htm.eq_ignore_ascii_case(request.method)
        && claims.htu == request.path_and_query
        && claims.body_sha256 == body_hash(request.body);
    if !bound {
        return Err(TokenError::WrongRequest);
    }
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000;

    fn request(body: &[u8]) -> RequestBinding<'_> {
        RequestBinding { method: "POST", path_and_query: "/v1/jobs/report/runs", body }
    }

    fn token(key: &PrivateKey, body: &[u8]) -> String {
        sign(key, &Claims::for_request("console", "asha", "prod", Scope::Operate, None, request(body), NOW))
    }

    #[test]
    fn a_signed_token_verifies_for_its_own_request() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let claims = verify(&token(&key, b"{}"), |_| Some(&keys[..]), "prod", request(b"{}"), NOW + 10).unwrap();
        assert_eq!((claims.iss.as_str(), claims.sub.as_str(), claims.scope), ("console", "asha", Scope::Operate));
    }

    #[test]
    fn a_token_is_refused_for_another_request_audience_or_key() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let other = [PrivateKey::generate().public()];
        let token = token(&key, b"{}");
        assert_eq!(
            verify(&token, |_| Some(&keys[..]), "prod", request(b"{\"x\":1}"), NOW),
            Err(TokenError::WrongRequest)
        );
        assert_eq!(
            verify(&token, |_| Some(&keys[..]), "uat", request(b"{}"), NOW),
            Err(TokenError::WrongAudience("prod".into()))
        );
        assert_eq!(verify(&token, |_| Some(&other[..]), "prod", request(b"{}"), NOW), Err(TokenError::BadSignature));
        assert_eq!(
            verify(&token, |_| None, "prod", request(b"{}"), NOW),
            Err(TokenError::UnknownCaller("console".into()))
        );
    }

    #[test]
    fn a_token_expires_after_a_minute() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let result = verify(&token(&key, b""), |_| Some(&keys[..]), "prod", request(b""), NOW + 120);
        assert!(matches!(result, Err(TokenError::Expired(_))));
    }

    #[test]
    fn scopes_are_ordered() {
        assert!(Scope::Read < Scope::Operate && Scope::Operate < Scope::Administer);
    }
}
