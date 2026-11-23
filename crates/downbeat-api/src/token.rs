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
/// How far apart the caller's and the scheduler's clocks may be. A token is accepted until its
/// `exp` plus this, so a record of a used token must be kept at least that long.
pub const LEEWAY_SECONDS: i64 = 5;

/// What a request may do. Each scope includes the ones before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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
        rand::fill(&mut jti);
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
#[non_exhaustive]
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
    fn a_token_verifies_against_any_of_its_callers_keys() {
        let old = PrivateKey::generate();
        let new = PrivateKey::generate();
        let keys = [old.public(), new.public()];
        for key in [&old, &new] {
            let result = verify(&token(key, b""), |_| Some(&keys[..]), "prod", request(b""), NOW);
            assert!(result.is_ok(), "{result:?}");
        }
        let unnamed = serde_json::json!({ "alg": "Ed25519", "typ": TOKEN_TYPE, "kid": "retired" });
        let result = verify(&with_header(&new, &unnamed, &claims()), |_| Some(&keys[..]), "prod", request(b""), NOW);
        assert!(result.is_ok(), "a key id that names none of the keys still tries each one: {result:?}");
    }

    #[test]
    fn a_token_expires_after_a_minute() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let result = verify(&token(&key, b""), |_| Some(&keys[..]), "prod", request(b""), NOW + 120);
        assert!(matches!(result, Err(TokenError::Expired(_))));
    }

    /// Signs claims under a header made of these fields, so a test can break one of them.
    fn with_header(key: &PrivateKey, header: &serde_json::Value, claims: &Claims) -> String {
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(header).unwrap());
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header}.{payload}");
        let signature: Signature = key.signing().sign(signing_input.as_bytes());
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }

    fn claims() -> Claims {
        Claims::for_request("console", "asha", "prod", Scope::Operate, None, request(b""), NOW)
    }

    #[test]
    fn a_token_with_the_wrong_type_algorithm_or_no_key_id_is_refused() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let check =
            |header| verify(&with_header(&key, header, &claims()), |_| Some(&keys[..]), "prod", request(b""), NOW);

        let good = serde_json::json!({ "alg": "EdDSA", "typ": TOKEN_TYPE, "kid": key.id() });
        assert!(check(&good).is_ok(), "the older algorithm name is accepted");
        let wrong_type = serde_json::json!({ "alg": "Ed25519", "typ": "JWT", "kid": key.id() });
        assert_eq!(check(&wrong_type), Err(TokenError::WrongType));
        let wrong_algorithm = serde_json::json!({ "alg": "HS256", "typ": TOKEN_TYPE, "kid": key.id() });
        assert_eq!(check(&wrong_algorithm), Err(TokenError::WrongAlgorithm));
        let no_key_id = serde_json::json!({ "alg": "Ed25519", "typ": TOKEN_TYPE });
        assert!(check(&no_key_id).is_err());
    }

    #[test]
    fn a_token_that_lives_too_long_or_starts_in_the_future_is_refused() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let check = |claims: &Claims| verify(&sign(&key, claims), |_| Some(&keys[..]), "prod", request(b""), NOW);

        let long = Claims { exp: NOW + LIFETIME_SECONDS + 1, ..claims() };
        assert_eq!(check(&long), Err(TokenError::TooLong));
        let at_the_leeway = Claims { iat: NOW + LEEWAY_SECONDS, exp: NOW + LEEWAY_SECONDS + 30, ..claims() };
        assert!(check(&at_the_leeway).is_ok());
        let future = Claims { iat: NOW + LEEWAY_SECONDS + 1, exp: NOW + LEEWAY_SECONDS + 31, ..claims() };
        assert!(matches!(check(&future), Err(TokenError::Expired(_))));
    }

    #[test]
    fn a_token_for_another_method_or_path_is_refused() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let token = token(&key, b"{}");
        let check = |method, path_and_query| {
            verify(&token, |_| Some(&keys[..]), "prod", RequestBinding { method, path_and_query, body: b"{}" }, NOW)
        };

        assert!(check("post", "/v1/jobs/report/runs").is_ok(), "the method is compared without case");
        assert_eq!(check("PUT", "/v1/jobs/report/runs"), Err(TokenError::WrongRequest));
        assert_eq!(check("POST", "/v1/jobs/other/runs"), Err(TokenError::WrongRequest));
        assert_eq!(check("POST", "/v1/jobs/report/runs?preview=true"), Err(TokenError::WrongRequest));
        assert_eq!(check("POST", "/jobs/report/runs"), Err(TokenError::WrongRequest), "htu includes /v1");
    }

    #[test]
    fn a_token_is_accepted_up_to_five_seconds_after_it_expires() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let claims = claims();
        let token = sign(&key, &claims);
        let at = |now| verify(&token, |_| Some(&keys[..]), "prod", request(b""), now);

        assert!(at(claims.exp + LEEWAY_SECONDS).is_ok(), "exp + 5 s is accepted");
        assert_eq!(at(claims.exp + 6), Err(TokenError::Expired(claims.exp + 6 - claims.iat)), "exp + 6 s is refused");
    }

    #[test]
    fn a_token_is_accepted_up_to_five_seconds_before_it_was_issued() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let claims = claims();
        let token = sign(&key, &claims);
        let at = |now| verify(&token, |_| Some(&keys[..]), "prod", request(b""), now);

        assert!(at(claims.iat - LEEWAY_SECONDS).is_ok(), "iat - 5 s is accepted");
        assert_eq!(at(claims.iat - 6), Err(TokenError::Expired(-6)), "iat - 6 s is refused");
    }

    /// The order of the Ed25519 group, little-endian.
    const GROUP_ORDER: [u8; 32] = [
        0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10,
    ];

    #[test]
    fn a_signature_whose_scalar_is_not_reduced_is_refused() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let token = sign(&key, &claims());
        let (signing_input, signature) = token.rsplit_once('.').unwrap();
        let mut bytes = URL_SAFE_NO_PAD.decode(signature).unwrap();
        // S + L is the same point equation, so only a strict check of S < L tells them apart.
        let mut carry = 0_u16;
        for (byte, order) in bytes[32..].iter_mut().zip(GROUP_ORDER) {
            let sum = u16::from(*byte) + u16::from(order) + carry;
            *byte = sum.to_le_bytes()[0];
            carry = sum >> 8;
        }
        assert_eq!(carry, 0, "S + L fits in 32 bytes, since S < L < 2^253");
        let malleated = format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(&bytes));

        assert!(verify(&token, |_| Some(&keys[..]), "prod", request(b""), NOW).is_ok());
        let result = verify(&malleated, |_| Some(&keys[..]), "prod", request(b""), NOW);
        assert!(
            matches!(result, Err(TokenError::BadSignature | TokenError::Malformed)),
            "a non-canonical signature was {result:?}"
        );
    }

    #[test]
    fn the_body_hash_is_absent_for_an_empty_body_and_must_match_its_presence() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let check =
            |claims: &Claims, body: &[u8]| verify(&sign(&key, claims), |_| Some(&keys[..]), "prod", request(body), NOW);

        let empty = claims();
        assert_eq!(empty.body_sha256, None);
        let encoded = serde_json::to_value(&empty).unwrap();
        assert!(encoded.get("body_sha256").is_none(), "no body_sha256 claim is written: {encoded}");
        assert!(check(&empty, b"").is_ok());

        let hash_of_nothing = Claims { body_sha256: Some(URL_SAFE_NO_PAD.encode(Sha256::digest(b""))), ..claims() };
        assert_eq!(check(&hash_of_nothing, b""), Err(TokenError::WrongRequest), "a hash for an empty body");

        let with_body = Claims::for_request("console", "asha", "prod", Scope::Operate, None, request(b"{}"), NOW);
        assert!(with_body.body_sha256.is_some());
        let no_hash = Claims { body_sha256: None, ..with_body };
        assert_eq!(check(&no_hash, b"{}"), Err(TokenError::WrongRequest), "no hash for a body");
    }

    #[test]
    fn a_token_without_exactly_three_parts_is_refused() {
        let key = PrivateKey::generate();
        let keys = [key.public()];
        let token = token(&key, b"{}");
        let check = |text: &str| verify(text, |_| Some(&keys[..]), "prod", request(b"{}"), NOW);
        assert!(check(&token).is_ok(), "the token itself verifies");
        let (signing_input, signature) = token.rsplit_once('.').unwrap();
        assert_eq!(check(signing_input), Err(TokenError::Malformed), "two parts");
        assert_eq!(check(&format!("{token}.{signature}")), Err(TokenError::Malformed), "four parts");
        assert_eq!(check(&format!("{token}.")), Err(TokenError::Malformed), "four parts, the last empty");
    }

    #[test]
    fn scopes_are_ordered() {
        assert!(Scope::Read < Scope::Operate && Scope::Operate < Scope::Administer);
    }
}
