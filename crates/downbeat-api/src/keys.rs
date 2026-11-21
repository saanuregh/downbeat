//! Ed25519 keys: a caller signs with its private key, a scheduler holds only public keys.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, spki::der::pem::LineEnding};
use ed25519_dalek::{SigningKey, VerifyingKey};

/// A caller's private key. Its `Debug` shows only the key id, never the key.
#[derive(Clone)]
pub struct PrivateKey(SigningKey);

/// A public key, written as the base64url encoding of its 32 bytes (as in a JWK's `x`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PublicKey(VerifyingKey);

/// Why a key could not be read.
#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("the private key is not an Ed25519 key in PKCS#8 PEM: {0}")]
    BadPrivate(String),
    #[error("the public key is not 32 bytes of base64url: {0}")]
    BadPublic(String),
}

impl PrivateKey {
    /// A new random key.
    pub fn generate() -> PrivateKey {
        PrivateKey(SigningKey::generate(&mut rand::rand_core::UnwrapErr(rand::rngs::SysRng)))
    }

    /// Reads a key written as PKCS#8 PEM, as `openssl genpkey -algorithm ed25519` writes it.
    pub fn from_pem(pem: &str) -> Result<PrivateKey, KeyError> {
        SigningKey::from_pkcs8_pem(pem.trim()).map(PrivateKey).map_err(|error| KeyError::BadPrivate(error.to_string()))
    }

    /// The key as PKCS#8 PEM, to keep in a secret store.
    pub fn to_pem(&self) -> String {
        self.0.to_pkcs8_pem(LineEnding::LF).map_or_default(|pem| pem.to_string())
    }

    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.verifying_key())
    }

    /// The key's id: a short name derived from its public half, carried in each token's `kid`.
    pub fn id(&self) -> String {
        self.public().id()
    }

    pub(crate) fn signing(&self) -> &SigningKey {
        &self.0
    }
}

impl std::fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PrivateKey").field(&self.id()).finish()
    }
}

impl PublicKey {
    pub fn from_base64(text: &str) -> Result<PublicKey, KeyError> {
        let bytes = URL_SAFE_NO_PAD.decode(text.trim()).map_err(|error| KeyError::BadPublic(error.to_string()))?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| KeyError::BadPublic("wrong length".into()))?;
        VerifyingKey::from_bytes(&bytes).map(PublicKey).map_err(|error| KeyError::BadPublic(error.to_string()))
    }

    pub fn to_base64(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0.as_bytes())
    }

    /// The first twelve characters of the key: enough to tell keys apart in a token's `kid`.
    pub fn id(&self) -> String {
        self.to_base64()[..12].to_owned()
    }

    pub(crate) fn verifying(&self) -> &VerifyingKey {
        &self.0
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_base64())
    }
}

/// The audience a scheduler started with `--local` answers to.
pub const DEVELOPMENT_AUDIENCE: &str = "local";

/// The development key: published here on purpose, so it is worthless anywhere but the local
/// machine. A scheduler trusts it only when started with `--local`, which listens on loopback only.
pub fn development_key() -> PrivateKey {
    let seed: [u8; 32] = *b"downbeat development key: local!";
    PrivateKey(SigningKey::from_bytes(&seed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_through_their_text_forms() {
        let key = PrivateKey::generate();
        let again = PrivateKey::from_pem(&key.to_pem()).unwrap();
        assert_eq!(key.public(), again.public());
        assert_eq!(PublicKey::from_base64(&key.public().to_base64()).unwrap(), key.public());
    }

    #[test]
    fn debug_never_shows_the_private_key() {
        let key = PrivateKey::generate();
        assert!(!format!("{key:?}").contains("PRIVATE"));
        assert_eq!(format!("{key:?}"), format!("PrivateKey({:?})", key.id()));
    }
}
