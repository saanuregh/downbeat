//! The control API of downbeat: the types its requests and responses use, and the signed tokens
//! every request carries.
//!
//! The scheduler, the Rust client and the console all depend on this crate, so a request is signed
//! and checked by the same code on both sides, and the wire types cannot drift apart.

mod keys;
mod token;
pub mod types;

pub use keys::{DEVELOPMENT_AUDIENCE, KeyError, PrivateKey, PublicKey, development_key};
pub use token::{Claims, RequestBinding, Scope, TOKEN_TYPE, TokenError, sign, verify};

/// The version of the control API this crate speaks. A console supports this one and the one before.
pub const API_VERSION: u32 = 1;

/// The header in which a scheduler reports the API version it speaks.
pub const VERSION_HEADER: &str = "downbeat-api-version";

/// The header an idempotent start request carries its key in.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";
