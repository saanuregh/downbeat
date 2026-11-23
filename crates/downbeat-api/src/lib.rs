//! The control API of downbeat: the types its requests and responses use, and the signed tokens
//! every request carries.
//!
//! The scheduler, the Rust client and the console all depend on this crate, so a request is signed
//! and checked by the same code on both sides, and the wire types cannot drift apart.

mod keys;
mod token;
pub mod types;

pub use keys::{DEVELOPMENT_AUDIENCE, KeyError, PrivateKey, PublicKey, development_key};
pub use token::{Claims, LEEWAY_SECONDS, RequestBinding, Scope, TOKEN_TYPE, TokenError, sign, verify};

/// A job a program outside the product's binary may start, named with its input type, so the
/// compiler checks what is sent. Declare them with [`job_name!`] in a small crate the product's
/// services share; the scheduler still checks every input against the job's real type.
pub trait JobName: Copy {
    type Input: serde::Serialize;
    const NAME: &'static str;
}

/// Declares a job's name and input type for programs that start it through the control API:
/// `job_name!(pub send_report: ReportRequest);`.
#[macro_export]
macro_rules! job_name {
    ($vis:vis $name:ident: $input:ty) => {
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        $vis struct $name;

        impl $crate::JobName for $name {
            type Input = $input;
            const NAME: &'static str = stringify!($name);
        }
    };
}

/// Builds one of the `#[non_exhaustive]` [`types`] outside this crate, written like a struct
/// literal (`with_defaults!(DayOff { schedule, reason: text })`): it starts from `Default` and sets
/// each named field, so the fields not named keep their defaults.
#[doc(hidden)]
#[macro_export]
macro_rules! with_defaults {
    ($type:ident { $($field:ident $(: $value:expr)?),* $(,)? }) => {{
        let mut built = $type::default();
        $( built.$field = $crate::with_defaults!(@value $field $(: $value)?); )*
        built
    }};
    (@value $field:ident : $value:expr) => { $value };
    (@value $field:ident) => { $field };
}

/// The version of the control API this crate speaks. A console supports this one and the one before.
pub const API_VERSION: u32 = 1;

/// The header in which a scheduler reports the API version it speaks.
pub const VERSION_HEADER: &str = "downbeat-api-version";

/// The header an idempotent start request carries its key in.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";
