//! A client for downbeat's control API, for Rust programs outside the product's binary: a service
//! that starts jobs, an admin tool, or a console.
//!
//! It handles the secure connection completely: HTTPS only (except against a scheduler started
//! with `--local`), a fresh signed token for every request, an idempotency key on every start that a
//! retry reuses, and a check that the scheduler speaks a supported API version.
//!
//! ```no_run
//! # async fn example() -> Result<(), downbeat_client::Error> {
//! let scheduler = downbeat_client::Client::builder("https://jobs.internal:7400")
//!     .caller("billing-service")
//!     .audience("production")
//!     .key_from_env("DOWNBEAT_KEY")?
//!     .connect()
//!     .await?;
//! let started = scheduler.start("send_report", &serde_json::json!({ "user_id": 42 })).send().await?;
//! # Ok(()) }
//! ```

mod client;
mod error;
mod pause;
mod start;

pub use client::{ActingClient, Builder, Client};
pub use downbeat_api::types;
pub use downbeat_api::{JobName, PrivateKey, PublicKey, Scope, development_key, job_name};
pub use error::{ConnectionError, Error};
pub use pause::PauseKind;
pub use start::Start;
