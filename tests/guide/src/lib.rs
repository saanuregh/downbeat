//! Compiles every Rust sample in the README and the user guide (see `build.rs`). There is nothing
//! to run: if this crate builds, every sample compiles against the current API.

pub mod scaffold;

include!(concat!(env!("OUT_DIR"), "/samples.rs"));
