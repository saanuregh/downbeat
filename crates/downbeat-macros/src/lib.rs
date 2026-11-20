//! The macros of downbeat: `#[job]`, `#[command]` and `#[derive(JobData)]`.
//!
//! Each turns what a product writes into the handle and trait implementations the engine uses.
//! The generated code refers to the `downbeat` crate by its absolute path.

mod data;
mod job;
mod policy;

use proc_macro::TokenStream;

/// Turns an async function into a job: a handle of the same name that pipelines refer to.
///
/// ```ignore
/// #[job(queue = "heavy", timeout = 2.hours(), attempts = 3)]
/// async fn bhav_candles(run: Run, archive: Archive, pools: &Pools) -> Result<Candles> { … }
/// ```
#[proc_macro_attribute]
pub fn job(attribute: TokenStream, item: TokenStream) -> TokenStream {
    job::expand(attribute.into(), item.into(), job::Kind::Function).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Turns a function that returns a [`Program`] into a job done by that program.
///
/// ```ignore
/// #[command(preset = heavy)]
/// fn eod_ohlc(input: OhlcInput) -> Program<Ohlc> { Program::new("./compute").arg(input.date) }
/// ```
#[proc_macro_attribute]
pub fn command(attribute: TokenStream, item: TokenStream) -> TokenStream {
    job::expand(attribute.into(), item.into(), job::Kind::Command).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Implements `JobData` for a struct or enum that also derives `Serialize` and `Deserialize`.
/// Mark a field that should not count when comparing inputs with `#[job_data(not_compared)]`.
#[proc_macro_derive(JobData, attributes(job_data))]
pub fn derive_job_data(item: TokenStream) -> TokenStream {
    data::expand(item.into()).unwrap_or_else(syn::Error::into_compile_error).into()
}
