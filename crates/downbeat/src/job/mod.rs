//! Jobs: typed async functions, and what the engine keeps about each.
//!
//! A product writes a job as a function with `#[job(…)]` (or `#[command(…)]`). The macro replaces
//! the function with a handle of the same name that implements [`Job`]; everything else refers to
//! the job by that handle. The engine itself works with [`JobSpec`], the same job with its types
//! erased, so it can store inputs and outputs as JSON and call the function by name in a child
//! process.

mod data;
mod outcome;
mod policy;
mod program;
mod run;

use std::any::TypeId;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;

pub use data::{FieldShape, JobData, Shape, without_fields};
pub(crate) use data::{decode_strict, identity_hash};
pub(crate) use outcome::Stop;
pub use outcome::{JobError, Outcome, Result, not_ready, skip};
pub use policy::{Backoff, Policy, RetryOn, RetryOnSet, Turn, presets};
pub(crate) use program::env_names;
pub use program::{EXIT_NOT_READY, Program, ProgramSpec};
pub use run::{MAX_VALUE_BYTES, Run, Start, Started};
pub(crate) use run::{RunLink, StartRequest, decode};

use crate::dependencies::{Dependencies, Dependency};

/// Every `#[job]` and `#[command]` in the binary registers its name here, so `check` can name a
/// job that no pipeline uses.
#[doc(hidden)]
#[derive(Debug)]
pub struct Registered {
    pub name: &'static str,
}

inventory::collect!(Registered);

/// The names of every job compiled into this binary.
pub(crate) fn compiled_jobs() -> impl Iterator<Item = &'static str> {
    inventory::iter::<Registered>.into_iter().map(|registered| registered.name)
}

/// A job: the handle `#[job]` or `#[command]` makes from a function.
pub trait Job: Copy + Send + Sync + 'static {
    /// What the function takes; `()` for a job with no input.
    type Input: JobData;
    /// What the function returns, and what its children receive.
    type Output: JobData;
    /// The job's name, stored with every run.
    const NAME: &'static str;

    /// The job with its types erased, for the engine.
    #[doc(hidden)]
    fn spec() -> JobSpec;
}

/// What the engine keeps about a job.
pub struct JobSpec {
    pub(crate) name: &'static str,
    pub(crate) policy: Policy,
    pub(crate) input: Shape,
    pub(crate) output: Shape,
    pub(crate) takes_input: bool,
    pub(crate) dependencies: Vec<Dependency>,
    pub(crate) body: Body,
    pub(crate) input_ops: InputOps,
    /// Decode and re-encode an output: for a person recording one by hand.
    pub(crate) normalize_output: fn(Value, bool) -> std::result::Result<Value, String>,
}

impl std::fmt::Debug for JobSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobSpec").field("name", &self.name).field("policy", &self.policy).finish_non_exhaustive()
    }
}

/// How a job's work is done.
pub(crate) enum Body {
    /// A Rust function, called in a child process.
    Function(FunctionBody),
    /// A program the worker starts, described by the command's function.
    Command { program: ProgramFn, output: fn(Value) -> std::result::Result<Value, String> },
}

/// A command's function with its types erased: from the input, the program to start.
pub type ProgramFn = fn(Value) -> std::result::Result<ProgramSpec, String>;

/// For `one_per = field`: the field's value in an input, as text.
pub type TurnValueFn = fn(&Value) -> std::result::Result<String, String>;

/// A job function with its types erased: decode the input, fetch the dependencies, call it,
/// encode the output.
pub type FunctionBody = fn(Run, Value, Arc<Dependencies>) -> BoxFuture<'static, Result<Value>>;

/// Operations on a job's input that need its type.
pub(crate) struct InputOps {
    /// Decode and re-encode an input; strictly (no unknown fields) for one sent by a caller.
    pub(crate) normalize: fn(Value, bool) -> std::result::Result<Value, String>,
    /// The hash that identifies an input's work.
    pub(crate) hash: fn(&Value) -> std::result::Result<String, String>,
    /// For `one_per = field`: the field's value, as text.
    pub(crate) turn_value: Option<TurnValueFn>,
}

/// An input ready to become a run: as its type writes it, the hash of its work, and its `one_per`
/// value.
pub(crate) struct KeyedInput {
    pub(crate) input: Value,
    pub(crate) hash: String,
    pub(crate) turn_value: Option<String>,
}

impl InputOps {
    /// Decodes, hashes and reads `one_per` from an input, outside a child process. These call the
    /// product's types, so a panic is caught and becomes an error that names `job`.
    pub(crate) fn keyed(&self, job: &str, value: Value, strict: bool) -> std::result::Result<KeyedInput, String> {
        crate::guard::guarded(&format!("the input type of {job}"), || {
            let input = (self.normalize)(value, strict)?;
            let hash = (self.hash)(&input)?;
            let turn_value = self.turn_value.map(|value_of| value_of(&input)).transpose()?;
            Ok(KeyedInput { input, hash, turn_value })
        })
    }
}

impl JobSpec {
    /// A job done by a Rust function. Used by `#[job]`.
    #[doc(hidden)]
    pub fn function<J: Job>(
        policy: Policy,
        dependencies: Vec<Dependency>,
        turn_value: Option<TurnValueFn>,
        body: FunctionBody,
    ) -> Self {
        Self::new::<J>(policy, dependencies, turn_value, Body::Function(body))
    }

    /// A job done by a program. Used by `#[command]`.
    #[doc(hidden)]
    pub fn command<J: Job>(policy: Policy, turn_value: Option<TurnValueFn>, program: ProgramFn) -> Self {
        let output = |value: Value| normalize::<J::Output>(value, false);
        Self::new::<J>(policy, Vec::new(), turn_value, Body::Command { program, output })
    }

    fn new<J: Job>(policy: Policy, dependencies: Vec<Dependency>, turn_value: Option<TurnValueFn>, body: Body) -> Self {
        JobSpec {
            name: J::NAME,
            policy,
            input: J::Input::shape(),
            output: J::Output::shape(),
            takes_input: TypeId::of::<J::Input>() != TypeId::of::<()>(),
            dependencies,
            body,
            input_ops: InputOps { normalize: normalize::<J::Input>, hash: hash::<J::Input>, turn_value },
            normalize_output: normalize::<J::Output>,
        }
    }

    pub(crate) fn is_command(&self) -> bool {
        matches!(self.body, Body::Command { .. })
    }
}

/// Decode a value into `T` and encode it again, so what is stored is what the type would write.
#[allow(clippy::needless_pass_by_value)] // Its type is the `fn(Value, bool)` stored in `InputOps`.
pub(crate) fn normalize<T: JobData>(value: Value, strict: bool) -> std::result::Result<Value, String> {
    let typed: T = if strict { decode_strict(&value)? } else { decode(value)? };
    let encoded = serde_json::to_value(&typed).map_err(|error| error.to_string())?;
    // Every value the engine stores (input or output) is held to the same limit.
    let size = encoded.to_string().len();
    if size > MAX_VALUE_BYTES {
        return Err(format!("the value is {size} bytes; the limit is {MAX_VALUE_BYTES}"));
    }
    Ok(encoded)
}

fn hash<T: JobData>(value: &Value) -> std::result::Result<String, String> {
    let typed: T = decode(value.clone())?;
    Ok(identity_hash(&typed.identity()))
}

/// Encode a job's output, for the code `#[job]` generates.
#[doc(hidden)]
pub fn encode_output<T: JobData>(output: &T) -> Result<Value> {
    serde_json::to_value(output).map_err(|error| JobError::msg(format!("the output could not be encoded: {error}")))
}

/// Decode a job's input, for the code `#[job]` generates.
#[doc(hidden)]
pub fn decode_input<T: JobData>(input: Value) -> Result<T> {
    decode(input).map_err(|error| JobError(Stop::BadInput(format!("the input does not decode: {error}"))))
}

/// Decode a value into its type, with the error as text; used by the code `#[job]` generates.
#[doc(hidden)]
pub fn decode_value<T: JobData>(value: &Value) -> std::result::Result<T, String> {
    decode(value.clone())
}

/// The value of one input field as text, for `one_per`; used by the code `#[job]` generates.
#[doc(hidden)]
pub fn field_text<T: serde::Serialize>(field: &T) -> std::result::Result<String, String> {
    serde_json::to_string(field).map_err(|error| error.to_string())
}
