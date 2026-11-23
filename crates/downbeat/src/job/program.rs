//! `Program`: what a `#[command]` function returns, the program to start in its place.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// The program a command starts, with its arguments and environment. `Out` is the type of the
/// JSON the program writes to `JOB_OUTPUT_FILE`.
///
/// Each `.arg(…)` is exactly one argument, whatever the value contains: no shell is involved.
#[derive(Debug)]
pub struct Program<Out> {
    spec: ProgramSpec,
    output: PhantomData<fn() -> Out>,
}

/// The program, its arguments and environment, as the worker receives it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramSpec {
    pub program: String,
    pub args: Vec<String>,
    /// Variables set to these values.
    pub env: BTreeMap<String, String>,
    /// Variables copied from the worker's environment, when set there.
    pub pass_env: Vec<String>,
    pub dir: Option<PathBuf>,
    /// Positions in `args` of the arguments that came from values, through `.arg(…)`.
    #[serde(default)]
    pub from_values: Vec<usize>,
    /// Whether an argument that came from a value may begin with `-`.
    #[serde(default)]
    pub options_allowed: bool,
}

impl ProgramSpec {
    /// An argument that came from a value but looks like an option, when options are not allowed.
    pub(crate) fn smuggled_option(&self) -> Option<&str> {
        if self.options_allowed {
            return None;
        }
        self.from_values.iter().map(|index| self.args[*index].as_str()).find(|arg| arg.starts_with('-'))
    }

    /// The size in bytes of the spec as JSON, as it is sent to the worker.
    pub(crate) fn size(&self) -> Result<usize, String> {
        serde_json::to_string(self)
            .map(|json| json.len())
            .map_err(|error| format!("the program could not be encoded: {error}"))
    }
}

impl<Out> Program<Out> {
    /// The program to start: a path, or a name looked up on the worker's `PATH`.
    pub fn new(program: impl Into<String>) -> Self {
        Program {
            spec: ProgramSpec {
                program: program.into(),
                args: Vec::new(),
                env: BTreeMap::new(),
                pass_env: Vec::new(),
                dir: None,
                from_values: Vec::new(),
                options_allowed: false,
            },
            output: PhantomData,
        }
    }

    /// Fixed arguments, written in the code.
    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.spec.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// One argument, from a value. It stays one argument whatever it contains.
    #[must_use]
    pub fn arg(mut self, value: impl Display) -> Self {
        self.spec.from_values.push(self.spec.args.len());
        self.spec.args.push(value.to_string());
        self
    }

    /// Set a variable in the program's environment.
    #[must_use]
    pub fn env(mut self, name: impl Into<String>, value: impl Display) -> Self {
        self.spec.env.insert(name.into(), value.to_string());
        self
    }

    /// Copy these variables from the worker's environment. No others are inherited.
    #[must_use]
    pub fn pass_env<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.spec.pass_env.extend(names.into_iter().map(Into::into));
        self
    }

    /// The working directory.
    #[must_use]
    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.spec.dir = Some(dir.into());
        self
    }

    /// Allow arguments built from values to begin with `-`. Off by default, so a value cannot
    /// smuggle in an option.
    #[must_use]
    pub fn values_may_be_options(mut self) -> Self {
        self.spec.options_allowed = true;
        self
    }

    #[doc(hidden)]
    pub fn into_spec(self) -> ProgramSpec {
        self.spec
    }
}

/// The exit status a program uses to say "not ready", like `not_ready(…)`.
pub const EXIT_NOT_READY: i32 = 75;

/// The variables the engine itself sets for a command.
pub(crate) mod env_names {
    pub(crate) const RUN_ID: &str = "JOB_RUN_ID";
    pub(crate) const ATTEMPT: &str = "JOB_ATTEMPT";
    pub(crate) const DRY_RUN: &str = "JOB_DRY_RUN";
    pub(crate) const OUTPUT_FILE: &str = "JOB_OUTPUT_FILE";
}
