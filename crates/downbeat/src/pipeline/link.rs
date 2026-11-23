//! Links: a child starts when its parents end, and receives what they produced.
//!
//! `.after(parent)` accepts anything that implements [`Parents`] for the child's input type: a job
//! whose output is that type, a tuple of jobs whose outputs make that tuple, or either adapted with
//! `.map(…)`. Nothing is converted silently, so a mismatch fails to compile.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::job::{Job, JobData, Shape, decode};

/// How a link's parent must have ended for the child to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LinkKind {
    /// `.after`: every parent succeeded.
    After,
    /// `.after_failure`: the parent failed, with its attempts used up.
    AfterFailure,
    /// `.after_any`: the parent ended in any way.
    AfterAny,
}

/// Builds the child's input from what its parents handed over, in the order they are listed.
pub(crate) type Adapter = Arc<dyn Fn(Vec<Value>) -> Result<Value, String> + Send + Sync>;

/// A link with its types erased.
#[derive(Clone)]
pub(crate) struct LinkSpec {
    pub(crate) kind: LinkKind,
    pub(crate) parents: Vec<&'static str>,
    pub(crate) adapt: Adapter,
    /// Whether the link adapts its parents' data with `.map`.
    pub(crate) mapped: bool,
}

/// What `.after(…)` accepts for a child whose input is `In`.
pub trait Parents<In>: Sized {
    #[doc(hidden)]
    fn into_link(self) -> (Vec<&'static str>, Adapter, bool);
}

impl<P: Job> Parents<P::Output> for P {
    fn into_link(self) -> (Vec<&'static str>, Adapter, bool) {
        (vec![P::NAME], Arc::new(|mut values| Ok(values.remove(0))), false)
    }
}

macro_rules! tuple_parents {
    ($($name:ident),+) => {
        impl<$($name: Job),+> Parents<($($name::Output,)+)> for ($($name,)+) {
            fn into_link(self) -> (Vec<&'static str>, Adapter, bool) {
                (vec![$($name::NAME),+], Arc::new(|values| Ok(Value::Array(values))), false)
            }
        }
    };
}

tuple_parents!(A, B);
tuple_parents!(A, B, C);
tuple_parents!(A, B, C, D);

/// Parents whose output is adapted with `.map(…)` before the child receives it.
#[derive(Debug, Clone, Copy)]
pub struct Mapped<P, F, From> {
    parents: P,
    map: F,
    from: PhantomData<fn(From)>,
}

impl<P, F, From, To> Parents<To> for Mapped<P, F, From>
where
    P: Parents<From>,
    From: JobData,
    To: JobData,
    F: Fn(From) -> To + Send + Sync + 'static,
{
    fn into_link(self) -> (Vec<&'static str>, Adapter, bool) {
        let (names, inner, _) = self.parents.into_link();
        let map = self.map;
        let adapt: Adapter = Arc::new(move |values| {
            let from: From = decode(inner(values)?)?;
            serde_json::to_value(map(from)).map_err(|error| error.to_string())
        });
        (names, adapt, true)
    }
}

/// `.map(…)` on a job, so its output can be adapted to what a child takes.
pub trait MapExt: Sized {
    type Data: JobData;
    /// Adapt what this hands its children.
    fn map<To, F>(self, map: F) -> Mapped<Self, F, Self::Data>
    where
        F: Fn(Self::Data) -> To,
    {
        Mapped { parents: self, map, from: PhantomData }
    }
}

impl<J: Job> MapExt for J {
    type Data = J::Output;
}

macro_rules! tuple_map {
    ($($name:ident),+) => {
        impl<$($name: Job),+> MapExt for ($($name,)+) {
            type Data = ($($name::Output,)+);
        }
    };
}

tuple_map!(A, B);
tuple_map!(A, B, C);
tuple_map!(A, B, C, D);

/// What an `.after_failure(…)` child receives: which run failed, how and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Failure {
    pub job: String,
    pub run_id: Uuid,
    /// The kind of failure, such as `error`, `timeout` or `crash`.
    pub kind: String,
    pub reason: String,
}

impl Failure {
    /// A failure for testing an `.after_failure(…)` child, with a nil run id, as
    /// [`Run::for_test`](crate::Run::for_test) is a run for testing a job.
    pub fn for_test(job: impl Into<String>, kind: impl Into<String>, reason: impl Into<String>) -> Self {
        Failure { job: job.into(), run_id: Uuid::nil(), kind: kind.into(), reason: reason.into() }
    }
}

impl JobData for Failure {
    fn shape() -> Shape {
        Shape::named("Failure")
    }
}

/// What an `.after_any(…)` child receives: how its parent ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ended", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Ended<T> {
    Succeeded(T),
    Skipped(String),
    Failed(Failure),
    Cancelled(String),
    Expired(String),
}

impl<T: JobData> JobData for Ended<T> {
    fn shape() -> Shape {
        Shape::named(&format!("Ended<{}>", T::shape().type_name))
    }
}
