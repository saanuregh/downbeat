//! Needs: "this job is only worth running if that other job has done its part". A need starts
//! nothing; it is checked before the function is first called.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::job::{Job, JobData, decode, identity_hash};

/// What a run does when a need is not met.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WhenUnmet {
    /// The default: the run ends as skipped.
    Skip,
    /// `.or_wait()`: back to the queue, checked again every `recheck_every`.
    Wait,
    /// `.or_fail()`: the run fails.
    Fail,
}

/// When a need is met.
#[derive(Clone)]
pub(crate) enum NeedTest {
    /// The needed job succeeded for exactly this input (its hash), computed from the child's input.
    DoneFor(NeededInput),
    /// The needed job, which takes no input, succeeded within this long.
    DoneWithin(Duration),
}

/// From the needing job's input, the hash of the input the needed job must have succeeded for.
pub(crate) type NeededInput = Arc<dyn Fn(&Value) -> Result<String, String> + Send + Sync>;

/// A need with its types erased.
#[derive(Clone)]
pub(crate) struct NeedSpec {
    pub(crate) job: &'static str,
    pub(crate) test: NeedTest,
    pub(crate) when_unmet: WhenUnmet,
}

/// A need on another job, for a job whose input is `In`. Made by `job.done_for(…)` or
/// `job.done_within(…)`.
pub struct Need<In> {
    pub(crate) spec: NeedSpec,
    input: PhantomData<fn(&In)>,
}

impl<In> std::fmt::Debug for Need<In> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Need").field("job", &self.spec.job).field("when_unmet", &self.spec.when_unmet).finish()
    }
}

impl<In> Need<In> {
    /// Wait for the need instead of skipping.
    #[must_use]
    pub fn or_wait(mut self) -> Self {
        self.spec.when_unmet = WhenUnmet::Wait;
        self
    }

    /// Fail instead of skipping.
    #[must_use]
    pub fn or_fail(mut self) -> Self {
        self.spec.when_unmet = WhenUnmet::Fail;
        self
    }
}

/// `job.done_for(|input| …)`: a need on a job that takes an input.
pub trait DoneFor: Job {
    /// Met when this job has succeeded for exactly the input the function returns, given the
    /// needing job's own input.
    fn done_for<In, F>(self, needed_input: F) -> Need<In>
    where
        In: JobData,
        F: Fn(&In) -> Self::Input + Send + Sync + 'static,
    {
        let test = NeedTest::DoneFor(Arc::new(move |input: &Value| {
            let input: In = decode(input.clone())?;
            Ok(identity_hash(&needed_input(&input).identity()))
        }));
        Need { spec: NeedSpec { job: Self::NAME, test, when_unmet: WhenUnmet::Skip }, input: PhantomData }
    }
}

impl<J: Job> DoneFor for J {}

/// `job.done_within(duration)`: a need on a job that takes no input.
pub trait DoneWithin: Job<Input = ()> {
    /// Met when this job has succeeded within this long.
    fn done_within<In>(self, within: Duration) -> Need<In> {
        Need {
            spec: NeedSpec { job: Self::NAME, test: NeedTest::DoneWithin(within), when_unmet: WhenUnmet::Skip },
            input: PhantomData,
        }
    }
}

impl<J: Job<Input = ()>> DoneWithin for J {}
