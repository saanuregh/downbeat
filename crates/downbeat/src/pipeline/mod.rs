//! Pipelines: when each job runs, after what, and on what conditions, written as one function per
//! flow that reads from top to bottom.

mod link;
mod need;

use std::marker::PhantomData;
use std::sync::Arc;

use serde_json::Value;

use crate::job::{Job, JobSpec, decode};
use crate::schedule::{Schedule, ScheduleSpec};

pub use link::{Ended, Failure, LinkKind, MapExt, Mapped, Parents};
pub(crate) use link::LinkSpec;
pub use need::{DoneFor, DoneWithin, Need, WhenUnmet};
pub(crate) use need::{NeedSpec, NeedTest};

/// A named group of jobs that belong to one flow. Its function names each job once and says what
/// starts it and on what conditions.
pub struct Pipeline {
    pub(crate) name: String,
    pub(crate) entries: Vec<Entry>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let jobs: Vec<&str> = self.entries.iter().map(|entry| entry.job.name).collect();
        f.debug_struct("Pipeline").field("name", &self.name).field("jobs", &jobs).finish()
    }
}

/// One job in a pipeline, with its triggers and conditions, types erased.
pub(crate) struct Entry {
    pub(crate) job: JobSpec,
    pub(crate) schedules: Vec<ScheduleSpec>,
    pub(crate) link: Option<LinkSpec>,
    pub(crate) if_changed: bool,
    pub(crate) needs: Vec<NeedSpec>,
    pub(crate) conditions: Vec<Condition>,
}

/// An `.only_if` condition with its types erased.
#[derive(Clone)]
pub(crate) struct Condition {
    pub(crate) name: String,
    pub(crate) test: Arc<dyn Fn(&Value) -> Result<bool, String> + Send + Sync>,
}

impl Pipeline {
    pub(crate) fn new(name: &str) -> Self {
        Pipeline { name: name.to_owned(), entries: Vec::new() }
    }

    /// Name a job in this pipeline, then say what starts it and on what conditions.
    pub fn job<J: Job>(&mut self, _job: J) -> JobBuilder<'_, J, NoParents> {
        self.entries.push(Entry {
            job: J::spec(),
            schedules: Vec::new(),
            link: None,
            if_changed: false,
            needs: Vec::new(),
            conditions: Vec::new(),
        });
        let entry = self.entries.last_mut().expect("just pushed");
        JobBuilder { entry, state: PhantomData }
    }
}

/// The job has no `.after` yet.
#[derive(Debug)]
pub struct NoParents;
/// The job has its `.after`; a job has one.
#[derive(Debug)]
pub struct HasParents;

/// Says what starts a job and on what conditions. Made by [`Pipeline::job`].
pub struct JobBuilder<'a, J, P> {
    entry: &'a mut Entry,
    state: PhantomData<(J, P)>,
}

impl<J, P> std::fmt::Debug for JobBuilder<'_, J, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobBuilder").field("job", &self.entry.job.name).finish()
    }
}

impl<'a, J: Job, P> JobBuilder<'a, J, P> {
    /// Start the job at a schedule's times. The schedule must supply the job's input.
    #[must_use]
    pub fn schedule(self, schedule: Schedule<J::Input>) -> Self {
        self.entry.schedules.push(schedule.spec);
        self
    }

    /// Skip a run whose input is the same as the one this job last succeeded on.
    #[must_use]
    pub fn if_changed(self) -> Self {
        self.entry.if_changed = true;
        self
    }

    /// Only run if another job has done its part.
    #[must_use]
    pub fn needs(self, need: Need<J::Input>) -> Self {
        self.entry.needs.push(need.spec);
        self
    }

    /// Only run if this named test of the input holds; otherwise the run is skipped with its name.
    #[must_use]
    pub fn only_if<F>(self, name: &str, test: F) -> Self
    where
        F: Fn(&J::Input) -> bool + Send + Sync + 'static,
    {
        let test = Arc::new(move |input: &Value| decode::<J::Input>(input.clone()).map(|input| test(&input)));
        self.entry.conditions.push(Condition { name: name.to_owned(), test });
        self
    }
}

impl<'a, J: Job> JobBuilder<'a, J, NoParents> {
    /// Start the job when its parents have all succeeded, with what they produced as its input.
    /// Several parents are listed as a tuple.
    pub fn after<Ps: Parents<J::Input>>(self, parents: Ps) -> JobBuilder<'a, J, HasParents> {
        let (parents, adapt, mapped) = parents.into_link();
        self.link(LinkSpec { kind: LinkKind::After, parents, adapt, mapped })
    }

    /// Start the job when its parent has failed, with its attempts used up.
    pub fn after_failure<Parent: Job>(self, _parent: Parent) -> JobBuilder<'a, J, HasParents>
    where
        J: Job<Input = Failure>,
    {
        self.link(LinkSpec { kind: LinkKind::AfterFailure, parents: vec![Parent::NAME], adapt: first_value(), mapped: false })
    }

    /// Start the job when its parent has ended in any way.
    pub fn after_any<Parent: Job>(self, _parent: Parent) -> JobBuilder<'a, J, HasParents>
    where
        J: Job<Input = Ended<Parent::Output>>,
    {
        self.link(LinkSpec { kind: LinkKind::AfterAny, parents: vec![Parent::NAME], adapt: first_value(), mapped: false })
    }

    fn link(self, link: LinkSpec) -> JobBuilder<'a, J, HasParents> {
        self.entry.link = Some(link);
        JobBuilder { entry: self.entry, state: PhantomData }
    }
}

fn first_value() -> link::Adapter {
    Arc::new(|mut values| Ok(values.remove(0)))
}
