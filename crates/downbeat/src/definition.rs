//! `Definition`: the complete description of one product's job system.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::dependencies::Dependencies;
use crate::events::{EventDelivery, FinishedRun};
use crate::pipeline::Pipeline;

pub(crate) type Callback<T> = Arc<dyn Fn(T) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;
pub(crate) type PipelineFn = fn(&mut Pipeline);
pub(crate) type Redact = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// The complete description of one product's job system: its zone, its environments, how to build
/// each dependency, its pipelines and through them every job, and its callbacks.
///
/// It describes; it does not run, and it holds no state. The engine calls the product's
/// describing function in every process it starts, so that function must have no effects of its own.
pub struct Definition {
    pub(crate) zone: String,
    pub(crate) environments: Vec<String>,
    pub(crate) dependencies: Dependencies,
    pub(crate) pipelines: Vec<(String, PipelineFn)>,
    pub(crate) on_finish: Option<Callback<FinishedRun>>,
    pub(crate) on_event: Option<Callback<EventDelivery>>,
    pub(crate) log_link: Option<String>,
    pub(crate) redact: Option<Redact>,
    pub(crate) settings: Settings,
}

impl std::fmt::Debug for Definition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pipelines: Vec<&str> = self.pipelines.iter().map(|(name, _)| name.as_str()).collect();
        f.debug_struct("Definition")
            .field("zone", &self.zone)
            .field("environments", &self.environments)
            .field("pipelines", &pipelines)
            .finish_non_exhaustive()
    }
}

impl Definition {
    /// A definition with no zone. Every definition must set one; there is no implicit UTC.
    pub fn new() -> Self {
        Definition {
            zone: String::new(),
            environments: Vec::new(),
            dependencies: Dependencies::default(),
            pipelines: Vec::new(),
            on_finish: None,
            on_event: None,
            log_link: None,
            redact: None,
            settings: Settings::default(),
        }
    }

    /// The zone schedules are written in, such as `"Asia/Kolkata"`.
    #[must_use]
    pub fn zone(mut self, zone: &str) -> Self {
        zone.clone_into(&mut self.zone);
        self
    }

    /// The environments the product uses. A schedule limited to another name fails `check`.
    #[must_use]
    pub fn environments<const N: usize>(mut self, environments: [&str; N]) -> Self {
        self.environments = environments.iter().map(|name| (*name).to_owned()).collect();
        self
    }

    /// The engine's timings, when the defaults do not fit.
    #[must_use]
    pub fn settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// How to build a value jobs take by reference. Built lazily, once per process.
    pub fn provide<T, F, Fut, E>(&mut self, build: F)
    where
        T: Send + Sync + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: Display,
    {
        self.dependencies.provide(build);
    }

    /// A pipeline: a named group of jobs written in one function.
    pub fn pipeline(&mut self, name: &str, pipeline: fn(&mut Pipeline)) {
        self.pipelines.push((name.to_owned(), pipeline));
    }

    /// Called for every finished run, at least once. An error is retried five minutes later.
    pub fn on_finish<F, Fut, E>(&mut self, callback: F)
    where
        F: Fn(FinishedRun) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Display,
    {
        self.on_finish = Some(callback_of(callback));
    }

    /// Called for every event (a lost worker, a slow run, an overdue job, dropped occurrences), at
    /// least once.
    pub fn on_event<F, Fut, E>(&mut self, callback: F)
    where
        F: Fn(EventDelivery) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Display,
    {
        self.on_event = Some(callback_of(callback));
    }

    /// A link to the product's log system for each run, with `{run_id}` replaced.
    pub fn log_link(&mut self, template: &str) {
        self.log_link = Some(template.to_owned());
    }

    /// Runs over a failed command's standard error before its tail is stored.
    pub fn redact<F>(&mut self, redact: F)
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        self.redact = Some(Arc::new(redact));
    }
}

impl Default for Definition {
    fn default() -> Self {
        Definition::new()
    }
}

fn callback_of<T, F, Fut, E>(callback: F) -> Callback<T>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Display,
{
    Arc::new(move |value| {
        let called = callback(value);
        Box::pin(async move { called.await.map_err(|error| error.to_string()) })
    })
}

/// The engine's timings. The defaults fit a platform that allows 30 seconds between SIGTERM and
/// SIGKILL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// How often a worker renews the leases of its runs.
    pub heartbeat: Duration,
    /// How long a run stays owned without a renewal.
    pub lease: Duration,
    /// How long a function has to stop after it is asked to, before its process is killed.
    pub kill_grace: Duration,
    /// On shutdown, how long a worker waits for functions to end on their own.
    pub drain: Duration,
    /// The time the platform allows between SIGTERM and SIGKILL.
    pub stop_within: Duration,
    /// How often schedulers and workers look for work.
    pub poll: Duration,
    /// A reused child process is replaced after this many runs.
    pub recycle_child_after: u32,
    /// A heartbeat and lease for particular queues, when theirs differ: a queue of short jobs can
    /// use a short lease, so a lost worker's runs come back sooner.
    pub queues: BTreeMap<String, QueueTimings>,
}

/// How often a queue's runs are renewed, and how long they stay owned without a renewal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueTimings {
    pub heartbeat: Duration,
    pub lease: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            heartbeat: Duration::from_secs(15),
            lease: Duration::from_secs(180),
            kill_grace: Duration::from_secs(10),
            drain: Duration::from_secs(10),
            stop_within: Duration::from_secs(30),
            poll: Duration::from_secs(1),
            recycle_child_after: 500,
            queues: BTreeMap::new(),
        }
    }
}

impl Settings {
    /// Shutdown keeps this long to release runs back to the queue.
    pub(crate) const RELEASE_TIME: Duration = Duration::from_secs(5);

    /// The heartbeat and lease of a queue's runs.
    pub(crate) fn timings(&self, queue: &str) -> QueueTimings {
        self.queues.get(queue).copied().unwrap_or(QueueTimings { heartbeat: self.heartbeat, lease: self.lease })
    }

    /// The reaper takes a run back only after its lease has been expired this long, so the old
    /// worker has killed its copy by then.
    pub(crate) fn reaper_margin(&self) -> Duration {
        self.kill_grace + Duration::from_secs(10)
    }

    /// Why a lease could not be kept with these settings, if it could not.
    pub(crate) fn timing_problems(&self) -> Vec<String> {
        let default = ("every queue".to_owned(), QueueTimings { heartbeat: self.heartbeat, lease: self.lease });
        std::iter::once(default)
            .chain(self.queues.iter().map(|(queue, timings)| (format!("queue {queue}"), *timings)))
            .filter(|(_, timings)| timings.lease < timings.heartbeat * 2)
            .map(|(what, timings)| {
                format!(
                    "the lease of {what} ({:?}) must be at least two heartbeats ({:?})",
                    timings.lease, timings.heartbeat
                )
            })
            .collect()
    }

    /// Why a worker cannot start with these settings, if it cannot.
    pub(crate) fn shutdown_problem(&self) -> Option<String> {
        let needed = self.drain + self.kill_grace + Self::RELEASE_TIME;
        (needed > self.stop_within).then(|| {
            format!(
                "drain ({:?}) + kill grace ({:?}) + release ({:?}) do not fit in stop_within ({:?})",
                self.drain,
                self.kill_grace,
                Self::RELEASE_TIME,
                self.stop_within
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_queue_may_have_its_own_heartbeat_and_lease() {
        let quick = QueueTimings { heartbeat: Duration::from_secs(5), lease: Duration::from_secs(30) };
        let settings = Settings { queues: [("quick".to_owned(), quick)].into(), ..Settings::default() };
        assert_eq!(settings.timings("quick"), quick);
        assert_eq!(settings.timings("default").lease, Duration::from_secs(180));
        assert!(settings.timing_problems().is_empty());
    }

    #[test]
    fn a_lease_shorter_than_two_heartbeats_is_a_problem() {
        let tight = QueueTimings { heartbeat: Duration::from_secs(20), lease: Duration::from_secs(30) };
        let settings = Settings { queues: [("tight".to_owned(), tight)].into(), ..Settings::default() };
        assert_eq!(settings.timing_problems().len(), 1);
        assert!(settings.timing_problems()[0].contains("queue tight"));
    }
}
