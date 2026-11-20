//! Policies: how a run of a job executes. Written only in `#[job(…)]`.

use std::ops::RangeInclusive;
use std::time::Duration;

use crate::units::{Bytes, format_duration};

/// Every policy a job can set, with the defaults from the design.
///
/// `#[job(…)]` builds one of these from its arguments, starting from a preset when one is named.
/// A product's own presets are constants of this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// The lane the run waits in. A worker serves chosen queues.
    pub queue: &'static str,
    /// Higher is picked first within a queue.
    pub priority: i16,
    /// The function is cancelled, then killed, after this long.
    pub timeout: Duration,
    /// How many failed calls a run may have before it fails for good.
    pub attempts: u16,
    /// The delay before the next attempt.
    pub backoff: Backoff,
    /// Which failures deserve another attempt.
    pub retry_on: RetryOnSet,
    /// How often a waiting run is looked at again.
    pub recheck_every: Duration,
    /// A run still waiting this long after it was created fails.
    pub give_up_after: Duration,
    /// What a run takes turns on, beyond "the same work".
    pub turn: Turn,
    /// A new child process for every run.
    pub fresh_process: bool,
    /// The run is killed above this resident memory.
    pub max_memory: Option<Bytes>,
    /// How long finished runs stay in the history.
    pub keep_runs_for: Duration,
    /// Also keep only this many of the newest runs that did not fail.
    pub keep_runs_at_most: Option<u32>,
    /// Tell the product, once, when the job has not succeeded for this long.
    pub expect_success_every: Option<Duration>,
    /// The job honours `run.is_dry_run()`; only such a job accepts a dry run.
    pub supports_dry_run: bool,
}

impl Policy {
    /// The defaults every job starts from.
    pub const DEFAULT: Policy = Policy {
        queue: "default",
        priority: 0,
        timeout: Duration::from_mins(5),
        attempts: 1,
        backoff: Backoff { first: Duration::from_secs(30), max: Duration::from_mins(10) },
        retry_on: RetryOnSet::ERROR,
        recheck_every: Duration::from_mins(1),
        give_up_after: Duration::from_hours(24),
        turn: Turn::SameWork,
        fresh_process: false,
        max_memory: None,
        keep_runs_for: Duration::from_hours(30 * 24),
        keep_runs_at_most: None,
        expect_success_every: None,
        supports_dry_run: false,
    };

    /// The delay before attempt `next_attempt` (2 for the first retry): doubling from the first
    /// delay up to the cap.
    pub(crate) fn backoff_before(&self, next_attempt: u32) -> Duration {
        let doublings = next_attempt.saturating_sub(2).min(30);
        self.backoff.first.saturating_mul(1 << doublings).min(self.backoff.max)
    }

    /// Whether a failure of this kind gets another attempt, given the failures so far.
    pub(crate) fn retries(&self, kind: RetryOn, failures_so_far: u16) -> bool {
        self.retry_on.contains(kind) && failures_so_far < self.attempts
    }

    /// The policies that differ from the defaults, in words, for `check` and the API.
    pub(crate) fn describe(&self) -> Vec<String> {
        let default = &Policy::DEFAULT;
        let mut lines = Vec::new();
        if self.queue != default.queue {
            lines.push(format!("queue {}", self.queue));
        }
        if self.priority != default.priority {
            lines.push(format!("priority {}", self.priority));
        }
        if self.timeout != default.timeout {
            lines.push(format!("timeout {}", format_duration(self.timeout)));
        }
        if self.attempts != default.attempts || self.backoff != default.backoff {
            lines.push(format!(
                "attempts {}, backoff {}..={}",
                self.attempts,
                format_duration(self.backoff.first),
                format_duration(self.backoff.max)
            ));
        }
        if self.retry_on != default.retry_on {
            lines.push(format!("retry on {}", self.retry_on.describe()));
        }
        if self.recheck_every != default.recheck_every {
            lines.push(format!("recheck every {}", format_duration(self.recheck_every)));
        }
        if self.give_up_after != default.give_up_after {
            lines.push(format!("give up after {}", format_duration(self.give_up_after)));
        }
        match self.turn {
            Turn::SameWork => {}
            Turn::OneAtATime => lines.push("one at a time".to_owned()),
            Turn::OnePer(field) => lines.push(format!("one per {field}")),
        }
        if self.fresh_process {
            lines.push("fresh process".to_owned());
        }
        if let Some(Bytes(bytes)) = self.max_memory {
            lines.push(format!("max memory {} MiB", bytes >> 20));
        }
        if self.keep_runs_for != default.keep_runs_for {
            lines.push(format!("keep runs for {}", format_duration(self.keep_runs_for)));
        }
        if let Some(count) = self.keep_runs_at_most {
            lines.push(format!("keep at most {count} runs"));
        }
        if let Some(every) = self.expect_success_every {
            lines.push(format!("expect success every {}", format_duration(every)));
        }
        if self.supports_dry_run {
            lines.push("supports dry run".to_owned());
        }
        lines
    }
}

/// The delay before another attempt: doubling from `first` up to `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl From<Duration> for Backoff {
    /// A single duration is a constant delay.
    fn from(delay: Duration) -> Self {
        Backoff { first: delay, max: delay }
    }
}

impl From<RangeInclusive<Duration>> for Backoff {
    fn from(range: RangeInclusive<Duration>) -> Self {
        Backoff { first: *range.start(), max: *range.end() }
    }
}

/// A kind of failure that may deserve another attempt. A panic never does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOn {
    /// The function returned an error (or a command exited with a failure status).
    Error,
    /// The function ran past its timeout.
    Timeout,
    /// The child process crashed.
    Crash,
    /// The child process went over its memory limit.
    Memory,
}

/// A set of [`RetryOn`] kinds, written `retry_on = [Error, Timeout]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryOnSet(u8);

impl RetryOnSet {
    pub const ERROR: RetryOnSet = RetryOnSet::of(&[RetryOn::Error]);

    pub const fn of(kinds: &[RetryOn]) -> Self {
        let mut bits = 0;
        let mut index = 0;
        while index < kinds.len() {
            bits |= 1 << kinds[index] as u8;
            index += 1;
        }
        RetryOnSet(bits)
    }

    pub const fn contains(self, kind: RetryOn) -> bool {
        self.0 & (1 << kind as u8) != 0
    }

    fn describe(self) -> String {
        [RetryOn::Error, RetryOn::Timeout, RetryOn::Crash, RetryOn::Memory]
            .into_iter()
            .filter(|kind| self.contains(*kind))
            .map(|kind| format!("{kind:?}").to_lowercase())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// What a run takes turns on, so two runs never execute at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Turn {
    /// The default: runs for the same work (same input) take turns.
    SameWork,
    /// `one_at_a_time`: no two runs of the job execute at once.
    OneAtATime,
    /// `one_per = field`: no two runs with the same value of that input field execute at once.
    OnePer(&'static str),
}

/// Ready-made policies for kinds of job that recur. A job starts from one and sets what differs:
/// `#[job(preset = heavy, timeout = 3.hours())]`.
#[allow(non_upper_case_globals)]
pub mod presets {
    use super::{Policy, RetryOn, RetryOnSet};
    use std::time::Duration;

    /// A job on a short interval: a 1-minute timeout and only the newest 20 runs kept.
    pub const frequent: Policy =
        Policy { timeout: Duration::from_mins(1), keep_runs_at_most: Some(20), ..Policy::DEFAULT };

    /// A long, CPU- or memory-heavy job: queue `heavy`, a 2-hour timeout, a fresh process per run.
    pub const heavy: Policy =
        Policy { queue: "heavy", timeout: Duration::from_hours(2), fresh_process: true, ..Policy::DEFAULT };

    /// A job calling an outside service that is sometimes slow or down.
    pub const external: Policy = Policy {
        timeout: Duration::from_mins(10),
        attempts: 3,
        retry_on: RetryOnSet::of(&[RetryOn::Error, RetryOn::Timeout]),
        ..Policy::DEFAULT
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::units::DurationExt;

    #[test]
    fn backoff_doubles_up_to_its_cap() {
        let policy = Policy { backoff: (30.seconds()..=2.minutes()).into(), ..Policy::DEFAULT };
        assert_eq!(policy.backoff_before(2), 30.seconds());
        assert_eq!(policy.backoff_before(3), 60.seconds());
        assert_eq!(policy.backoff_before(4), 2.minutes());
        assert_eq!(policy.backoff_before(9), 2.minutes());
    }

    #[test]
    fn retries_only_listed_kinds_while_attempts_remain() {
        let policy = Policy { attempts: 3, ..presets::external };
        assert!(policy.retries(RetryOn::Timeout, 1));
        assert!(!policy.retries(RetryOn::Crash, 1));
        assert!(!policy.retries(RetryOn::Error, 3));
    }

    #[test]
    fn describes_only_what_differs() {
        assert!(Policy::DEFAULT.describe().is_empty());
        assert_eq!(presets::heavy.describe(), vec!["queue heavy", "timeout 2h", "fresh process"]);
    }
}
