//! What the control API's actions take. Each request is built with `new(…)` from its required
//! fields and a setter for each optional one, so adding an optional field later breaks no caller.
//!
//! The scheduler refuses a field a request does not have, so a typo or a field it does not know
//! yet cannot silently change what an action does. In turn a request leaves out every optional
//! field that is not set, so a newer client still talks to an older scheduler while it uses only
//! what that scheduler knows.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `POST /jobs/{name}/runs`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
#[non_exhaustive]
pub struct StartRequest {
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub without_children: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub even_if_queued: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_conditions: bool,
    /// A timeout for this run only, in seconds; 0 for none. Needs `administer`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

impl StartRequest {
    /// A start with this input and every option at its default.
    pub fn new(input: Value) -> Self {
        StartRequest { input, ..StartRequest::default() }
    }

    /// The run's priority in its queue; higher runs first.
    #[must_use]
    pub fn priority(mut self, priority: i16) -> Self {
        self.priority = Some(priority);
        self
    }

    /// The run waits in the queue until this time.
    #[must_use]
    pub fn not_before(mut self, at: DateTime<Utc>) -> Self {
        self.not_before = Some(at);
        self
    }

    /// Ask the job to do a dry run, for a job that supports one.
    #[must_use]
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Start no children when the run ends.
    #[must_use]
    pub fn without_children(mut self) -> Self {
        self.without_children = true;
        self
    }

    /// Create a run even when the same work is already queued.
    #[must_use]
    pub fn even_if_queued(mut self) -> Self {
        self.even_if_queued = true;
        self
    }

    /// Run even when the job's conditions are false. Needs `administer`.
    #[must_use]
    pub fn ignore_conditions(mut self) -> Self {
        self.ignore_conditions = true;
        self
    }

    /// A timeout for this run only, in seconds; 0 for none. Needs `administer`.
    #[must_use]
    pub fn timeout_seconds(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }
}

/// `POST …/pause`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PauseRequest {
    pub note: String,
    /// When the pause ends. Required unless `until_resumed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub until_resumed: bool,
}

impl PauseRequest {
    /// A pause with this note. Give it an end with [`PauseRequest::until`] or
    /// [`PauseRequest::until_resumed`]; the scheduler refuses one with neither.
    pub fn new(note: impl Into<String>) -> Self {
        PauseRequest { note: note.into(), until: None, until_resumed: false }
    }

    /// The pause ends at this time.
    #[must_use]
    pub fn until(mut self, at: DateTime<Utc>) -> Self {
        self.until = Some(at);
        self
    }

    /// The pause lasts until someone resumes.
    #[must_use]
    pub fn until_resumed(mut self) -> Self {
        self.until_resumed = true;
        self
    }
}

/// `POST /runs/{id}/mark-succeeded`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct MarkSucceededRequest {
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub output: Value,
    pub note: String,
}

impl MarkSucceededRequest {
    /// Marks the run with this note and a `null` output.
    pub fn new(note: impl Into<String>) -> Self {
        MarkSucceededRequest { output: Value::Null, note: note.into() }
    }

    /// The output the run is recorded with, which its children receive.
    #[must_use]
    pub fn output(mut self, output: Value) -> Self {
        self.output = output;
        self
    }
}

/// `POST /overrides`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct OverrideRequest {
    pub schedules: Vec<String>,
    pub date: NaiveDate,
    /// `off`, `times` or `window`.
    pub change: String,
    /// For `times`: the day's times; for `window`: its start and end. `HH:MM`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub times: Vec<String>,
    pub note: String,
}

impl OverrideRequest {
    /// An override of these schedules on one date: `change` is `off`, `times` or `window`. Set the
    /// times of `times` and `window` with [`OverrideRequest::times`].
    pub fn new<I, S>(schedules: I, date: NaiveDate, change: impl Into<String>, note: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        OverrideRequest {
            schedules: schedules.into_iter().map(Into::into).collect(),
            date,
            change: change.into(),
            times: Vec::new(),
            note: note.into(),
        }
    }

    /// For `times`: the day's times; for `window`: its start and end. `HH:MM`.
    #[must_use]
    pub fn times<I, S>(mut self, times: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.times = times.into_iter().map(Into::into).collect();
        self
    }
}

/// `POST /backfills`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BackfillRequest {
    pub job: String,
    /// Which of the job's schedules supplies the input; default the job's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

impl BackfillRequest {
    /// A backfill of the job's own schedule over `from` to `to`, both included, that stops at the
    /// first failure.
    pub fn new(job: impl Into<String>, from: NaiveDate, to: NaiveDate) -> Self {
        BackfillRequest { job: job.into(), schedule: None, from, to, continue_on_failure: false }
    }

    /// Which of the job's schedules supplies the input.
    #[must_use]
    pub fn schedule(mut self, schedule: impl Into<String>) -> Self {
        self.schedule = Some(schedule.into());
        self
    }

    /// Go on to the next date after a failure.
    #[must_use]
    pub fn continue_on_failure(mut self) -> Self {
        self.continue_on_failure = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sent<T: Serialize>(request: &T) -> Value {
        serde_json::to_value(request).unwrap()
    }

    #[test]
    fn a_request_sends_only_the_options_that_are_set() {
        let date = NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();
        assert_eq!(sent(&StartRequest::new(json!({ "n": 1 }))), json!({ "input": { "n": 1 } }));
        assert_eq!(sent(&StartRequest::default()), json!({}));
        assert_eq!(
            sent(&StartRequest::new(json!(1)).priority(5).dry_run().timeout_seconds(0)),
            json!({ "input": 1, "priority": 5, "dry_run": true, "timeout_seconds": 0 })
        );
        assert_eq!(sent(&PauseRequest::new("hold").until_resumed()), json!({ "note": "hold", "until_resumed": true }));
        assert_eq!(sent(&MarkSucceededRequest::new("fixed")), json!({ "note": "fixed" }));
        assert_eq!(
            sent(&OverrideRequest::new(["report"], date, "off", "holiday")),
            json!({ "schedules": ["report"], "date": "2030-01-07", "change": "off", "note": "holiday" })
        );
        assert_eq!(
            sent(&BackfillRequest::new("dated", date, date).continue_on_failure()),
            json!({ "job": "dated", "from": "2030-01-07", "to": "2030-01-07", "continue_on_failure": true })
        );
    }

    #[test]
    fn what_a_request_sends_reads_back_the_same() {
        let request = StartRequest::new(json!({ "n": 1 })).even_if_queued().without_children();
        assert_eq!(serde_json::from_value::<StartRequest>(sent(&request)).unwrap(), request);
        let pause = PauseRequest::new("hold");
        assert_eq!(serde_json::from_value::<PauseRequest>(sent(&pause)).unwrap(), pause);
    }
}
