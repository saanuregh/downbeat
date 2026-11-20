//! Schedules: values that say when a job starts, built from [`daily_at`] or [`every`] and passed
//! to `.schedule(…)` in a pipeline.

pub(crate) mod times;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta, Utc, Weekday};
use futures::future::BoxFuture;
use serde_json::Value;

use crate::dependencies::{Dependencies, Dependency};
use crate::job::JobData;

pub(crate) use times::{CompiledSchedule, DayPlan, Times};

/// Fire every day at these wall-clock times, such as `daily_at(["07:33", "08:01"])`.
pub fn daily_at<const N: usize>(times: [&str; N]) -> Schedule<()> {
    Schedule::new(ScheduleForm::DailyAt(times.iter().map(|time| (*time).to_owned()).collect()))
}

/// Fire every interval, counted from local midnight, or from the start of a window when it has
/// one. One second is the minimum.
pub fn every(interval: Duration) -> Schedule<()> {
    Schedule::new(ScheduleForm::Every(interval))
}

/// A schedule whose runs take an input of type `I`. `I` is `()` until `.input(…)` is given.
pub struct Schedule<I> {
    pub(crate) spec: ScheduleSpec,
    input: PhantomData<fn() -> I>,
}

impl<I> std::fmt::Debug for Schedule<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schedule").field("form", &self.spec.form).field("name", &self.spec.name).finish_non_exhaustive()
    }
}

/// One moment a schedule fires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Occurrence {
    pub(crate) at: DateTime<Utc>,
    pub(crate) local: NaiveDateTime,
    pub(crate) date: NaiveDate,
}

impl Occurrence {
    /// The local date the occurrence belongs to. For a window that crosses midnight, the date it
    /// started on.
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    /// The local date and time, in the schedule's zone.
    pub fn local(&self) -> NaiveDateTime {
        self.local
    }

    /// The instant, in UTC.
    pub fn at(&self) -> DateTime<Utc> {
        self.at
    }
}

/// What a day rule says about one date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Day {
    /// The schedule's normal times.
    Normal,
    /// No occurrences, with the reason shown wherever the gap is shown.
    Off(String),
    /// Every time (or the window) moves by this much. No effect on an all-day `every`.
    Shifted(TimeDelta),
}

pub(crate) type CalendarCall =
    Arc<dyn Fn(NaiveDate, Arc<Dependencies>) -> BoxFuture<'static, Result<Day, String>> + Send + Sync>;

/// A day rule with its types erased.
#[derive(Clone)]
pub(crate) struct CalendarRule {
    pub(crate) name: String,
    pub(crate) dependency: Dependency,
    pub(crate) call: CalendarCall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScheduleForm {
    DailyAt(Vec<String>),
    Every(Duration),
}

pub(crate) type InputFn = Arc<dyn Fn(&Occurrence) -> Result<Value, String> + Send + Sync>;

/// A schedule with its types erased, as written; checked when the definition is built.
#[derive(Clone)]
pub(crate) struct ScheduleSpec {
    pub(crate) name: Option<String>,
    pub(crate) form: ScheduleForm,
    pub(crate) window: Option<(String, String)>,
    pub(crate) weekdays: Option<Vec<Weekday>>,
    pub(crate) month_days: Option<Vec<u32>>,
    pub(crate) calendar: Option<CalendarRule>,
    pub(crate) input: Option<InputFn>,
    pub(crate) zone: Option<String>,
    pub(crate) environments: Option<Vec<String>>,
    pub(crate) expire_after: Option<Duration>,
}

impl Schedule<()> {
    fn new(form: ScheduleForm) -> Self {
        Schedule {
            spec: ScheduleSpec {
                name: None,
                form,
                window: None,
                weekdays: None,
                month_days: None,
                calendar: None,
                input: None,
                zone: None,
                environments: None,
                expire_after: None,
            },
            input: PhantomData,
        }
    }

    /// The input of each run, computed from the occurrence. Required when the job takes an input.
    pub fn input<T, F>(self, input: F) -> Schedule<T>
    where
        T: JobData,
        F: Fn(&Occurrence) -> T + Send + Sync + 'static,
    {
        let mut spec = self.spec;
        spec.input = Some(Arc::new(move |occurrence| {
            serde_json::to_value(input(occurrence)).map_err(|error| error.to_string())
        }));
        Schedule { spec, input: PhantomData }
    }
}

impl<I> Schedule<I> {
    /// For `every`: fire from the first time to the second, inclusive. A window that crosses
    /// midnight belongs to the date it started on.
    #[must_use]
    pub fn between(mut self, start: &str, end: &str) -> Self {
        self.spec.window = Some((start.to_owned(), end.to_owned()));
        self
    }

    /// Only on these weekdays.
    #[must_use]
    pub fn on_weekdays<const N: usize>(mut self, weekdays: [Weekday; N]) -> Self {
        self.spec.weekdays = Some(weekdays.to_vec());
        self
    }

    /// Only on these days of the month.
    #[must_use]
    pub fn on_days_of_month<const N: usize>(mut self, days: [u32; N]) -> Self {
        self.spec.month_days = Some(days.to_vec());
        self
    }

    /// A day rule that can turn a day off or move its times. It runs in the scheduler, so it must
    /// be quick and read only what the app has loaded, never the clock or the network.
    #[must_use]
    pub fn by_calendar<C, R>(mut self, rule: R) -> Self
    where
        C: Send + Sync + 'static,
        R: Fn(NaiveDate, &C) -> Day + Copy + Send + Sync + 'static,
    {
        let name = rule_name(std::any::type_name::<R>());
        let call: CalendarCall = Arc::new(move |date, dependencies: Arc<Dependencies>| {
            Box::pin(async move {
                let calendar = dependencies.get::<C>().await.map_err(|error| format!("{error:?}"))?;
                Ok(rule(date, &calendar))
            })
        });
        self.spec.calendar = Some(CalendarRule { name, dependency: Dependency::of::<C>(), call });
        self
    }

    /// Use this zone for this schedule only, instead of the definition's.
    #[must_use]
    pub fn in_zone(mut self, zone: &str) -> Self {
        self.spec.zone = Some(zone.to_owned());
        self
    }

    /// The schedule exists only in these environments.
    #[must_use]
    pub fn in_environments<const N: usize>(mut self, environments: [&str; N]) -> Self {
        self.spec.environments = Some(environments.iter().map(|name| (*name).to_owned()).collect());
        self
    }

    /// An occurrence is only worth running within this long of its time. Default: one interval for
    /// `every`, no limit for `daily_at`.
    #[must_use]
    pub fn expire_after(mut self, limit: Duration) -> Self {
        self.spec.expire_after = Some(limit);
        self
    }

    /// The schedule's name, needed from a job's second schedule on. Default: the job's name.
    #[must_use]
    pub fn named(mut self, name: &str) -> Self {
        self.spec.name = Some(name.to_owned());
        self
    }
}

/// A function's own name, from the path `type_name` gives for its type.
fn rule_name(type_name: &str) -> String {
    type_name.rsplit("::").next().unwrap_or(type_name).to_owned()
}
