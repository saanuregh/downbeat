//! A test kit: check a definition's schedules against any date or instant, with no database and
//! no waiting. The clock is whatever the test passes in.
//!
//! ```ignore
//! let schedules = downbeat::testing::Schedules::of(definition(), Some("production")).unwrap();
//! let monday = NaiveDate::from_ymd_opt(2026, 11, 23).unwrap();
//! let times = schedules.on("fetch_prices", monday).await.unwrap();
//! assert_eq!(times.first().unwrap().local().time(), NaiveTime::from_hms_opt(9, 15, 0).unwrap());
//! ```

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;

use crate::definition::Definition;
use crate::registry::Registry;
use crate::schedule::Occurrence;
use crate::scheduler::plan;

/// A definition's schedules, ready to be asked about any date or instant.
#[derive(Debug)]
pub struct Schedules {
    registry: Registry,
}

impl Schedules {
    /// Builds the definition as a role would in `environment` (`None`: every schedule). Fails with
    /// the problems `check` would report.
    pub fn of(definition: Definition, environment: Option<&str>) -> Result<Schedules, Vec<String>> {
        Registry::build(definition, environment)
            .map(|registry| Schedules { registry })
            .map_err(|problems| problems.iter().map(ToString::to_string).collect())
    }

    /// The schedule's occurrences on a local date, after its day rule. Dated overrides live in the
    /// database and are not applied.
    pub async fn on(&self, schedule: &str, date: NaiveDate) -> Result<Vec<Occurrence>, String> {
        let compiled = self.schedule(schedule)?;
        let (day, _) = plan::plan_for(&self.registry, compiled, &[], date).await?;
        Ok(compiled.occurrences_on(date, &day))
    }

    /// The schedule's first occurrence after an instant.
    pub async fn next_after(&self, schedule: &str, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, String> {
        plan::next_after(&self.registry, self.schedule(schedule)?, &[], after).await
    }

    /// Every occurrence after one instant and at or before another: what a scheduler that was down
    /// in between would find missed.
    pub async fn between(
        &self,
        schedule: &str,
        after: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<Occurrence>, String> {
        plan::occurrences_between(&self.registry, self.schedule(schedule)?, &[], after, until).await
    }

    /// The input a run created for this occurrence would receive, as JSON.
    pub fn input_for(&self, schedule: &str, occurrence: &Occurrence) -> Result<Value, String> {
        let compiled = self.schedule(schedule)?;
        match &compiled.input {
            Some(input) => input(occurrence),
            None => Ok(Value::Null),
        }
    }

    fn schedule(&self, name: &str) -> Result<&crate::schedule::CompiledSchedule, String> {
        self.registry.schedules.get(name).ok_or_else(|| format!("there is no schedule named {name}"))
    }
}
