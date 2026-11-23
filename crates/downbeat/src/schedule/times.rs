//! Turning a schedule and a date into occurrences. Pure functions of their arguments, so every
//! scheduler computes the same times.

use std::time::Duration;

use chrono::{DateTime, Datelike, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Utc, Weekday};
use chrono_tz::Tz;

use super::{CalendarRule, InputFn, Occurrence};

/// A schedule's times, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Times {
    DailyAt(Vec<NaiveTime>),
    Every { interval: Duration, window: Option<(NaiveTime, NaiveTime)> },
}

/// A schedule ready to fire.
#[derive(Clone)]
pub(crate) struct CompiledSchedule {
    pub(crate) name: String,
    pub(crate) job: &'static str,
    pub(crate) times: Times,
    pub(crate) zone: Tz,
    pub(crate) weekdays: Option<Vec<Weekday>>,
    pub(crate) month_days: Option<Vec<u32>>,
    pub(crate) calendar: Option<CalendarRule>,
    pub(crate) input: Option<InputFn>,
    pub(crate) expire_after: Option<Duration>,
}

/// What happens on one date, after dated overrides and the day rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DayPlan {
    Normal,
    Off(String),
    Shifted(TimeDelta),
    /// A dated override's times, for a `daily_at` schedule.
    Times(Vec<NaiveTime>),
    /// A dated override's window, for an `every(…).between(…)` schedule.
    Window(NaiveTime, NaiveTime),
}

impl CompiledSchedule {
    /// How long an occurrence stays worth running: `expire_after`, or one interval for `every`.
    pub(crate) fn expiry(&self) -> Option<Duration> {
        self.expire_after.or(match self.times {
            Times::Every { interval, .. } => Some(interval),
            Times::DailyAt(_) => None,
        })
    }

    /// The schedule's times on one local date, in order.
    pub(crate) fn occurrences_on(&self, date: NaiveDate, plan: &DayPlan) -> Vec<Occurrence> {
        let normal = matches!(plan, DayPlan::Normal | DayPlan::Shifted(_));
        if normal && !self.runs_on(date) {
            return Vec::new();
        }
        let shift = match plan {
            DayPlan::Shifted(by) => *by,
            _ => TimeDelta::zero(),
        };
        let instants: Vec<DateTime<Utc>> = match (&self.times, plan) {
            (_, DayPlan::Off(_)) => Vec::new(),
            (Times::DailyAt(_), DayPlan::Times(times)) => {
                times.iter().filter_map(|time| self.instant(date, *time, TimeDelta::zero())).collect()
            }
            // A day rule's shift too large to apply skips the date rather than part of it.
            (Times::DailyAt(times), _) => {
                times.iter().map(|time| self.instant(date, *time, shift)).collect::<Option<_>>().unwrap_or_default()
            }
            (Times::Every { interval, window: Some(_) }, DayPlan::Window(start, end)) => {
                self.window(date, *interval, *start, *end, TimeDelta::zero())
            }
            (Times::Every { interval, window: Some((start, end)) }, _) => {
                self.window(date, *interval, *start, *end, shift)
            }
            (Times::Every { interval, window: None }, _) => self.all_day(date, *interval),
        };
        let mut occurrences: Vec<Occurrence> = instants
            .into_iter()
            .map(|at| Occurrence { at, local: at.with_timezone(&self.zone).naive_local(), date })
            .collect();
        occurrences.sort_by_key(|occurrence| occurrence.at);
        occurrences.dedup_by_key(|occurrence| occurrence.at);
        occurrences
    }

    fn runs_on(&self, date: NaiveDate) -> bool {
        self.weekdays.as_ref().is_none_or(|days| days.contains(&date.weekday()))
            && self.month_days.as_ref().is_none_or(|days| days.contains(&date.day()))
    }

    /// The instant of a local time on a date, moved by `shift`; `None` when the shift moves it out
    /// of the calendar's range.
    fn instant(&self, date: NaiveDate, time: NaiveTime, shift: TimeDelta) -> Option<DateTime<Utc>> {
        date.and_time(time).checked_add_signed(shift).map(|local| resolve(self.zone, local))
    }

    fn window(
        &self,
        date: NaiveDate,
        interval: Duration,
        start: NaiveTime,
        end: NaiveTime,
        shift: TimeDelta,
    ) -> Vec<DateTime<Utc>> {
        let end_date = if end < start { date.succ_opt().unwrap_or(date) } else { date };
        let (Some(first), Some(last)) = (self.instant(date, start, shift), self.instant(end_date, end, shift)) else {
            return Vec::new();
        };
        steps(first, interval).take_while(|at| *at <= last).collect()
    }

    fn all_day(&self, date: NaiveDate, interval: Duration) -> Vec<DateTime<Utc>> {
        let midnight = resolve(self.zone, date.and_time(NaiveTime::MIN));
        let next = resolve(self.zone, date.succ_opt().unwrap_or(date).and_time(NaiveTime::MIN));
        steps(midnight, interval).take_while(|at| *at < next).collect()
    }
}

fn steps(first: DateTime<Utc>, interval: Duration) -> impl Iterator<Item = DateTime<Utc>> {
    let step = TimeDelta::from_std(interval.max(Duration::from_secs(1))).unwrap_or(TimeDelta::MAX);
    std::iter::successors(Some(first), move |at| at.checked_add_signed(step))
}

/// A local wall-clock time as an instant. Where the time does not exist (clocks moved forward)
/// the next valid instant; where it happens twice (clocks moved back) the first.
pub(crate) fn resolve(zone: Tz, local: NaiveDateTime) -> DateTime<Utc> {
    let mut probe = local;
    for _ in 0..=24 * 60 {
        match zone.from_local_datetime(&probe) {
            LocalResult::Single(at) | LocalResult::Ambiguous(at, _) => return at.with_timezone(&Utc),
            LocalResult::None => probe += TimeDelta::minutes(1),
        }
    }
    Utc.from_utc_datetime(&local)
}

/// The local dates from the one containing `from` to the one containing `to`, inclusive, with a
/// day before the first, since a window that crosses midnight belongs to the date it started on.
pub(crate) fn dates_covering(zone: Tz, from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<NaiveDate> {
    let first = from.with_timezone(&zone).date_naive().pred_opt().unwrap_or(NaiveDate::MIN);
    let last = to.with_timezone(&zone).date_naive();
    first.iter_days().take_while(|date| *date <= last).collect()
}

/// Parses a wall-clock time written as `HH:MM` or `HH:MM:SS`.
pub(crate) fn parse_time(text: &str) -> Result<NaiveTime, String> {
    NaiveTime::parse_from_str(text, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M:%S"))
        .map_err(|_| format!("'{text}' is not a time; write it as HH:MM"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::units::DurationExt;

    fn schedule(times: Times, zone: Tz) -> CompiledSchedule {
        CompiledSchedule {
            name: "test".into(),
            job: "test",
            times,
            zone,
            weekdays: None,
            month_days: None,
            calendar: None,
            input: None,
            expire_after: None,
        }
    }

    fn date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    fn local_times(occurrences: &[Occurrence]) -> Vec<String> {
        occurrences.iter().map(|o| o.local.format("%m-%d %H:%M").to_string()).collect()
    }

    #[test]
    fn daily_times_are_wall_clock_in_the_zone() {
        let s = schedule(Times::DailyAt(vec![parse_time("07:33").unwrap()]), chrono_tz::Asia::Kolkata);
        let occurrences = s.occurrences_on(date("2026-11-20"), &DayPlan::Normal);
        assert_eq!(occurrences[0].at.to_rfc3339(), "2026-11-20T02:03:00+00:00");
    }

    #[test]
    fn a_window_fires_from_its_start_up_to_and_including_its_end() {
        let window = Some((parse_time("18:45").unwrap(), parse_time("20:15").unwrap()));
        let s = schedule(Times::Every { interval: 30.minutes(), window }, chrono_tz::UTC);
        let occurrences = s.occurrences_on(date("2026-11-20"), &DayPlan::Normal);
        assert_eq!(local_times(&occurrences), ["11-20 18:45", "11-20 19:15", "11-20 19:45", "11-20 20:15"]);
    }

    #[test]
    fn a_window_across_midnight_belongs_to_the_date_it_started() {
        let window = Some((parse_time("23:00").unwrap(), parse_time("01:00").unwrap()));
        let s = schedule(Times::Every { interval: 1.hours(), window }, chrono_tz::UTC);
        let occurrences = s.occurrences_on(date("2026-11-20"), &DayPlan::Normal);
        assert_eq!(local_times(&occurrences), ["11-20 23:00", "11-21 00:00", "11-21 01:00"]);
        assert!(occurrences.iter().all(|o| o.date == date("2026-11-20")));
    }

    #[test]
    fn day_rules_turn_off_and_shift() {
        let s = schedule(Times::DailyAt(vec![parse_time("17:00").unwrap()]), chrono_tz::UTC);
        assert!(s.occurrences_on(date("2026-11-20"), &DayPlan::Off("holiday".into())).is_empty());
        let shifted = s.occurrences_on(date("2026-11-20"), &DayPlan::Shifted(TimeDelta::hours(-1)));
        assert_eq!(local_times(&shifted), ["11-20 16:00"]);
    }

    #[test]
    fn overrides_replace_times_even_on_a_filtered_weekday() {
        let mut s = schedule(Times::DailyAt(vec![parse_time("09:00").unwrap()]), chrono_tz::UTC);
        s.weekdays = Some(vec![Weekday::Mon]);
        // 2026-11-21 is a Saturday.
        assert!(s.occurrences_on(date("2026-11-21"), &DayPlan::Normal).is_empty());
        let replaced = s.occurrences_on(date("2026-11-21"), &DayPlan::Times(vec![parse_time("10:00").unwrap()]));
        assert_eq!(local_times(&replaced), ["11-21 10:00"]);
    }

    #[test]
    fn a_time_skipped_by_a_clock_change_fires_at_the_next_valid_instant() {
        // Clocks in New York moved from 02:00 to 03:00 on 2026-03-08.
        let s = schedule(Times::DailyAt(vec![parse_time("02:30").unwrap()]), chrono_tz::America::New_York);
        let occurrences = s.occurrences_on(date("2026-03-08"), &DayPlan::Normal);
        assert_eq!(local_times(&occurrences), ["03-08 03:00"]);
    }

    #[test]
    fn every_counts_from_local_midnight() {
        let s = schedule(Times::Every { interval: 6.hours(), window: None }, chrono_tz::Asia::Kolkata);
        let occurrences = s.occurrences_on(date("2026-11-20"), &DayPlan::Normal);
        assert_eq!(local_times(&occurrences), ["11-20 00:00", "11-20 06:00", "11-20 12:00", "11-20 18:00"]);
    }

    #[test]
    fn a_shift_has_no_effect_on_every_with_no_window() {
        let s = schedule(Times::Every { interval: 6.hours(), window: None }, chrono_tz::UTC);
        let normal = s.occurrences_on(date("2026-11-20"), &DayPlan::Normal);
        let shifted = s.occurrences_on(date("2026-11-20"), &DayPlan::Shifted(TimeDelta::minutes(-90)));
        assert_eq!(shifted, normal);
    }

    #[test]
    fn a_time_that_happens_twice_resolves_to_the_earlier_instant() {
        // Clocks in New York moved back from 02:00 EDT to 01:00 EST on 2026-11-01, so 01:30 happened
        // at 05:30 UTC and again at 06:30 UTC.
        let s = schedule(Times::DailyAt(vec![parse_time("01:30").unwrap()]), chrono_tz::America::New_York);
        let occurrences = s.occurrences_on(date("2026-11-01"), &DayPlan::Normal);
        assert_eq!(occurrences.len(), 1);
        assert_eq!(occurrences[0].at.to_rfc3339(), "2026-11-01T05:30:00+00:00");
    }

    // Spec 8.2: the filters apply to a shifted plan as they do to a normal one.
    #[test]
    fn weekday_and_day_of_month_filters_apply_to_a_shifted_plan() {
        let shift = DayPlan::Shifted(TimeDelta::minutes(30));
        let mut by_weekday = schedule(Times::DailyAt(vec![parse_time("09:00").unwrap()]), chrono_tz::UTC);
        by_weekday.weekdays = Some(vec![Weekday::Mon]);
        // 2026-11-21 is a Saturday, 2026-11-23 a Monday.
        assert!(by_weekday.occurrences_on(date("2026-11-21"), &shift).is_empty());
        assert_eq!(local_times(&by_weekday.occurrences_on(date("2026-11-23"), &shift)), ["11-23 09:30"]);

        let window = Some((parse_time("09:00").unwrap(), parse_time("10:00").unwrap()));
        let mut by_day = schedule(Times::Every { interval: 1.hours(), window }, chrono_tz::UTC);
        by_day.month_days = Some(vec![20]);
        assert!(by_day.occurrences_on(date("2026-11-21"), &shift).is_empty());
        assert_eq!(local_times(&by_day.occurrences_on(date("2026-11-20"), &shift)), ["11-20 09:30", "11-20 10:30"]);
    }

    #[test]
    fn a_shift_too_large_to_apply_skips_the_date() {
        let daily = schedule(Times::DailyAt(vec![parse_time("09:00").unwrap()]), chrono_tz::UTC);
        let window = Some((parse_time("09:00").unwrap(), parse_time("10:00").unwrap()));
        let every = schedule(Times::Every { interval: 1.hours(), window }, chrono_tz::UTC);
        for s in [daily, every] {
            assert!(s.occurrences_on(date("2026-11-20"), &DayPlan::Shifted(TimeDelta::MAX)).is_empty());
            assert!(s.occurrences_on(date("2026-11-20"), &DayPlan::Shifted(TimeDelta::MIN)).is_empty());
        }
    }

    #[test]
    fn an_expiry_too_large_to_add_means_no_expiry() {
        use crate::scheduler::plan::expires_at;
        let from = "2026-11-20T09:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let mut s = schedule(Times::DailyAt(vec![parse_time("09:00").unwrap()]), chrono_tz::UTC);
        assert_eq!(expires_at(&s, from), None, "a daily schedule has no expiry of its own");
        s.expire_after = Some(2.hours());
        assert_eq!(expires_at(&s, from), Some(from + TimeDelta::hours(2)));
        s.expire_after = Some(Duration::MAX);
        assert_eq!(expires_at(&s, from), None);
        s.expire_after = Some(Duration::from_secs(i64::MAX.unsigned_abs() / 1000));
        assert_eq!(expires_at(&s, from), None);
        let every = schedule(Times::Every { interval: 30.minutes(), window: None }, chrono_tz::UTC);
        assert_eq!(expires_at(&every, from), Some(from + TimeDelta::minutes(30)), "every expires after one interval");
    }

    // Spec 8.15: the digest covers the weekdays, the days of the month and the zone.
    #[test]
    fn the_times_digest_changes_with_weekdays_days_of_month_and_zone() {
        use crate::scheduler::plan::times_hash;
        let base = schedule(Times::DailyAt(vec![parse_time("09:00").unwrap()]), chrono_tz::UTC);
        assert_eq!(times_hash(&base), times_hash(&base.clone()));
        let mut weekdays = base.clone();
        weekdays.weekdays = Some(vec![Weekday::Mon]);
        let mut month_days = base.clone();
        month_days.month_days = Some(vec![1]);
        let mut zone = base.clone();
        zone.zone = chrono_tz::Asia::Kolkata;
        let mut other_weekday = weekdays.clone();
        other_weekday.weekdays = Some(vec![Weekday::Tue]);
        let digests = [&base, &weekdays, &month_days, &zone, &other_weekday].map(times_hash);
        for (i, a) in digests.iter().enumerate() {
            for b in &digests[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
