//! Firing and registering schedules (spec section 8, rules 1 and 7–18, and 10.1), one engine step
//! at a time against a real database, with no app running. A test moves a schedule's watermark
//! into the past to stand for missed time, then fires once.

mod support;

use chrono::{DateTime, NaiveTime, TimeDelta, Utc};
use downbeat::measure;
use downbeat_e2e::definition;
use downbeat_e2e::scheduling_rules::CROWD;
use serde_json::Value;
use sqlx::PgPool;
use support::Database;

const ENVIRONMENT: &str = "rules";

async fn registered() -> Database {
    let database = Database::fresh().await;
    measure::register_schedules(&database.pool, definition(), ENVIRONMENT).await.unwrap();
    database
}

async fn register(pool: &PgPool) {
    measure::register_schedules(pool, definition(), ENVIRONMENT).await.unwrap();
}

async fn fire(pool: &PgPool) {
    measure::fire_due_schedules(pool, definition(), ENVIRONMENT).await.unwrap();
}

/// Makes only these schedules due, each with this watermark.
async fn due_only(pool: &PgPool, schedules: &[&str], watermark: DateTime<Utc>) {
    sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = NULL WHERE schedule <> ALL($1)")
        .bind(schedules)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = now(), watermark = $2 WHERE schedule = ANY($1)")
        .bind(schedules)
        .bind(watermark)
        .execute(pool)
        .await
        .unwrap();
}

/// This many minutes ago, to the whole second. The definition's zone is UTC.
fn ago(minutes: i64) -> DateTime<Utc> {
    let at = Utc::now() - TimeDelta::minutes(minutes);
    DateTime::from_timestamp(at.timestamp(), 0).unwrap()
}

/// Saves a dated override straight into its table, on the local date of the first time.
async fn add_override(pool: &PgPool, schedule: &str, change: &str, times: &[DateTime<Utc>]) {
    let date = times[0].date_naive();
    let times: Vec<NaiveTime> = times.iter().map(DateTime::time).collect();
    sqlx::query(
        "INSERT INTO downbeat.schedule_override (schedule, on_date, change, times, note, created_by)
         VALUES ($1, $2, $3, $4, 'test', 'test')",
    )
    .bind(schedule)
    .bind(date)
    .bind(change)
    .bind(&times)
    .execute(pool)
    .await
    .unwrap();
}

/// The `due_at` and `expires_at` of each run a schedule created, in order.
async fn runs(pool: &PgPool, schedule: &str) -> Vec<(DateTime<Utc>, Option<DateTime<Utc>>)> {
    sqlx::query_as("SELECT due_at, expires_at FROM downbeat.run WHERE schedule = $1 ORDER BY due_at")
        .bind(schedule)
        .fetch_all(pool)
        .await
        .unwrap()
}

/// The details of the events of this kind recorded for a schedule.
async fn events(pool: &PgPool, schedule: &str, event: &str) -> Vec<Value> {
    sqlx::query_scalar(
        "SELECT detail FROM downbeat.audit WHERE action = 'event' AND schedule = $1 AND detail->>'event' = $2 ORDER BY audit_id",
    )
    .bind(schedule)
    .bind(event)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// A column of the schedule's state, in seconds after the database's now; `None` when null.
async fn seconds_from_now(pool: &PgPool, schedule: &str, column: &str) -> Option<f64> {
    sqlx::query_scalar(&format!(
        "SELECT extract(epoch FROM {column} - now())::float8 FROM downbeat.schedule_state WHERE schedule = $1"
    ))
    .bind(schedule)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn state(pool: &PgPool, schedule: &str) -> (DateTime<Utc>, Option<DateTime<Utc>>) {
    sqlx::query_as("SELECT watermark, next_due_at FROM downbeat.schedule_state WHERE schedule = $1")
        .bind(schedule)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn instant(value: &Value) -> DateTime<Utc> {
    value.as_str().unwrap().parse().unwrap()
}

fn close_to(seconds: Option<f64>, expected: f64) -> bool {
    seconds.is_some_and(|seconds| (seconds - expected).abs() < 3.0)
}

// Rule 1, and rule 11 for `daily_at`.
#[tokio::test]
async fn an_override_takes_precedence_over_a_day_rule() {
    let database = registered().await;
    let pool = &database.pool;
    due_only(pool, &["ruled_off"], ago(60)).await;
    fire(pool).await;
    assert!(runs(pool, "ruled_off").await.is_empty(), "the day rule turns every day off");

    due_only(pool, &["ruled_off"], ago(60)).await;
    let at = ago(30);
    add_override(pool, "ruled_off", "times", &[at]).await;
    fire(pool).await;
    assert_eq!(runs(pool, "ruled_off").await, [(at, None)], "a daily_at run has no expiry");
}

// Rule 18.
#[tokio::test]
async fn an_override_never_fires_a_time_before_the_watermark() {
    let database = registered().await;
    let pool = &database.pool;
    due_only(pool, &["never_fires"], ago(10)).await;
    add_override(pool, "never_fires", "times", &[ago(30)]).await;
    fire(pool).await;
    assert!(runs(pool, "never_fires").await.is_empty());
}

// Rules 4 (a window override), 8 and 11 for `expire_after`.
#[tokio::test]
async fn only_the_latest_missed_occurrence_creates_a_run_and_the_earlier_ones_are_reported_dropped() {
    let database = registered().await;
    let pool = &database.pool;
    due_only(pool, &["missed_window"], ago(60)).await;
    // Every ten minutes: 55, 45, 35, 25, 15 and 5 minutes ago.
    let start = ago(55);
    let end = start + TimeDelta::minutes(50);
    add_override(pool, "missed_window", "window", &[start, end]).await;
    fire(pool).await;

    assert_eq!(runs(pool, "missed_window").await, [(end, Some(end + TimeDelta::hours(1)))]);
    let dropped = events(pool, "missed_window", "occurrences_dropped").await;
    assert_eq!(dropped.len(), 1, "{dropped:?}");
    assert_eq!(dropped[0]["count"], 5);
    assert_eq!(instant(&dropped[0]["first"]), start);
    assert_eq!(instant(&dropped[0]["last"]), start + TimeDelta::minutes(40));
}

// Rule 8.
#[tokio::test]
async fn an_expired_latest_occurrence_creates_no_run_and_is_reported_with_the_dropped_ones() {
    let database = registered().await;
    let pool = &database.pool;
    due_only(pool, &["missed_and_expired"], ago(60)).await;
    let start = ago(55);
    let end = start + TimeDelta::minutes(50);
    add_override(pool, "missed_and_expired", "window", &[start, end]).await;
    fire(pool).await;

    assert!(runs(pool, "missed_and_expired").await.is_empty(), "five minutes late is past its two-minute expiry");
    let dropped = events(pool, "missed_and_expired", "occurrences_dropped").await;
    assert_eq!(dropped.len(), 1, "{dropped:?}");
    assert_eq!(dropped[0]["count"], 6);
    assert_eq!(instant(&dropped[0]["last"]), end);
}

// Rules 8 and 9.
#[tokio::test]
async fn a_paused_schedule_creates_no_run_and_its_watermark_still_moves_to_now() {
    let database = registered().await;
    let pool = &database.pool;
    sqlx::query(
        "INSERT INTO downbeat.pause (kind, name, paused_by, note) VALUES ('schedule', 'missed_window', 'test', 'held')",
    )
    .execute(pool)
    .await
    .unwrap();
    due_only(pool, &["missed_window"], ago(60)).await;
    add_override(pool, "missed_window", "window", &[ago(55), ago(5)]).await;
    fire(pool).await;

    assert!(runs(pool, "missed_window").await.is_empty());
    assert!(events(pool, "missed_window", "occurrences_dropped").await.is_empty());
    assert!(close_to(seconds_from_now(pool, "missed_window", "watermark").await, 0.0));
}

// Rule 9.
#[tokio::test]
async fn after_firing_the_watermark_is_now_and_next_due_at_is_the_next_occurrence() {
    let database = registered().await;
    let pool = &database.pool;
    let tomorrow = Utc::now().date_naive() + TimeDelta::days(1);
    let start = tomorrow.and_hms_opt(10, 0, 0).unwrap().and_utc();
    add_override(pool, "missed_window", "window", &[start, start + TimeDelta::minutes(30)]).await;
    due_only(pool, &["missed_window", "never_fires"], ago(60)).await;
    let before = Utc::now();
    fire(pool).await;
    let after = Utc::now();

    let (watermark, next_due_at) = state(pool, "missed_window").await;
    assert!(watermark >= before - TimeDelta::seconds(1) && watermark <= after, "{watermark}");
    assert_eq!(next_due_at, Some(start));
    let (_, never) = state(pool, "never_fires").await;
    assert_eq!(never, None, "a schedule with no occurrence ahead has no next_due_at");
}

// Rule 12, when firing and when registering.
#[tokio::test]
async fn the_watermark_never_moves_backwards() {
    let database = registered().await;
    let pool = &database.pool;
    let ahead = ago(-60);
    due_only(pool, &["never_fires"], ahead).await;
    fire(pool).await;
    assert_eq!(state(pool, "never_fires").await.0, ahead, "firing");

    sqlx::query(
        "UPDATE downbeat.schedule_state SET times_hash = 'an older digest', seen_at = now() - interval '1 hour'
          WHERE schedule = 'never_fires'",
    )
    .execute(pool)
    .await
    .unwrap();
    register(pool).await;
    assert_eq!(state(pool, "never_fires").await.0, ahead, "registering a stale, changed schedule");
}

// Rule 10, for a day rule.
#[tokio::test]
async fn a_failing_day_rule_raises_schedule_failed_and_is_tried_again_a_minute_later() {
    let database = registered().await;
    let pool = &database.pool;
    let watermark = ago(60);
    due_only(pool, &["day_rule_fails"], watermark).await;
    fire(pool).await;

    let failed = events(pool, "day_rule_fails", "schedule_failed").await;
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0]["error"].as_str().unwrap().contains("did not load"), "{failed:?}");
    assert!(close_to(seconds_from_now(pool, "day_rule_fails", "next_due_at").await, 60.0));
    assert_eq!(state(pool, "day_rule_fails").await.0, watermark, "the failed firing is rolled back");
}

// Rules 10 and 10.1, for a schedule input and a day rule that panic.
#[tokio::test]
async fn a_panicking_input_or_day_rule_costs_only_its_own_schedule() {
    let database = registered().await;
    let pool = &database.pool;
    due_only(pool, &["input_panics", "day_rule_panics", "missed_window"], ago(60)).await;
    add_override(pool, "input_panics", "times", &[ago(30)]).await;
    add_override(pool, "missed_window", "window", &[ago(55), ago(5)]).await;
    fire(pool).await;

    assert_eq!(runs(pool, "missed_window").await.len(), 1, "the other schedule still fires");
    for schedule in ["input_panics", "day_rule_panics"] {
        assert!(runs(pool, schedule).await.is_empty(), "{schedule}");
        let failed = events(pool, schedule, "schedule_failed").await;
        assert_eq!(failed.len(), 1, "{schedule}: {failed:?}");
        assert!(failed[0]["error"].as_str().unwrap().contains("panicked"), "{schedule}: {failed:?}");
        assert!(close_to(seconds_from_now(pool, schedule, "next_due_at").await, 60.0), "{schedule}");
    }
}

// Rule 7.
#[tokio::test]
async fn at_most_fifty_schedules_are_handled_in_one_poll() {
    let database = registered().await;
    let pool = &database.pool;
    let crowd: Vec<String> = (0..CROWD).map(|index| format!("crowd_{index:02}")).collect();
    let names: Vec<&str> = crowd.iter().map(String::as_str).collect();
    due_only(pool, &names, ago(1)).await;
    let still_due = || async {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM downbeat.schedule_state WHERE next_due_at IS NOT NULL AND next_due_at <= now()",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        count
    };
    assert_eq!(still_due().await, i64::try_from(CROWD).unwrap());
    fire(pool).await;
    assert_eq!(still_due().await, i64::try_from(CROWD - 50).unwrap());
    fire(pool).await;
    assert_eq!(still_due().await, 0);
}

// Rule 13.
#[tokio::test]
async fn a_new_schedule_starts_with_its_watermark_and_next_due_at_at_now() {
    let database = registered().await;
    let pool = &database.pool;
    assert!(close_to(seconds_from_now(pool, "never_fires", "watermark").await, 0.0));
    assert!(close_to(seconds_from_now(pool, "never_fires", "next_due_at").await, 0.0));
}

// Rule 14.
#[tokio::test]
async fn a_schedule_registered_more_than_ten_minutes_ago_starts_again_from_now() {
    let database = registered().await;
    let pool = &database.pool;
    let watermark = ago(60);
    for (schedule, seen) in [("never_fires", "11 minutes"), ("all_day_every", "9 minutes")] {
        sqlx::query(
            "UPDATE downbeat.schedule_state SET watermark = $2, next_due_at = NULL, seen_at = now() - $3::interval
              WHERE schedule = $1",
        )
        .bind(schedule)
        .bind(watermark)
        .bind(seen)
        .execute(pool)
        .await
        .unwrap();
    }
    register(pool).await;
    assert!(close_to(seconds_from_now(pool, "never_fires", "watermark").await, 0.0), "stale");
    assert_eq!(state(pool, "all_day_every").await.0, watermark, "seen nine minutes ago");
}

// Rule 15.
#[tokio::test]
async fn a_schedule_whose_times_changed_starts_five_seconds_before_now_and_is_due_at_once() {
    let database = registered().await;
    let pool = &database.pool;
    let watermark = ago(60);
    sqlx::query("UPDATE downbeat.schedule_state SET watermark = $1, next_due_at = NULL WHERE schedule = ANY($2)")
        .bind(watermark)
        .bind(["never_fires", "all_day_every"])
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE downbeat.schedule_state SET times_hash = 'an older digest' WHERE schedule = 'never_fires'")
        .execute(pool)
        .await
        .unwrap();
    register(pool).await;

    assert!(close_to(seconds_from_now(pool, "never_fires", "watermark").await, -5.0), "changed");
    assert!(close_to(seconds_from_now(pool, "never_fires", "next_due_at").await, 0.0), "changed");
    assert_eq!(state(pool, "all_day_every").await, (watermark, None), "unchanged");
}
