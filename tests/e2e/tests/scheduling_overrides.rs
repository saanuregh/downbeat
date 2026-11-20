//! Dated overrides and backfills through the control API (spec section 8, rules 4, 16, 17 and 19),
//! with the test app running in the `rules` environment.

mod support;

use std::time::Duration;

use chrono::{NaiveDate, TimeDelta, Utc};
use downbeat_client::types::{BackfillRequest, Done, OverrideRequest};
use support::{Database, SOON, eventually, with_api_in};

fn request(schedule: &str, date: NaiveDate, change: &str, times: &[&str]) -> OverrideRequest {
    OverrideRequest {
        schedules: vec![schedule.into()],
        date,
        change: change.into(),
        times: times.iter().map(|time| (*time).to_owned()).collect(),
        note: "a test".into(),
    }
}

fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

// Rule 4.
#[tokio::test]
async fn a_window_override_replaces_the_window() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let day = date("2030-01-31");
    let times = |calendar: &downbeat_client::types::CalendarDay| -> Vec<(String, String)> {
        calendar
            .entries
            .iter()
            .filter(|entry| entry.schedule == "replaced_window")
            .map(|entry| (entry.at.format("%H:%M").to_string(), entry.source.clone()))
            .collect()
    };
    let normal = client.calendar(day).await.unwrap();
    let normal_source = |time: &str| (time.to_owned(), "normal".to_owned());
    assert_eq!(times(&normal), [normal_source("09:00"), normal_source("09:30"), normal_source("10:00")]);

    client.as_caller().add_override(&request("replaced_window", day, "window", &["13:00", "13:40"])).await.unwrap();
    let replaced = client.calendar(day).await.unwrap();
    let override_source = |time: &str| (time.to_owned(), "override".to_owned());
    assert_eq!(times(&replaced), [override_source("13:00"), override_source("13:30")]);
}

// Rule 16.
#[tokio::test]
async fn an_override_is_refused_when_its_change_does_not_fit_the_schedule() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let admin = client.as_caller();
    let day = date("2030-01-07");
    let refused = [
        request("never_fires", day, "times", &[]),
        request("all_day_every", day, "times", &["10:00"]),
        request("missed_window", day, "window", &["10:00"]),
        request("missed_window", day, "window", &["10:00", "11:00", "12:00"]),
        request("never_fires", day, "window", &["10:00", "11:00"]),
        request("all_day_every", day, "window", &["10:00", "11:00"]),
        OverrideRequest { note: " ".into(), ..request("never_fires", day, "off", &[]) },
    ];
    for refused in &refused {
        assert!(admin.add_override(refused).await.is_err(), "{refused:?}");
    }
    let saved: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downbeat.schedule_override").fetch_one(&database.pool).await.unwrap();
    assert_eq!(saved, 0);

    admin.add_override(&request("never_fires", day, "times", &["10:00", "11:00"])).await.unwrap();
    admin.add_override(&request("missed_window", day, "window", &["10:00", "11:00"])).await.unwrap();
}

// Rule 16.
#[tokio::test]
async fn a_second_override_for_the_same_schedule_and_date_replaces_the_first() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let admin = client.as_caller();
    let day = date("2030-01-07");
    admin.add_override(&request("never_fires", day, "off", &[])).await.unwrap();
    admin.add_override(&request("never_fires", day, "times", &["10:15"])).await.unwrap();

    let overrides = admin.overrides().await.unwrap();
    let saved: Vec<_> = overrides.iter().filter(|row| row.schedule == "never_fires").collect();
    assert_eq!(saved.len(), 1, "{overrides:?}");
    assert_eq!(saved[0].change, "times");
    assert_eq!(saved[0].date, day);
}

// Rule 17. The running scheduler fires the schedule as soon as it is due, so the test sees the
// next time it worked out, not the moment it became due.
#[tokio::test]
async fn saving_or_removing_an_override_works_out_the_next_time_at_once() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let pool = &database.pool;
    eventually(
        pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.schedule_state WHERE schedule = 'missed_window' AND next_due_at IS NULL)",
        SOON,
    )
    .await;
    let put_off = || async {
        sqlx::query(
            "UPDATE downbeat.schedule_state SET next_due_at = now() + interval '1 day' WHERE schedule = 'missed_window'",
        )
        .execute(pool)
        .await
        .unwrap();
    };
    put_off().await;

    let tomorrow = Utc::now().date_naive() + TimeDelta::days(1);
    let saved = client
        .as_caller()
        .add_override(&request("missed_window", tomorrow, "window", &["10:00", "10:30"]))
        .await
        .unwrap();
    let start = tomorrow.and_hms_opt(10, 0, 0).unwrap().and_utc();
    eventually(
        pool,
        &format!(
            "SELECT next_due_at = '{}'::timestamptz FROM downbeat.schedule_state WHERE schedule = 'missed_window'",
            start.to_rfc3339()
        ),
        Duration::from_secs(5),
    )
    .await;

    put_off().await;
    let id = saved.id.expect("the override's id");
    let _: Done = client.as_caller().delete(&format!("/v1/overrides/{id}")).await.unwrap();
    eventually(
        pool,
        "SELECT next_due_at IS NULL FROM downbeat.schedule_state WHERE schedule = 'missed_window'",
        Duration::from_secs(5),
    )
    .await;
}

// Rule 19, and its counterpart when the backfill continues on failure.
#[tokio::test]
async fn a_backfill_fails_at_a_failed_date_unless_it_continues_on_failure() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let pool = &database.pool;
    // The schedule fires on the 31st only: 2030-01-31 and 2030-03-31 in this range.
    let backfill = |continue_on_failure| BackfillRequest {
        job: "fails_on_its_day".into(),
        schedule: None,
        from: date("2030-01-31"),
        to: date("2030-03-31"),
        continue_on_failure,
    };
    client.as_caller().start_backfill(&backfill(false)).await.unwrap();
    eventually(pool, "SELECT EXISTS (SELECT 1 FROM downbeat.backfill WHERE state = 'failed')", SOON).await;
    let reason: Option<String> =
        sqlx::query_scalar("SELECT reason FROM downbeat.backfill").fetch_one(pool).await.unwrap();
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE job_name = 'fails_on_its_day'")
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(reason.as_deref().is_some_and(|reason| reason.contains("Stopped at 2030-01-31")), "{reason:?}");
    assert_eq!(runs, 1, "no date after the failed one runs");

    client.as_caller().start_backfill(&backfill(true)).await.unwrap();
    eventually(pool, "SELECT EXISTS (SELECT 1 FROM downbeat.backfill WHERE state = 'done')", Duration::from_secs(30))
        .await;
    let days: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE job_name = 'fails_on_its_day'")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(days, 3, "one failed run before, then both dates");
}

#[tokio::test]
async fn the_calendar_lists_a_schedule_whose_day_rule_fails_as_off_and_shows_the_others() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let day = date("2030-01-31");
    let calendar = client.calendar(day).await.expect("one failing day rule does not fail the calendar");

    for failing in ["day_rule_panics", "day_rule_fails"] {
        let off = calendar.days_off.iter().find(|off| off.schedule == failing).expect(failing);
        assert!(off.reason.starts_with("Its day rule failed"), "{failing}: {}", off.reason);
    }
    assert!(calendar.entries.iter().any(|entry| entry.schedule == "replaced_window"), "other schedules still show");
}
