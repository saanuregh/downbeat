//! How a product tests its jobs and its schedules: functions are called directly, and schedules
//! are asked about any date, with no database and no waiting.

use std::sync::atomic::AtomicU32;

use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use downbeat::testing::Schedules;
use downbeat::{Outcome, Run};
use market::{Exchange, TradingDay, definition, fetch_prices};

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

#[tokio::test]
async fn prices_are_fetched_every_half_hour_of_a_trading_day() {
    let schedules = Schedules::of(definition(), Some("production")).unwrap();
    let monday = schedules.on("fetch_prices", date(2026, 11, 23)).await.unwrap();
    let times: Vec<NaiveTime> = monday.iter().map(|occurrence| occurrence.local().time()).collect();
    assert_eq!(times.first(), Some(&NaiveTime::from_hms_opt(9, 15, 0).unwrap()));
    assert_eq!(times.last(), Some(&NaiveTime::from_hms_opt(15, 15, 0).unwrap()));
    assert_eq!(times.len(), 13);

    let saturday = schedules.on("fetch_prices", date(2026, 11, 21)).await.unwrap();
    assert!(saturday.is_empty(), "no fetching at the weekend");

    let input = schedules.input_for("fetch_prices", &monday[0]).unwrap();
    assert_eq!(input, serde_json::json!({ "date": "2026-11-23" }));
}

#[tokio::test]
async fn after_a_friday_evening_the_next_fetch_is_monday_morning() {
    let schedules = Schedules::of(definition(), Some("production")).unwrap();
    let friday_evening = Utc.with_ymd_and_hms(2026, 11, 20, 14, 0, 0).unwrap();
    let next = schedules.next_after("fetch_prices", friday_evening).await.unwrap().unwrap();
    // 09:15 in Kolkata is 03:45 UTC.
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 11, 23, 3, 45, 0).unwrap());
}

#[tokio::test]
async fn a_scheduler_down_for_an_hour_finds_two_missed_fetches() {
    let schedules = Schedules::of(definition(), Some("production")).unwrap();
    let down = Utc.with_ymd_and_hms(2026, 11, 23, 4, 0, 0).unwrap();
    let back = Utc.with_ymd_and_hms(2026, 11, 23, 5, 0, 0).unwrap();
    assert_eq!(schedules.between("fetch_prices", down, back).await.unwrap().len(), 2);
}

#[tokio::test]
async fn fetching_skips_until_the_exchange_has_published() {
    let exchange = Exchange { asked: AtomicU32::new(0) };
    let day = TradingDay { date: date(2026, 11, 23) };
    let run = Run::for_test().due(Utc.with_ymd_and_hms(2026, 11, 23, 3, 45, 0).unwrap());
    let first = fetch_prices.test(run.clone(), day, &exchange).await;
    assert_eq!(first, Outcome::Skipped("the exchange has published nothing yet".into()));
    assert_eq!(run.reported(), ["progress: asking the exchange"]);

    let _ = fetch_prices.test(Run::for_test(), day, &exchange).await;
    let third = fetch_prices.test(Run::for_test().on_attempt(2), day, &exchange).await;
    assert!(matches!(third, Outcome::Succeeded(prices) if prices.count == 50));
}
