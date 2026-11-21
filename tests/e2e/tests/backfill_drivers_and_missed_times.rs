//! Schedule rules (spec section 8): missed occurrences under ten seconds old, and backfills
//! driven by two schedulers at once, ignoring overrides and passing over days a rule turns off.

mod support;

use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use downbeat::measure;
use downbeat_client::types::BackfillRequest;
use downbeat_e2e::definition;
use serde_json::Value;
use sqlx::PgPool;
use support::{App, Database, eventually, with_api_in};

fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

/// This many seconds ago, to the whole second.
fn seconds_ago(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(Utc::now().timestamp() - seconds, 0).unwrap()
}

async fn add_override(pool: &PgPool, schedule: &str, on: NaiveDate, change: &str, times: &[NaiveTime]) {
    sqlx::query(
        "INSERT INTO downbeat.schedule_override (schedule, on_date, change, times, note, created_by)
         VALUES ($1, $2, $3, $4, 'test', 'test')",
    )
    .bind(schedule)
    .bind(on)
    .bind(change)
    .bind(times)
    .execute(pool)
    .await
    .unwrap();
}

// 8.8: of the earlier occurrences, only those more than ten seconds old are reported dropped.
#[tokio::test]
async fn missed_occurrences_under_ten_seconds_old_are_not_reported_dropped() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    measure::register_schedules(pool, definition(), "rules").await.unwrap();
    let (old, young, latest) = (seconds_ago(40), seconds_ago(6), seconds_ago(3));
    if old.date_naive() != latest.date_naive() {
        // The override is for one date; near midnight the times would fall on two.
        eprintln!("skipped: too close to midnight UTC");
        return;
    }
    sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = NULL WHERE schedule <> 'never_fires'")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE downbeat.schedule_state SET next_due_at = now(), watermark = now() - interval '1 minute'
          WHERE schedule = 'never_fires'",
    )
    .execute(pool)
    .await
    .unwrap();
    add_override(pool, "never_fires", latest.date_naive(), "times", &[old.time(), young.time(), latest.time()]).await;
    measure::fire_due_schedules(pool, definition(), "rules").await.unwrap();

    let due: Vec<DateTime<Utc>> = sqlx::query_scalar("SELECT due_at FROM downbeat.run WHERE schedule = 'never_fires'")
        .fetch_all(pool)
        .await
        .unwrap();
    assert_eq!(due, [latest], "only the latest occurrence creates a run");
    let events: Vec<Value> = sqlx::query_scalar(
        "SELECT detail FROM downbeat.audit
          WHERE action = 'event' AND schedule = 'never_fires' AND detail->>'event' = 'occurrences_dropped'",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["count"], 1, "the occurrence six seconds old is not reported: {}", events[0]);
    let first: DateTime<Utc> = events[0]["first"].as_str().unwrap().parse().unwrap();
    let last: DateTime<Utc> = events[0]["last"].as_str().unwrap().parse().unwrap();
    assert_eq!((first, last), (old, old));
}

async fn start_backfill(client: &downbeat_client::Client, job: &str, from: &str, to: &str) -> i64 {
    let request =
        BackfillRequest { job: job.into(), schedule: None, from: date(from), to: date(to), continue_on_failure: false };
    client.as_caller().start_backfill(&request).await.unwrap().id.expect("a backfill names its id")
}

async fn backfilled_inputs(pool: &PgPool, job: &str) -> Vec<Value> {
    sqlx::query_scalar(
        "SELECT input FROM downbeat.run_history WHERE job_name = $1 AND started_by = 'backfill' ORDER BY created_at",
    )
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn wait_done(pool: &PgPool, id: i64) {
    let sql = format!("SELECT state = 'done' FROM downbeat.backfill WHERE backfill_id = {id}");
    eventually(pool, &sql, Duration::from_secs(60)).await;
}

// 8.19: the driver locks the backfill row, so two schedulers never both start a date.
#[tokio::test]
async fn two_schedulers_driving_one_backfill_start_each_date_once() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let (_first, client, _) = with_api_in(&database, "rules").await;
    let _second = App::with(&database, &["scheduler", "worker", "--queues", "default=2"], "rules");
    let _third = App::with(&database, &["scheduler"], "rules");
    eventually(pool, "SELECT count(*) >= 3 FROM downbeat.worker WHERE role = 'scheduler'", Duration::from_secs(20))
        .await;

    // The 31sts of 2030: seven dates.
    let id = start_backfill(&client, "backfilled_day", "2030-01-01", "2030-12-31").await;
    wait_done(pool, id).await;

    let inputs = backfilled_inputs(pool, "backfilled_day").await;
    let expected: Vec<Value> = ["01", "03", "05", "07", "08", "10", "12"]
        .iter()
        .map(|month| Value::from(format!("2030-{month}-31")))
        .collect();
    assert_eq!(inputs, expected, "each date started once, in order");
}

// 8.20: a backfill uses no dated overrides, and passes over a date its day rule turns off.
#[tokio::test]
async fn a_backfill_ignores_overrides_and_passes_over_a_date_its_day_rule_turns_off() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let schedule = "early_days_but_the_second";
    let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    // Turned off by an override, but the backfill still runs it.
    add_override(pool, schedule, date("2030-01-01"), "off", &[]).await;
    // Turned off by the day rule and by the filter; the overrides giving them times are ignored.
    add_override(pool, schedule, date("2030-01-02"), "times", &[noon]).await;
    add_override(pool, schedule, date("2030-01-05"), "times", &[noon]).await;
    let (_app, client, _) = with_api_in(&database, "rules").await;

    let id = start_backfill(&client, "backfilled_early_day", "2030-01-01", "2030-01-06").await;
    wait_done(pool, id).await;

    let inputs = backfilled_inputs(pool, "backfilled_early_day").await;
    assert_eq!(inputs, [Value::from("2030-01-01"), Value::from("2030-01-03")]);
}
