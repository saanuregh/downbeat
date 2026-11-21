//! The daily report: each job's recent days, and yesterday's failures.

use std::collections::{BTreeMap, BTreeSet};

use axum::Json;
use axum::extract::{Extension, Query, State as Extract};
use chrono::{DateTime, Days, NaiveDate, Utc};
use downbeat_api::Scope;
use downbeat_api::types::{DayReport, FailedRun, JobReport, Report};
use serde::Deserialize;
use uuid::Uuid;

use crate::api::State;
use crate::api::auth::Caller;
use crate::api::error::{Answer, Failure};

/// The most days one report covers: the default history kept.
const MAX_DAYS: u32 = 30;
/// The most of yesterday's failures one report lists.
const MAX_FAILURES: i64 = 50;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReportQuery {
    days: Option<u32>,
}

/// One job's runs on one day, as the query returns them.
#[derive(sqlx::FromRow)]
struct DayRow {
    job_name: String,
    day: NaiveDate,
    succeeded: i64,
    failed: i64,
    skipped: i64,
    cancelled: i64,
    expired: i64,
    median_seconds: Option<f64>,
    p90_seconds: Option<f64>,
    longest_seconds: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct FailedRow {
    run_id: Uuid,
    job_name: String,
    failed_because: Option<String>,
    reason: String,
    finished_at: DateTime<Utc>,
}

pub(crate) async fn report(
    Extract(state): Extract<State>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<ReportQuery>,
) -> Answer<Report> {
    caller.require(Scope::Read)?;
    let days = query.days.unwrap_or(7);
    if !(1..=MAX_DAYS).contains(&days) {
        return Err(Failure::bad_request(format!("days must be from 1 to {MAX_DAYS}")));
    }
    let zone = state.registry.zone.name();
    // Today by the database's clock, like every other time rule.
    let today: NaiveDate =
        sqlx::query_scalar("SELECT (now() AT TIME ZONE $1)::date").bind(zone).fetch_one(&state.pool).await?;
    let yesterday = today - Days::new(1);
    // Yesterday is always read, so jobs that failed then come first even in a one-day report.
    let first = today - Days::new(u64::from(days.max(2) - 1));
    // The runs still in the history, and the day totals of those pruning removed.
    let rows: Vec<DayRow> = sqlx::query_as(
        "SELECT job_name, day, sum(succeeded)::int8 AS succeeded, sum(failed)::int8 AS failed,
                sum(skipped)::int8 AS skipped, sum(cancelled)::int8 AS cancelled, sum(expired)::int8 AS expired,
                max(median_seconds) AS median_seconds, max(p90_seconds) AS p90_seconds,
                max(longest_seconds) AS longest_seconds
           FROM (SELECT job_name, (finished_at AT TIME ZONE $1)::date AS day,
                        count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                        count(*) FILTER (WHERE status = 'failed') AS failed,
                        count(*) FILTER (WHERE status = 'skipped') AS skipped,
                        count(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                        count(*) FILTER (WHERE status = 'expired') AS expired,
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY seconds) FILTER (WHERE status = 'succeeded') AS median_seconds,
                        percentile_cont(0.9) WITHIN GROUP (ORDER BY seconds) FILTER (WHERE status = 'succeeded') AS p90_seconds,
                        max(seconds) FILTER (WHERE status = 'succeeded') AS longest_seconds
                   FROM (SELECT *, EXTRACT(EPOCH FROM finished_at - started_at)::float8 AS seconds FROM downbeat.run_history
                          WHERE finished_at >= $2::date::timestamp AT TIME ZONE $1
                            AND joined_run IS NULL AND NOT dry_run AND ($3::text[] IS NULL OR job_name = ANY($3))) history
                  GROUP BY 1, 2
                 UNION ALL
                 SELECT job_name, day, succeeded, failed, skipped, cancelled, expired, NULL, NULL, longest_seconds
                   FROM downbeat.day_totals
                  WHERE day >= $2 AND ($3::text[] IS NULL OR job_name = ANY($3))) both_sources
          GROUP BY 1, 2",
    )
    .bind(zone)
    .bind(first)
    .bind(&caller.jobs)
    .fetch_all(&state.pool)
    .await?;
    let failed: Vec<FailedRow> = sqlx::query_as(
        "SELECT run_id, job_name, failed_because, reason, finished_at FROM downbeat.run_history
          WHERE status = 'failed' AND joined_run IS NULL AND NOT dry_run
            AND finished_at >= $2::date::timestamp AT TIME ZONE $1 AND finished_at < $3::date::timestamp AT TIME ZONE $1
            AND ($4::text[] IS NULL OR job_name = ANY($4))
          ORDER BY finished_at DESC LIMIT $5",
    )
    .bind(zone)
    .bind(yesterday)
    .bind(today)
    .bind(&caller.jobs)
    .bind(MAX_FAILURES)
    .fetch_all(&state.pool)
    .await?;

    let dates: Vec<NaiveDate> = (0..days).rev().map(|back| today - Days::new(u64::from(back))).collect();
    // Every registered job the caller covers, and any other covered job that ran: a removed job's
    // failures still show.
    let mut names: BTreeSet<String> =
        state.registry.jobs.keys().filter(|name| caller.covers(name)).map(ToString::to_string).collect();
    let window_start = today - Days::new(u64::from(days - 1));
    names.extend(rows.iter().filter(|row| row.day >= window_start).map(|row| row.job_name.clone()));
    let by_day: BTreeMap<(&str, NaiveDate), &DayRow> =
        rows.iter().map(|row| ((row.job_name.as_str(), row.day), row)).collect();
    let failed_on = |job: &str| by_day.get(&(job, yesterday)).map_or(0, |row| row.failed);
    let mut names: Vec<String> = names.into_iter().collect();
    names.sort_by_key(|name| std::cmp::Reverse(failed_on(name)));
    let jobs = names
        .iter()
        .map(|job| JobReport {
            job: job.clone(),
            days: dates.iter().map(|date| day_report(*date, by_day.get(&(job.as_str(), *date)).copied())).collect(),
        })
        .collect();
    let failed_yesterday = failed
        .into_iter()
        .map(|row| FailedRun {
            log_link: state.registry.log_link_for(row.run_id),
            run_id: row.run_id,
            job: row.job_name,
            failed_because: row.failed_because,
            reason: row.reason,
            finished_at: row.finished_at,
        })
        .collect();
    Ok(Json(Report { zone: zone.to_owned(), days: dates, jobs, failed_yesterday }))
}

fn day_report(date: NaiveDate, row: Option<&DayRow>) -> DayReport {
    let Some(row) = row else {
        return DayReport {
            date,
            succeeded: 0,
            failed: 0,
            skipped: 0,
            cancelled: 0,
            expired: 0,
            failure_rate: None,
            median_seconds: None,
            p90_seconds: None,
            longest_seconds: None,
        };
    };
    let decided = row.succeeded + row.failed;
    #[expect(clippy::cast_precision_loss, reason = "a day's run counts are far below 2^52")]
    let failure_rate = (decided > 0).then(|| row.failed as f64 / decided as f64);
    DayReport {
        date,
        succeeded: row.succeeded,
        failed: row.failed,
        skipped: row.skipped,
        cancelled: row.cancelled,
        expired: row.expired,
        failure_rate,
        median_seconds: row.median_seconds,
        p90_seconds: row.p90_seconds,
        longest_seconds: row.longest_seconds,
    }
}
