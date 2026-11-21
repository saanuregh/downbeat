//! The daily report: each job's recent days, counted from the history, and yesterday's failures.

mod support;

use downbeat_client::{Error, Scope};
use downbeat_e2e::Text;
use support::{Database, SOON, eventually, finished, with_api};
use uuid::Uuid;

/// Moves finished runs back by whole days, as if they had run then.
async fn move_back(database: &Database, runs: &[Uuid], days: i32) {
    sqlx::query(
        "UPDATE downbeat.run_history SET finished_at = finished_at - make_interval(days => $2),
                started_at = started_at - make_interval(days => $2)
          WHERE run_id = ANY($1)",
    )
    .bind(runs)
    .bind(days)
    .execute(&database.pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn report_counts_each_day_and_lists_yesterdays_failures() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let mut echoes = Vec::new();
    for text in ["a", "b", "c"] {
        let started = client.start("echo", &Text { text: text.into() }).send().await.unwrap();
        echoes.push(started.run_id);
    }
    let panicked = client.start("panics", &()).send().await.unwrap().run_id;
    for run in echoes.iter().chain([&panicked]) {
        finished(&database.pool, *run, SOON).await;
    }
    move_back(&database, &[echoes[0], panicked], 1).await;

    let summary = client.report(7).await.unwrap();
    assert_eq!(summary.zone, "UTC");
    assert_eq!(summary.days.len(), 7);
    let today = *summary.days.last().unwrap();
    assert_eq!(today, chrono::Utc::now().date_naive());

    assert_eq!(summary.jobs[0].job, "panics", "a job that failed yesterday comes first");
    let panics = &summary.jobs[0].days;
    assert_eq!(panics.len(), 7);
    assert_eq!((panics[5].failed, panics[5].failure_rate), (1, Some(1.0)));
    assert_eq!(panics[6].failed, 0);

    let echo = &summary.jobs.iter().find(|job| job.job == "echo").unwrap().days;
    assert_eq!((echo[5].succeeded, echo[6].succeeded), (1, 2));
    assert_eq!(echo[6].failure_rate, Some(0.0));
    assert!(echo[6].median_seconds.is_some() && echo[6].longest_seconds.is_some());
    assert_eq!(echo[4].failure_rate, None, "a day without runs has no rate");

    let idle = summary.jobs.iter().find(|job| job.job == "hungry").expect("a registered job with no runs is listed");
    assert!(idle.days.iter().all(|day| day.succeeded + day.failed == 0));

    let failures: Vec<Uuid> = summary.failed_yesterday.iter().map(|run| run.run_id).collect();
    assert_eq!(failures, [panicked]);
    assert_eq!(summary.failed_yesterday[0].failed_because.as_deref(), Some("panic"));
}

#[tokio::test]
async fn report_shows_only_the_jobs_a_token_covers() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let panicked = client.start("panics", &()).send().await.unwrap().run_id;
    finished(&database.pool, panicked, SOON).await;
    move_back(&database, &[panicked], 1).await;

    let reader = client.acting_for("viewer", Scope::Read, Some(vec!["echo".into()]));
    let summary = reader.report(7).await.unwrap();
    let jobs: Vec<&str> = summary.jobs.iter().map(|job| job.job.as_str()).collect();
    assert_eq!(jobs, ["echo"]);
    assert!(summary.failed_yesterday.is_empty());
}

#[tokio::test]
async fn report_refuses_days_outside_one_to_thirty() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    for days in [0, 31] {
        let refused = client.report(days).await.unwrap_err();
        assert!(matches!(refused, Error::Api { status: 400, .. }), "{days}: {refused}");
    }
    assert_eq!(client.report(30).await.unwrap().days.len(), 30);
}

#[tokio::test]
async fn report_counts_runs_that_pruning_removed() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let mut runs = Vec::new();
    for text in ["a", "b", "c", "d"] {
        runs.push(client.start("echo", &Text { text: text.into() }).send().await.unwrap().run_id);
    }
    // One after the other: a second start while the first waits would join it.
    for _ in 0..2 {
        let run = client.start("panics", &()).send().await.unwrap().run_id;
        finished(&database.pool, run, SOON).await;
        runs.push(run);
    }
    for run in &runs {
        finished(&database.pool, *run, SOON).await;
    }
    // The links' runs (echo starts shout) end too, and every callback is delivered.
    eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run)
            AND NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)",
        SOON,
    )
    .await;
    let everything: Vec<Uuid> =
        sqlx::query_scalar("SELECT run_id FROM downbeat.run_history").fetch_all(&database.pool).await.unwrap();
    move_back(&database, &everything, 1).await;
    let before = client.report(7).await.unwrap();

    // Every chain is past its retention.
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();
    downbeat::measure::prune(&database.pool, downbeat_e2e::definition()).await.unwrap();
    let kept: Vec<String> =
        sqlx::query_scalar("SELECT job_name FROM downbeat.run_history").fetch_all(&database.pool).await.unwrap();
    // A chain holding a job's latest success stays, so one panics chain (its on_failure run) stays.
    let count = |name: &str| kept.iter().filter(|job| *job == name).count();
    assert!(count("echo") < 4 && count("panics") < 2, "some runs were pruned: {kept:?}");

    let after = client.report(7).await.unwrap();
    let yesterday = |report: &downbeat_client::types::Report, job: &str| {
        let day = &report.jobs.iter().find(|row| row.job == job).unwrap().days[5];
        (day.succeeded, day.failed, day.failure_rate, day.longest_seconds)
    };
    assert_eq!(yesterday(&after, "echo"), yesterday(&before, "echo"));
    assert_eq!(yesterday(&after, "panics"), yesterday(&before, "panics"));
    assert_eq!(yesterday(&after, "echo").0, 4);
    assert_eq!(yesterday(&after, "panics").1, 2);

    // A second pass finds nothing more to add.
    downbeat::measure::prune(&database.pool, downbeat_e2e::definition()).await.unwrap();
    assert_eq!(yesterday(&client.report(7).await.unwrap(), "echo").0, 4);
}
