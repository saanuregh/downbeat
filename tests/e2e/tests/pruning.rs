//! Pruning removes old history a whole chain at a time, and never a chain that is still in use.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

#[tokio::test]
async fn old_chains_go_unless_they_are_unfinished_awaiting_a_callback_or_a_latest_success() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let mut echoes = Vec::new();
    for text in ["first", "second", "latest"] {
        let run = client.start(echo, Text { text: text.into() }).await.unwrap();
        finished(&database.pool, run.run_id(), SOON).await;
        echoes.push(run.run_id());
    }
    let waiting = client.start(waits_for_file, Text { text: "/nonexistent/downbeat-test".into() }).await.unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts > 0)", waiting.run_id()),
        SOON,
    )
    .await;
    eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)",
        SOON,
    )
    .await;
    app.stop();

    let [first, second, latest, unfinished] = [
        chain_of(&database, echoes[0]).await,
        chain_of(&database, echoes[1]).await,
        chain_of(&database, echoes[2]).await,
        chain_of(&database, waiting.run_id()).await,
    ];

    // Everything is far past its retention; the second echo's callback has not been delivered.
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE downbeat.run_history SET notify_after = now() + interval '1 day' WHERE run_id = $1")
        .bind(echoes[1])
        .execute(&database.pool)
        .await
        .unwrap();

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();

    let kept: Vec<Uuid> =
        sqlx::query_scalar("SELECT chain_id FROM downbeat.chain").fetch_all(&database.pool).await.unwrap();
    assert!(!kept.contains(&first), "an old, finished, delivered chain goes");
    assert!(kept.contains(&second), "a chain awaiting its callback stays");
    assert!(kept.contains(&latest), "a chain holding the job's latest success stays");
    assert!(kept.contains(&unfinished), "a chain with an unfinished run stays");
    let first_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE chain_id = $1")
        .bind(first)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(first_rows, 0, "a pruned chain's history goes with it");
}

// Decision 61: the latest success is dated by when it was marked, if later, and a dry run is none.
#[tokio::test]
async fn the_latest_success_kept_is_the_one_overdue_checks_read() {
    let database = Database::fresh().await;
    let runs = successes_in_their_own_chains(&database, 3).await;
    let [marked, older, dry] =
        [chain_of(&database, runs[0]).await, chain_of(&database, runs[1]).await, chain_of(&database, runs[2]).await];
    sqlx::query("UPDATE downbeat.run_history SET marked_at = now() + interval '1 minute' WHERE run_id = $1")
        .bind(runs[0])
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE downbeat.run_history SET dry_run = true WHERE run_id = $1")
        .bind(runs[2])
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();

    let kept: Vec<Uuid> =
        sqlx::query_scalar("SELECT chain_id FROM downbeat.chain").fetch_all(&database.pool).await.unwrap();
    assert!(kept.contains(&marked), "the success marked last stays");
    assert!(!kept.contains(&older), "an older success goes");
    assert!(!kept.contains(&dry), "a dry run that finished last goes: it is not a success");
}

#[tokio::test]
async fn a_dry_run_that_ended_last_does_not_replace_the_latest_success() {
    let database = Database::fresh().await;
    let runs = successes_in_their_own_chains(&database, 2).await;
    let [real, dry] = [chain_of(&database, runs[0]).await, chain_of(&database, runs[1]).await];
    sqlx::query("UPDATE downbeat.run_history SET dry_run = true WHERE run_id = $1")
        .bind(runs[1])
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();

    let kept: Vec<Uuid> =
        sqlx::query_scalar("SELECT chain_id FROM downbeat.chain").fetch_all(&database.pool).await.unwrap();
    assert!(kept.contains(&real), "the last real success stays");
    assert!(!kept.contains(&dry), "the dry run goes");
}

/// Runs `child_pid`, which starts nothing after it, `count` times one after the other, so each
/// chain holds one succeeded run, and waits until every callback is delivered.
async fn successes_in_their_own_chains(database: &Database, count: i64) -> Vec<Uuid> {
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(database);
    let mut runs = Vec::new();
    for n in 1..=count {
        let run = client.start(child_pid, Number { n }).await.unwrap().run_id();
        finished(&database.pool, run, SOON).await;
        runs.push(run);
    }
    eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)",
        SOON,
    )
    .await;
    app.stop();
    runs
}

async fn chain_of(database: &Database, run: Uuid) -> Uuid {
    sqlx::query_scalar(
        "SELECT chain_id FROM downbeat.run_history WHERE run_id = $1
         UNION ALL SELECT chain_id FROM downbeat.run WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn keep_runs_at_most_keeps_that_many_of_the_newest_chains() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    // `ticker` uses the `frequent` preset: keep_runs_at_most = 20.
    let mut runs = Vec::new();
    for _ in 0..23 {
        let run = client.start(ticker, ()).await.unwrap();
        finished(&database.pool, run.run_id(), SOON).await;
        runs.push(run.run_id());
    }
    eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)",
        SOON,
    )
    .await;

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();

    let kept: Vec<Uuid> = sqlx::query_scalar(
        "SELECT h.run_id FROM downbeat.run_history h JOIN downbeat.chain c USING (chain_id)
          WHERE h.job_name = 'ticker' ORDER BY h.finished_at",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap();
    assert_eq!(kept, runs[3..], "the three oldest chains go, the newest twenty stay");
}

#[tokio::test]
async fn pruning_passes_over_a_chain_whose_lock_is_held() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let mut runs = Vec::new();
    for text in ["old", "latest"] {
        let run = client.start(echo, Text { text: text.into() }).await.unwrap().run_id();
        finished(&database.pool, run, SOON).await;
        runs.push(run);
    }
    eventually(
        &database.pool,
        "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)",
        SOON,
    )
    .await;
    app.stop();
    let old = chain_of(&database, runs[0]).await;
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();

    // As if a run in the chain were ending right now.
    let mut ending = database.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
        .bind(old)
        .execute(&mut *ending)
        .await
        .unwrap();
    downbeat::measure::prune(&database.pool, definition()).await.unwrap();
    let exists = || async {
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM downbeat.chain WHERE chain_id = $1)")
            .bind(old)
            .fetch_one(&database.pool)
            .await
            .unwrap()
    };
    assert!(exists().await, "a chain whose lock is held is passed over");
    ending.commit().await.unwrap();

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();
    assert!(!exists().await, "and removed on the next pass");
}
