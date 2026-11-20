//! Retention rules: which chains `keep_runs_at_most` counts, the check pruning makes after taking
//! a chain's lock, and the size of one callback delivery pass.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

const ALL_DELIVERED: &str = "SELECT NOT EXISTS (SELECT 1 FROM downbeat.run_history WHERE notify_after IS NOT NULL)";

async fn chains_of(database: &Database, status: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(DISTINCT c.chain_id) FROM downbeat.chain c JOIN downbeat.run_history h USING (chain_id)
          WHERE c.first_job = 'frequent_flop' AND h.status = $1",
    )
    .bind(status)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn keep_runs_at_most_leaves_out_chains_whose_first_run_failed() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);

    let mut runs = Vec::new();
    for n in [-1, -1].into_iter().chain(1..=21) {
        let run = client.start(frequent_flop, Number { n }).await.unwrap().run_id();
        let done = finished(&database.pool, run, SOON).await;
        assert_eq!(done.status, if n < 0 { "failed" } else { "succeeded" }, "{}", done.reason);
        runs.push(run);
    }
    eventually(&database.pool, ALL_DELIVERED, SOON).await;
    app.stop();

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();

    assert_eq!(chains_of(&database, "failed").await, 2, "chains whose first run failed are not counted");
    assert_eq!(chains_of(&database, "succeeded").await, 20, "the newest twenty successful chains stay");
    let oldest_success_kept: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE run_id = $1)")
            .bind(runs[2])
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert!(!oldest_success_kept, "the oldest success is the one beyond twenty");
}

#[tokio::test]
async fn pruning_checks_a_locked_chain_again_as_the_data_is_then() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let mut chains = Vec::new();
    for n in [1, 2] {
        let run = client.start(noop, Number { n }).await.unwrap();
        chains.push(finished(&database.pool, run.run_id(), SOON).await.chain_id);
    }
    eventually(&database.pool, ALL_DELIVERED, SOON).await;
    app.stop();
    let old = chains[0];
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();

    // As a retry would: take the chain's lock, and queue a new run in it, in one transaction.
    let mut retry = database.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
        .bind(old)
        .execute(&mut *retry)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO downbeat.run (run_id, job_name, queue, due_at, started_by, chain_id)
         VALUES ($1, 'noop', 'default', now(), 'person', $2)",
    )
    .bind(Uuid::new_v4())
    .bind(old)
    .execute(&mut *retry)
    .await
    .unwrap();

    downbeat::measure::prune(&database.pool, definition()).await.unwrap();
    assert!(chain_exists(&database, old).await, "a chain whose lock is held is passed over");

    retry.commit().await.unwrap();
    downbeat::measure::prune(&database.pool, definition()).await.unwrap();
    assert!(chain_exists(&database, old).await, "the chain now has an unfinished run, so it stays");
}

async fn chain_exists(database: &Database, chain: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM downbeat.chain WHERE chain_id = $1)")
        .bind(chain)
        .fetch_one(&database.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn one_delivery_pass_handles_at_most_twenty_rows() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let mut runs = Vec::new();
    for i in 0..25 {
        runs.push(client.start(noop, Number { n: i }).await.unwrap().run_id());
    }
    for run in &runs {
        finished(&database.pool, *run, SOON).await;
    }
    eventually(&database.pool, ALL_DELIVERED, SOON).await;
    app.stop();

    sqlx::query("UPDATE downbeat.run_history SET notify_after = now()").execute(&database.pool).await.unwrap();
    downbeat::measure::deliver_callbacks(&database.pool, definition()).await.unwrap();

    let (handled, untouched): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE notify_after IS NULL OR notify_after > now()),
                count(*) FILTER (WHERE notify_after <= now())
           FROM downbeat.run_history",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!((handled, untouched), (20, 5));
}

#[tokio::test]
async fn a_change_committed_between_pruning_s_two_steps_keeps_the_chain() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let mut runs = Vec::new();
    for n in [1, 2] {
        let run = client.start(noop, Number { n }).await.unwrap().run_id();
        finished(&database.pool, run, SOON).await;
        runs.push(run);
    }
    eventually(&database.pool, ALL_DELIVERED, SOON).await;
    app.stop();
    sqlx::query("UPDATE downbeat.chain SET started_at = now() - interval '10 years'")
        .execute(&database.pool)
        .await
        .unwrap();
    let old: Uuid = sqlx::query_scalar("SELECT chain_id FROM downbeat.run_history WHERE run_id = $1")
        .bind(runs[0])
        .fetch_one(&database.pool)
        .await
        .unwrap();

    // Between taking the locks and removing, another connection commits a change that makes the
    // old chain one that may not go: a delivery its first run still owes.
    let owe_a_delivery = || {
        let pool = database.pool.clone();
        async move {
            sqlx::query("UPDATE downbeat.run_history SET notify_after = now() WHERE run_id = $1")
                .bind(runs[0])
                .execute(&pool)
                .await
                .unwrap();
        }
    };
    let removed =
        downbeat::measure::prune_old_chains_with_a_pause(&database.pool, definition(), "noop", owe_a_delivery)
            .await
            .unwrap();
    assert_eq!(removed, 0, "the second step saw the change and kept the chain");
    let exists = || async {
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM downbeat.chain WHERE chain_id = $1)")
            .bind(old)
            .fetch_one(&database.pool)
            .await
            .unwrap()
    };
    assert!(exists().await);

    // Without that change the same chain goes, so it was a candidate all along.
    sqlx::query("UPDATE downbeat.run_history SET notify_after = NULL").execute(&database.pool).await.unwrap();
    let removed = downbeat::measure::prune_old_chains_with_a_pause(&database.pool, definition(), "noop", || async {})
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert!(!exists().await);
}
