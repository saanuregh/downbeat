//! A measurement of the engine's hot paths against a real Postgres, for `docs/performance.md`.
//! It seeds a large table, so it does not run with the other tests:
//! `mise-tasks/test-rust -p downbeat-e2e --test measure -- --ignored --nocapture`.

mod support;

use std::time::{Duration, Instant};

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, eventually, finished};

fn summary(name: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let mean = samples.iter().sum::<Duration>() / u32::try_from(samples.len()).unwrap();
    let p95 = samples[samples.len() * 95 / 100];
    println!("{name}: mean {mean:?}, p95 {p95:?}, over {} samples", samples.len());
}

/// Puts `count` queued runs of a job straight into the table, each its own chain and input.
async fn seed_queued(database: &Database, job: &str, count: i64) {
    sqlx::query(
        "WITH c AS (INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by)
                    SELECT gen_random_uuid(), 'basics', $1, 'program' FROM generate_series(1, $2) RETURNING chain_id)
         INSERT INTO downbeat.run (run_id, job_name, queue, priority, due_at, started_by, chain_id, input, input_hash, turn_key)
         SELECT gen_random_uuid(), $1, 'default', (random() * 10)::int, now(), 'program', chain_id,
                jsonb_build_object('n', (random() * 1000000)::int, 'text', chain_id::text), md5(chain_id::text), $1 || ':' || md5(chain_id::text)
           FROM c",
    )
    .bind(job)
    .bind(count)
    .execute(&database.pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE downbeat.run").execute(&database.pool).await.unwrap();
}

#[tokio::test]
#[ignore = "a measurement, not a test: run it on purpose"]
async fn claim_and_heartbeat_with_a_hundred_thousand_queued_runs() {
    let database = Database::fresh().await;
    seed_queued(&database, "noop", 100_000).await;
    let jobs = vec!["noop".to_owned()];
    let mut claims = Vec::new();
    let mut held = Vec::new();
    for _ in 0..500 {
        let started = Instant::now();
        held.push(downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap().unwrap());
        claims.push(started.elapsed());
    }
    summary("claim, 100k queued", claims);
    let mut beats = Vec::new();
    for (run_id, fence) in held {
        let started = Instant::now();
        assert!(downbeat::measure::heartbeat(&database.pool, run_id, fence).await.unwrap());
        beats.push(started.elapsed());
    }
    summary("heartbeat", beats);
}

#[tokio::test]
#[ignore = "a measurement, not a test: run it on purpose"]
async fn pruning_a_month_of_history() {
    let database = Database::fresh().await;
    // 60,000 finished chains of one run each, 40 days old: past the default 30-day retention.
    sqlx::query(
        "WITH c AS (INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by, started_at)
                    SELECT gen_random_uuid(), 'basics', 'noop', 'program', now() - interval '40 days' - n * interval '1 second'
                      FROM generate_series(1, 60000) AS n RETURNING chain_id, started_at)
         INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id, created_at,
                                           status, reason, finished_at)
         SELECT gen_random_uuid(), 'noop', 'default', 0, started_at, 'program', chain_id, started_at, 'succeeded', 'Succeeded.', started_at
           FROM c",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE").execute(&database.pool).await.unwrap();
    let started = Instant::now();
    downbeat::measure::prune(&database.pool, definition()).await.unwrap();
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history").fetch_one(&database.pool).await.unwrap();
    println!("one pruning pass over 60,000 old chains: {:?}, {left} history rows left", started.elapsed());
}

#[tokio::test]
#[ignore = "a measurement, not a test: run it on purpose"]
async fn throughput_and_child_start_up() {
    let database = Database::fresh().await;
    seed_queued(&database, "noop", 2000).await;
    let started = Instant::now();
    let app = App::with(&database, &["worker", "--queues", "default=8"], "test");
    eventually(&database.pool, "SELECT count(*) = 2000 FROM downbeat.run_history", Duration::from_secs(300)).await;
    let elapsed = started.elapsed();
    let succeeded: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE status = 'succeeded'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(succeeded, 2000);
    println!("2,000 runs on 8 slots: {elapsed:?}, {:.0} runs a second", 2000.0 / elapsed.as_secs_f64());
    drop(app);

    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    for (label, fresh) in [("a reused child", false), ("a fresh child", true)] {
        let mut samples = Vec::new();
        for n in 0..20 {
            let started = Instant::now();
            let run = if fresh {
                client.start(fresh_noop, Number { n }).await
            } else {
                client.start(noop, Number { n: 1000 + n }).await
            };
            finished(&database.pool, run.unwrap().run_id(), Duration::from_secs(30)).await;
            samples.push(started.elapsed());
        }
        summary(&format!("one run, start to end, on {label}"), samples);
    }
}
