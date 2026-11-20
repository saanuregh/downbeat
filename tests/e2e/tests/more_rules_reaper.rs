//! More reaper, delivery and retention rules (spec 9.5, 9.6, 12.4 and 13.2).

mod support;

use std::process::{Command, Stdio};
use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use sqlx::PgPool;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

async fn state_of(pool: &PgPool, run: Uuid) -> (String, i16) {
    sqlx::query_as("SELECT state, lost_count FROM downbeat.run WHERE run_id = $1")
        .bind(run)
        .fetch_one(pool)
        .await
        .unwrap()
}

// 9.5: the reaper passes over a lost run whose row another transaction holds, and takes it back
// once the row is free.
#[tokio::test]
async fn the_reaper_skips_a_lost_run_whose_row_is_locked() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let first = App::start(&database);
    let mut runs = Vec::new();
    // Different inputs, so the second does not join the first.
    for seconds in [30, 31] {
        runs.push(client.start(counts, Seconds { seconds }).await.unwrap().run_id());
    }
    eventually(
        pool,
        "SELECT count(*) = 2 FROM downbeat.run WHERE job_name = 'counts' AND function_started_at IS NOT NULL",
        SOON,
    )
    .await;
    first.kill();
    sqlx::query("UPDATE downbeat.run SET lease_expires_at = now() - interval '1 hour' WHERE run_id = ANY($1)")
        .bind(&runs)
        .execute(pool)
        .await
        .unwrap();
    let (locked, free) = (runs[0], runs[1]);
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM downbeat.run WHERE run_id = $1 FOR UPDATE")
        .bind(locked)
        .execute(&mut *holder)
        .await
        .unwrap();

    // A scheduler alone runs the reaper and claims nothing.
    let _reaper = App::with(&database, &["scheduler"], "test");
    let taken_back = format!("SELECT state = 'queued' FROM downbeat.run WHERE run_id = '{free}'");
    eventually(pool, &taken_back, SOON).await;
    // A sweep every five seconds: at least one more passes while the row is held.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(state_of(pool, locked).await, ("running".to_owned(), 0), "the locked row was left alone");

    holder.commit().await.unwrap();
    let locked_back = format!("SELECT state = 'queued' FROM downbeat.run WHERE run_id = '{locked}'");
    eventually(pool, &locked_back, SOON).await;
    assert_eq!(state_of(pool, locked).await.1, 1);
}

// 9.6: a worker row that lists the job but was last seen over an hour ago does not keep the run.
#[tokio::test]
async fn a_worker_last_seen_over_an_hour_ago_does_not_keep_a_job_registered() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    sqlx::query(
        "INSERT INTO downbeat.worker (worker_id, role, jobs, build, started_at, last_seen_at)
         VALUES ('long-gone', 'worker', '{echo}', 'old', now() - interval '3 hours', now() - interval '61 minutes')",
    )
    .execute(pool)
    .await
    .unwrap();
    let run = client.start(echo, Text { text: "old".into() }).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET created_at = now() - interval '2 hours' WHERE run_id = $1")
        .bind(run)
        .execute(pool)
        .await
        .unwrap();

    let _reaper = App::with(&database, &["scheduler"], "test");
    let done = finished(pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("job_not_registered")));
}

// 12.4: dropping a stale delivery writes an error line to the log.
#[tokio::test]
async fn dropping_a_stale_delivery_logs_an_error() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let chain = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by) VALUES ($1, 'basics', 'echo', 'person')",
    )
    .bind(chain)
    .execute(pool)
    .await
    .unwrap();
    let stale = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id,
                created_at, status, reason, finished_at, notify_after)
         VALUES ($1, 'echo', 'default', 0, now(), 'person', $2, now() - interval '25 hours', 'succeeded',
                 'Succeeded.', now() - interval '25 hours', now())",
    )
    .bind(stale)
    .bind(chain)
    .execute(pool)
    .await
    .unwrap();

    // Started here rather than with `App`, to keep its standard error.
    let dir = std::env::temp_dir().join(format!("stale-delivery-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let log = dir.join("stderr.log");
    let mut process = Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .arg("scheduler")
        .env(FINISHED_DIR, &dir)
        .env("DOWNBEAT_DATABASE_URL", &database.url)
        .env("DOWNBEAT_ENVIRONMENT", "test")
        .env("RUST_LOG", "warn,downbeat=info")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let delivered =
        format!("SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE run_id = '{stale}' AND notify_after IS NULL)");
    eventually(pool, &delivered, SOON).await;
    let _ = process.kill();
    let _ = process.wait();

    let text = std::fs::read_to_string(&log).unwrap();
    let called = dir.join(stale.to_string()).exists();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(!called, "a stale row is not delivered");
    let line = text.lines().find(|line| line.contains(&stale.to_string()));
    assert!(
        line.is_some_and(|line| line.contains("ERROR") && line.contains("dropped")),
        "an error line names the dropped run: {text}"
    );
}

/// Puts a chain straight into the tables, started by `first_job`, holding one finished run per
/// `(job, minutes ago, joined)`; returns the chain.
async fn chain_of(pool: &PgPool, first_job: &str, runs: &[(&str, i32, bool)]) -> Uuid {
    let chain = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by, started_at)
         VALUES ($1, 'chain_rules', $2, 'person', now() - make_interval(mins => $3))",
    )
    .bind(chain)
    .bind(first_job)
    .bind(runs[0].1)
    .execute(pool)
    .await
    .unwrap();
    for (job, minutes, joined) in runs {
        sqlx::query(
            "INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id,
                    created_at, status, skipped_because, reason, finished_at, joined_run)
             VALUES ($1, $2, 'default', 0, now(), 'person', $3, now() - make_interval(mins => $4),
                     CASE WHEN $5 THEN 'skipped' ELSE 'succeeded' END, CASE WHEN $5 THEN 'joined' END,
                     'Test.', now() - make_interval(mins => $4), CASE WHEN $5 THEN gen_random_uuid() END)",
        )
        .bind(Uuid::new_v4())
        .bind(job)
        .bind(chain)
        .bind(minutes)
        .bind(joined)
        .execute(pool)
        .await
        .unwrap();
    }
    chain
}

async fn kept(pool: &PgPool) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT chain_id FROM downbeat.chain").fetch_all(pool).await.unwrap()
}

// 13.2: a chain whose first run of the job joined other work is left out of the count.
#[tokio::test]
async fn keep_runs_at_most_leaves_out_chains_whose_first_run_joined() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let oldest = chain_of(pool, "keeps_three", &[("keeps_three", 50, false)]).await;
    let older = chain_of(pool, "keeps_three", &[("keeps_three", 40, false)]).await;
    let joined = chain_of(pool, "keeps_three", &[("keeps_three", 35, true)]).await;
    let newer = chain_of(pool, "keeps_three", &[("keeps_three", 30, false)]).await;
    let newest = chain_of(pool, "keeps_three", &[("keeps_three", 20, false)]).await;

    downbeat::measure::prune(pool, definition()).await.unwrap();

    let left = kept(pool).await;
    for (name, chain) in [("newest", newest), ("newer", newer), ("older", older), ("joined", joined)] {
        assert!(left.contains(&chain), "{name} stays: the joined chain is not one of the three");
    }
    assert!(!left.contains(&oldest), "the fourth counted chain goes");
}

// 13.2: retention comes from the chain's first job: runs of the job in chains another job started
// are not counted, and a pruned chain goes with the runs of other jobs in it.
#[tokio::test]
async fn keep_runs_at_most_counts_only_chains_the_job_started() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let mut started = Vec::new();
    for minutes in [50, 40, 30, 20] {
        started.push(
            chain_of(pool, "keeps_three", &[("keeps_three", minutes, false), ("echo", minutes - 1, false)]).await,
        );
    }
    let mut others = Vec::new();
    for minutes in [15, 10] {
        others.push(chain_of(pool, "echo", &[("echo", minutes, false), ("keeps_three", minutes - 1, false)]).await);
    }

    downbeat::measure::prune(pool, definition()).await.unwrap();

    let left = kept(pool).await;
    assert!(!left.contains(&started[0]), "the oldest chain keeps_three started goes");
    for chain in started[1..].iter().chain(&others) {
        assert!(left.contains(chain), "{chain} stays");
    }
    let orphans: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM downbeat.run_history h WHERE NOT EXISTS (SELECT 1 FROM downbeat.chain c WHERE c.chain_id = h.chain_id)",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(orphans, 0, "the pruned chain's echo run went with it");
}
