//! Pruning works through any number of old chains in one pass, and removes the other rows that
//! expire. Needs the throwaway Postgres cluster `mise-tasks/test-rust` starts.

use downbeat::prelude::*;
use sqlx::PgPool;

#[job]
async fn echo(_run: Run) -> Result<()> {
    Ok(())
}

fn echoes(pipeline: &mut Pipeline) {
    pipeline.job(echo);
}

fn definition() -> Definition {
    let mut definition = Definition::new().zone("UTC");
    definition.pipeline("echoes", echoes);
    definition
}

/// A database of its own for one test, in the test cluster.
async fn fresh_database() -> PgPool {
    let base = std::env::var("DOWNBEAT_TEST_DATABASE_URL")
        .expect("DOWNBEAT_TEST_DATABASE_URL is not set; run the tests with mise-tasks/test-rust");
    let admin = PgPool::connect(&base).await.expect("the test cluster is reachable");
    let name: String = sqlx::query_scalar("SELECT 'test_' || replace(gen_random_uuid()::text, '-', '')")
        .fetch_one(&admin)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}"))).execute(&admin).await.expect("a test database");
    admin.close().await;
    let url = base.replacen("/postgres", &format!("/{name}"), 1);
    downbeat::migrate(&url).await.expect("the engine's tables are created");
    PgPool::connect(&url).await.expect("the test database is reachable")
}

async fn count(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn one_pass_removes_more_old_chains_than_one_batch_holds() {
    let pool = fresh_database().await;
    // Pruning deletes 500 chains per statement; 1,201 needs three statements in the same pass.
    sqlx::query(
        "INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_at, started_by)
         SELECT gen_random_uuid(), 'echoes', 'echo', now() - interval '10 years', 'program'
           FROM generate_series(1, 1201)",
    )
    .execute(&pool)
    .await
    .unwrap();

    downbeat::measure::prune(&pool, definition()).await.unwrap();

    assert_eq!(count(&pool, "SELECT count(*) FROM downbeat.chain").await, 0);
}

#[tokio::test]
async fn pruning_removes_expired_keys_overrides_audit_rows_pauses_and_workers() {
    let pool = fresh_database().await;
    for statement in [
        "INSERT INTO downbeat.idempotency (actor, job_name, key, run_id, created_at) VALUES
            ('local/local', 'echo', 'old', gen_random_uuid(), now() - interval '8 days'),
            ('local/local', 'echo', 'recent', gen_random_uuid(), now() - interval '6 days')",
        "INSERT INTO downbeat.schedule_override (schedule, on_date, change, note, created_by) VALUES
            ('old', (now() - interval '40 days')::date, 'off', 'gone', 'test'),
            ('recent', (now() - interval '10 days')::date, 'off', 'kept', 'test')",
        "INSERT INTO downbeat.audit (at, actor, action, notify_after) VALUES
            (now() - interval '13 months', 'old delivered', 'test', NULL),
            (now() - interval '13 months', 'old undelivered', 'test', now()),
            (now() - interval '11 months', 'recent delivered', 'test', NULL)",
        "INSERT INTO downbeat.pause (kind, name, until, paused_by, note) VALUES
            ('job', 'ended', now() - interval '1 minute', 'test', 'over'),
            ('job', 'later', now() + interval '1 day', 'test', 'still on'),
            ('job', 'until_resumed', NULL, 'test', 'still on')",
        "INSERT INTO downbeat.worker (worker_id, role, build, last_seen_at) VALUES
            ('gone', 'worker', 'test', now() - interval '25 hours'),
            ('seen', 'worker', 'test', now() - interval '1 hour')",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }

    downbeat::measure::prune(&pool, definition()).await.unwrap();

    let keys: Vec<String> = sqlx::query_scalar("SELECT key FROM downbeat.idempotency").fetch_all(&pool).await.unwrap();
    assert_eq!(keys, ["recent"], "idempotency keys go after seven days");
    let overrides: Vec<String> =
        sqlx::query_scalar("SELECT schedule FROM downbeat.schedule_override").fetch_all(&pool).await.unwrap();
    assert_eq!(overrides, ["recent"], "overrides go 30 days after their date");
    let mut audit: Vec<String> =
        sqlx::query_scalar("SELECT actor FROM downbeat.audit WHERE action = 'test'").fetch_all(&pool).await.unwrap();
    audit.sort();
    assert_eq!(audit, ["old undelivered", "recent delivered"], "delivered audit rows go after a year");
    let mut pauses: Vec<String> = sqlx::query_scalar("SELECT name FROM downbeat.pause").fetch_all(&pool).await.unwrap();
    pauses.sort();
    assert_eq!(pauses, ["later", "until_resumed"], "pauses that have ended go");
    let workers: Vec<String> =
        sqlx::query_scalar("SELECT worker_id FROM downbeat.worker").fetch_all(&pool).await.unwrap();
    assert_eq!(workers, ["seen"], "workers unseen for a day go");
}
