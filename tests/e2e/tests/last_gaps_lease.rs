//! Rule 9.3: a worker times its lease from when the last successful heartbeat was sent, not from
//! when its reply came, so a slow reply does not stretch the time the worker believes it holds
//! the run.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::runtime_rules::naps_on_the_quick_queue;
use downbeat_e2e::{Number, definition};
use sqlx::{Connection, PgConnection, PgPool};
use support::{App, Database, SOON, eventually, finished};
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

/// Sessions waiting for a row lock, other than this test's own lock holders.
async fn lock_waiters(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
          WHERE datname = current_database() AND wait_event_type = 'Lock' AND query NOT LIKE '%pg_stat_activity%'",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn wait_for_lock_waiters(pool: &PgPool, count: i64) -> Instant {
    let deadline = Instant::now() + SOON;
    while lock_waiters(pool).await < count {
        assert!(Instant::now() < deadline, "{count} sessions never waited for the run's row");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Instant::now()
}

async fn lock_row(connection: &mut PgConnection, run: Uuid) {
    sqlx::query("SELECT 1 FROM downbeat.run WHERE run_id = $1 FOR UPDATE").bind(run).execute(connection).await.unwrap();
}

// The `quick` queue's heartbeat is half a second and its lease two seconds. A heartbeat sent at S
// is held back until S + 1 s, then succeeds. The next one, sent at about S + 1 s, is held back until
// S + 2.5 s. Timed from the send, the lease ran out at S + 2 s, so the worker must stop the call as
// unreachable and the reaper takes the run back (`lost_count = 1`). Timed from the reply, it would
// run out only at S + 3 s, after the second heartbeat succeeded, and the run would finish untouched.
#[tokio::test]
async fn a_slow_heartbeat_reply_does_not_stretch_the_lease() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["worker", "--queues", "quick=1"], "test");
    let run = client.start(naps_on_the_quick_queue, Number { n: 8000 }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'running' AND starts = 1)"
        ),
        SOON,
    )
    .await;

    let mut first_holder = PgConnection::connect(&database.url).await.unwrap();
    let mut transaction = first_holder.begin().await.unwrap();
    lock_row(&mut transaction, run).await;
    let sent = wait_for_lock_waiters(&database.pool, 1).await;
    let lease_before: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT lease_expires_at FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .fetch_one(&database.pool)
            .await
            .unwrap();

    // Queued behind the held heartbeat, so it takes the row the moment that heartbeat commits.
    let url = database.url.clone();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let second_holder = tokio::spawn(async move {
        let mut connection = PgConnection::connect(&url).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();
        lock_row(&mut transaction, run).await;
        released.await.unwrap();
        transaction.commit().await.unwrap();
    });
    wait_for_lock_waiters(&database.pool, 2).await;

    sleep_until(sent + Duration::from_millis(1000)).await;
    transaction.commit().await.unwrap();
    let deadline = Instant::now() + Duration::from_millis(300);
    loop {
        let renewed: bool = sqlx::query_scalar("SELECT lease_expires_at > $2 FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .bind(lease_before)
            .fetch_one(&database.pool)
            .await
            .unwrap();
        if renewed {
            break;
        }
        assert!(Instant::now() < deadline, "the held heartbeat did not succeed once its row was free");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    sleep_until(sent + Duration::from_millis(2500)).await;
    release.send(()).unwrap();
    second_holder.await.unwrap();

    let ended = finished(&database.pool, run, Duration::from_secs(60)).await;
    assert_eq!(ended.status, "succeeded", "{}", ended.reason);
    assert_eq!(ended.lost_count, 1, "the worker kept the call past a lease timed from the last good send");
    eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.audit WHERE action = 'event' AND detail::text LIKE '%unreachable%')",
        SOON,
    )
    .await;
}
