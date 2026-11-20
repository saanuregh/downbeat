//! A worker keeps a run's lease with a heartbeat every `heartbeat`, exactly, even when that is
//! less than a second.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::runtime_rules::naps_on_the_quick_queue;
use downbeat_e2e::{Number, definition};
use support::{App, Database, SOON, finished};

#[tokio::test]
async fn a_half_second_heartbeat_renews_the_lease_twice_a_second() {
    let database = Database::fresh().await;
    // Counts every renewal of a running run's lease.
    sqlx::raw_sql(
        "CREATE TABLE renewals (at timestamptz NOT NULL DEFAULT clock_timestamp());
         CREATE FUNCTION count_renewal() RETURNS trigger AS $$
           BEGIN
             IF NEW.state = 'running' AND OLD.state = 'running'
                AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at THEN
               INSERT INTO renewals DEFAULT VALUES;
             END IF;
             RETURN NEW;
           END $$ LANGUAGE plpgsql;
         CREATE TRIGGER count_renewal AFTER UPDATE ON downbeat.run FOR EACH ROW EXECUTE FUNCTION count_renewal();",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["scheduler", "worker", "--queues", "quick=1"], "test");

    // The `quick` queue's heartbeat is half a second and its lease two seconds.
    let run = client.start(naps_on_the_quick_queue, Number { n: 4000 }).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let renewals: i64 = sqlx::query_scalar("SELECT count(*) FROM renewals").fetch_one(&database.pool).await.unwrap();
    // About eight in four seconds; a one-second rhythm would give about four.
    assert!(renewals >= 6, "{renewals} renewals in four seconds");
}
