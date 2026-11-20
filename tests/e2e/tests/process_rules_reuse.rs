//! When a reused child is replaced (spec 10.4): after a call that ended in a panic, a crash, a
//! timeout, a memory kill, a stop or a shutdown; and `fresh_process` has no effect on a command.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::process_rules::fresh_command;
use downbeat_e2e::{
    Number, Seconds, Text, aborts, child_pid, definition, hungry, naps, panics, sleeps, stops_when_asked,
};
use support::{App, Database, Finished, SOON, eventually, finished};
use uuid::Uuid;

/// A worker with one slot, so every run uses the same warm child until it is replaced.
struct OneSlot {
    database: Database,
    client: InAppClient,
    app: App,
}

impl OneSlot {
    async fn start() -> OneSlot {
        let database = Database::fresh().await;
        let client = InAppClient::connect(definition, &database.url).await.unwrap();
        let app = App::with(&database, &["scheduler", "worker", "--queues", "default=1"], "test");
        OneSlot { database, client, app }
    }

    /// The id of the child the next run is called in.
    async fn child(&self) -> u32 {
        let run = self.client.start(child_pid, Number { n: 1 }).await.unwrap().run_id();
        let done = self.finished(run).await;
        assert_eq!(done.status, "succeeded", "{}", done.reason);
        u32::try_from(done.output.unwrap().as_u64().unwrap()).unwrap()
    }

    async fn finished(&self, run: Uuid) -> Finished {
        finished(&self.database.pool, run, Duration::from_secs(30)).await
    }

    async fn until_called(&self, run: Uuid) {
        eventually(
            &self.database.pool,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND function_started_at IS NOT NULL)"
            ),
            SOON,
        )
        .await;
    }
}

async fn replaced_after(slot: &OneSlot, start: impl AsyncFnOnce(&OneSlot) -> Uuid, ended: (&str, Option<&str>)) {
    let before = slot.child().await;
    assert_eq!(slot.child().await, before, "an ordinary run reuses the child");
    let run = start(slot).await;
    let done = slot.finished(run).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ended, "{}", done.reason);
    assert_ne!(slot.child().await, before, "the child was replaced");
}

#[tokio::test]
async fn a_child_is_replaced_after_a_panic() {
    let slot = OneSlot::start().await;
    let start = async |slot: &OneSlot| slot.client.start(panics, ()).await.unwrap().run_id();
    replaced_after(&slot, start, ("failed", Some("panic"))).await;
}

#[tokio::test]
async fn a_child_is_replaced_after_a_crash() {
    let slot = OneSlot::start().await;
    let start = async |slot: &OneSlot| slot.client.start(aborts, ()).await.unwrap().run_id();
    replaced_after(&slot, start, ("failed", Some("crash"))).await;
}

#[tokio::test]
async fn a_child_is_replaced_after_a_timeout() {
    let slot = OneSlot::start().await;
    let start = async |slot: &OneSlot| slot.client.start(sleeps, Seconds { seconds: 5 }).await.unwrap().run_id();
    replaced_after(&slot, start, ("failed", Some("timeout"))).await;
}

#[tokio::test]
async fn a_child_is_replaced_after_a_memory_kill() {
    let slot = OneSlot::start().await;
    let start = async |slot: &OneSlot| slot.client.start(hungry, ()).await.unwrap().run_id();
    replaced_after(&slot, start, ("failed", Some("out_of_memory"))).await;
}

#[tokio::test]
async fn a_child_is_replaced_after_a_stop() {
    let slot = OneSlot::start().await;
    let marker = std::env::temp_dir().join(format!("asked-{}", Uuid::new_v4()));
    let text = marker.display().to_string();
    // The function returns as soon as it is asked to stop, so its child survives the call.
    let start = async |slot: &OneSlot| {
        let run = slot.client.start(stops_when_asked, Text { text }).await.unwrap().run_id();
        slot.until_called(run).await;
        sqlx::query("UPDATE downbeat.run SET cancel_requested_by = 'test' WHERE run_id = $1")
            .bind(run)
            .execute(&slot.database.pool)
            .await
            .unwrap();
        run
    };
    replaced_after(&slot, start, ("cancelled", None)).await;
    let asked = marker.exists();
    let _ = std::fs::remove_file(&marker);
    assert!(asked, "the function was asked to stop and returned by itself");
}

#[tokio::test]
async fn a_child_does_not_outlive_a_shutdown() {
    let slot = OneSlot::start().await;
    let before = slot.child().await;
    let run = slot.client.start(naps, Number { n: 10_000 }).await.unwrap().run_id();
    slot.until_called(run).await;
    let OneSlot { database, app, .. } = slot;
    app.stop();

    let state: String = sqlx::query_scalar("SELECT state FROM downbeat.run WHERE run_id = $1")
        .bind(run)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(state, "queued", "the run was interrupted");
    // The worker exits at a shutdown, so the only child it could reuse is the one that ran the
    // interrupted call: it must be gone.
    let proc_entry = format!("/proc/{before}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while std::path::Path::new(&proc_entry).exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let survived = std::path::Path::new(&proc_entry).exists();
    if survived {
        let _ = std::process::Command::new("kill").args(["-KILL", &before.to_string()]).status();
    }
    assert!(!survived, "the child {before} outlived the shutdown");
}

#[tokio::test]
async fn fresh_process_has_no_effect_on_a_command() {
    let slot = OneSlot::start().await;
    let before = slot.child().await;
    let namer_file = slot.app.finished_dir.join("namer.pid");

    let run = slot.client.start(fresh_command, Text { text: namer_file.display().to_string() }).await.unwrap();
    let done = slot.finished(run.run_id()).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let namer: u32 = std::fs::read_to_string(&namer_file).unwrap().trim().parse().unwrap();
    assert_eq!(namer, before, "the command named its program in the slot's warm child");
    assert_eq!(slot.child().await, before, "and the warm child is kept");
}
