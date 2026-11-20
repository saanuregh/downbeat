//! A command that runs past its timeout takes every process it started with it.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, finished};

#[tokio::test]
async fn a_timed_out_command_leaves_no_helper_process_behind() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let app = App::start(&database);
    let pid_file = app.finished_dir.join("helper.pid");

    let run = client.start(times_out_with_a_helper, Text { text: pid_file.display().to_string() }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "failed", "{}", done.reason);
    assert_eq!(done.failed_because.as_deref(), Some("timeout"));

    let pid = std::fs::read_to_string(&pid_file).unwrap().trim().to_owned();
    let proc_entry = std::path::PathBuf::from(format!("/proc/{pid}"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while proc_entry.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let survived = proc_entry.exists();
    if survived {
        let _ = std::process::Command::new("kill").args(["-KILL", &pid]).status();
    }
    assert!(!survived, "the helper {pid} outlived its command's timeout");
}
