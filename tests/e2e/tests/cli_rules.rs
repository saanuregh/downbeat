//! The test product's command line refuses a start it cannot serve safely: an environment the
//! definition does not list, an API with no trusted callers or open beyond this machine in
//! `--local` mode, and a callers file that is wrong.

mod support;

use std::process::{Command, Stdio};
use std::time::Duration;

use support::{Database, free_port};
use uuid::Uuid;

/// What a refused start printed, after checking it exited with a failure.
fn refused(database: &Database, args: &[&str], environment: &str) -> String {
    let mut process = Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .args(args)
        .env("DOWNBEAT_DATABASE_URL", &database.url)
        .env("DOWNBEAT_ENVIRONMENT", environment)
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the test app starts");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = process.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() > deadline {
            let _ = process.kill();
            let _ = process.wait();
            panic!("the app started with {args:?} in {environment:?} instead of refusing");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut process.stderr.take().unwrap(), &mut stderr).unwrap();
    assert!(!status.success(), "{args:?} exited successfully: {stderr}");
    stderr
}

/// A callers file with this text, and its path.
fn callers_file(text: &str) -> String {
    let file = std::env::temp_dir().join(format!("callers-{}.toml", Uuid::new_v4()));
    std::fs::write(&file, text).unwrap();
    file.display().to_string()
}

fn caller_entry(name: &str) -> String {
    let key = downbeat_api::PrivateKey::generate().public().to_base64();
    format!("[[caller]]\nname = \"{name}\"\nkeys = [\"{key}\"]\nmax_scope = \"operate\"\n")
}

/// The callers file is refused when the scheduler starts with it.
fn refused_callers(database: &Database, text: &str) -> String {
    let path = callers_file(text);
    let listen = format!("127.0.0.1:{}", free_port());
    let stderr = refused(database, &["scheduler", "--listen", &listen, "--callers", &path], "test");
    let _ = std::fs::remove_file(&path);
    stderr
}

#[tokio::test]
async fn an_environment_the_definition_does_not_list_is_refused_at_start() {
    let database = Database::fresh().await;
    let stderr = refused(&database, &["scheduler", "worker", "--queues", "default=1"], "nowhere");
    assert!(stderr.contains("environment 'nowhere' is not one the definition lists"), "{stderr}");
}

#[tokio::test]
async fn listen_without_callers_is_refused() {
    let database = Database::fresh().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let stderr = refused(&database, &["scheduler", "--listen", &listen], "test");
    assert!(stderr.contains("--listen needs --callers"), "{stderr}");
}

#[tokio::test]
async fn local_with_an_address_beyond_this_machine_is_refused() {
    let database = Database::fresh().await;
    let listen = format!("0.0.0.0:{}", free_port());
    let stderr = refused(&database, &["scheduler", "--local", "--listen", &listen], "test");
    assert!(stderr.contains("is not a loopback address"), "{stderr}");
}

#[tokio::test]
async fn a_callers_file_with_an_unknown_field_is_refused() {
    let database = Database::fresh().await;
    let text = format!("audience = \"test\"\n{}acts_for_everyone = true\n", caller_entry("billing"));
    let stderr = refused_callers(&database, &text);
    assert!(stderr.contains("acts_for_everyone"), "{stderr}");
}

#[tokio::test]
async fn a_caller_with_no_keys_is_refused() {
    let database = Database::fresh().await;
    let text = "audience = \"test\"\n[[caller]]\nname = \"billing\"\nkeys = []\nmax_scope = \"operate\"\n";
    let stderr = refused_callers(&database, text);
    assert!(stderr.contains("caller billing has no keys"), "{stderr}");
}

#[tokio::test]
async fn a_caller_listed_twice_is_refused() {
    let database = Database::fresh().await;
    let text = format!("audience = \"test\"\n{}{}", caller_entry("billing"), caller_entry("billing"));
    let stderr = refused_callers(&database, &text);
    assert!(stderr.contains("caller billing is listed twice"), "{stderr}");
}
