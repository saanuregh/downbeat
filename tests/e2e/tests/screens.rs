//! Every screen of the console renders against a real scheduler, in a real browser: each route
//! shows its heading and its data, and none shows an error. Runs when a Chromium-based browser is
//! found (`DOWNBEAT_TEST_BROWSER`, or `chromium`, `chromium-browser` or `google-chrome` on `PATH`);
//! otherwise it reports itself skipped.

mod support;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use downbeat_e2e::*;
use support::{Database, SOON, finished, finished_job, with_api_at};

fn browser() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("DOWNBEAT_TEST_BROWSER") {
        return Some(PathBuf::from(path));
    }
    let path = std::env::var_os("PATH")?;
    ["chromium", "chromium-browser", "google-chrome"]
        .iter()
        .flat_map(|name| std::env::split_paths(&path).map(move |dir| dir.join(name)))
        .find(|candidate| candidate.is_file())
}

fn console_binary() -> PathBuf {
    let test = std::env::current_exe().unwrap();
    test.parent().and_then(std::path::Path::parent).unwrap().join("downbeat-console")
}

struct Console(Child);

impl Drop for Console {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The page as the browser builds it, once the screens have fetched their data.
async fn rendered(browser: &Path, url: String) -> String {
    let browser = browser.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let output = Command::new(browser)
            .args(["--headless=new", "--no-sandbox", "--disable-gpu", "--virtual-time-budget=8000", "--dump-dom"])
            .arg(&url)
            .stderr(Stdio::null())
            .output()
            .expect("the browser runs");
        String::from_utf8_lossy(&output.stdout).into_owned()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn every_screen_renders_its_data_without_an_error() {
    let Some(browser) = browser() else {
        eprintln!("skipped: no Chromium-based browser found; set DOWNBEAT_TEST_BROWSER");
        return;
    };
    let database = Database::fresh().await;
    let (_app, client, scheduler) = with_api_at(&database).await;
    let port = support::free_port();
    let listen = format!("127.0.0.1:{port}");
    let _console = Console(
        Command::new(console_binary())
            .args(["--local", "--scheduler", &format!("http://{scheduler}"), "--listen", &listen])
            .env("DOWNBEAT_CONSOLE_DATABASE_URL", &database.url)
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .spawn()
            .expect("the console starts"),
    );
    let base = format!("http://{listen}");
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(format!("{base}/api/me")).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "the console did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Something to show: a chain of echo and its children.
    let started = client.start("echo", &Text { text: "on screen".into() }).send().await.unwrap();
    let run = finished(&database.pool, started.run_id, SOON).await;
    finished_job(&database.pool, "both", SOON).await;

    let screens: [(String, &[&str]); 12] = [
        ("/s/local".into(), &["Overview", "Failing", "Paused"]),
        ("/s/local/calendar".into(), &["Calendar", "Timeline"]),
        ("/s/local/pipelines".into(), &["Pipelines", "basics", "latest chain began"]),
        ("/s/local/pipelines/basics".into(), &["Jobs and links", "Recent chains", "shout"]),
        ("/s/local/jobs".into(), &["Jobs", "echo", "Succeeded."]),
        ("/s/local/jobs/echo".into(), &["What starts it", "Recent runs", "Input"]),
        ("/s/local/runs".into(), &["Runs", "Succeeded.", "echo"]),
        (format!("/s/local/runs/{}", run.run_id), &["echo", "Input", "Output", "on screen"]),
        (format!("/s/local/chains/{}", run.chain_id), &["Job by job", "shout", "both"]),
        ("/s/local/queues".into(), &["Queues", "default"]),
        ("/s/local/audit".into(), &["Audit", "start"]),
        ("/access".into(), &["Access", "Grants", "People"]),
    ];
    for (path, expected) in screens {
        let page = rendered(&browser, format!("{base}{path}")).await;
        assert!(!page.contains("screens were not built"), "the console was built without its screens");
        for text in expected {
            assert!(page.contains(text), "{path} does not show {text:?}");
        }
        assert!(!page.contains("note-bad"), "{path} shows an error");
    }
}
