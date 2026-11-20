//! Signing in to the console through a real OpenID Connect provider: Dex, run as a local process
//! with an in-memory store and one password user. Skipped unless `DOWNBEAT_TEST_DEX` names a Dex
//! binary, for example `$(nix build --no-link --print-out-paths nixpkgs#dex-oidc)/bin/dex`.

mod support;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use support::{Database, SOON};

/// Dex's example password hash: the password is "password".
const PASSWORD_HASH: &str = "$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W";

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn wait_until_up(url: &str) {
    let deadline = tokio::time::Instant::now() + SOON;
    while reqwest::get(url).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "{url} did not come up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn a_person_signs_in_through_dex() {
    let Ok(dex) = std::env::var("DOWNBEAT_TEST_DEX") else {
        eprintln!("skipped: set DOWNBEAT_TEST_DEX to a Dex binary to run this test");
        return;
    };
    let database = Database::fresh().await;
    let key = downbeat_client::PrivateKey::generate();
    let key_file = std::env::temp_dir().join(format!("console-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&key_file, key.to_pem()).unwrap();
    let callers = format!(
        "audience = \"test\"\n[[caller]]\nname = \"console\"\nkeys = [\"{}\"]\nmax_scope = \"administer\"\nacts_for_people = true\n",
        key.public().to_base64()
    );
    let (_app, scheduler) = support::with_callers(&database, &callers).await;

    let (dex_port, console_port) = (free_port(), free_port());
    let issuer = format!("http://127.0.0.1:{dex_port}/dex");
    let console = format!("http://127.0.0.1:{console_port}");
    let dex_config = format!(
        r#"issuer: {issuer}
storage:
  type: memory
web:
  http: 127.0.0.1:{dex_port}
oauth2:
  skipApprovalScreen: true
enablePasswordDB: true
staticPasswords:
  - email: "asha@example.com"
    hash: "{PASSWORD_HASH}"
    username: "asha"
    userID: "08a8684b-db88-4b73-90a9-3cd1661f5466"
staticClients:
  - id: console
    name: downbeat console
    secret: console-secret
    redirectURIs: ["{console}/auth/callback"]
"#
    );
    let dex_file = std::env::temp_dir().join(format!("dex-{}.yaml", uuid::Uuid::new_v4()));
    std::fs::write(&dex_file, dex_config).unwrap();
    let _dex = Process(
        Command::new(dex).arg("serve").arg(&dex_file).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap(),
    );
    wait_until_up(&format!("{issuer}/.well-known/openid-configuration")).await;

    let console_config = format!(
        r#"listen = "127.0.0.1:{console_port}"
public_url = "{console}"
first_administrators = ["asha@example.com"]

[sign_in]
issuer = "{issuer}"
client_id = "console"
client_secret_env = "TEST_CLIENT_SECRET"
allowed_domains = ["example.com"]

[[scheduler]]
name = "test"
url = "{scheduler}"
audience = "test"
key_file = "{key}"
"#,
        key = key_file.display()
    );
    let console_file = std::env::temp_dir().join(format!("console-{}.toml", uuid::Uuid::new_v4()));
    std::fs::write(&console_file, console_config).unwrap();
    let binary =
        std::env::current_exe().unwrap().parent().and_then(std::path::Path::parent).unwrap().join("downbeat-console");
    let _console = Process(
        Command::new(binary)
            .args(["--config", &console_file.display().to_string()])
            .env("DOWNBEAT_CONSOLE_DATABASE_URL", &database.url)
            .env("TEST_CLIENT_SECRET", "console-secret")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_until_up(&format!("{console}/api/me")).await;

    // The browser's part, by hand: to Dex, through its login form, and back with a code.
    let http =
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).cookie_store(true).build().unwrap();
    let mut location = format!("{console}/auth/login?return=/runs");
    let mut session = None;
    for _ in 0..10 {
        if location == "/runs" {
            break;
        }
        let url = if location.starts_with('/') && !location.starts_with("//") {
            let origin =
                if location.starts_with("/dex") { format!("http://127.0.0.1:{dex_port}") } else { console.clone() };
            format!("{origin}{location}")
        } else {
            location.clone()
        };
        let response = http.get(&url).send().await.unwrap();
        if let Some(cookie) = response
            .headers()
            .get("set-cookie")
            .and_then(|value| value.to_str().ok())
            .filter(|value| value.starts_with("downbeat_session="))
        {
            session = Some(cookie.split(';').next().unwrap().to_owned());
        }
        if response.status().is_redirection() {
            location = response.headers()["location"].to_str().unwrap().to_owned();
            continue;
        }
        // Dex's login form: post the password user's credentials to it.
        let page = response.text().await.unwrap();
        assert!(page.contains("password"), "expected Dex's login form at {url}");
        let posted =
            http.post(&url).form(&[("login", "asha@example.com"), ("password", "password")]).send().await.unwrap();
        assert!(posted.status().is_redirection(), "Dex refused the login: {}", posted.status());
        location = posted.headers()["location"].to_str().unwrap().to_owned();
        if location.contains("/auth/callback") || location == "/runs" {
            break;
        }
    }
    // Follow the remaining redirects back to the console.
    for _ in 0..5 {
        if session.is_some() || location == "/runs" {
            break;
        }
        let url =
            if location.starts_with('/') { format!("http://127.0.0.1:{dex_port}{location}") } else { location.clone() };
        let response = http.get(&url).send().await.unwrap();
        if let Some(cookie) = response
            .headers()
            .get("set-cookie")
            .and_then(|value| value.to_str().ok())
            .filter(|value| value.starts_with("downbeat_session="))
        {
            session = Some(cookie.split(';').next().unwrap().to_owned());
        }
        match response.headers().get("location") {
            Some(next) => location = next.to_str().unwrap().to_owned(),
            None => break,
        }
    }
    assert_eq!(location, "/runs", "the browser ends where it asked to go");
    let session = session.expect("the console set a session after Dex signed the person in");

    let me: Value = reqwest::Client::new()
        .get(format!("{console}/api/me"))
        .header("cookie", &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["person"]["email"], "asha@example.com");
    assert_eq!(me["schedulers"][0]["scope"], "administer");
    let jobs = reqwest::Client::new()
        .get(format!("{console}/api/schedulers/test/v1/jobs"))
        .header("cookie", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(jobs.status(), 200);
}
