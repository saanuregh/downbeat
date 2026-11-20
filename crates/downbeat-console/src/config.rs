//! The console's configuration: where it listens, how people sign in, and which schedulers it
//! connects to. Secrets (the database URL, the client secret, the signing key) come from the
//! environment or files, never from this file.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use downbeat_api::{DEVELOPMENT_AUDIENCE, PrivateKey, development_key};
use serde::Deserialize;

/// Where `--local` listens.
const LOCAL_LISTEN: &str = "127.0.0.1:7401";

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) listen: SocketAddr,
    /// The address people reach the console at; sign-in redirects back to it.
    pub(crate) public_url: String,
    pub(crate) sign_in: Option<SignIn>,
    pub(crate) first_administrators: Vec<String>,
    pub(crate) schedulers: Vec<Scheduler>,
    pub(crate) session_idle: Duration,
    pub(crate) session_absolute: Duration,
    /// `--local`: no sign-in; one stand-in person who administers everything.
    pub(crate) local: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignIn {
    /// The identity provider's issuer URL; its configuration is discovered from there.
    pub(crate) issuer: String,
    pub(crate) client_id: String,
    /// The environment variable holding the client secret.
    pub(crate) client_secret_env: String,
    /// People with an email in these domains may sign in.
    #[serde(default)]
    pub(crate) allowed_domains: Vec<String>,
    /// People in these provider groups may sign in.
    #[serde(default)]
    pub(crate) allowed_groups: Vec<String>,
    /// The claim of the identity token that lists a person's groups.
    #[serde(default = "groups_claim")]
    pub(crate) groups_claim: String,
}

fn groups_claim() -> String {
    "groups".to_owned()
}

/// A scheduler the console connects to.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    pub(crate) name: String,
    pub(crate) label: String,
    pub(crate) url: String,
    pub(crate) audience: String,
    /// The console's name in that scheduler's callers file.
    pub(crate) caller: String,
    pub(crate) key: PrivateKey,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    listen: SocketAddr,
    public_url: String,
    sign_in: SignIn,
    #[serde(default)]
    first_administrators: Vec<String>,
    #[serde(default = "idle_hours")]
    session_idle_hours: u64,
    #[serde(default = "absolute_hours")]
    session_absolute_hours: u64,
    #[serde(rename = "scheduler")]
    schedulers: Vec<SchedulerEntry>,
}

fn idle_hours() -> u64 {
    12
}

fn absolute_hours() -> u64 {
    7 * 24
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerEntry {
    name: String,
    label: Option<String>,
    url: String,
    audience: String,
    #[serde(default = "console_caller")]
    caller: String,
    key_file: Option<PathBuf>,
    key_env: Option<String>,
}

fn console_caller() -> String {
    "console".to_owned()
}

impl Config {
    pub(crate) fn from_file(path: &std::path::Path) -> anyhow::Result<Config> {
        let text =
            std::fs::read_to_string(path).map_err(|error| anyhow::anyhow!("reading {}: {error}", path.display()))?;
        let file: File = toml::from_str(&text).map_err(|error| anyhow::anyhow!("{}: {error}", path.display()))?;
        anyhow::ensure!(!file.schedulers.is_empty(), "list at least one [[scheduler]]");
        let public_url = reqwest::Url::parse(&file.public_url)
            .map_err(|error| anyhow::anyhow!("public_url {}: {error}", file.public_url))?;
        let loopback = public_url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        anyhow::ensure!(
            public_url.scheme() == "https" || (public_url.scheme() == "http" && loopback),
            "public_url must be https: the session cookie is only sent over an encrypted connection"
        );
        anyhow::ensure!(
            !file.sign_in.allowed_domains.is_empty() || !file.sign_in.allowed_groups.is_empty(),
            "sign_in needs allowed_domains or allowed_groups: otherwise anyone the provider knows could sign in"
        );
        let mut schedulers: Vec<Scheduler> = Vec::new();
        for entry in file.schedulers {
            anyhow::ensure!(
                !schedulers.iter().any(|known| known.name == entry.name),
                "scheduler {} is listed twice; names are used in URLs and grants, so each must be unique",
                entry.name
            );
            let pem = match (&entry.key_file, &entry.key_env) {
                (Some(path), None) => std::fs::read_to_string(path)
                    .map_err(|error| anyhow::anyhow!("reading {}: {error}", path.display()))?,
                (None, Some(variable)) => {
                    std::env::var(variable).map_err(|_| anyhow::anyhow!("{variable} is not set"))?
                }
                _ => anyhow::bail!("scheduler {}: give exactly one of key_file and key_env", entry.name),
            };
            schedulers.push(Scheduler {
                label: entry.label.unwrap_or_else(|| entry.name.clone()),
                name: entry.name,
                url: entry.url.trim_end_matches('/').to_owned(),
                audience: entry.audience,
                caller: entry.caller,
                key: PrivateKey::from_pem(&pem)?,
            });
        }
        Ok(Config {
            listen: file.listen,
            public_url: file.public_url.trim_end_matches('/').to_owned(),
            sign_in: Some(file.sign_in),
            first_administrators: file.first_administrators,
            schedulers,
            session_idle: Duration::from_hours(file.session_idle_hours),
            session_absolute: Duration::from_hours(file.session_absolute_hours),
            local: false,
        })
    }

    /// `--local`: the development key against a scheduler on this machine, no sign-in.
    pub(crate) fn local(listen: Option<SocketAddr>, scheduler_url: &str) -> anyhow::Result<Config> {
        let listen = listen.unwrap_or_else(|| LOCAL_LISTEN.parse().expect("a valid address"));
        anyhow::ensure!(
            listen.ip().is_loopback(),
            "--local listens on this machine only; {listen} is not a loopback address"
        );
        Ok(Config {
            listen,
            public_url: format!("http://{listen}"),
            sign_in: None,
            first_administrators: Vec::new(),
            schedulers: vec![Scheduler {
                name: "local".to_owned(),
                label: "This machine".to_owned(),
                url: scheduler_url.trim_end_matches('/').to_owned(),
                audience: DEVELOPMENT_AUDIENCE.to_owned(),
                caller: "local".to_owned(),
                key: development_key(),
            }],
            session_idle: Duration::from_hours(12),
            session_absolute: Duration::from_hours(7 * 24),
            local: true,
        })
    }

    pub(crate) fn scheduler(&self, name: &str) -> Option<&Scheduler> {
        self.schedulers.iter().find(|scheduler| scheduler.name == name)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::Config;

    /// Writes a config file with this `public_url` and these scheduler names, each with its own
    /// key file, in a fresh directory, and reads it.
    fn read(public_url: &str, schedulers: &[&str]) -> anyhow::Result<Config> {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "console-config-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let key_file = dir.join("key.pem");
        std::fs::write(&key_file, downbeat_api::PrivateKey::generate().to_pem()).unwrap();
        let mut text = format!(
            "listen = \"127.0.0.1:7401\"\npublic_url = \"{public_url}\"\n\n[sign_in]\nissuer = \"https://id.example.com\"\n\
             client_id = \"console\"\nclient_secret_env = \"CONSOLE_SECRET\"\nallowed_domains = [\"example.com\"]\n"
        );
        for name in schedulers {
            write!(
                text,
                "\n[[scheduler]]\nname = \"{name}\"\nurl = \"https://{name}.example.com\"\naudience = \"{name}\"\n\
                 key_file = \"{}\"\n",
                key_file.display()
            )
            .unwrap();
        }
        let path = dir.join("console.toml");
        std::fs::write(&path, text).unwrap();
        let config = Config::from_file(&path);
        std::fs::remove_dir_all(&dir).unwrap();
        config
    }

    #[test]
    fn public_url_must_be_https_or_http_on_a_loopback_host() {
        for accepted in
            ["https://console.example.com", "http://localhost:7401", "http://127.0.0.1:7401", "http://[::1]:7401"]
        {
            assert!(read(accepted, &["prod"]).is_ok(), "{accepted} is refused");
        }
        for refused in ["http://console.example.com", "http://10.0.0.5:7401", "ftp://console.example.com"] {
            let error = read(refused, &["prod"]).expect_err(refused).to_string();
            assert!(error.contains("public_url must be https"), "{refused}: {error}");
        }
    }

    #[test]
    fn a_scheduler_listed_twice_is_refused() {
        assert!(read("https://console.example.com", &["prod", "uat"]).is_ok());
        let error = read("https://console.example.com", &["prod", "prod"]).expect_err("a duplicate").to_string();
        assert!(error.contains("scheduler prod is listed twice"), "{error}");
    }
}
