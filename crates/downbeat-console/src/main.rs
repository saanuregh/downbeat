//! The downbeat console: signs people in, decides what each may do, serves the screens, and talks
//! to schedulers through their control API as a trusted caller. It holds no product code and no
//! product credentials.

mod access;
mod api;
mod assets;
mod config;
mod error;
mod proxy;
mod session;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderValue, header};
use axum::routing::{any, delete, get, post};
use clap::{Parser, Subcommand};
use tower_http::set_header::SetResponseHeaderLayer;

use config::Config;
use proxy::PipelineCache;
use session::Provider;

/// What every handler shares.
pub(crate) struct Console {
    pub(crate) pool: sqlx::PgPool,
    pub(crate) config: Config,
    pub(crate) provider: Option<Provider>,
    pub(crate) http: reqwest::Client,
    pub(crate) pipelines: PipelineCache,
    /// In `--local` mode there is no session; the stand-in person uses this CSRF token.
    pub(crate) local_csrf: String,
}

pub(crate) type State = Arc<Console>;

#[derive(Debug, Parser)]
#[command(about = "The downbeat console: sign-in, grants and the screens.", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// The configuration file.
    #[arg(long, env = "DOWNBEAT_CONSOLE_CONFIG", conflicts_with = "local")]
    config: Option<PathBuf>,
    /// Try it out: no sign-in, one stand-in person who administers everything, the development key,
    /// and a scheduler on this machine. Listens on this machine only.
    #[arg(long)]
    local: bool,
    /// With --local: the scheduler's address.
    #[arg(long, default_value = "http://127.0.0.1:7400")]
    scheduler: String,
    /// With --local: where to listen.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// The console's own Postgres database.
    #[arg(long, env = "DOWNBEAT_CONSOLE_DATABASE_URL")]
    database_url: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Make a key pair for a caller: the private key (PKCS#8 PEM) is written to the file, the public
    /// key is printed for the scheduler's callers file.
    Keygen {
        /// Where to write the private key. It is created readable by its owner only.
        private_key_file: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a tokio runtime");
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    if let Some(Command::Keygen { private_key_file }) = cli.command {
        return keygen(&private_key_file);
    }
    let config = match (&cli.config, cli.local) {
        (_, true) => Config::local(cli.listen, &cli.scheduler)?,
        (Some(path), false) => Config::from_file(path)?,
        (None, false) => anyhow::bail!("give --config <file>, or --local to try the console out"),
    };
    let database_url = cli
        .database_url
        .ok_or_else(|| anyhow::anyhow!("set DOWNBEAT_CONSOLE_DATABASE_URL to the console's own Postgres database"))?;
    let pool = store::connect(&database_url).await?;
    let provider = match &config.sign_in {
        Some(sign_in) => Some(Provider::discover(sign_in).await?),
        None => None,
    };
    let listen = config.listen;
    let state = Arc::new(Console {
        pool,
        provider,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        pipelines: PipelineCache::default(),
        local_csrf: format!("local-{}", rand::random::<u64>()),
        config,
    });
    tokio::spawn(prune(Arc::clone(&state)));
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| anyhow::anyhow!("listening on {listen}: {error}"))?;
    tracing::info!(%listen, local = state.config.local, "console listening");
    axum::serve(listener, router(state)).with_graceful_shutdown(shutdown()).await?;
    Ok(())
}

fn router(state: State) -> Router {
    let api = Router::new()
        .route("/me", get(api::me))
        .route("/grants", get(api::grants).post(api::add_grant))
        .route("/grants/{id}", delete(api::remove_grant))
        .route("/people", get(api::people))
        .route("/audit", get(api::audit))
        .route("/preferences", get(api::preferences).put(api::save_preferences))
        .route("/schedulers/{name}/{*rest}", any(proxy::forward))
        .layer(axum::middleware::from_fn_with_state(Arc::clone(&state), session::require));
    Router::new()
        .nest("/api", api)
        .route("/auth/login", get(session::login))
        .route("/auth/callback", get(session::callback))
        .route("/auth/logout", post(session::logout))
        .fallback(assets::serve)
        .layer(SetResponseHeaderLayer::overriding(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            ),
        ))
        .layer(SetResponseHeaderLayer::overriding(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")))
        .layer(SetResponseHeaderLayer::overriding(header::REFERRER_POLICY, HeaderValue::from_static("same-origin")))
        .layer(tower_http::compression::CompressionLayer::new())
        .with_state(state)
}

async fn prune(state: State) {
    let mut tick = tokio::time::interval(Duration::from_secs(600));
    loop {
        tick.tick().await;
        if let Err(error) = store::prune(&state.pool, state.config.session_idle).await {
            tracing::warn!(%error, "pruning the console's database failed");
        }
    }
}

async fn shutdown() {
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

fn keygen(path: &std::path::Path) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let key = downbeat_api::PrivateKey::generate();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| anyhow::anyhow!("{}: {error}", path.display()))?;
    file.write_all(key.to_pem().as_bytes())?;
    println!("{}", key.public().to_base64());
    eprintln!("Wrote the private key to {}. Put the public key above in the scheduler's callers file.", path.display());
    Ok(())
}
