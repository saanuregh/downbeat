//! The product binary's command line: start roles, check the definition, run one job, health.

use crate::process_error::ProcessError;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use tokio_util::sync::CancellationToken;

use crate::definition::Definition;
use crate::registry::Registry;
use crate::store::Store;
use crate::worker::Shutdown;

#[derive(Debug, Parser)]
#[command(about = "Runs this product's jobs with downbeat.", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fire schedules, drive backfills, deliver callbacks and serve the control API. Write
    /// `scheduler worker` to run jobs in the same process.
    Scheduler {
        /// `worker`, to also run the worker role in this process.
        #[arg(value_parser = ["worker"])]
        also: Option<String>,
        /// With `worker`: queues and slots, such as `heavy=1,default=8`. Default: `default=4`.
        #[arg(long, value_parser = parse_queues, requires = "also")]
        queues: Option<Queues>,
        #[command(flatten)]
        api: ApiArgs,
        #[command(flatten)]
        common: Common,
    },
    /// Claim and run jobs from the named queues.
    Worker {
        #[command(flatten)]
        worker: WorkerArgs,
        #[command(flatten)]
        common: Common,
    },
    /// Build the definition as a real start would, print every pipeline, and fail on problems.
    Check {
        /// Write the pipelines to this file, as a stable text to commit.
        #[arg(long)]
        snapshot: Option<PathBuf>,
        /// Fail when this committed snapshot differs from the definition.
        #[arg(long)]
        verify_snapshot: Option<PathBuf>,
    },
    /// Call one job's function in this process: no supervisor, no history and no limits.
    RunOnce {
        job: String,
        /// The input, as JSON.
        #[arg(long)]
        input: Option<String>,
        #[arg(long, env = "DOWNBEAT_ENVIRONMENT")]
        environment: Option<String>,
    },
    /// Succeed when a worker on this machine has refreshed its row within two heartbeats.
    Health {
        #[command(flatten)]
        common: Common,
    },
}

#[derive(Debug, Args)]
struct Common {
    /// The product's Postgres; the engine's tables are in its `downbeat` schema.
    #[arg(long, env = "DOWNBEAT_DATABASE_URL")]
    database_url: String,
    /// Which of the definition's environments this is; it selects the schedules.
    #[arg(long, env = "DOWNBEAT_ENVIRONMENT")]
    environment: Option<String>,
}

#[derive(Debug, Args)]
struct WorkerArgs {
    /// Queues and slots: `heavy=1,default=8`.
    #[arg(long, value_parser = parse_queues, default_value = "default=4")]
    queues: Queues,
}

#[derive(Debug, Clone)]
struct Queues(Vec<(String, u32)>);

#[derive(Debug, Args)]
struct ApiArgs {
    /// Where to serve the control API. Off when unset.
    #[arg(long, env = "DOWNBEAT_LISTEN")]
    listen: Option<SocketAddr>,
    /// The file listing trusted callers and this scheduler's audience.
    #[arg(long, env = "DOWNBEAT_CALLERS")]
    callers: Option<PathBuf>,
    /// Trust the development key and listen on this machine only. For trying downbeat out.
    #[arg(long)]
    local: bool,
}

fn parse_queues(text: &str) -> Result<Queues, String> {
    let mut queues = Vec::new();
    for part in text.split(',').filter(|part| !part.trim().is_empty()) {
        let (name, slots) =
            part.split_once('=').ok_or_else(|| format!("'{part}' should be name=slots, such as default=4"))?;
        let slots: u32 = slots.trim().parse().map_err(|_| format!("'{slots}' is not a number of slots"))?;
        if slots == 0 {
            return Err(format!("queue {name} needs at least one slot"));
        }
        queues.push((name.trim().to_owned(), slots));
    }
    Ok(Queues(queues))
}

/// Runs the command line given in `args` (the first item is the program's name).
pub(crate) fn run(args: Vec<OsString>, definition: fn() -> Definition) -> ExitCode {
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            let _ = error.print();
            return if error.use_stderr() { ExitCode::from(2) } else { ExitCode::SUCCESS };
        }
    };
    init_logging();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a tokio runtime");
    match runtime.block_on(dispatch(cli.command, definition)) {
        Ok(code) => code,
        Err(error) => {
            tracing::error!(%error, "stopped");
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

pub(crate) fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into());
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    let _ = if std::env::var("DOWNBEAT_LOG_FORMAT").is_ok_and(|format| format == "json") {
        builder.json().try_init()
    } else {
        builder.try_init()
    };
}

async fn dispatch(command: Command, definition: fn() -> Definition) -> Result<ExitCode, ProcessError> {
    match command {
        Command::Check { snapshot, verify_snapshot } => Ok(crate::check::run(definition(), snapshot, verify_snapshot)),
        Command::RunOnce { job, input, environment } => {
            crate::check::run_once(definition(), environment.as_deref(), &job, input.as_deref()).await
        }
        Command::Health { common } => health(&common, definition()).await,
        Command::Worker { worker, common } => serve(definition, &common, Some(worker.queues.0), None).await,
        Command::Scheduler { also, queues, api, common } => {
            let queues = also.map(|_| queues.map_or_else(|| vec![("default".to_owned(), 4)], |queues| queues.0));
            serve(definition, &common, queues, Some(api)).await
        }
    }
}

fn build_registry(definition: Definition, environment: Option<&str>) -> Result<Arc<Registry>, crate::Error> {
    Ok(Arc::new(Registry::build_or_explain(definition, environment)?))
}

/// Starts the scheduler role, the worker role, or both, until SIGTERM or Ctrl-C.
async fn serve(
    definition: fn() -> Definition,
    common: &Common,
    queues: Option<Vec<(String, u32)>>,
    api: Option<ApiArgs>,
) -> Result<ExitCode, ProcessError> {
    let registry = build_registry(definition(), common.environment.as_deref())?;
    let slots: u32 = queues.iter().flatten().map(|(_, slots)| *slots).sum();
    let store = Store::connect(&common.database_url, 4 + slots * 2).await?;
    let shutdown = Shutdown::default();
    let stop_scheduler = CancellationToken::new();
    tokio::spawn(wait_for_signal(shutdown.clone(), stop_scheduler.clone()));

    let mut roles = tokio::task::JoinSet::new();
    let (pool, stop) = (store.pool.clone(), stop_scheduler.clone());
    let reaper_registry = Arc::clone(&registry);
    roles.spawn(async move {
        crate::reaper::serve(pool, reaper_registry, stop).await;
        Ok::<(), ProcessError>(())
    });
    if let Some(api) = api {
        let config = crate::api::Config::load(api.listen, api.callers.as_deref(), api.local)?;
        if let Some(config) = config {
            roles.spawn(crate::api::serve(store.pool.clone(), Arc::clone(&registry), config, stop_scheduler.clone()));
        }
        let scheduler = crate::scheduler::serve(store.pool.clone(), Arc::clone(&registry), stop_scheduler.clone());
        roles.spawn(async move { Ok(scheduler.await?) });
    }
    if let Some(queues) = queues {
        let worker = crate::worker::serve(store.pool.clone(), Arc::clone(&registry), queues, shutdown.clone());
        roles.spawn(async move { Ok(worker.await?) });
    }
    while let Some(ended) = roles.join_next().await {
        ended??;
    }
    Ok(ExitCode::SUCCESS)
}

/// SIGTERM or Ctrl-C: the worker stops claiming and finishes as `drain` allows, while the scheduler
/// and reaper stop at once.
async fn wait_for_signal(shutdown: Shutdown, stop_scheduler: CancellationToken) {
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    tracing::info!("stopping");
    shutdown.stop_claiming.cancel();
    // The worker decides when to stop its runs; the scheduler and reaper stop now.
    stop_scheduler.cancel();
}

async fn health(common: &Common, definition: Definition) -> Result<ExitCode, ProcessError> {
    let registry = build_registry(definition, common.environment.as_deref())?;
    let options: sqlx::postgres::PgConnectOptions = common.database_url.parse()?;
    let pool = sqlx::postgres::PgPoolOptions::new().max_connections(1).connect_with(options).await?;
    // The host is the worker id without its last two parts (see `worker_id`), compared whole.
    let fresh: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM downbeat.worker
                         WHERE role = 'worker' AND regexp_replace(worker_id, '-[0-9]+-[0-9a-f]{4}$', '') = $1
                           AND last_seen_at > now() - $2)",
    )
    .bind(crate::worker::host_name())
    .bind(crate::store::interval(registry.settings.heartbeat * 2))
    .fetch_one(&pool)
    .await?;
    Ok(if fresh { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}
