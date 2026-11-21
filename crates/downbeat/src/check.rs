//! `check` and `run-once`: the two commands that work without the engine's tables.

use crate::process_error::ProcessError;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use serde_json::Value;

use crate::definition::Definition;
use crate::job::{Body, Outcome, Run};
use crate::registry::Registry;

/// Builds the definition with every schedule, prints it, and writes or verifies the snapshot.
pub(crate) fn run(definition: Definition, snapshot: Option<PathBuf>, verify: Option<PathBuf>) -> ExitCode {
    let registry = match Registry::build(definition, None) {
        Ok(registry) => registry,
        Err(problems) => {
            eprintln!("The definition has {} problem(s):", problems.len());
            for problem in problems {
                eprintln!("  {problem}");
            }
            return ExitCode::FAILURE;
        }
    };
    let text = crate::describe::snapshot(&registry);
    print!("{text}");
    for warning in warnings(&registry) {
        eprintln!("warning: {warning}");
    }
    if let Some(path) = snapshot
        && let Err(error) = std::fs::write(&path, &text)
    {
        eprintln!("could not write {}: {error}", path.display());
        return ExitCode::FAILURE;
    }
    if let Some(path) = verify {
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        if committed != text {
            eprintln!("{} is stale: run `check --snapshot {}` and commit the result", path.display(), path.display());
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

/// Things that are allowed but worth a second look. They do not fail `check`.
fn warnings(registry: &Registry) -> Vec<String> {
    registry
        .jobs
        .values()
        .filter(|job| !matches!(job.spec.policy.turn, crate::job::Turn::SameWork))
        .map(|job| {
            format!(
                "{}: it takes turns ({}); a run that returns not_ready keeps its turn while it waits, holding back the job's other runs",
                job.spec.name,
                job.spec.policy.describe().into_iter().find(|line| line.starts_with("one ")).unwrap_or_default()
            )
        })
        .collect()
}

/// Calls one job's function here, in the foreground.
pub(crate) async fn run_once(
    definition: Definition,
    environment: Option<&str>,
    job: &str,
    input: Option<&str>,
) -> Result<ExitCode, ProcessError> {
    let registry = Registry::build_or_explain(definition, environment)?;
    let registry = Arc::new(registry);
    let found = registry.job(job).ok_or_else(|| crate::Error::UnknownJob(job.to_owned()))?;
    let input: Value = match input {
        Some(text) => serde_json::from_str(text)?,
        None => Value::Null,
    };
    let input = (found.spec.input_ops.normalize)(input, true)
        .map_err(|error| crate::Error::Input(format!("the input does not fit {job}: {error}")))?;
    match &found.spec.body {
        Body::Function(body) => {
            let outcome: Outcome<Value> = body(Run::for_test(), input, Arc::clone(&registry.dependencies)).await.into();
            println!("{outcome:?}");
            Ok(if matches!(outcome, Outcome::Failed(_)) { ExitCode::FAILURE } else { ExitCode::SUCCESS })
        }
        Body::Command { program, .. } => {
            let spec = program(input.clone()).map_err(crate::Error::Input)?;
            println!("starting {} {:?}", spec.program, spec.args);
            let mut command = std::process::Command::new(&spec.program);
            command.args(&spec.args).envs(&spec.env);
            if let Some(dir) = &spec.dir {
                command.current_dir(dir);
            }
            let status =
                command.status().map_err(|source| ProcessError::io(format!("starting {}", spec.program), source))?;
            Ok(if status.success() { ExitCode::SUCCESS } else { ExitCode::FAILURE })
        }
    }
}
