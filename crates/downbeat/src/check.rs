//! `check` and `run-once`: the two commands that work without the engine's tables.

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

/// Calls one job's function here, in the foreground.
pub(crate) async fn run_once(definition: Definition, environment: Option<&str>, job: &str, input: Option<&str>) -> anyhow::Result<ExitCode> {
    let registry = Registry::build(definition, environment).map_err(|problems| anyhow::anyhow!("the definition has problems: {problems:?}"))?;
    let registry = Arc::new(registry);
    let found = registry.job(job).ok_or_else(|| anyhow::anyhow!("there is no job named {job}"))?;
    let input: Value = match input {
        Some(text) => serde_json::from_str(text)?,
        None => Value::Null,
    };
    let input = (found.spec.input_ops.normalize)(input, true).map_err(|error| anyhow::anyhow!("the input does not fit {job}: {error}"))?;
    match &found.spec.body {
        Body::Function(body) => {
            let outcome: Outcome<Value> = body(Run::for_test(), input, Arc::clone(&registry.dependencies)).await.into();
            println!("{outcome:?}");
            Ok(if matches!(outcome, Outcome::Failed(_)) { ExitCode::FAILURE } else { ExitCode::SUCCESS })
        }
        Body::Command { program, .. } => {
            let spec = program(input.clone()).map_err(|error| anyhow::anyhow!(error))?;
            println!("starting {} {:?}", spec.program, spec.args);
            let mut command = std::process::Command::new(&spec.program);
            command.args(&spec.args).envs(&spec.env);
            if let Some(dir) = &spec.dir {
                command.current_dir(dir);
            }
            let status = command.status()?;
            Ok(if status.success() { ExitCode::SUCCESS } else { ExitCode::FAILURE })
        }
    }
}
