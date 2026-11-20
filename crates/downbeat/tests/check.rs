//! `check` rejects every kind of mistake the compiler cannot catch, naming the job and the fix.

use downbeat::prelude::*;

struct Calendar;
struct Pool;

#[job]
async fn first(_run: Run) -> Result<u32> {
    Ok(1)
}

#[job]
async fn second(_run: Run) -> Result<u32> {
    Ok(2)
}

#[job]
async fn needs_both(_run: Run, (a, b): (u32, u32)) -> Result<()> {
    let _ = (a, b);
    Ok(())
}

#[job]
async fn loop_a(_run: Run, n: u32) -> Result<u32> {
    Ok(n)
}

#[job]
async fn loop_b(_run: Run, n: u32) -> Result<u32> {
    Ok(n)
}

#[job]
async fn uses_pool(_run: Run, _pool: &Pool) -> Result<()> {
    Ok(())
}

#[job]
async fn waits_on_itself(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
async fn forgotten(_run: Run) -> Result<()> {
    Ok(())
}

fn holidays(_date: NaiveDate, _calendar: &Calendar) -> Day {
    Day::Normal
}

fn broken(pipeline: &mut Pipeline) {
    pipeline.job(first).schedule(daily_at(["25:00"])).if_changed();
    pipeline.job(second).schedule(every(1.hours()).in_environments(["prodution"]).by_calendar(holidays));
    pipeline.job(needs_both).after((first, second));
    pipeline.job(loop_a).after(loop_b);
    pipeline.job(loop_b).after(loop_a);
    pipeline.job(uses_pool);
    pipeline.job(waits_on_itself).needs(waits_on_itself.done_within(1.hours()));
}

fn definition() -> Definition {
    let mut definition = Definition::new().environments(["production"]);
    definition.pipeline("broken", broken);
    definition
}

#[test]
fn check_names_each_problem_and_its_fix() {
    let problems = downbeat::check_problems(definition());
    let expect = [
        "definition: no zone is set",
        "first: .if_changed() needs a job that takes an input",
        "first: '25:00' is not a time",
        "second: environment 'prodution' is not one the definition lists",
        "second: its day rule holidays takes &check::Calendar but the definition does not provide one",
        "needs_both: its parents can never end in the same chain",
        "loop_a: its links form a cycle",
        "uses_pool: it takes &check::Pool but the definition does not provide one",
        "forgotten: no pipeline names this job",
        "waits_on_itself: a job cannot need itself",
    ];
    for expected in expect {
        assert!(problems.iter().any(|problem| problem.starts_with(expected)), "missing {expected:?} in {problems:#?}");
    }
}

#[test]
fn check_refuses_shutdown_timings_that_cannot_fit_in_the_stop_window() {
    let settings = downbeat::Settings {
        drain: std::time::Duration::from_secs(30),
        stop_within: std::time::Duration::from_secs(30),
        ..downbeat::Settings::default()
    };
    let problems = downbeat::check_problems(Definition::new().zone("UTC").settings(settings));
    assert!(
        problems.iter().any(|problem| problem.starts_with("settings:") && problem.contains("stop_within")),
        "{problems:#?}"
    );
}
