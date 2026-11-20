//! Two jobs with the same name are refused by `check`. A separate test binary, because jobs
//! register for the whole binary they are compiled into.

use downbeat::prelude::*;

#[job]
async fn report(_run: Run) -> Result<()> {
    Ok(())
}

#[job(name = "report")]
async fn report_again(_run: Run) -> Result<()> {
    Ok(())
}

fn reports(pipeline: &mut Pipeline) {
    pipeline.job(report);
    pipeline.job(report_again);
}

#[test]
fn two_jobs_with_the_same_name_are_refused() {
    let mut definition = Definition::new().zone("UTC");
    definition.pipeline("reports", reports);
    let problems = downbeat::check_problems(definition);
    assert!(
        problems.iter().any(|problem| problem.starts_with("report: two jobs in this binary have this name")),
        "{problems:#?}"
    );
}
