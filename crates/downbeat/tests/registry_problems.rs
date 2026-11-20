//! `check` reports each registry problem a definition can have, naming the job or schedule.
//! Every test builds its own definition; the jobs a test leaves out only add "no pipeline names
//! this job" problems, which the assertions ignore.

use std::time::Duration;

use downbeat::prelude::*;

#[job]
async fn plain(_run: Run) -> Result<u32> {
    Ok(1)
}

#[job]
async fn other(_run: Run) -> Result<u32> {
    Ok(2)
}

#[job]
async fn takes_number(_run: Run, n: u32) -> Result<u32> {
    Ok(n)
}

#[job]
async fn takes_pair(_run: Run, (a, b): (u32, u32)) -> Result<u32> {
    Ok(a + b)
}

#[job]
async fn never_named(_run: Run) -> Result<u32> {
    Ok(3)
}

/// A pipeline's name and the function that describes it.
type NamedPipeline<'a> = (&'a str, fn(&mut Pipeline));

fn problems_of(pipelines: &[NamedPipeline<'_>]) -> Vec<String> {
    let mut definition = Definition::new().zone("UTC");
    for (name, pipeline) in pipelines {
        definition.pipeline(name, *pipeline);
    }
    downbeat::check_problems(definition)
}

fn assert_reported(problems: &[String], expected: &str) {
    assert!(problems.iter().any(|problem| problem.starts_with(expected)), "missing {expected:?} in {problems:#?}");
}

fn only_plain(pipeline: &mut Pipeline) {
    pipeline.job(plain);
}

fn only_other(pipeline: &mut Pipeline) {
    pipeline.job(other);
}

#[test]
fn two_pipelines_with_one_name_are_reported() {
    let problems = problems_of(&[("reports", only_plain), ("reports", only_other)]);
    assert_reported(&problems, "reports: two pipelines have this name");
}

#[test]
fn a_job_in_two_pipelines_is_reported() {
    let problems = problems_of(&[("first", only_plain), ("second", only_plain)]);
    assert_reported(&problems, "plain: named in pipeline 'first' and again in 'second'");
}

fn shared_schedule_name(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(every(Duration::from_secs(60)).named("hourly"));
    pipeline.job(other).schedule(every(Duration::from_secs(60)).named("hourly"));
}

#[test]
fn two_schedules_with_one_name_are_reported() {
    let problems = problems_of(&[("p", shared_schedule_name)]);
    assert_reported(&problems, "hourly: two schedules have this name");
}

fn link_to_unknown(pipeline: &mut Pipeline) {
    pipeline.job(takes_number).after(never_named);
}

#[test]
fn a_link_to_a_job_in_no_pipeline_is_reported() {
    let problems = problems_of(&[("p", link_to_unknown)]);
    assert_reported(&problems, "takes_number: its parent 'never_named' is named in no pipeline");
}

fn parent_twice(pipeline: &mut Pipeline) {
    pipeline.job(plain);
    pipeline.job(takes_pair).after((plain, plain));
}

#[test]
fn a_parent_listed_twice_is_reported() {
    let problems = problems_of(&[("p", parent_twice)]);
    assert_reported(&problems, "takes_pair: a parent is listed twice");
}

fn done_for_without_input(pipeline: &mut Pipeline) {
    pipeline.job(plain);
    pipeline.job(takes_number).needs(plain.done_for(|_: &u32| ()));
}

#[test]
fn done_for_on_a_job_with_no_input_is_reported() {
    let problems = problems_of(&[("p", done_for_without_input)]);
    assert_reported(&problems, "takes_number: 'plain' takes no input; use done_within(…) instead of done_for(…)");
}

fn too_short_interval(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(every(Duration::from_millis(500)));
}

#[test]
fn an_interval_under_one_second_is_reported() {
    let problems = problems_of(&[("p", too_short_interval)]);
    assert_reported(&problems, "plain: the shortest interval is one second");
}

fn day_zero(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(daily_at(["07:00"]).on_days_of_month([0]));
}

fn day_thirty_two(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(daily_at(["07:00"]).on_days_of_month([1, 32]));
}

#[test]
fn a_day_of_month_outside_1_to_31_is_reported() {
    for pipeline in [day_zero as fn(&mut Pipeline), day_thirty_two] {
        let problems = problems_of(&[("p", pipeline)]);
        assert_reported(&problems, "plain: days of the month run from 1 to 31");
    }
}

fn second_schedule_unnamed(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(daily_at(["07:00"])).schedule(daily_at(["19:00"]));
}

#[test]
fn a_second_schedule_without_a_name_is_reported() {
    let problems = problems_of(&[("p", second_schedule_unnamed)]);
    assert_reported(&problems, "plain: a job's second schedule needs a name");
}

fn empty_daily_at(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(daily_at([]));
}

#[test]
fn an_empty_daily_at_is_reported() {
    let problems = problems_of(&[("p", empty_daily_at)]);
    assert_reported(&problems, "plain: daily_at needs at least one time");
}

fn daily_at_between(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(daily_at(["07:00"]).between("06:00", "08:00"));
}

#[test]
fn between_on_daily_at_is_reported() {
    let problems = problems_of(&[("p", daily_at_between)]);
    assert_reported(&problems, "plain: .between(…) is for every(…)");
}

fn unknown_schedule_zone(pipeline: &mut Pipeline) {
    pipeline.job(plain).schedule(daily_at(["07:00"]).in_zone("Mars/Olympus_Mons"));
}

#[test]
fn an_unknown_zone_is_reported_for_a_schedule_and_for_the_definition() {
    let problems = problems_of(&[("p", unknown_schedule_zone)]);
    assert_reported(&problems, "plain: 'Mars/Olympus_Mons' is not a time zone");

    let mut definition = Definition::new().zone("Mars/Olympus_Mons");
    definition.pipeline("p", only_plain);
    assert_reported(&downbeat::check_problems(definition), "definition: 'Mars/Olympus_Mons' is not a time zone");
}
