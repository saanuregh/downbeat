//! A product's view of the API: jobs, commands and pipelines compile as the design writes them,
//! and job functions can be tested directly.

use downbeat::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JobData)]
struct TradingDay(NaiveDate);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JobData)]
struct Archive {
    day: TradingDay,
    sha256: String,
    count: usize,
    #[job_data(not_compared)]
    fetched_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JobData)]
struct Candles {
    day: TradingDay,
    rows: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JobData)]
struct OhlcInput {
    date: NaiveDate,
    broker_id: u32,
}

struct Exchange {
    files: Vec<String>,
}

#[job(attempts = 3, backoff = 30.seconds()..=10.minutes())]
async fn bhav_archive(run: Run, day: TradingDay, exchange: &Exchange) -> Result<Archive> {
    run.progress("downloading");
    if exchange.files.is_empty() {
        return skip("the exchange has not published yet");
    }
    Ok(Archive { day, sha256: exchange.files.join(","), count: exchange.files.len(), fetched_at: "now".into() })
}

#[job(preset = heavy, timeout = 3.hours(), max_memory = 8.gb())]
async fn bhav_candles(_run: Run, archive: Archive) -> Result<Candles> {
    Ok(Candles { day: archive.day, rows: archive.count as u64 * 100 })
}

#[job(one_per = broker_id, retry_on = [Error, Timeout])]
async fn eod_report(_run: Run, input: OhlcInput) -> Result<()> {
    let _ = input;
    Ok(())
}

#[job]
async fn snapshot_candles(_run: Run, day: TradingDay) -> Result<Candles> {
    Ok(Candles { day, rows: 1 })
}

#[job(one_at_a_time, keep_runs_at_most = 20)]
async fn token_refresh(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
async fn page_oncall(_run: Run, failure: Failure) -> Result<()> {
    let _ = failure;
    Ok(())
}

#[job]
async fn audit_trail(_run: Run, ended: Ended<Candles>) -> Result<()> {
    let _ = ended;
    Ok(())
}

#[job]
async fn day_report(_run: Run, (archive, candles): (Archive, Candles)) -> Result<()> {
    let _ = (archive, candles);
    Ok(())
}

#[command(preset = heavy)]
fn eod_ohlc(input: OhlcInput) -> Program<Candles> {
    Program::new("./compute")
        .args(["eod-ohlc", "--date"])
        .arg(input.date)
        .env("BROKER_ID", input.broker_id)
        .pass_env(["DATABASE_URL"])
}

fn nse_days(date: NaiveDate, _calendar: &Exchange) -> Day {
    if date.format("%a").to_string() == "Sun" { Day::Off("weekend".into()) } else { Day::Normal }
}

fn end_of_day(pipeline: &mut Pipeline) {
    pipeline
        .job(bhav_archive)
        .schedule(every(30.minutes()).between("18:45", "22:30").by_calendar(nse_days).input(|o| TradingDay(o.date())))
        .schedule(daily_at(["07:30"]).named("morning").input(|o| TradingDay(o.date())));
    pipeline
        .job(bhav_candles)
        .after(bhav_archive)
        .if_changed()
        .needs(snapshot_candles.done_for(|a: &Archive| a.day))
        .only_if("has files", |a: &Archive| a.count > 0);
    pipeline
        .job(snapshot_candles)
        .schedule(daily_at(["17:15"]).expire_after(3.hours()).input(|o| TradingDay(o.date())));
    pipeline.job(eod_ohlc).after(bhav_archive.map(|a: Archive| OhlcInput { date: a.day.0, broker_id: 1 }));
    pipeline.job(eod_report).after(bhav_archive.map(|a: Archive| OhlcInput { date: a.day.0, broker_id: 2 }));
    pipeline.job(day_report).after((bhav_archive, bhav_candles));
    pipeline.job(page_oncall).after_failure(bhav_archive);
    pipeline.job(audit_trail).after_any(bhav_candles);
}

fn pre_market(pipeline: &mut Pipeline) {
    pipeline.job(token_refresh).schedule(daily_at(["07:33"]).expire_after(1.hours()));
}

fn definition() -> Definition {
    let mut definition = Definition::new().zone("Asia/Kolkata").environments(["production", "dev"]);
    definition.provide(|| async { Ok::<_, std::convert::Infallible>(Exchange { files: vec!["a".into()] }) });
    definition.pipeline("end_of_day", end_of_day);
    definition.pipeline("pre_market", pre_market);
    definition
}

#[test]
fn the_definition_is_described_without_problems() {
    let problems = downbeat::check_problems(definition());
    assert!(problems.is_empty(), "{problems:#?}");
}

#[tokio::test]
async fn a_job_function_can_be_called_directly() {
    let day = TradingDay(NaiveDate::from_ymd_opt(2026, 11, 20).unwrap());
    let run = Run::for_test();
    let outcome = bhav_archive.test(run.clone(), day, &Exchange { files: vec![] }).await;
    assert_eq!(outcome, Outcome::Skipped("the exchange has not published yet".into()));
    assert_eq!(run.reported(), vec!["progress: downloading".to_owned()]);
}

#[test]
fn policies_come_from_the_attribute() {
    let spec = <bhav_candles as Job>::spec();
    let policy = downbeat::policy_of(&spec);
    assert_eq!(policy.queue, "heavy");
    assert_eq!(policy.timeout, 3.hours());
    assert!(policy.fresh_process);
}

#[test]
fn a_command_describes_its_program() {
    let program = eod_ohlc.test(OhlcInput { date: NaiveDate::from_ymd_opt(2026, 11, 20).unwrap(), broker_id: 7 });
    assert_eq!(program.args, ["eod-ohlc", "--date", "2026-11-20"]);
    assert_eq!(program.env["BROKER_ID"], "7");
}

#[test]
fn not_compared_fields_do_not_change_identity() {
    let day = TradingDay(NaiveDate::from_ymd_opt(2026, 11, 20).unwrap());
    let a = Archive { day, sha256: "x".into(), count: 1, fetched_at: "t1".into() };
    let b = Archive { fetched_at: "t2".into(), ..a.clone() };
    assert_eq!(a.identity(), b.identity());
}
