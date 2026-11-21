//! An example product: a small market-data system.
//!
//! Every half hour during market hours it fetches the day's prices; whenever they changed it builds
//! candles; after the close it writes a report, once the candles for that day are in. A command job
//! stands in for code not yet ported to Rust. Run it with `mise run demo` and open the console.
//! `tests/market.rs` shows how a product tests its jobs and schedules. This file holds everything
//! that uses downbeat; `exchange.rs` holds the product code it schedules.

mod exchange;

use std::sync::atomic::AtomicU32;

use downbeat::prelude::*;
use exchange::chrono_now;
pub use exchange::{Candles, Exchange, Holidays, Prices, TradingDay};

#[job(attempts = 3, backoff = 10.seconds()..=2.minutes(), keep_runs_at_most = 50)]
pub async fn fetch_prices(run: Run, day: TradingDay, exchange: &Exchange) -> JobResult<Prices> {
    run.progress("asking the exchange");
    let count = exchange.prices_for(day);
    if count == 0 {
        return skip("the exchange has published nothing yet");
    }
    Ok(Prices { day, count, digest: format!("{:x}", u64::from(count) * 2_654_435_761), fetched_at: chrono_now() })
}

#[job(preset = heavy, timeout = 30.minutes(), max_memory = 512.mb())]
async fn build_candles(run: Run, prices: Prices) -> JobResult<Candles> {
    let mut built: u32 = run.last_checkpoint().unwrap_or(0);
    while built < prices.count {
        built = (built + 10).min(prices.count);
        run.progress(format!("{built} of {} candles", prices.count));
        run.checkpoint(&built)?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if run.is_cancelled() {
            return Err(JobError::msg("stopped on request"));
        }
    }
    Ok(Candles { day: prices.day, rows: built })
}

#[job(expect_success_every = 26.hours(), supports_dry_run)]
async fn daily_report(run: Run, day: TradingDay) -> JobResult<String> {
    if run.is_dry_run() {
        return Ok(format!("would write the report for {}", day.date));
    }
    Ok(format!("report for {} written", day.date))
}

#[command]
fn export_csv(candles: Candles) -> Program<()> {
    Program::new("/bin/sh")
        .args(["-c", "echo \"exporting $ROWS rows for $DAY\" >&2; sleep 1"])
        .env("ROWS", candles.rows)
        .env("DAY", candles.day.date)
}

#[job(one_at_a_time)]
async fn housekeeping(run: Run) -> JobResult<()> {
    run.progress("removing old files");
    Ok(())
}

pub fn trading_days(date: NaiveDate, holidays: &Holidays) -> Day {
    if holidays.0.contains(&date) { Day::Off("exchange holiday".into()) } else { Day::Normal }
}

fn market_hours(pipeline: &mut Pipeline) {
    pipeline.job(fetch_prices).schedule(
        every(30.minutes())
            .between("09:15", "15:30")
            .on_weekdays([Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri])
            .by_calendar(trading_days)
            .input(|occurrence| TradingDay { date: occurrence.date() }),
    );
    pipeline.job(build_candles).after(fetch_prices).if_changed();
    pipeline.job(export_csv).after(build_candles);
}

fn after_close(pipeline: &mut Pipeline) {
    pipeline
        .job(daily_report)
        .schedule(
            daily_at(["16:00"]).by_calendar(trading_days).input(|occurrence| TradingDay { date: occurrence.date() }),
        )
        .needs(fetch_prices.done_for(|day: &TradingDay| *day).or_wait());
}

fn upkeep(pipeline: &mut Pipeline) {
    pipeline.job(housekeeping).schedule(daily_at(["02:00"]));
}

/// The product's definition: every role and every child process builds it.
pub fn definition() -> Definition {
    let mut definition = Definition::new().zone("Asia/Kolkata").environments(["production", "dev"]);
    definition.provide(|| async { Ok::<_, std::convert::Infallible>(Exchange { asked: AtomicU32::new(0) }) });
    definition.provide(|| async { Ok::<_, std::convert::Infallible>(Holidays(Vec::new())) });
    definition.pipeline("market_hours", market_hours);
    definition.pipeline("after_close", after_close);
    definition.pipeline("upkeep", upkeep);
    definition.on_finish(|run| async move {
        if run.failed() {
            eprintln!("[alert] {} failed: {}", run.job, run.reason);
        }
        Ok::<_, std::convert::Infallible>(())
    });
    definition.log_link("https://logs.example.com/search?run={run_id}");
    definition
}
