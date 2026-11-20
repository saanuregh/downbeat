//! The names the guide's samples assume are already there: the market example's types and jobs,
//! and the few helpers a sample calls. A sample that defines one of these itself shadows it.

#![allow(dead_code, unused_imports, clippy::all, clippy::pedantic)]

pub use std::sync::atomic::AtomicU32;

pub use downbeat::prelude::*;
pub use downbeat::{EventDelivery, FinishedRun, Started};
pub use downbeat_client::PrivateKey as ClientKey;
pub use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct TradingDay {
    pub date: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Prices {
    pub day: TradingDay,
    pub count: u32,
    pub digest: String,
    #[job_data(not_compared)]
    pub fetched_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Candles {
    pub day: TradingDay,
    pub rows: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Summary {
    pub day: TradingDay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct RefreshReport {
    pub refreshed: u32,
    pub failed: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct ReportRequest {
    pub user_id: u64,
}

#[derive(Debug)]
pub struct Exchange {
    pub asked: AtomicU32,
}

impl Exchange {
    pub fn new() -> Exchange {
        Exchange { asked: AtomicU32::new(0) }
    }

    pub fn prices_for(&self, _day: TradingDay) -> u32 {
        0
    }
}

#[derive(Debug)]
pub struct Holidays(pub Vec<NaiveDate>);

pub fn chrono_now() -> String {
    String::new()
}

pub fn load_holidays() -> Vec<NaiveDate> {
    Vec::new()
}

pub fn secret_prefix() -> String {
    "secret".to_owned()
}

pub fn trading_days(_date: NaiveDate, _holidays: &Holidays) -> Day {
    Day::Normal
}

pub fn definition() -> Definition {
    Definition::new().zone("UTC")
}

pub mod alerts {
    pub async fn page(_job: &str, _reason: &str, _run: uuid::Uuid) -> anyhow::Result<()> {
        Ok(())
    }
}

pub mod metrics {
    pub fn record(_event: &downbeat::Event) {}
}

/// As in the market example.
#[job]
pub async fn fetch_prices(run: Run, day: TradingDay, exchange: &Exchange) -> Result<Prices> {
    run.progress("asking the exchange");
    let count = exchange.prices_for(day);
    if count == 0 {
        return skip("the exchange has published nothing yet");
    }
    Ok(Prices { day, count, digest: String::new(), fetched_at: String::new() })
}

#[job]
pub async fn build_candles(_run: Run, prices: Prices) -> Result<Candles> {
    Ok(Candles { day: prices.day, rows: prices.count })
}

#[command]
pub fn export_csv(candles: Candles) -> Program<()> {
    Program::new("./export").arg(candles.rows)
}

#[job]
pub async fn send_summary(_run: Run, _summary: Summary) -> Result<()> {
    Ok(())
}

#[job]
pub async fn refresh_cache(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
pub async fn day_digest(_run: Run, _day: TradingDay) -> Result<()> {
    Ok(())
}

#[job]
pub async fn refresh_tokens(_run: Run) -> Result<RefreshReport> {
    Ok(RefreshReport { refreshed: 0, failed: 0 })
}

#[job]
pub async fn notify_users(_run: Run, _report: RefreshReport) -> Result<()> {
    Ok(())
}

#[job]
pub async fn page_oncall(_run: Run, _failure: Failure) -> Result<()> {
    Ok(())
}

#[job]
pub async fn daily_report(_run: Run, _day: TradingDay) -> Result<()> {
    Ok(())
}

#[job]
pub async fn refresh_token(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
pub async fn sync_accounts(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
pub async fn housekeeping(_run: Run) -> Result<()> {
    Ok(())
}
