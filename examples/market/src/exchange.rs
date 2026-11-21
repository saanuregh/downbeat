//! The product's own code, which knows nothing of scheduling: the data its jobs pass along, the
//! exchange it asks for prices, and the exchange's holidays.

use std::sync::atomic::{AtomicU32, Ordering};

use downbeat::prelude::*;
use serde::{Deserialize, Serialize};

/// A trading day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct TradingDay {
    pub date: NaiveDate,
}

/// What a fetch found: how many prices, and a hash that changes when they do.
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

/// The exchange: here, a stand-in that publishes a little more each time it is asked.
#[derive(Debug)]
pub struct Exchange {
    pub asked: AtomicU32,
}

impl Exchange {
    pub(crate) fn prices_for(&self, _day: TradingDay) -> u32 {
        let asked = self.asked.fetch_add(1, Ordering::SeqCst);
        (asked / 2) * 50
    }
}

/// The exchange's holidays: a calendar the app loads once.
#[derive(Debug)]
pub struct Holidays(pub Vec<NaiveDate>);

pub(crate) fn chrono_now() -> String {
    format!("{:?}", std::time::SystemTime::now())
}
