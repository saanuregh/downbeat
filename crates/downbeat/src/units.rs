//! `30.minutes()` and `8.gb()`: the one notation for durations and sizes in policies and schedules.

use std::time::Duration;

/// Durations written as `30.seconds()`, `5.minutes()`, `2.hours()` or `30.days()`.
pub trait DurationExt {
    fn seconds(self) -> Duration;
    fn minutes(self) -> Duration;
    fn hours(self) -> Duration;
    fn days(self) -> Duration;
}

impl DurationExt for u64 {
    fn seconds(self) -> Duration {
        Duration::from_secs(self)
    }
    fn minutes(self) -> Duration {
        Duration::from_secs(self * 60)
    }
    fn hours(self) -> Duration {
        Duration::from_secs(self * 3600)
    }
    fn days(self) -> Duration {
        Duration::from_secs(self * 86_400)
    }
}

/// A size in bytes, written as `512.mb()` or `8.gb()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bytes(pub u64);

/// Sizes written as `512.mb()` or `8.gb()` (powers of 1024).
pub trait BytesExt {
    fn kb(self) -> Bytes;
    fn mb(self) -> Bytes;
    fn gb(self) -> Bytes;
}

impl BytesExt for u64 {
    fn kb(self) -> Bytes {
        Bytes(self << 10)
    }
    fn mb(self) -> Bytes {
        Bytes(self << 20)
    }
    fn gb(self) -> Bytes {
        Bytes(self << 30)
    }
}

/// Short human form used in snapshots and reasons: `90s`, `30m`, `2h`, `1d`, `1h30m`.
pub fn format_duration(duration: Duration) -> String {
    let mut seconds = duration.as_secs();
    if seconds == 0 {
        return "0s".to_owned();
    }
    let mut out = String::new();
    for (unit, size) in [("d", 86_400), ("h", 3600), ("m", 60), ("s", 1)] {
        if seconds >= size {
            out.push_str(&format!("{}{unit}", seconds / size));
            seconds %= size;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_durations_compactly() {
        assert_eq!(format_duration(90.seconds()), "1m30s");
        assert_eq!(format_duration(2.hours()), "2h");
        assert_eq!(format_duration(26.hours()), "1d2h");
        assert_eq!(format_duration(Duration::ZERO), "0s");
    }

    #[test]
    fn sizes_are_powers_of_1024() {
        assert_eq!(1.kb(), Bytes(1024));
        assert_eq!(8.gb(), Bytes(8 * 1024 * 1024 * 1024));
    }
}
