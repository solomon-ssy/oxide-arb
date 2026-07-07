//! Canonical point-in-time trade-tape window parameters.

use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

/// Canonical PIT parameters for trade-tape window reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeTapePitParams {
    /// Decision/trigger instant (report trigger or monitor `Utc::now()`).
    pub trigger_time: DateTime<Utc>,
    /// Source visibility delay from runtime schedule.
    pub source_delay: Duration,
    /// Trailing lookback from `features.structural.trade_tape_window_secs`.
    pub lookback: Duration,
}

impl TradeTapePitParams {
    /// Exclusive end of the readable trade-tape window (`trigger - delay`).
    #[must_use]
    pub fn cutoff(&self) -> DateTime<Utc> {
        self.trigger_time
            .checked_sub_signed(to_chrono(self.source_delay))
            .unwrap_or(self.trigger_time)
    }

    /// Half-open window start in epoch milliseconds.
    #[must_use]
    pub fn ch_from_ms(&self) -> i64 {
        (self.cutoff() - to_chrono(self.lookback)).timestamp_millis()
    }

    /// Half-open window end in epoch milliseconds.
    #[must_use]
    pub fn ch_to_ms(&self) -> i64 {
        self.cutoff().timestamp_millis()
    }
}

fn to_chrono(duration: Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::zero())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::time::Duration;

    #[test]
    fn pit_window_bounds_are_half_open() {
        let trigger = Utc.with_ymd_and_hms(2026, 1, 15, 12, 0, 0).unwrap();
        let pit = TradeTapePitParams {
            trigger_time: trigger,
            source_delay: Duration::from_mins(5),
            lookback: Duration::from_hours(1),
        };
        assert_eq!(pit.cutoff(), trigger - ChronoDuration::seconds(300));
        assert_eq!(pit.ch_to_ms(), pit.cutoff().timestamp_millis());
        assert_eq!(
            pit.ch_from_ms(),
            (pit.cutoff() - ChronoDuration::seconds(3600)).timestamp_millis()
        );
        assert!(pit.ch_from_ms() < pit.ch_to_ms());
    }
}
