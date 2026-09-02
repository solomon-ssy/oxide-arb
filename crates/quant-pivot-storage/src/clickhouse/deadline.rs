//! Strongly typed network deadlines for every `ClickHouse` I/O family.

use std::time::Duration;

use quant_pivot_models::config::{ClickHouseInsertIoConfig, ClickHouseIoConfig};

#[derive(Debug, Clone, Copy)]
pub struct InsertDeadlines {
    send: Duration,
    end: Duration,
    attempt: Duration,
}

impl InsertDeadlines {
    #[must_use]
    pub const fn send(self) -> Duration {
        self.send
    }

    #[must_use]
    pub const fn end(self) -> Duration {
        self.end
    }

    #[must_use]
    pub const fn attempt(self) -> Duration {
        self.attempt
    }

    #[must_use]
    pub fn attempt_seconds_ceil(self) -> u64 {
        let seconds = self.attempt.as_millis().saturating_add(999) / 1_000;
        u64::try_from(seconds).map_or(u64::MAX, |seconds| seconds.max(1))
    }
}

impl From<ClickHouseInsertIoConfig> for InsertDeadlines {
    fn from(config: ClickHouseInsertIoConfig) -> Self {
        Self {
            send: Duration::from_millis(config.send_timeout_ms),
            end: Duration::from_millis(config.end_timeout_ms),
            attempt: Duration::from_millis(config.attempt_timeout_ms),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClickHouseIoDeadlines {
    query: Duration,
    maintenance: Duration,
    critical_insert: InsertDeadlines,
    bulk_insert: InsertDeadlines,
}

impl ClickHouseIoDeadlines {
    #[must_use]
    pub const fn query(self) -> Duration {
        self.query
    }

    #[must_use]
    pub fn query_seconds_ceil(self) -> u64 {
        let seconds = self.query.as_millis().saturating_add(999) / 1_000;
        u64::try_from(seconds).map_or(u64::MAX, |seconds| seconds.max(1))
    }

    #[must_use]
    pub const fn maintenance(self) -> Duration {
        self.maintenance
    }

    #[must_use]
    pub const fn critical_insert(self) -> InsertDeadlines {
        self.critical_insert
    }

    #[must_use]
    pub const fn bulk_insert(self) -> InsertDeadlines {
        self.bulk_insert
    }
}

impl From<&ClickHouseIoConfig> for ClickHouseIoDeadlines {
    fn from(config: &ClickHouseIoConfig) -> Self {
        Self {
            query: Duration::from_millis(config.query_timeout_ms),
            maintenance: Duration::from_millis(config.maintenance_timeout_ms),
            critical_insert: InsertDeadlines::from(config.critical_insert),
            bulk_insert: InsertDeadlines::from(config.bulk_insert),
        }
    }
}
