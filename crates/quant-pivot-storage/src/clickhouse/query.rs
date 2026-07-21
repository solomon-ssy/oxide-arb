//! Fail-closed limits for native `ClickHouse` reads.

use clickhouse::{Client, query::Query};
/// Server-enforced result limits attached to one native query family.
///
/// `ClickHouse` aborts with `throw` when either limit is exceeded, so callers
/// cannot accidentally accept a silently truncated result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHouseQueryLimits {
    operation: &'static str,
    max_result_rows: u64,
    max_result_bytes: u64,
}

impl ClickHouseQueryLimits {
    #[must_use]
    pub const fn new(operation: &'static str, max_result_rows: u64, max_result_bytes: u64) -> Self {
        Self {
            operation,
            max_result_rows,
            max_result_bytes,
        }
    }

    #[must_use]
    pub const fn max_result_rows(self) -> u64 {
        self.max_result_rows
    }

    /// Build a query carrying server-side limits and an observability label.
    pub fn query(self, client: &Client, sql: &str) -> Query {
        client
            .query(sql)
            .with_setting("log_comment", self.operation)
            .with_setting("max_result_rows", self.max_result_rows.to_string())
            .with_setting("max_result_bytes", self.max_result_bytes.to_string())
            .with_setting("result_overflow_mode", "throw")
    }
}
