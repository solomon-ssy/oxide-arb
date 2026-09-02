//! Storage-owned native `ClickHouse` query limits.

use super::query::ClickHouseQueryLimits;

const KIB: u64 = 1_024;

pub const CLICKHOUSE_HEALTH: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.health.v1", 1, 64);
pub const CLICKHOUSE_RESOURCE_GOVERNANCE: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.resource_governance.v1", 16, 16 * KIB);
pub const CLICKHOUSE_DATABASE_BOOTSTRAP: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.database_bootstrap.v1", 1, KIB);
pub const CLICKHOUSE_DATABASE_OBJECT_COUNT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.database_object_count.v1", 1, 64);
pub const CLICKHOUSE_PREPRODUCTION_INSPECT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.preproduction_inspect.v1", 1, 64);
pub const CLICKHOUSE_PREPRODUCTION_RESET: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.preproduction_reset.v1", 1, KIB);
pub const CLICKHOUSE_RAW_HISTORY_READINESS: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.raw_history_readiness.v1", 3, 64 * KIB);
pub const CLICKHOUSE_BOOK_LATENCY_READINESS: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.book_latency_readiness.v1", 1, KIB);
pub const CLICKHOUSE_SCHEMA_BOOTSTRAP: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.schema_bootstrap.v1", 10_000, 64 * 1_024 * KIB);
pub const CLICKHOUSE_SCHEMA_VERIFY: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.storage.schema_verify.v1", 10_000, 64 * 1_024 * KIB);
