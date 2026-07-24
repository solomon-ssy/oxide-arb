//! Repository-owned native `ClickHouse` query limits.

use quant_pivot_storage::clickhouse::ClickHouseQueryLimits;

const KIB: u64 = 1_024;
const MIB: u64 = KIB * KIB;
const API_PAGE_ROWS: u64 = 100;
const ONLINE_ROWS: u64 = 50_000;
const RESEARCH_ROWS: u64 = 2_000_000;

pub const REPORT_FUNNEL_COUNTS: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.report_funnel_counts.v1", 64, 64 * KIB);
pub const REPORT_FUNNEL_COUNT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.report_funnel_count.v1", 1, KIB);
pub const REPORT_FUNNEL_PAGE: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.report_funnel_page.v1",
    API_PAGE_ROWS,
    8 * MIB,
);
pub const ENTRY_EVALUATION_LATEST: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.entry_evaluation_latest.v1", 1, 64 * KIB);
pub const CRYPTO_REPORT_AT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.crypto_report_at.v1", 1, 64 * KIB);
pub const CRYPTO_REPORTS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.crypto_reports_between.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const CRYPTO_REPORTS_AVAILABLE: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.crypto_reports_available.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const WEATHER_OBSERVATIONS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.weather_observations_between.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const WEATHER_FORECASTS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.weather_forecasts_between.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const MICROSTRUCTURE_WINDOW: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.microstructure_window.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const MICROSTRUCTURE_SERIES: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.microstructure_series.v1",
    ONLINE_ROWS,
    128 * MIB,
);
pub const LAST_TRADES: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.last_trades.v1", ONLINE_ROWS, 128 * MIB);
pub const TRADE_TAPE_WINDOW: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.trade_tape_window.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const MID_PRICE_SERIES: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.mid_price_series.v1", ONLINE_ROWS, 128 * MIB);
pub const BOOK_LEDGER_SNAPSHOT_AT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.book_ledger_snapshot_at.v1", 1, 4 * MIB);
pub const BOOK_LEDGER_FROM: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.book_l2_ledger_from.v1",
    ONLINE_ROWS,
    256 * MIB,
);
pub const BOOK_LEDGER_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.book_l2_ledger_between.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const BOOK_STREAM_SESSION_AT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.book_stream_session_at.v1", 1, 64 * KIB);
pub const BOOK_STREAM_SESSIONS: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.book_stream_sessions.v1",
    ONLINE_ROWS,
    128 * MIB,
);
pub const BOOK_LEDGER_SNAPSHOTS_AT: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.book_ledger_snapshots_at.v1",
    ONLINE_ROWS,
    256 * MIB,
);
pub const BOOK_LEDGER_SNAPSHOTS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.book_ledger_snapshots_between.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const RESOLUTION_AT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.resolution_at.v1", 2, 128 * KIB);
pub const RESOLUTION_BY_CHECKPOINT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.resolution_by_checkpoint.v1", 2, 128 * KIB);
pub const RESOLUTION_BY_MARKET: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.resolution_by_market.v1", 2, 128 * KIB);
pub const RESOLUTIONS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.resolutions_between.v1",
    RESEARCH_ROWS,
    256 * MIB,
);
pub const OBSERVED_MARKETS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.observed_markets_between.v1",
    RESEARCH_ROWS,
    128 * MIB,
);
pub const DOMAIN_OBSERVATIONS_BETWEEN: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.domain_observations_between.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const DOMAIN_OBSERVATION_AT: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.domain_observation_at.v1", 1, 64 * KIB);
pub const FEATURE_PARITY_PAGE: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.feature_parity_page.v1",
    API_PAGE_ROWS + 1,
    8 * MIB,
);
pub const FEATURE_PARITY_SUMMARY: ClickHouseQueryLimits =
    ClickHouseQueryLimits::new("ch.repository.feature_parity_summary.v1", 256, 256 * KIB);
pub const SERVING_COMPLETIONS_FOR_RUNS: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.serving_completions_for_runs.v1",
    ONLINE_ROWS,
    128 * MIB,
);
pub const MODEL_INPUTS_FOR_RUNS: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.model_inputs_for_runs.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const FEATURE_CELLS_FOR_VECTORS: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.feature_cells_for_vectors.v1",
    RESEARCH_ROWS,
    512 * MIB,
);
pub const TRADE_TAPE_RECONCILIATION: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.trade_tape_reconciliation.v1",
    1_000_001,
    512 * MIB,
);
pub const REPORT_RECOMMENDATION_VERIFY: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.report_recommendation_verify.v1",
    100_000,
    256 * MIB,
);
pub const REPORT_FUNNEL_VERIFY: ClickHouseQueryLimits = ClickHouseQueryLimits::new(
    "ch.repository.report_funnel_verify.v1",
    1_000_000,
    512 * MIB,
);
