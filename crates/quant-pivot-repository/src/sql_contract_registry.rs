//! Compiled native-SQL registry owned by `quant-pivot-repository`.

use quant_pivot_sql_contract::{SqlBudget, SqlContract, SqlDialect, SqlSafetyClass};

const KIB: u64 = 1_024;
const MIB: u64 = KIB * KIB;
const API_PAGE_ROWS: u64 = 100;
const ONLINE_ROWS: u64 = 50_000;
const RESEARCH_ROWS: u64 = 2_000_000;

macro_rules! ch_contract {
    ($name:ident, $id:literal, $owner:literal, $input:literal, $output:literal, $statements:literal, $rows:expr, $bytes:expr, $safety:ident) => {
        pub(crate) const $name: SqlContract = SqlContract::new(
            $id,
            SqlDialect::ClickHouse,
            $owner,
            $input,
            $output,
            SqlBudget::new($statements, $rows, $bytes),
            SqlSafetyClass::$safety,
        );
    };
}

ch_contract!(
    REPORT_FUNNEL_COUNTS,
    "ch.repository.report_funnel_counts.v1",
    "ChQuantFactReadRepository::report_market_funnel_counts",
    "RecommendationReportId",
    "Vec<ReportMarketFunnelCountRow>",
    1,
    64,
    64 * KIB,
    AggregateRead
);
ch_contract!(
    REPORT_FUNNEL_COUNT,
    "ch.repository.report_funnel_count.v1",
    "ChQuantFactReadRepository::report_market_funnel_count",
    "RecommendationReportId + OptionalFunnelFilters",
    "u64",
    1,
    1,
    KIB,
    AggregateRead
);
ch_contract!(
    REPORT_FUNNEL_PAGE,
    "ch.repository.report_funnel_page.v1",
    "ChQuantFactReadRepository::report_market_funnel_page",
    "RecommendationReportId + OptionalFunnelFilters + PageWindow",
    "Vec<ReportMarketFunnelRow>",
    1,
    API_PAGE_ROWS,
    8 * MIB,
    BoundedRead
);
ch_contract!(
    ENTRY_EVALUATION_LATEST,
    "ch.repository.entry_evaluation_latest.v1",
    "ChQuantFactReadRepository::latest_applied_entry_condition_evaluation",
    "EntryConditionInstanceId",
    "Option<EntryConditionEvaluationEventRow>",
    1,
    1,
    64 * KIB,
    BoundedRead
);
ch_contract!(
    CRYPTO_REPORT_AT,
    "ch.repository.crypto_report_at.v1",
    "ChQuantFactReadRepository::crypto_price_report_at",
    "DomainSourceId + DomainInstrumentKey + PitBoundary",
    "Option<CryptoPriceReportRow>",
    1,
    1,
    64 * KIB,
    BoundedRead
);
ch_contract!(
    CRYPTO_REPORTS_BETWEEN,
    "ch.repository.crypto_reports_between.v1",
    "ChQuantFactReadRepository::crypto_price_reports_between",
    "Vec<DomainInstrumentKey> + PitWindow",
    "Vec<CryptoPriceReportRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    CRYPTO_REPORTS_AVAILABLE,
    "ch.repository.crypto_reports_available.v1",
    "ChQuantFactReadRepository::crypto_price_reports_available_between",
    "Vec<DomainInstrumentKey> + AvailabilityWindow",
    "Vec<CryptoPriceReportRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    WEATHER_OBSERVATIONS_BETWEEN,
    "ch.repository.weather_observations_between.v1",
    "ChQuantFactReadRepository::weather_observation_facts_between",
    "Vec<StationId> + PitWindow",
    "Vec<WeatherObservationFactRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    WEATHER_FORECASTS_BETWEEN,
    "ch.repository.weather_forecasts_between.v1",
    "ChQuantFactReadRepository::weather_forecast_facts_between",
    "Vec<StationId> + PitWindow",
    "Vec<WeatherForecastFactRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    MICROSTRUCTURE_WINDOW,
    "ch.repository.microstructure_window.v1",
    "ChQuantFactReadRepository::microstructure_window",
    "Vec<TokenId> + PitWindow",
    "Vec<BookMicrostructureRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    MICROSTRUCTURE_SERIES,
    "ch.repository.microstructure_series.v1",
    "ChQuantFactReadRepository::microstructure_series",
    "Vec<TokenId> + PitWindow + Resolution",
    "Vec<BookMicrostructureRow>",
    1,
    ONLINE_ROWS,
    128 * MIB,
    BoundedRead
);
ch_contract!(
    LAST_TRADES,
    "ch.repository.last_trades.v1",
    "ChQuantFactReadRepository::last_trades",
    "Vec<TokenId> + TimeWindow + Limit",
    "Vec<TradeTapeRow>",
    1,
    ONLINE_ROWS,
    128 * MIB,
    BoundedRead
);
ch_contract!(
    TRADE_TAPE_WINDOW,
    "ch.repository.trade_tape_window.v1",
    "ChQuantFactReadRepository::trade_tape_window_by_market",
    "Vec<MarketId> + PitWindow",
    "Vec<TradeTapeRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    MID_PRICE_SERIES,
    "ch.repository.mid_price_series.v1",
    "ChQuantFactReadRepository::mid_price_series",
    "Vec<TokenId> + PitWindow + BucketSeconds",
    "Vec<MidPriceBucketRow>",
    1,
    ONLINE_ROWS,
    128 * MIB,
    BoundedRead
);
ch_contract!(
    BOOK_CHECKPOINT_AT,
    "ch.repository.book_checkpoint_at.v1",
    "ChQuantFactReadRepository::book_checkpoint_at",
    "TokenId + PitBoundary",
    "Option<BookL2CheckpointRow>",
    1,
    1,
    4 * MIB,
    BoundedRead
);
ch_contract!(
    BOOK_EVENTS_FROM,
    "ch.repository.book_events_from.v1",
    "ChQuantFactReadRepository::book_l2_events_from",
    "TokenId + StreamSessionId + Sequence + PitBoundary",
    "Vec<BookL2EventRow>",
    1,
    ONLINE_ROWS,
    256 * MIB,
    BoundedRead
);
ch_contract!(
    BOOK_EVENTS_BETWEEN,
    "ch.repository.book_events_between.v1",
    "ChQuantFactReadRepository::book_l2_events_between",
    "Vec<TokenId> + PitWindow",
    "Vec<BookL2EventRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    MARKET_WS_TRADES_FROM,
    "ch.repository.market_ws_trades_from.v1",
    "ChQuantFactReadRepository::market_ws_trades_from",
    "TokenId + StreamSessionId + Sequence + PitBoundary",
    "Vec<TradeTapeRow>",
    1,
    ONLINE_ROWS,
    256 * MIB,
    BoundedRead
);
ch_contract!(
    BOOK_STREAM_SESSION_AT,
    "ch.repository.book_stream_session_at.v1",
    "ChQuantFactReadRepository::book_stream_session_at",
    "StreamSessionId + PitBoundary",
    "Option<BookStreamSessionRow>",
    1,
    1,
    64 * KIB,
    BoundedRead
);
ch_contract!(
    BOOK_STREAM_SESSIONS,
    "ch.repository.book_stream_sessions.v1",
    "ChQuantFactReadRepository::book_stream_sessions",
    "Vec<StreamSessionId> + PitBoundary",
    "Vec<BookStreamSessionRow>",
    1,
    ONLINE_ROWS,
    128 * MIB,
    BoundedRead
);
ch_contract!(
    BOOK_CHECKPOINTS_AT,
    "ch.repository.book_checkpoints_at.v1",
    "ChQuantFactReadRepository::book_checkpoints_at",
    "Vec<TokenId> + PitBoundary",
    "Vec<BookL2CheckpointRow>",
    1,
    ONLINE_ROWS,
    256 * MIB,
    BoundedRead
);
ch_contract!(
    BOOK_CHECKPOINTS_BETWEEN,
    "ch.repository.book_checkpoints_between.v1",
    "ChQuantFactReadRepository::book_checkpoints_between",
    "Vec<TokenId> + PitWindow",
    "Vec<BookL2CheckpointRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    RESOLUTION_AT,
    "ch.repository.resolution_at.v1",
    "ChQuantFactReadRepository::resolution_at",
    "MarketId + PitBoundary",
    "Option<MarketResolutionRow>",
    1,
    1,
    64 * KIB,
    BoundedRead
);
ch_contract!(
    RESOLUTIONS_BETWEEN,
    "ch.repository.resolutions_between.v1",
    "ChQuantFactReadRepository::resolutions_between",
    "Vec<MarketId> + PitWindow",
    "Vec<MarketResolutionRow>",
    1,
    RESEARCH_ROWS,
    256 * MIB,
    BoundedRead
);
ch_contract!(
    OBSERVED_MARKETS_BETWEEN,
    "ch.repository.observed_markets_between.v1",
    "ChQuantFactReadRepository::observed_markets_between",
    "PitWindow",
    "Vec<MarketId>",
    1,
    RESEARCH_ROWS,
    128 * MIB,
    BoundedRead
);
ch_contract!(
    DOMAIN_OBSERVATIONS_BETWEEN,
    "ch.repository.domain_observations_between.v1",
    "ChQuantFactReadRepository::domain_observations_between",
    "Vec<DomainInstrumentKey> + PitWindow",
    "Vec<DomainObservationRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    DOMAIN_OBSERVATION_AT,
    "ch.repository.domain_observation_at.v1",
    "ChQuantFactReadRepository::domain_observation_at",
    "DomainInstrumentKey + Metric + PitBoundary",
    "Option<DomainObservationRow>",
    1,
    1,
    64 * KIB,
    BoundedRead
);
ch_contract!(
    FEATURE_PARITY_PAGE,
    "ch.repository.feature_parity_page.v1",
    "ChFeatureParityEventRepository::page_events",
    "FeatureParityEventListQuery + PageWindow",
    "Paginated<FeatureParityEventView>",
    2,
    API_PAGE_ROWS + 1,
    8 * MIB,
    BoundedRead
);
ch_contract!(
    FEATURE_PARITY_SUMMARY,
    "ch.repository.feature_parity_summary.v1",
    "ChFeatureParityEventRepository::summary_counts",
    "Trailing24Hours",
    "FeatureIntegrityCounts",
    2,
    256,
    256 * KIB,
    AggregateRead
);
ch_contract!(
    SERVING_COMPLETIONS_FOR_RUNS,
    "ch.repository.serving_completions_for_runs.v1",
    "ChFeatureParityEventRepository::completions_for_runs",
    "Vec<ModelRunId>",
    "Vec<QuantServingEvidenceCompletionRow>",
    1,
    ONLINE_ROWS,
    128 * MIB,
    BoundedRead
);
ch_contract!(
    MODEL_INPUTS_FOR_RUNS,
    "ch.repository.model_inputs_for_runs.v1",
    "ChFeatureParityEventRepository::model_inputs_for_runs",
    "Vec<ModelRunId>",
    "Vec<QuantModelInputEventRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    FEATURE_CELLS_FOR_VECTORS,
    "ch.repository.feature_cells_for_vectors.v1",
    "ChFeatureParityEventRepository::feature_cells_for_vectors",
    "Vec<FeatureVectorId>",
    "Vec<QuantFeatureEventRow>",
    1,
    RESEARCH_ROWS,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    TRADE_TAPE_RECONCILIATION,
    "ch.repository.trade_tape_reconciliation.v1",
    "ChNativeReadRepository::trade_tape_reconciliation_rows",
    "HalfOpenTimeWindow + HardRowLimit",
    "Vec<TradeTapeRow>",
    1,
    1_000_001,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    REPORT_RECOMMENDATION_VERIFY,
    "ch.repository.report_recommendation_verify.v1",
    "ChNativeReadRepository::report_recommendation_rows",
    "RecommendationReportId",
    "Vec<QuantReportRecommendationFactRow>",
    1,
    100_000,
    256 * MIB,
    BoundedRead
);
ch_contract!(
    REPORT_FUNNEL_VERIFY,
    "ch.repository.report_funnel_verify.v1",
    "ChNativeReadRepository::report_funnel_rows",
    "RecommendationReportId",
    "Vec<ReportMarketFunnelRow>",
    1,
    1_000_000,
    512 * MIB,
    BoundedRead
);
ch_contract!(
    PHASE119_GISTEMP_EVIDENCE,
    "ch.repository.phase119_gistemp_evidence.v1",
    "ChNativeReadRepository::gistemp_historical_time",
    "NasaGistempSource",
    "GistempHistoricalTimeRaw",
    7,
    1,
    KIB,
    AggregateRead
);
ch_contract!(
    PHASE119_FACT_IDEMPOTENCY,
    "ch.repository.phase119_fact_idempotency.v1",
    "ChNativeReadRepository::fact_idempotency",
    "FactEvidenceTable",
    "FactIdempotencyCounts",
    4,
    1,
    KIB,
    AggregateRead
);

pub(crate) const POSTGRES_SHADOW_LATENCY: SqlContract = SqlContract::new(
    "pg.repository.shadow_latency.v1",
    SqlDialect::Postgres,
    "postgres::primitives::shadow_latency_aggregate",
    "HalfOpenTimeWindow",
    "ShadowLatencyAggregate",
    SqlBudget::new(1, 1, KIB),
    SqlSafetyClass::AggregateRead,
);

const REPOSITORY_SQL_CONTRACTS: &[SqlContract] = &[
    REPORT_FUNNEL_COUNTS,
    REPORT_FUNNEL_COUNT,
    REPORT_FUNNEL_PAGE,
    ENTRY_EVALUATION_LATEST,
    CRYPTO_REPORT_AT,
    CRYPTO_REPORTS_BETWEEN,
    CRYPTO_REPORTS_AVAILABLE,
    WEATHER_OBSERVATIONS_BETWEEN,
    WEATHER_FORECASTS_BETWEEN,
    MICROSTRUCTURE_WINDOW,
    MICROSTRUCTURE_SERIES,
    LAST_TRADES,
    TRADE_TAPE_WINDOW,
    MID_PRICE_SERIES,
    BOOK_CHECKPOINT_AT,
    BOOK_EVENTS_FROM,
    BOOK_EVENTS_BETWEEN,
    MARKET_WS_TRADES_FROM,
    BOOK_STREAM_SESSION_AT,
    BOOK_STREAM_SESSIONS,
    BOOK_CHECKPOINTS_AT,
    BOOK_CHECKPOINTS_BETWEEN,
    RESOLUTION_AT,
    RESOLUTIONS_BETWEEN,
    OBSERVED_MARKETS_BETWEEN,
    DOMAIN_OBSERVATIONS_BETWEEN,
    DOMAIN_OBSERVATION_AT,
    FEATURE_PARITY_PAGE,
    FEATURE_PARITY_SUMMARY,
    SERVING_COMPLETIONS_FOR_RUNS,
    MODEL_INPUTS_FOR_RUNS,
    FEATURE_CELLS_FOR_VECTORS,
    TRADE_TAPE_RECONCILIATION,
    REPORT_RECOMMENDATION_VERIFY,
    REPORT_FUNNEL_VERIFY,
    PHASE119_GISTEMP_EVIDENCE,
    PHASE119_FACT_IDEMPOTENCY,
    POSTGRES_SHADOW_LATENCY,
];

/// Return the compiled repository-owned native-SQL registry.
#[must_use]
pub const fn repository_sql_contracts() -> &'static [SqlContract] {
    REPOSITORY_SQL_CONTRACTS
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::config::MAX_TRADE_TAPE_RECONCILIATION_ROWS;
    use quant_pivot_sql_contract::validate_registry;
    use quant_pivot_storage::sql_contract_registry::storage_sql_contracts;

    use super::{
        FEATURE_PARITY_PAGE, LAST_TRADES, REPORT_FUNNEL_PAGE, REPOSITORY_SQL_CONTRACTS,
        TRADE_TAPE_RECONCILIATION,
    };

    #[test]
    fn repository_and_storage_contract_ids_are_globally_unique() {
        let contracts = storage_sql_contracts()
            .iter()
            .chain(REPOSITORY_SQL_CONTRACTS)
            .copied()
            .collect::<Vec<_>>();
        assert!(validate_registry(&contracts).is_ok());
    }

    #[test]
    fn critical_read_budgets_are_stable() {
        assert_eq!(REPORT_FUNNEL_PAGE.statement_budget(), 1);
        assert_eq!(REPORT_FUNNEL_PAGE.result_row_budget(), 100);
        assert_eq!(FEATURE_PARITY_PAGE.statement_budget(), 2);
        assert_eq!(FEATURE_PARITY_PAGE.result_row_budget(), 101);
        assert_eq!(LAST_TRADES.result_row_budget(), 50_000);
        assert_eq!(TRADE_TAPE_RECONCILIATION.statement_budget(), 1);
        assert_eq!(
            TRADE_TAPE_RECONCILIATION.result_row_budget(),
            u64::try_from(MAX_TRADE_TAPE_RECONCILIATION_ROWS)
                .expect("reconciliation row cap fits u64")
                + 1
        );
    }
}
