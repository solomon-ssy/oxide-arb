CREATE TABLE IF NOT EXISTS book_decision_contexts (
    context_id            String,
    recommendation_id     Nullable(String),
    execution_id          Nullable(String),
    market_id             String,
    yes_token_id          String,
    no_token_id           String,
    decision_stage        Enum8('FeatureGenerated' = 1, 'FactorScored' = 2, 'ModelScored' = 3, 'PortfolioPruned' = 4, 'RecommendationPublished' = 5, 'IntentCreated' = 6, 'ExecutionUpdated' = 7),
    evidence_tier         Enum8('ExactReplay' = 1, 'DecisionContext' = 2, 'AggregateOnly' = 3, 'Insufficient' = 4),
    decision_time         DateTime64(3, 'UTC'),
    yes_book_version      Nullable(UInt64),
    no_book_version       Nullable(UInt64),
    yes_book_age_ms       Nullable(UInt64),
    no_book_age_ms        Nullable(UInt64),
    top_n                 UInt16,
    yes_bids_json         String CODEC(ZSTD(3)),
    yes_asks_json         String CODEC(ZSTD(3)),
    no_bids_json          String CODEC(ZSTD(3)),
    no_asks_json          String CODEC(ZSTD(3)),
    yes_depth_usd         Nullable(Decimal128(18)),
    no_depth_usd          Nullable(Decimal128(18)),
    spread_bps            Nullable(Decimal64(4)),
    mid_price             Nullable(Decimal64(8)),
    imbalance             Nullable(String),
    slippage_curve_json   Nullable(String) CODEC(ZSTD(3)),
    book_quality          Enum8('Fresh' = 1, 'Stale' = 2, 'Crossed' = 3, 'Gap' = 4, 'Invalid' = 5, 'Insufficient' = 6),
    latency_trace_json    Nullable(String) CODEC(ZSTD(3)),
    source                Enum8('WsSnapshot' = 1, 'WsDelta' = 2, 'WsBbo' = 3, 'WsTickSize' = 4, 'WsLastTrade' = 5, 'WsMarketResolved' = 6, 'QuantPipeline' = 7, 'Execution' = 8, 'WsShardStatus' = 9),
    ingestion_time        DateTime64(3, 'UTC'),
    sequence              UInt64,
    schema_version        UInt32,
    decision_date         Date MATERIALIZED toDate(decision_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(decision_date)
ORDER BY (market_id, decision_time, ingestion_time, sequence, context_id)
TTL decision_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 4096
