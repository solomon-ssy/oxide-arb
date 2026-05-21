CREATE TABLE IF NOT EXISTS tick_events_1min_bar (
    token_id        String,
    bar_start       DateTime64(3, 'UTC'),
    open_bid        AggregateFunction(argMin, Decimal64(8), DateTime64(3, 'UTC')),
    close_bid       AggregateFunction(argMax, Decimal64(8), DateTime64(3, 'UTC')),
    high_bid        SimpleAggregateFunction(max, Decimal64(8)),
    low_bid         SimpleAggregateFunction(min, Decimal64(8)),
    open_ask        AggregateFunction(argMin, Decimal64(8), DateTime64(3, 'UTC')),
    close_ask       AggregateFunction(argMax, Decimal64(8), DateTime64(3, 'UTC')),
    high_ask        SimpleAggregateFunction(max, Decimal64(8)),
    low_ask         SimpleAggregateFunction(min, Decimal64(8)),
    avg_spread_bps  SimpleAggregateFunction(avg, Float64),
    max_bid_depth   SimpleAggregateFunction(max, Decimal64(8)),
    max_ask_depth   SimpleAggregateFunction(max, Decimal64(8)),
    tick_count      SimpleAggregateFunction(sum, UInt64),
    bar_date        Date MATERIALIZED toDate(bar_start)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bar_date)
ORDER BY (token_id, bar_start)
TTL bar_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192
