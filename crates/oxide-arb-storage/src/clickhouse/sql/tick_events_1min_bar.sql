CREATE TABLE IF NOT EXISTS tick_events_1min_bar (
    token_id        String,
    bar_start       DateTime64(3, 'UTC'),
    open_bid        AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    close_bid       AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    high_bid        SimpleAggregateFunction(max, Float64),
    low_bid         SimpleAggregateFunction(min, Float64),
    open_ask        AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    close_ask       AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    high_ask        SimpleAggregateFunction(max, Float64),
    low_ask         SimpleAggregateFunction(min, Float64),
    avg_spread_bps  AggregateFunction(avg, Float64),
    max_bid_depth   SimpleAggregateFunction(max, Float64),
    max_ask_depth   SimpleAggregateFunction(max, Float64),
    tick_count      SimpleAggregateFunction(sum, UInt64),
    bar_date        Date MATERIALIZED toDate(bar_start)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bar_date)
ORDER BY (token_id, bar_start)
TTL bar_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192
