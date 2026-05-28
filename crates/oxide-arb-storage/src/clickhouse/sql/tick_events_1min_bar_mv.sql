CREATE MATERIALIZED VIEW IF NOT EXISTS tick_events_1min_bar_mv
TO tick_events_1min_bar
AS SELECT
    token_id,
    toStartOfMinute(received_at) AS bar_start,
    argMinState(best_bid, received_at) AS open_bid,
    argMaxState(best_bid, received_at) AS close_bid,
    max(best_bid) AS high_bid,
    min(best_bid) AS low_bid,
    argMinState(best_ask, received_at) AS open_ask,
    argMaxState(best_ask, received_at) AS close_ask,
    max(best_ask) AS high_ask,
    min(best_ask) AS low_ask,
    avgState(toFloat64(spread_bps)) AS avg_spread_bps,
    max(bid_depth_usd) AS max_bid_depth,
    max(ask_depth_usd) AS max_ask_depth,
    toUInt64(count()) AS tick_count
FROM tick_events
GROUP BY token_id, bar_start
