CREATE MATERIALIZED VIEW IF NOT EXISTS tick_events_1min_bar_mv
TO tick_events_1min_bar
AS SELECT
    token_id,
    toStartOfMinute(event_time) AS bar_start,
    argMinState(toFloat64(assumeNotNull(best_bid)), event_time) AS open_bid,
    argMaxState(toFloat64(assumeNotNull(best_bid)), event_time) AS close_bid,
    max(toFloat64(assumeNotNull(best_bid))) AS high_bid,
    min(toFloat64(assumeNotNull(best_bid))) AS low_bid,
    argMinState(toFloat64(assumeNotNull(best_ask)), event_time) AS open_ask,
    argMaxState(toFloat64(assumeNotNull(best_ask)), event_time) AS close_ask,
    max(toFloat64(assumeNotNull(best_ask))) AS high_ask,
    min(toFloat64(assumeNotNull(best_ask))) AS low_ask,
    avgState(ifNull(toFloat64(spread_bps), 0.0)) AS avg_spread_bps,
    max(ifNull(toFloat64(bid_depth_usd), 0.0)) AS max_bid_depth,
    max(ifNull(toFloat64(ask_depth_usd), 0.0)) AS max_ask_depth,
    toUInt64(count()) AS tick_count
FROM tick_events
WHERE event_type = 'Bbo' AND isNotNull(best_bid) AND isNotNull(best_ask)
GROUP BY token_id, bar_start
