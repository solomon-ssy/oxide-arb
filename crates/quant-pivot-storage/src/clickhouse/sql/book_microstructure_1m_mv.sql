CREATE MATERIALIZED VIEW IF NOT EXISTS book_microstructure_1m_mv
TO book_microstructure_1m
AS
SELECT
    token_id,
    any(market_id) AS market_id,
    toStartOfMinute(bucket_time) AS bucket_time,
    argMin(best_bid_open, bucket_time) AS best_bid_open,
    max(best_bid_high) AS best_bid_high,
    min(best_bid_low) AS best_bid_low,
    argMax(best_bid_close, bucket_time) AS best_bid_close,
    argMin(best_ask_open, bucket_time) AS best_ask_open,
    max(best_ask_high) AS best_ask_high,
    min(best_ask_low) AS best_ask_low,
    argMax(best_ask_close, bucket_time) AS best_ask_close,
    min(spread_bps_min) AS spread_bps_min,
    avg(spread_bps_avg) AS spread_bps_avg,
    max(spread_bps_max) AS spread_bps_max,
    argMin(mid_price_open, bucket_time) AS mid_price_open,
    argMax(mid_price_close, bucket_time) AS mid_price_close,
    avg(top1_depth_usd_avg) AS top1_depth_usd_avg,
    avg(top5_depth_usd_avg) AS top5_depth_usd_avg,
    avg(top20_depth_usd_avg) AS top20_depth_usd_avg,
    avg(imbalance_avg) AS imbalance_avg,
    sum(update_count) AS update_count,
    sum(snapshot_count) AS snapshot_count,
    sum(delta_count) AS delta_count,
    sum(delete_count) AS delete_count,
    sum(crossed_count) AS crossed_count,
    sum(invalid_level_count) AS invalid_level_count,
    sum(gap_count) AS gap_count,
    sum(last_trade_count) AS last_trade_count,
    max(max_book_age_ms) AS max_book_age_ms,
    max(schema_version) AS schema_version
FROM book_microstructure_1s
GROUP BY token_id, bucket_time
