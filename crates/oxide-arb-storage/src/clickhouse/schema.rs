//! `ClickHouse` DDL statements for `MergeTree` tables and materialized views.

pub fn all_ddl() -> Vec<&'static str> {
    vec![
        include_str!("sql/tick_events.sql"),
        include_str!("sql/tick_events_l2.sql"),
        include_str!("sql/book_snapshots.sql"),
        include_str!("sql/opportunity_audit.sql"),
        include_str!("sql/opportunity_detection.sql"),
        include_str!("sql/calibration_snapshots.sql"),
    ]
}
