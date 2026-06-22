//! `ClickHouse` DDL statements for `MergeTree` tables and materialized views.

pub fn all_ddl() -> Vec<&'static str> {
    vec![
        include_str!("sql/tick_events.sql"),
        include_str!("sql/book_l2_replay_hot.sql"),
        include_str!("sql/book_snapshots.sql"),
        include_str!("sql/book_decision_contexts.sql"),
        include_str!("sql/book_microstructure_1s.sql"),
        include_str!("sql/book_microstructure_1m.sql"),
        include_str!("sql/book_microstructure_1m_mv.sql"),
        include_str!("sql/opportunity_audit.sql"),
        include_str!("sql/opportunity_detection.sql"),
        include_str!("sql/calibration_snapshots.sql"),
    ]
}
