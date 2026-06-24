//! `ClickHouse` DDL statements for `MergeTree` tables and materialized views.

pub fn all_ddl() -> Vec<String> {
    [
        include_str!("sql/tick_events.sql"),
        include_str!("sql/book_l2_replay_hot.sql"),
        include_str!("sql/book_snapshots.sql"),
        include_str!("sql/book_microstructure_1s.sql"),
        include_str!("sql/book_microstructure_1m.sql"),
        include_str!("sql/book_microstructure_1m_mv.sql"),
        include_str!("sql/book_decision_contexts.sql"),
        include_str!("sql/market_resolution_event.sql"),
        include_str!("sql/quant_facts.sql"),
    ]
    .into_iter()
    .flat_map(split_statements)
    .collect()
}

fn split_statements(sql: &'static str) -> Vec<String> {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
