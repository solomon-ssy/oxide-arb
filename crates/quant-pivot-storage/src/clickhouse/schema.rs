//! `ClickHouse` DDL statements for `MergeTree` tables and materialized views.

pub fn all_ddl() -> Vec<String> {
    [
        include_str!("sql/tick_events.sql"),
        include_str!("sql/book_l2_replay_hot.sql"),
        include_str!("sql/book_snapshots.sql"),
        include_str!("sql/book_microstructure_1s.sql"),
        include_str!("sql/book_microstructure_1m.sql"),
        include_str!("sql/book_microstructure_1m_mv.sql"),
        include_str!("sql/market_resolution_event.sql"),
        include_str!("sql/quant_trade_tape.sql"),
        include_str!("sql/quant_domain_observation.sql"),
        include_str!("sql/quant_facts.sql"),
        include_str!("sql/quant_feature_parity.sql"),
    ]
    .into_iter()
    .flat_map(split_statements)
    .collect()
}

fn split_statements(sql: &'static str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut statement = String::new();
    let mut quote = None;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(delimiter) = quote {
            statement.push(ch);
            if ch == delimiter {
                if chars.peek() == Some(&delimiter) {
                    statement.push(delimiter);
                    let _ = chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => {
                quote = Some(ch);
                statement.push(ch);
            }
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                for comment_ch in chars.by_ref() {
                    if comment_ch == '\n' {
                        statement.push('\n');
                        break;
                    }
                }
            }
            ';' => push_statement(&mut statements, &mut statement),
            _ => statement.push(ch),
        }
    }
    push_statement(&mut statements, &mut statement);
    statements
}

fn push_statement(statements: &mut Vec<String>, statement: &mut String) {
    let trimmed = statement.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_owned());
    }
    statement.clear();
}

#[cfg(test)]
mod tests {
    use super::{all_ddl, split_statements};

    #[test]
    fn schema_replaces_legacy_microstructure_view_and_has_no_parallel_capture_table() {
        let ddl = all_ddl().join("\n");
        assert!(ddl.contains("DROP TABLE IF EXISTS book_microstructure_1m_mv"));
        assert!(ddl.contains("book_microstructure_1m_availability_v2_mv"));
        assert!(ddl.contains("max(available_at) AS available_at"));
    }

    #[test]
    fn statement_splitter_never_executes_line_comments() {
        let statements = split_statements(
            "-- an operator note; punctuation is not SQL\n\
             SELECT '-- remains string data'; -- trailing note; also not SQL\n",
        );

        assert_eq!(statements, ["SELECT '-- remains string data'"]);
    }
}
