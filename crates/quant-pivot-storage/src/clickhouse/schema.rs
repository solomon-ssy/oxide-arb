//! `ClickHouse` DDL statements for `MergeTree` tables and materialized views.

/// Complete boot schema generated from the reviewed semantic manifest.
///
/// The project has not entered production, so `ClickHouse` owns one clean-slate
/// migration instead of replaying historical ALTER/data-repair waves.
pub(super) const BOOTSTRAP_SOURCES: &[&str] = &[include_str!("sql/bootstrap.sql")];

pub(super) const REQUIRED_SCHEMA_OBJECTS: [&str; 30] = [
    "book_microstructure_1m",
    "book_microstructure_1m_mv",
    "book_microstructure_1s",
    "market_resolution_event",
    "quant_book_l2_ledger",
    "quant_book_stream_session",
    "quant_capital_allocation_event",
    "quant_crypto_price_report",
    "quant_domain_event",
    "quant_domain_observation",
    "quant_entry_condition_evaluation_event",
    "quant_exchange_event",
    "quant_exchange_history_acceptance",
    "quant_exchange_log_raw",
    "quant_exchange_match",
    "quant_execution_event",
    "quant_execution_participant",
    "quant_exit_signal_evaluation_event",
    "quant_factor_event",
    "quant_feature_event",
    "quant_feature_parity_event",
    "quant_model_input_event",
    "quant_market_execution",
    "quant_position_event",
    "quant_report_recommendation_fact",
    "quant_report_market_funnel",
    "quant_serving_evidence_completion",
    "quant_signal_candidate_event",
    "quant_weather_forecast_fact",
    "quant_weather_observation_fact",
];

/// Extract the table-level TTL expression from normalized `CREATE TABLE` SQL.
///
/// Column TTLs are nested in the column list and intentionally ignored.
#[must_use]
pub fn extract_table_ttl(create_table_query: &str) -> Option<String> {
    let (_, ttl_end) = find_top_level_keyword(create_table_query, "TTL", 0)?;
    let expression_end = find_top_level_keyword(create_table_query, "SETTINGS", ttl_end)
        .map_or(create_table_query.len(), |(start, _)| start);
    let expression = create_table_query[ttl_end..expression_end]
        .trim()
        .trim_end_matches(';')
        .trim();
    (!expression.is_empty()).then(|| expression.to_owned())
}

pub(super) fn split_statements(sql: &str) -> Vec<String> {
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

fn find_top_level_keyword(sql: &str, keyword: &str, start_at: usize) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    let keyword = keyword.as_bytes();
    let mut index = start_at;
    let mut depth = 0_u32;
    let mut quote = None;

    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(delimiter) = quote {
            if byte == b'\\' && delimiter == b'\'' && index + 1 < bytes.len() {
                index += 2;
                continue;
            }
            if byte == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }

        match byte {
            b'\'' | b'"' | b'`' => {
                quote = Some(byte);
                index += 1;
            }
            b'(' => {
                depth = depth.saturating_add(1);
                index += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            _ if depth == 0 && is_identifier_start(byte) => {
                let start = index;
                index += 1;
                while index < bytes.len() && is_identifier_continue(bytes[index]) {
                    index += 1;
                }
                if bytes[start..index].eq_ignore_ascii_case(keyword) {
                    return Some((start, index));
                }
            }
            _ => index += 1,
        }
    }
    None
}

const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

const fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use super::{BOOTSTRAP_SOURCES, extract_table_ttl, split_statements};

    #[test]
    fn microstructure_rollup_preserves_availability() {
        let ddl = BOOTSTRAP_SOURCES
            .iter()
            .flat_map(|source| split_statements(source))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(ddl.contains("book_microstructure_1m_mv"));
        assert!(ddl.contains("max(available_at) AS available_at"));
    }

    #[test]
    fn market_resolution_uses_vectors() {
        let ddl = BOOTSTRAP_SOURCES
            .iter()
            .flat_map(|source| split_statements(source))
            .collect::<Vec<_>>()
            .join("\n");
        let normalized_ddl = ddl.split_whitespace().collect::<Vec<_>>().join(" ");

        assert!(normalized_ddl.contains("`token_ids` Array(String)"));
        assert!(normalized_ddl.contains("`payout_ratios` Array(Decimal(20, 18))"));
        assert!(!normalized_ddl.contains("`winning_token_id` String"));
        assert!(!normalized_ddl.contains("`winning_outcome` String"));
    }

    #[test]
    fn execution_facts_are_canonical() {
        let ddl = BOOTSTRAP_SOURCES
            .iter()
            .flat_map(|source| split_statements(source))
            .collect::<Vec<_>>()
            .join("\n");
        let normalized_ddl = ddl.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(normalized_ddl.contains("CREATE TABLE IF NOT EXISTS quant_market_execution"));
        assert!(normalized_ddl.contains("`model_available_at` DateTime64(3, 'UTC')"));
        assert!(normalized_ddl.contains("`availability_basis` Enum8('BlockConfirmation' = 1)"));
        assert!(normalized_ddl.contains(
            "ORDER BY (market_id, token_id, effective_at, block_number, transaction_index, log_index)"
        ));
        assert!(normalized_ddl.contains("`participant_role` Enum8('Maker' = 1, 'Taker' = 2)"));
        assert!(
            !normalized_ddl.contains("FROM quant_book_l2_ledger WHERE event_type = 'LastTrade'")
        );
        assert!(ddl.contains("non_replicated_deduplication_window = 10000"));
    }

    #[test]
    fn statement_splitter_never_comments() {
        let statements = split_statements(
            "-- an operator note; punctuation is not SQL\n\
             SELECT '-- remains string data'; -- trailing note; also not SQL\n",
        );

        assert_eq!(statements, ["SELECT '-- remains string data'"]);
    }

    #[test]
    fn table_ttl_ignores_keywords() {
        let ddl = "CREATE TABLE events (`TTL` String DEFAULT 'SETTINGS', \
                   event_time DateTime TTL event_time + toIntervalDay(1)) \
                   ENGINE = MergeTree ORDER BY event_time \
                   TTL event_time + toIntervalDay(365) DELETE \
                   SETTINGS index_granularity = 8192";
        assert_eq!(
            extract_table_ttl(ddl).as_deref(),
            Some("event_time + toIntervalDay(365) DELETE")
        );
        assert_eq!(
            extract_table_ttl(
                "CREATE TABLE x (d DateTime TTL d + toIntervalDay(1)) ENGINE = MergeTree ORDER BY d"
            ),
            None
        );
    }
}
