//! `ClickHouse` DDL statements for `MergeTree` tables and materialized views.

/// Complete boot schema generated from the reviewed semantic manifest.
///
/// The project has not entered production, so `ClickHouse` owns one clean-slate
/// migration instead of replaying historical ALTER/data-repair waves.
pub(super) const BOOTSTRAP_SOURCES: &[&str] = &[include_str!("sql/bootstrap.sql")];

pub(super) const REQUIRED_SCHEMA_OBJECTS: [&str; 27] = [
    "book_microstructure_1m",
    "book_microstructure_1m_mv",
    "book_microstructure_1s",
    "market_resolution_event",
    "quant_book_l2_checkpoint",
    "quant_book_l2_event",
    "quant_book_stream_session",
    "quant_capital_allocation_event",
    "quant_crypto_price_report",
    "quant_domain_event",
    "quant_domain_observation",
    "quant_entry_condition_evaluation_event",
    "quant_execution_event",
    "quant_exit_signal_evaluation_event",
    "quant_factor_event",
    "quant_feature_event",
    "quant_feature_parity_event",
    "quant_model_input_event",
    "quant_position_event",
    "quant_recommendation_attribution_event",
    "quant_report_recommendation_fact",
    "quant_report_market_funnel",
    "quant_serving_evidence_completion",
    "quant_signal_candidate_event",
    "quant_trade_tape",
    "quant_weather_forecast_fact",
    "quant_weather_observation_fact",
];

pub(super) const FORBIDDEN_SCHEMA_OBJECTS: [&str; 4] = [
    "book_microstructure_1m_availability_v2_mv",
    "quant_recommendation_event",
    "quant_weather_forecast_point",
    "quant_weather_observation_report",
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
    fn microstructure_rollup_preserves_latest_availability() {
        let ddl = BOOTSTRAP_SOURCES
            .iter()
            .flat_map(|source| split_statements(source))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(ddl.contains("book_microstructure_1m_mv"));
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

    #[test]
    fn table_ttl_extraction_ignores_column_ttl_and_quoted_keywords() {
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
