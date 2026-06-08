use std::{collections::HashMap, str::FromStr};

use chrono::{DateTime, TimeZone, Utc};
use oxide_arb_models::{
    clickhouse::{BookSnapshotRow, ChPrice, ChShares, TickEventL2Row},
    domain::{
        book::BookLevel,
        control_factor::{
            EvidenceSourceBundle, InputResolutionReport, MarketReplayContext, QueryFingerprint,
        },
    },
    enums::clickhouse::ChBookEventType,
    types::{MarketId, Price, Shares, TokenId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::materialization::{MaterializationError, MaterializationResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookReconstructionArtifact {
    pub report: BookReconstructionReport,
    pub market_books: Vec<MarketBookReconstruction>,
    pub token_timelines: Vec<ReconstructedTokenBookTimeline>,
    pub decision_views: Vec<DecisionBookView>,
    pub source_bundle: EvidenceSourceBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookReconstructionReport {
    pub token_count_expected: u64,
    pub token_count_reconstructed: u64,
    pub l2_event_count: u64,
    pub snapshot_bootstrap_count: u64,
    pub gap_count: u64,
    pub max_gap_ms: u64,
    pub median_book_age_ms: u64,
    pub p95_book_age_ms: u64,
    pub crossed_book_count: u64,
    pub invalid_level_count: u64,
    pub stale_interval_ms: u64,
    pub insufficient_reasons: Vec<String>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

impl BookReconstructionReport {
    #[must_use]
    pub fn production_eligible(&self) -> bool {
        self.insufficient_reasons.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketBookReconstruction {
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub settlement_deadline: Option<DateTime<Utc>>,
    pub yes_book: Option<ReconstructedTokenBook>,
    pub no_book: Option<ReconstructedTokenBook>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconstructedTokenBook {
    pub token_id: TokenId,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub event_time: DateTime<Utc>,
    pub book_version: u64,
    pub source_event_count: u64,
    pub invalid_level_count: u64,
    pub crossed: bool,
    pub max_gap_ms: u64,
    pub stale_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconstructedTokenBookTimeline {
    pub token_id: TokenId,
    pub books: Vec<ReconstructedTokenBook>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionMarketBook {
    pub market_id: MarketId,
    pub decision_time: DateTime<Utc>,
    pub yes_book: Option<ReconstructedTokenBook>,
    pub no_book: Option<ReconstructedTokenBook>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionBookViewRequest {
    pub market_id: MarketId,
    pub decision_time: DateTime<Utc>,
    pub purpose: DecisionBookViewPurpose,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionBookViewPurpose {
    Detection,
    TerminalExecution,
    ExitSimulation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionBookView {
    pub market_id: MarketId,
    pub decision_time: DateTime<Utc>,
    pub purpose: DecisionBookViewPurpose,
    pub yes_book: Option<DecisionTokenBookView>,
    pub no_book: Option<DecisionTokenBookView>,
    pub production_eligible: bool,
    pub insufficient_reasons: Vec<String>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionTokenBookView {
    pub book: ReconstructedTokenBook,
    pub book_age_ms: u64,
    pub max_gap_ms: u64,
    pub stale: bool,
    pub crossed: bool,
    pub invalid_level_count: u64,
}

impl BookReconstructionArtifact {
    #[must_use]
    pub fn book_at(
        &self,
        market_id: &MarketId,
        decision_time: DateTime<Utc>,
    ) -> Option<DecisionMarketBook> {
        let market_book = self
            .market_books
            .iter()
            .find(|market_book| &market_book.market_id == market_id)?;
        Some(DecisionMarketBook {
            market_id: market_id.clone(),
            decision_time,
            yes_book: self.token_book_at(&market_book.yes_token_id, decision_time),
            no_book: self.token_book_at(&market_book.no_token_id, decision_time),
        })
    }

    #[must_use]
    pub fn token_book_at(
        &self,
        token_id: &TokenId,
        decision_time: DateTime<Utc>,
    ) -> Option<ReconstructedTokenBook> {
        self.token_timelines
            .iter()
            .find(|timeline| &timeline.token_id == token_id)?
            .books
            .iter()
            .rev()
            .find(|book| book.event_time <= decision_time)
            .cloned()
    }

    pub fn materialize_decision_views(
        &mut self,
        requests: Vec<DecisionBookViewRequest>,
        stale_book_after_ms: u64,
    ) {
        self.decision_views = requests
            .into_iter()
            .map(|request| self.decision_view(request, stale_book_after_ms))
            .collect();
    }

    #[must_use]
    pub fn decision_view(
        &self,
        request: DecisionBookViewRequest,
        stale_book_after_ms: u64,
    ) -> DecisionBookView {
        let market_book = self
            .market_books
            .iter()
            .find(|market_book| market_book.market_id == request.market_id);
        let yes_book = market_book
            .and_then(|market_book| {
                self.token_book_at(&market_book.yes_token_id, request.decision_time)
            })
            .map(|book| token_view(book, request.decision_time, stale_book_after_ms));
        let no_book = market_book
            .and_then(|market_book| {
                self.token_book_at(&market_book.no_token_id, request.decision_time)
            })
            .map(|book| token_view(book, request.decision_time, stale_book_after_ms));
        let mut insufficient_reasons = Vec::new();
        record_decision_view_reason(&mut insufficient_reasons, "yes", yes_book.as_ref());
        record_decision_view_reason(&mut insufficient_reasons, "no", no_book.as_ref());
        DecisionBookView {
            market_id: request.market_id,
            decision_time: request.decision_time,
            purpose: request.purpose,
            yes_book,
            no_book,
            production_eligible: insufficient_reasons.is_empty(),
            insufficient_reasons,
            query_fingerprints: self.report.query_fingerprints.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BookReconstructionInput {
    pub input_report: InputResolutionReport,
    pub snapshots: Vec<BookSnapshotRow>,
    pub l2_events: Vec<TickEventL2Row>,
    pub max_replay_gap_ms: u64,
    pub stale_book_after_ms: u64,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

#[must_use]
pub fn expected_tokens(input_report: &InputResolutionReport) -> Vec<TokenId> {
    let mut tokens = Vec::with_capacity(input_report.market_contexts.len() * 2);
    for context in &input_report.market_contexts {
        tokens.push(context.yes_token_id.clone());
        tokens.push(context.no_token_id.clone());
    }
    tokens.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    tokens.dedup();
    tokens
}

pub fn reconstruct(
    input: &BookReconstructionInput,
) -> MaterializationResult<BookReconstructionArtifact> {
    let token_ids = expected_tokens(&input.input_report);
    let state = reconstruct_tokens(input, &token_ids)?;

    let market_books = input
        .input_report
        .market_contexts
        .iter()
        .map(|context| market_book(context, &state.books_by_token))
        .collect::<Vec<_>>();
    let report = BookReconstructionReport {
        token_count_expected: u64::try_from(token_ids.len()).unwrap_or(u64::MAX),
        token_count_reconstructed: u64::try_from(state.books_by_token.len()).unwrap_or(u64::MAX),
        l2_event_count: state.l2_event_count,
        snapshot_bootstrap_count: state.snapshot_bootstrap_count,
        gap_count: state.gap_count,
        max_gap_ms: state.max_gap_ms,
        median_book_age_ms: percentile(&mut state.book_ages.clone(), 50),
        p95_book_age_ms: percentile(&mut state.book_ages.clone(), 95),
        crossed_book_count: state.crossed_book_count,
        invalid_level_count: state.invalid_level_count,
        stale_interval_ms: state.stale_interval_ms,
        insufficient_reasons: state.insufficient_reasons,
        query_fingerprints: input.query_fingerprints.clone(),
    };
    let mut token_timelines = state.timelines_by_token.into_values().collect::<Vec<_>>();
    token_timelines.sort_by(|left, right| left.token_id.as_str().cmp(right.token_id.as_str()));
    Ok(BookReconstructionArtifact {
        report,
        market_books,
        token_timelines,
        decision_views: Vec::new(),
        source_bundle: input.input_report.source_bundle.clone(),
    })
}

#[derive(Debug, Default)]
struct ReconstructionState {
    books_by_token: HashMap<TokenId, ReconstructedTokenBook>,
    timelines_by_token: HashMap<TokenId, ReconstructedTokenBookTimeline>,
    book_ages: Vec<u64>,
    l2_event_count: u64,
    snapshot_bootstrap_count: u64,
    gap_count: u64,
    max_gap_ms: u64,
    crossed_book_count: u64,
    invalid_level_count: u64,
    stale_interval_ms: u64,
    insufficient_reasons: Vec<String>,
}

fn reconstruct_tokens(
    input: &BookReconstructionInput,
    token_ids: &[TokenId],
) -> MaterializationResult<ReconstructionState> {
    let mut snapshots_by_token = latest_snapshots_by_token(input.snapshots.clone());
    let events_by_token = l2_events_by_token(input.l2_events.clone());
    let mut state = ReconstructionState::default();
    for token_id in token_ids {
        reconstruct_token(
            input,
            token_id,
            snapshots_by_token.remove(token_id),
            events_by_token.get(token_id).map_or(&[][..], Vec::as_slice),
            &mut state,
        )?;
    }
    if state.gap_count > 0 {
        state.insufficient_reasons.push(format!(
            "max L2 gap {}ms exceeds configured {}ms",
            state.max_gap_ms, input.max_replay_gap_ms
        ));
    }
    Ok(state)
}

fn reconstruct_token(
    input: &BookReconstructionInput,
    token_id: &TokenId,
    snapshot: Option<BookSnapshotRow>,
    events: &[TickEventL2Row],
    state: &mut ReconstructionState,
) -> MaterializationResult<()> {
    state.l2_event_count = state
        .l2_event_count
        .saturating_add(u64::try_from(events.len()).unwrap_or(u64::MAX));
    let Some(snapshot) = snapshot else {
        state.insufficient_reasons.push(format!(
            "missing bootstrap snapshot for token {}",
            token_id.as_str()
        ));
        return Ok(());
    };
    state.snapshot_bootstrap_count = state.snapshot_bootstrap_count.saturating_add(1);
    let mut book = token_book_from_snapshot(snapshot)?;
    let mut timeline = ReconstructedTokenBookTimeline {
        token_id: token_id.clone(),
        books: vec![book.clone()],
    };
    for event in events {
        let event_time = Utc
            .timestamp_millis_opt(event.event_time)
            .single()
            .ok_or_else(|| {
                MaterializationError::Codec(format!(
                    "invalid L2 event timestamp millis: {}",
                    event.event_time
                ))
            })?;
        let gap_ms = u64::try_from(
            event_time
                .signed_duration_since(book.event_time)
                .num_milliseconds()
                .max(0),
        )
        .unwrap_or(u64::MAX);
        if gap_ms > input.max_replay_gap_ms {
            state.gap_count = state.gap_count.saturating_add(1);
            state.max_gap_ms = state.max_gap_ms.max(gap_ms);
        }
        apply_l2_event(&mut book, event)?;
        timeline.books.push(book.clone());
    }
    let age_ms = u64::try_from(
        input
            .input_report
            .window
            .to
            .signed_duration_since(book.event_time)
            .num_milliseconds()
            .max(0),
    )
    .unwrap_or(u64::MAX);
    state.book_ages.push(age_ms);
    if age_ms > input.stale_book_after_ms {
        book.stale_interval_ms = age_ms.saturating_sub(input.stale_book_after_ms);
        state.stale_interval_ms = state
            .stale_interval_ms
            .saturating_add(book.stale_interval_ms);
    }
    book.crossed = is_crossed(&book);
    if book.crossed {
        state.crossed_book_count = state.crossed_book_count.saturating_add(1);
        state
            .insufficient_reasons
            .push(format!("crossed book for token {}", token_id.as_str()));
    }
    state.invalid_level_count = state
        .invalid_level_count
        .saturating_add(book.invalid_level_count);
    state.max_gap_ms = state.max_gap_ms.max(book.max_gap_ms);
    if book.crossed {
        return Ok(());
    }
    state.books_by_token.insert(token_id.clone(), book);
    state.timelines_by_token.insert(token_id.clone(), timeline);
    Ok(())
}

fn latest_snapshots_by_token(rows: Vec<BookSnapshotRow>) -> HashMap<TokenId, BookSnapshotRow> {
    let mut rows = rows;
    rows.sort_by(|left, right| {
        left.token_id
            .as_str()
            .cmp(right.token_id.as_str())
            .then(left.event_time.cmp(&right.event_time).reverse())
            .then(left.ingestion_time.cmp(&right.ingestion_time).reverse())
            .then(left.sequence.cmp(&right.sequence).reverse())
    });
    let mut by_token = HashMap::new();
    for row in rows {
        by_token.entry(row.token_id.clone()).or_insert(row);
    }
    by_token
}

fn l2_events_by_token(rows: Vec<TickEventL2Row>) -> HashMap<TokenId, Vec<TickEventL2Row>> {
    let mut by_token: HashMap<TokenId, Vec<TickEventL2Row>> = HashMap::new();
    for row in rows {
        by_token.entry(row.token_id.clone()).or_default().push(row);
    }
    for rows in by_token.values_mut() {
        rows.sort_by(|left, right| {
            left.event_time
                .cmp(&right.event_time)
                .then(left.ingestion_time.cmp(&right.ingestion_time))
                .then(left.sequence.cmp(&right.sequence))
        });
    }
    by_token
}

fn token_book_from_snapshot(row: BookSnapshotRow) -> MaterializationResult<ReconstructedTokenBook> {
    let (bids, invalid_bids) = parse_json_levels(&row.bids_json)?;
    let (asks, invalid_asks) = parse_json_levels(&row.asks_json)?;
    let invalid_level_count = invalid_bids.saturating_add(invalid_asks);
    Ok(ReconstructedTokenBook {
        token_id: row.token_id,
        bids: sort_bid_levels(bids),
        asks: sort_ask_levels(asks),
        event_time: Utc
            .timestamp_millis_opt(row.event_time)
            .single()
            .ok_or_else(|| {
                MaterializationError::Codec(format!(
                    "invalid snapshot timestamp millis: {}",
                    row.event_time
                ))
            })?,
        book_version: row.book_version,
        source_event_count: 1,
        invalid_level_count,
        crossed: false,
        max_gap_ms: 0,
        stale_interval_ms: 0,
    })
}

fn apply_l2_event(
    book: &mut ReconstructedTokenBook,
    row: &TickEventL2Row,
) -> MaterializationResult<()> {
    let event_time = Utc
        .timestamp_millis_opt(row.event_time)
        .single()
        .ok_or_else(|| {
            MaterializationError::Codec(format!(
                "invalid L2 event timestamp millis: {}",
                row.event_time
            ))
        })?;
    let gap_ms = u64::try_from(
        event_time
            .signed_duration_since(book.event_time)
            .num_milliseconds()
            .max(0),
    )
    .unwrap_or(u64::MAX);
    book.max_gap_ms = book.max_gap_ms.max(gap_ms);
    let full_replacement = row.is_full_snapshot || row.event_type == ChBookEventType::Snapshot;
    let bids = levels_from_columns(&row.bid_prices, &row.bid_sizes, !full_replacement);
    let asks = levels_from_columns(&row.ask_prices, &row.ask_sizes, !full_replacement);
    book.invalid_level_count = book
        .invalid_level_count
        .saturating_add(bids.invalid_level_count)
        .saturating_add(asks.invalid_level_count);
    if full_replacement {
        book.bids = sort_bid_levels(bids.levels);
        book.asks = sort_ask_levels(asks.levels);
    } else if row.event_type == ChBookEventType::Delta {
        apply_level_changes(&mut book.bids, bids.changes, true);
        apply_level_changes(&mut book.asks, asks.changes, false);
    }
    book.event_time = event_time;
    book.book_version = row.book_version;
    book.source_event_count = book.source_event_count.saturating_add(1);
    Ok(())
}

fn parse_json_levels(raw: &str) -> MaterializationResult<(Vec<BookLevel>, u64)> {
    let pairs: Vec<[String; 2]> = serde_json::from_str(raw)
        .map_err(|error| MaterializationError::Codec(error.to_string()))?;
    let mut levels = Vec::new();
    let mut invalid_level_count = 0_u64;
    for pair in pairs {
        let price = Decimal::from_str(&pair[0])
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let size = Decimal::from_str(&pair[1])
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        if price <= Decimal::ZERO || price > Decimal::ONE || size <= Decimal::ZERO {
            invalid_level_count = invalid_level_count.saturating_add(1);
            continue;
        }
        if let Some(level) = BookLevel::try_from_decimal(Price::new(price), Shares::new(size)) {
            levels.push(level);
        } else {
            invalid_level_count = invalid_level_count.saturating_add(1);
        }
    }
    Ok((levels, invalid_level_count))
}

#[derive(Debug)]
struct ColumnLevels {
    levels: Vec<BookLevel>,
    changes: Vec<LevelChange>,
    invalid_level_count: u64,
}

#[derive(Debug)]
struct LevelChange {
    price: Price,
    size: Shares,
}

fn levels_from_columns(prices: &[ChPrice], sizes: &[ChShares], zero_deletes: bool) -> ColumnLevels {
    let mut levels = Vec::new();
    let mut changes = Vec::new();
    let mut invalid_level_count =
        u64::try_from(prices.len().abs_diff(sizes.len())).unwrap_or(u64::MAX);
    for (price, size) in prices.iter().zip(sizes.iter()) {
        let price = price.to_price();
        let size = size.to_shares();
        let price_value = price.inner();
        let size_value = size.inner();
        if price_value <= Decimal::ZERO || price_value > Decimal::ONE {
            invalid_level_count = invalid_level_count.saturating_add(1);
            continue;
        }
        if size_value < Decimal::ZERO || (!zero_deletes && size_value <= Decimal::ZERO) {
            invalid_level_count = invalid_level_count.saturating_add(1);
            continue;
        }
        changes.push(LevelChange { price, size });
        if size_value > Decimal::ZERO {
            if let Some(level) = BookLevel::try_from_decimal(price, size) {
                levels.push(level);
            } else {
                invalid_level_count = invalid_level_count.saturating_add(1);
            }
        }
    }
    ColumnLevels {
        levels,
        changes,
        invalid_level_count,
    }
}

fn apply_level_changes(levels: &mut Vec<BookLevel>, changes: Vec<LevelChange>, bid_side: bool) {
    for change in changes {
        let price = change.price.inner();
        levels.retain(|level| level.price_decimal().inner() != price);
        if change.size.inner() > Decimal::ZERO {
            if let Some(level) = BookLevel::try_from_decimal(change.price, change.size) {
                levels.push(level);
            }
        }
    }
    if bid_side {
        sort_bid_levels_in_place(levels);
    } else {
        sort_ask_levels_in_place(levels);
    }
}

fn sort_bid_levels(mut levels: Vec<BookLevel>) -> Vec<BookLevel> {
    sort_bid_levels_in_place(&mut levels);
    levels
}

fn sort_bid_levels_in_place(levels: &mut [BookLevel]) {
    levels.sort_by(|left, right| {
        right
            .price_decimal()
            .inner()
            .cmp(&left.price_decimal().inner())
    });
}

fn sort_ask_levels(mut levels: Vec<BookLevel>) -> Vec<BookLevel> {
    sort_ask_levels_in_place(&mut levels);
    levels
}

fn sort_ask_levels_in_place(levels: &mut [BookLevel]) {
    levels.sort_by(|left, right| {
        left.price_decimal()
            .inner()
            .cmp(&right.price_decimal().inner())
    });
}

fn is_crossed(book: &ReconstructedTokenBook) -> bool {
    match (book.bids.first(), book.asks.first()) {
        (Some(bid), Some(ask)) => bid.price_decimal().inner() >= ask.price_decimal().inner(),
        _ => false,
    }
}

fn token_view(
    book: ReconstructedTokenBook,
    decision_time: DateTime<Utc>,
    stale_book_after_ms: u64,
) -> DecisionTokenBookView {
    let book_age_ms = u64::try_from(
        decision_time
            .signed_duration_since(book.event_time)
            .num_milliseconds()
            .max(0),
    )
    .unwrap_or(u64::MAX);
    DecisionTokenBookView {
        max_gap_ms: book.max_gap_ms,
        stale: book_age_ms > stale_book_after_ms,
        crossed: book.crossed,
        invalid_level_count: book.invalid_level_count,
        book,
        book_age_ms,
    }
}

fn record_decision_view_reason(
    insufficient_reasons: &mut Vec<String>,
    leg: &str,
    token_book: Option<&DecisionTokenBookView>,
) {
    let Some(token_book) = token_book else {
        insufficient_reasons.push(format!("{leg} decision book is missing"));
        return;
    };
    if token_book.stale {
        insufficient_reasons.push(format!(
            "{leg} decision book is stale: age={}ms",
            token_book.book_age_ms
        ));
    }
    if token_book.crossed {
        insufficient_reasons.push(format!("{leg} decision book is crossed"));
    }
    if token_book.invalid_level_count > 0 {
        insufficient_reasons.push(format!(
            "{leg} decision book has {} invalid levels",
            token_book.invalid_level_count
        ));
    }
}

fn market_book(
    context: &MarketReplayContext,
    books_by_token: &HashMap<TokenId, ReconstructedTokenBook>,
) -> MarketBookReconstruction {
    MarketBookReconstruction {
        market_id: context.market_id.clone(),
        yes_token_id: context.yes_token_id.clone(),
        no_token_id: context.no_token_id.clone(),
        settlement_deadline: context.settlement_deadline,
        yes_book: books_by_token.get(&context.yes_token_id).cloned(),
        no_book: books_by_token.get(&context.no_token_id).cloned(),
    }
}

fn percentile(values: &mut [u64], pct: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let idx = values
        .len()
        .saturating_sub(1)
        .saturating_mul(pct)
        .saturating_div(100);
    values[idx]
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use oxide_arb_models::{
        clickhouse::{BookSnapshotRow, ChPrice, ChSchemaVersion, ChShares, TickEventL2Row},
        domain::control_factor::{
            EvidenceSourceBundle, InputResolutionReport, MarketReplayContext,
            PointInTimeInputManifest, QueryFingerprint, TimeWindowSpec,
        },
        enums::{
            clickhouse::{ChBookEventType, ChFactSource, ChSnapshotReason},
            common::MarketCategory,
        },
        types::{EventId, MarketId, MaterializationRunId, Price, Shares, TokenId},
    };
    use rust_decimal_macros::dec;

    use crate::evidence::book::{BookReconstructionInput, reconstruct};

    #[test]
    fn missing_snapshot_blocks_production_book_reconstruction() {
        let input_report = input_report();
        let artifact = reconstruct(&BookReconstructionInput {
            input_report,
            snapshots: Vec::new(),
            l2_events: Vec::new(),
            max_replay_gap_ms: 1_000,
            stale_book_after_ms: 5_000,
            query_fingerprints: query_fingerprints(),
        })
        .expect("reconstruct");

        assert_eq!(artifact.report.token_count_expected, 2);
        assert_eq!(artifact.report.token_count_reconstructed, 0);
        assert!(!artifact.report.production_eligible());
        assert!(
            artifact
                .report
                .insufficient_reasons
                .iter()
                .any(|reason| reason.contains("missing bootstrap snapshot"))
        );
    }

    #[test]
    fn l2_gap_blocks_production_book_reconstruction() {
        let input_report = input_report();
        let artifact = reconstruct(&BookReconstructionInput {
            input_report,
            snapshots: vec![snapshot("yes", 1_000), snapshot("no", 1_000)],
            l2_events: vec![l2("yes", 3_000), l2("no", 3_000)],
            max_replay_gap_ms: 500,
            stale_book_after_ms: 5_000,
            query_fingerprints: query_fingerprints(),
        })
        .expect("reconstruct");

        assert_eq!(artifact.report.gap_count, 2);
        assert_eq!(artifact.report.max_gap_ms, 2_000);
        assert!(!artifact.report.production_eligible());
    }

    #[test]
    fn delta_updates_do_not_replace_full_book() {
        let input_report = input_report();
        let artifact = reconstruct(&BookReconstructionInput {
            input_report,
            snapshots: vec![snapshot("yes", 1_000), snapshot("no", 1_000)],
            l2_events: vec![l2("yes", 2_000)],
            max_replay_gap_ms: 5_000,
            stale_book_after_ms: 5_000,
            query_fingerprints: query_fingerprints(),
        })
        .expect("reconstruct");

        let yes_book = artifact.market_books[0]
            .yes_book
            .as_ref()
            .expect("yes book");
        assert_eq!(yes_book.bids.len(), 2);
        assert_eq!(yes_book.asks.len(), 2);
        assert!(
            yes_book
                .bids
                .iter()
                .any(|level| level.price_decimal() == Price::new(dec!(0.94)))
        );
        assert!(
            yes_book
                .bids
                .iter()
                .any(|level| level.price_decimal() == Price::new(dec!(0.93)))
        );
    }

    #[test]
    fn delta_zero_size_removes_price_level() {
        let input_report = input_report();
        let artifact = reconstruct(&BookReconstructionInput {
            input_report,
            snapshots: vec![snapshot("yes", 1_000), snapshot("no", 1_000)],
            l2_events: vec![l2_with("yes", 2_000, &[(dec!(0.94), dec!(0))], &[])],
            max_replay_gap_ms: 5_000,
            stale_book_after_ms: 5_000,
            query_fingerprints: query_fingerprints(),
        })
        .expect("reconstruct");

        let yes_book = artifact.market_books[0]
            .yes_book
            .as_ref()
            .expect("yes book");
        assert!(yes_book.bids.is_empty());
        assert_eq!(yes_book.invalid_level_count, 0);
    }

    #[test]
    fn book_at_returns_latest_book_before_decision_time() {
        let input_report = input_report();
        let artifact = reconstruct(&BookReconstructionInput {
            input_report,
            snapshots: vec![snapshot("yes", 1_000), snapshot("no", 1_000)],
            l2_events: vec![l2_with("yes", 2_000, &[(dec!(0.93), dec!(7))], &[])],
            max_replay_gap_ms: 5_000,
            stale_book_after_ms: 5_000,
            query_fingerprints: query_fingerprints(),
        })
        .expect("reconstruct");

        let before_delta = artifact
            .book_at(
                &MarketId::new("market"),
                Utc.timestamp_millis_opt(1_500).single().expect("decision"),
            )
            .expect("book before delta");
        let after_delta = artifact
            .book_at(
                &MarketId::new("market"),
                Utc.timestamp_millis_opt(2_500).single().expect("decision"),
            )
            .expect("book after delta");
        assert_eq!(
            before_delta
                .yes_book
                .expect("yes before")
                .bids
                .iter()
                .find(|level| level.price_decimal() == Price::new(dec!(0.94)))
                .expect("old level")
                .size_decimal(),
            Shares::new(dec!(10))
        );
        assert_eq!(
            after_delta
                .yes_book
                .expect("yes after")
                .bids
                .iter()
                .find(|level| level.price_decimal() == Price::new(dec!(0.93)))
                .expect("updated level")
                .size_decimal(),
            Shares::new(dec!(7))
        );
    }

    fn input_report() -> InputResolutionReport {
        let from = Utc.timestamp_millis_opt(1_000).single().expect("from");
        let to = Utc.timestamp_millis_opt(4_000).single().expect("to");
        InputResolutionReport {
            run_id: MaterializationRunId::new(oxide_arb_test_support::seeded_uuid("run")),
            window: TimeWindowSpec::new(from, to),
            manifest: PointInTimeInputManifest {
                inputs: Vec::new(),
                production_eligible: true,
                missing_inputs: Vec::new(),
                fatal_errors: Vec::new(),
                warnings: Vec::new(),
                manifest_hash: "hash".to_owned(),
            },
            market_contexts: vec![MarketReplayContext {
                market_id: MarketId::new("market"),
                event_id: Some(EventId::new("event")),
                yes_token_id: TokenId::new("yes"),
                no_token_id: TokenId::new("no"),
                category: Some(MarketCategory::Politics),
                settlement_deadline: Some(to),
                resolved_as_of: from,
                source_hash: "market_hash".to_owned(),
            }],
            source_bundle: EvidenceSourceBundle::empty(),
        }
    }

    fn query_fingerprints() -> Vec<QueryFingerprint> {
        vec![
            QueryFingerprint("test.book_snapshots:v1:blake3:fixture".to_owned()),
            QueryFingerprint("test.l2_events:v1:blake3:fixture".to_owned()),
        ]
    }

    fn snapshot(token_id: &str, event_time: i64) -> BookSnapshotRow {
        BookSnapshotRow {
            token_id: TokenId::new(token_id),
            market_id: Some(MarketId::new("market")),
            snapshot_reason: ChSnapshotReason::Periodic,
            top_n: 2,
            bids_json: r#"[["0.94","10"]]"#.to_owned(),
            asks_json: r#"[["0.95","10"]]"#.to_owned(),
            bid_depth_usd: None,
            ask_depth_usd: None,
            mid_price: None,
            spread_bps: None,
            book_version: 1,
            levels_count: 2,
            event_time,
            ingestion_time: event_time,
            sequence: 1,
            source: ChFactSource::WsSnapshot,
            schema_version: ChSchemaVersion(1),
        }
    }

    fn l2(token_id: &str, event_time: i64) -> TickEventL2Row {
        l2_with(
            token_id,
            event_time,
            &[(dec!(0.93), dec!(9))],
            &[(dec!(0.96), dec!(9))],
        )
    }

    fn l2_with(
        token_id: &str,
        event_time: i64,
        bids: &[(rust_decimal::Decimal, rust_decimal::Decimal)],
        asks: &[(rust_decimal::Decimal, rust_decimal::Decimal)],
    ) -> TickEventL2Row {
        TickEventL2Row {
            token_id: TokenId::new(token_id),
            market_id: Some(MarketId::new("market")),
            event_type: ChBookEventType::Delta,
            bid_prices: bids
                .iter()
                .map(|(price, _)| ChPrice::from(Price::new(*price)))
                .collect(),
            bid_sizes: bids
                .iter()
                .map(|(_, size)| ChShares::from(Shares::new(*size)))
                .collect(),
            ask_prices: asks
                .iter()
                .map(|(price, _)| ChPrice::from(Price::new(*price)))
                .collect(),
            ask_sizes: asks
                .iter()
                .map(|(_, size)| ChShares::from(Shares::new(*size)))
                .collect(),
            changed_levels_json: None,
            book_version: 2,
            levels_count: 2,
            is_full_snapshot: false,
            event_time,
            ingestion_time: event_time,
            sequence: 2,
            source: ChFactSource::WsDelta,
            schema_version: ChSchemaVersion(1),
        }
    }
}
