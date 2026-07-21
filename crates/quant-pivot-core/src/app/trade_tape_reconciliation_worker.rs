//! One-to-one reconciliation of Market WS prints with finalized on-chain fills.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::TradeTapeRow,
    config::TradeTapeOnChainConfig,
    domain::data_plane::trade_tape::trade_tape_coverage::{SIDE, SIZE},
    enums::clickhouse::{ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource},
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChNativeReadRepository},
    traits::FactWriter,
};
use tokio_util::sync::CancellationToken;

use crate::infra::periodic_task::PeriodicTask;

pub struct TradeTapeReconciliationWorker {
    reader: Arc<ChNativeReadRepository>,
    writer: Arc<ChFactWriter<TradeTapeRow>>,
    config: TradeTapeOnChainConfig,
}

impl TradeTapeReconciliationWorker {
    #[must_use]
    pub const fn new(
        reader: Arc<ChNativeReadRepository>,
        writer: Arc<ChFactWriter<TradeTapeRow>>,
        config: TradeTapeOnChainConfig,
    ) -> Self {
        Self {
            reader,
            writer,
            config,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        let poll_secs = self.config.poll_secs;
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "trade-tape-reconciliation-worker",
            move || Duration::from_secs(poll_secs),
            0.05,
            false,
            shutdown,
            move || {
                let worker = Arc::clone(&worker);
                async move { worker.run_once().await }
            },
        )
        .await
    }

    pub async fn run_once(&self) -> QuantResult<()> {
        let now_ms = Utc::now().timestamp_millis();
        let lookback_ms = millis_from_secs(self.config.reconciliation_lookback_secs)?;
        let rows = self
            .reader
            .trade_tape_reconciliation_rows(
                now_ms.saturating_sub(lookback_ms),
                now_ms,
                self.config.reconciliation_max_rows,
            )
            .await?;
        let revisions = reconcile_rows(
            &rows,
            now_ms,
            self.config.reconciliation_match_window_ms,
            millis_from_secs(self.config.reconciliation_terminal_age_secs)?,
        )?;
        for batch in revisions.chunks(self.config.batch_size.max(1)) {
            self.writer.write_batch(batch.to_vec()).await?;
        }
        Ok(())
    }
}

fn millis_from_secs(seconds: u64) -> QuantResult<i64> {
    i64::try_from(seconds)
        .ok()
        .and_then(|value| value.checked_mul(1_000))
        .ok_or_else(|| QuantError::config("trade reconciliation duration overflow"))
}

fn reconcile_rows(
    rows: &[TradeTapeRow],
    now_ms: i64,
    match_window_ms: u64,
    terminal_age_ms: i64,
) -> QuantResult<Vec<TradeTapeRow>> {
    let match_window_ms = i64::try_from(match_window_ms)
        .map_err(|error| QuantError::config(format!("match window overflow: {error}")))?;
    let ws = pending_indices(rows, ChTradeTapeSource::MarketWs);
    let chain = pending_indices(rows, ChTradeTapeSource::OnChainOrderFilled);
    let candidates = ws
        .iter()
        .map(|ws_index| {
            chain
                .iter()
                .copied()
                .filter(|chain_index| {
                    observations_match(&rows[*ws_index], &rows[*chain_index], match_window_ms)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut chain_degree = vec![0_usize; rows.len()];
    for matches in &candidates {
        for index in matches {
            chain_degree[*index] += 1;
        }
    }
    build_reconciliation_revisions(
        rows,
        &ws,
        &candidates,
        &chain_degree,
        now_ms,
        terminal_age_ms,
    )
}

fn pending_indices(rows: &[TradeTapeRow], source: ChTradeTapeSource) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let pending = match source {
                ChTradeTapeSource::MarketWs => {
                    row.reconciliation_status == ChTradeReconciliationStatus::Pending
                }
                ChTradeTapeSource::OnChainOrderFilled => {
                    row.reconciliation_status == ChTradeReconciliationStatus::OnChainOnly
                }
            };
            (row.source == source && pending).then_some(index)
        })
        .collect()
}

fn observations_match(ws: &TradeTapeRow, chain: &TradeTapeRow, window_ms: i64) -> bool {
    let fields_observed =
        ws.observed_field_flags & SIDE != 0 && ws.observed_field_flags & SIZE != 0;
    fields_observed
        && ws.market_id == chain.market_id
        && ws.token_id == chain.token_id
        && ws.side != ChTradeSide::Unknown
        && ws.side == chain.side
        && ws.price == chain.price
        && ws.size_shares == chain.size_shares
        && ws.event_time.abs_diff(chain.event_time) <= window_ms.unsigned_abs()
}

fn build_reconciliation_revisions(
    rows: &[TradeTapeRow],
    ws: &[usize],
    candidates: &[Vec<usize>],
    chain_degree: &[usize],
    now_ms: i64,
    terminal_age_ms: i64,
) -> QuantResult<Vec<TradeTapeRow>> {
    let mut revisions = Vec::new();
    let mut ambiguous_chain = BTreeSet::new();
    for (position, ws_index) in ws.iter().copied().enumerate() {
        let matches = &candidates[position];
        if matches.len() == 1 && chain_degree[matches[0]] == 1 {
            let chain_index = matches[0];
            revisions.push(revision(
                &rows[ws_index],
                ChTradeReconciliationStatus::Matched,
                Some(rows[chain_index].source_event_id.clone()),
                now_ms,
            )?);
            revisions.push(revision(
                &rows[chain_index],
                ChTradeReconciliationStatus::Matched,
                Some(rows[ws_index].source_event_id.clone()),
                now_ms,
            )?);
        } else if !matches.is_empty() {
            revisions.push(revision(
                &rows[ws_index],
                ChTradeReconciliationStatus::Ambiguous,
                None,
                now_ms,
            )?);
            ambiguous_chain.extend(matches.iter().copied());
        } else if now_ms.saturating_sub(rows[ws_index].event_time) >= terminal_age_ms {
            revisions.push(revision(
                &rows[ws_index],
                ChTradeReconciliationStatus::Unavailable,
                None,
                now_ms,
            )?);
        }
    }
    for index in ambiguous_chain {
        revisions.push(revision(
            &rows[index],
            ChTradeReconciliationStatus::Ambiguous,
            None,
            now_ms,
        )?);
    }
    Ok(revisions)
}

fn revision(
    row: &TradeTapeRow,
    status: ChTradeReconciliationStatus,
    matched_source_event_id: Option<String>,
    now_ms: i64,
) -> QuantResult<TradeTapeRow> {
    let revision = row
        .revision
        .checked_add(1)
        .ok_or_else(|| QuantError::config("trade-tape revision overflow"))?;
    Ok(TradeTapeRow {
        ingestion_time: now_ms,
        reconciliation_status: status,
        matched_source_event_id,
        revision,
        reconciled_at: Some(now_ms),
        ..row.clone()
    })
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        clickhouse::{ChPrice, ChShares, ChUsd, TradeTapeRow},
        domain::data_plane::trade_tape::trade_tape_coverage::{SIDE, SIZE},
        enums::clickhouse::{
            ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
        },
        types::{MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal_macros::dec;

    use super::reconcile_rows;

    fn row(source: ChTradeTapeSource, id: &str, event_time: i64) -> TradeTapeRow {
        TradeTapeRow {
            market_id: MarketId::new("market"),
            token_id: TokenId::new("token"),
            event_time,
            ingestion_time: event_time,
            stream_session_id: None,
            token_sequence: None,
            participant_address: String::new(),
            participant_role: ChTradeParticipantRole::Unknown,
            side: ChTradeSide::Buy,
            price: ChPrice::from(Price::new(dec!(0.5))),
            size_shares: ChShares::from(Shares::new(dec!(10))),
            notional_usd: ChUsd::from(Usd::new(dec!(5))),
            tx_hash: None,
            source_event_id: id.to_owned(),
            source,
            observed_field_flags: SIDE | SIZE,
            fee_rate_bps: None,
            reconciliation_status: match source {
                ChTradeTapeSource::MarketWs => ChTradeReconciliationStatus::Pending,
                ChTradeTapeSource::OnChainOrderFilled => ChTradeReconciliationStatus::OnChainOnly,
            },
            matched_source_event_id: None,
            revision: 1,
            reconciled_at: None,
            raw_payload_json: None,
            schema_version: TradeTapeRow::SCHEMA_VERSION,
        }
    }

    #[test]
    fn unique_exact_pair_is_reconciled_both_ways() {
        let rows = vec![
            row(ChTradeTapeSource::MarketWs, "ws", 10_000),
            row(ChTradeTapeSource::OnChainOrderFilled, "chain", 11_000),
        ];
        let revisions = reconcile_rows(&rows, 20_000, 2_000, 5_000).expect("reconcile");
        assert_eq!(revisions.len(), 2);
        assert!(revisions.iter().all(|row| {
            row.reconciliation_status == ChTradeReconciliationStatus::Matched
                && row.matched_source_event_id.is_some()
                && row.revision == 2
        }));
    }

    #[test]
    fn duplicate_candidate_is_ambiguous_not_optimistically_matched() {
        let rows = vec![
            row(ChTradeTapeSource::MarketWs, "ws", 10_000),
            row(ChTradeTapeSource::OnChainOrderFilled, "chain-1", 10_500),
            row(ChTradeTapeSource::OnChainOrderFilled, "chain-2", 10_700),
        ];
        let revisions = reconcile_rows(&rows, 20_000, 2_000, 5_000).expect("reconcile");
        assert_eq!(revisions.len(), 3);
        assert!(revisions.iter().all(|row| {
            row.reconciliation_status == ChTradeReconciliationStatus::Ambiguous
                && row.matched_source_event_id.is_none()
        }));
    }

    #[test]
    fn old_unmatched_ws_print_becomes_unavailable() {
        let rows = vec![row(ChTradeTapeSource::MarketWs, "ws", 10_000)];
        let revisions = reconcile_rows(&rows, 20_000, 2_000, 5_000).expect("reconcile");
        assert_eq!(revisions.len(), 1);
        assert_eq!(
            revisions[0].reconciliation_status,
            ChTradeReconciliationStatus::Unavailable
        );
    }
}
