//! On-chain `OrderFilled` trade-tape ingestion worker.

use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_api::exchange::{
    DecodeRejectReason, EXCHANGE_CONTRACTS, ExchangeContract, ExchangeLogClient, FetchedLog,
    constants::ExchangeVersion, normalize_v1_log, normalize_v2_log,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::TradeTapeRow,
    config::TradeTapeOnChainConfig,
    domain::{
        TradeTapeBlockCursorInfo, TradeTapeBlockCursorStatus, TradeTapePrint, TradeTapeSourceKind,
        UpsertTradeTapeBlockCursor,
    },
    types::TokenId,
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{FactWriter, TradeTapeBlockCursorRepository},
};

use crate::{infra::periodic_task::PeriodicTask, ingest::market_registry::MarketRegistry};
use tokio_util::sync::CancellationToken;

/// Periodically ingests Polygon `OrderFilled` logs into `quant_trade_tape`.
pub struct TradeTapeWorker {
    log_client: Arc<ExchangeLogClient>,
    market_registry: Arc<MarketRegistry>,
    block_cursor_repo: Arc<dyn TradeTapeBlockCursorRepository>,
    writer: Arc<ChFactWriter<TradeTapeRow>>,
    config: TradeTapeOnChainConfig,
}

/// Checkpoint advanced only after a successful `ClickHouse` write (or after a
/// no-print block-range scan that made forward progress).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ContractCheckpoint {
    contract_address: String,
    last_finalized_block: u64,
    last_log_index: i32,
    head_lag_blocks: u64,
    status: TradeTapeBlockCursorStatus,
}

/// One contract scan tick — prints to persist plus the cursor row to commit
/// after `ClickHouse` acknowledges the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ContractScanOutcome {
    prints: Vec<TradeTapePrint>,
    checkpoint: ContractCheckpoint,
}

impl TradeTapeWorker {
    #[must_use]
    pub fn new(
        log_client: Arc<ExchangeLogClient>,
        market_registry: Arc<MarketRegistry>,
        block_cursor_repo: Arc<dyn TradeTapeBlockCursorRepository>,
        writer: Arc<ChFactWriter<TradeTapeRow>>,
        config: TradeTapeOnChainConfig,
    ) -> Self {
        Self {
            log_client,
            market_registry,
            block_cursor_repo,
            writer,
            config,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        if !self.config.enabled {
            tracing::info!("trade-tape worker disabled");
            shutdown.cancelled().await;
            return Ok(());
        }
        let poll_secs = self.config.poll_secs;
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "trade-tape-worker",
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
        let head = self
            .log_client
            .head_block()
            .await
            .map_err(|error| QuantError::Rpc(error.into()))?;
        let safe_head = head.saturating_sub(self.config.confirmations);

        let mut outcomes = Vec::with_capacity(EXCHANGE_CONTRACTS.len());
        for contract in EXCHANGE_CONTRACTS {
            outcomes.push(self.scan_contract(contract, safe_head).await?);
        }

        let mut all_prints = Vec::new();
        for outcome in &outcomes {
            all_prints.extend(outcome.prints.iter().cloned());
        }
        let prints = dedup_prints(all_prints);

        if !prints.is_empty() {
            let ingestion_time = Utc::now();
            let rows = prints
                .into_iter()
                .map(|print| print.into_clickhouse_row(ingestion_time))
                .collect::<Vec<_>>();
            for batch in rows.chunks(self.config.batch_size.max(1)) {
                self.writer.write_batch(batch.to_vec()).await?;
            }
        }

        for checkpoint in outcomes.into_iter().map(|outcome| outcome.checkpoint) {
            self.commit_checkpoint(checkpoint).await?;
        }
        Ok(())
    }

    async fn scan_contract(
        &self,
        contract: ExchangeContract,
        safe_head: u64,
    ) -> QuantResult<ContractScanOutcome> {
        let source = TradeTapeSourceKind::OnChain.as_str();
        let contract_address = format!("{:#x}", contract.address);
        let cursor = self
            .block_cursor_repo
            .find(source, &contract_address)
            .await?;
        let (resume_block, resume_log_index) =
            resume_point(cursor.as_ref(), contract.bootstrap_block);

        if resume_block > safe_head {
            let lag = safe_head.saturating_sub(resume_block);
            return Ok(ContractScanOutcome {
                prints: Vec::new(),
                checkpoint: ContractCheckpoint {
                    contract_address,
                    last_finalized_block: resume_block,
                    last_log_index: cursor.as_ref().map_or(0, |row| row.last_log_index),
                    head_lag_blocks: lag,
                    status: TradeTapeBlockCursorStatus::Live,
                },
            });
        }

        let end_block = resume_block
            .saturating_add(self.config.max_blocks_per_tick)
            .min(safe_head);
        let fetched_logs = self
            .log_client
            .fetch_order_filled_logs(contract.address, contract.topic, resume_block, end_block)
            .await
            .map_err(|error| QuantError::Rpc(error.into()))?;

        let market_for_token = |token_id: &TokenId| self.market_registry.market_for_token(token_id);

        let mut prints = Vec::new();
        let mut rejected_unknown_token = 0_u64;
        for fetched in &fetched_logs {
            let log_index = fetched.log.log_index.unwrap_or(0);
            if !should_process_log(
                fetched.block_number,
                log_index,
                resume_block,
                resume_log_index,
            ) {
                continue;
            }
            let normalized = match contract.version {
                ExchangeVersion::V1 => normalize_v1_log(contract, fetched, market_for_token),
                ExchangeVersion::V2 => normalize_v2_log(contract, fetched, market_for_token),
            };
            match normalized {
                Ok(normalized) => {
                    prints.push(normalized.primary);
                    if let Some(secondary) = normalized.secondary_taker {
                        prints.push(secondary);
                    }
                }
                Err(DecodeRejectReason::UnknownToken) => {
                    rejected_unknown_token += 1;
                }
                Err(_) => {}
            }
        }

        if rejected_unknown_token > 0 {
            tracing::debug!(
                contract = contract.key,
                rejected_unknown_token,
                "trade-tape fills skipped: token not in Gamma catalog"
            );
        }

        let lag = safe_head.saturating_sub(end_block);
        let status = if end_block < safe_head {
            TradeTapeBlockCursorStatus::CatchingUp
        } else if cursor.is_none() {
            TradeTapeBlockCursorStatus::Bootstrap
        } else {
            TradeTapeBlockCursorStatus::Live
        };
        let last_log_index =
            checkpoint_log_index(resume_block, resume_log_index, end_block, &fetched_logs);

        Ok(ContractScanOutcome {
            prints,
            checkpoint: ContractCheckpoint {
                contract_address,
                last_finalized_block: end_block,
                last_log_index,
                head_lag_blocks: lag,
                status,
            },
        })
    }

    async fn commit_checkpoint(&self, checkpoint: ContractCheckpoint) -> QuantResult<()> {
        let source = TradeTapeSourceKind::OnChain.as_str();
        self.block_cursor_repo
            .upsert(UpsertTradeTapeBlockCursor {
                source: source.to_owned(),
                contract_address: checkpoint.contract_address,
                last_finalized_block: i64::try_from(checkpoint.last_finalized_block)
                    .unwrap_or(i64::MAX),
                last_log_index: checkpoint.last_log_index,
                head_lag_blocks: i64::try_from(checkpoint.head_lag_blocks).unwrap_or(i64::MAX),
                status: checkpoint.status.as_str().to_owned(),
                updated_at: Utc::now(),
            })
            .await?;
        Ok(())
    }
}

/// Resume point: `(block, log_index)` of the last persisted log.
fn resume_point(cursor: Option<&TradeTapeBlockCursorInfo>, bootstrap_block: u64) -> (u64, i32) {
    cursor.map_or((bootstrap_block, -1), |row| {
        (
            u64::try_from(row.last_finalized_block).unwrap_or(bootstrap_block),
            row.last_log_index,
        )
    })
}

/// Whether a fetched log lies strictly after the persisted checkpoint.
#[must_use]
fn should_process_log(
    block_number: u64,
    log_index: u64,
    resume_block: u64,
    resume_log_index: i32,
) -> bool {
    if block_number > resume_block {
        return true;
    }
    if block_number < resume_block {
        return false;
    }
    i32::try_from(log_index).is_ok_and(|index| index > resume_log_index)
}

/// Highest processed log index on `end_block` after scanning `[resume_block, end_block]`.
#[must_use]
fn checkpoint_log_index(
    resume_block: u64,
    resume_log_index: i32,
    end_block: u64,
    fetched_logs: &[FetchedLog],
) -> i32 {
    let mut max_index = if end_block > resume_block {
        0
    } else {
        resume_log_index
    };
    for fetched in fetched_logs {
        if fetched.block_number != end_block {
            continue;
        }
        if !should_process_log(
            fetched.block_number,
            fetched.log.log_index.unwrap_or(0),
            resume_block,
            resume_log_index,
        ) {
            continue;
        }
        if let Some(index) = fetched
            .log
            .log_index
            .and_then(|value| i32::try_from(value).ok())
        {
            max_index = max_index.max(index);
        }
    }
    max_index
}

fn dedup_prints(prints: Vec<TradeTapePrint>) -> Vec<TradeTapePrint> {
    let mut seen = HashSet::<String>::new();
    let mut deduped = Vec::with_capacity(prints.len());
    for print in prints {
        if seen.insert(print.trade_id.clone()) {
            deduped.push(print);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::{
        domain::TradeParticipantRole,
        types::{MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal_macros::dec;

    #[test]
    fn should_process_log_respects_checkpoint() {
        assert!(should_process_log(101, 0, 100, 5));
        assert!(!should_process_log(100, 5, 100, 5));
        assert!(should_process_log(100, 6, 100, 5));
        assert!(!should_process_log(99, 99, 100, 5));
        assert!(should_process_log(100, 0, 100, -1));
    }

    #[test]
    fn resume_point_bootstraps_from_contract_block() {
        assert_eq!(resume_point(None, 57_000_000), (57_000_000, -1));
    }

    #[test]
    fn dedup_prints_keeps_first_occurrence() {
        let print = TradeTapePrint {
            market_id: MarketId::new("m1"),
            token_id: TokenId::new("t1"),
            event_time: Utc::now(),
            available_at: None,
            participant_address: "0xabc".to_owned(),
            participant_role: TradeParticipantRole::Maker,
            side: None,
            price: Price::new(dec!(0.5)),
            size_shares: Shares::new(dec!(1)),
            notional_usd: Usd::new(dec!(0.5)),
            tx_hash: None,
            trade_id: "trade-1:maker".to_owned(),
            source: TradeTapeSourceKind::OnChain,
            coverage_flags: 0,
            raw_payload_json: None,
        };
        let deduped = dedup_prints(vec![print.clone(), print]);
        assert_eq!(deduped.len(), 1);
    }
}
