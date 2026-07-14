//! Settlement redemption service for resolved standard binary CTF markets.

use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{Arc, OnceLock},
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::{
    ctf::{CtfClient, CtfPendingRedeem, CtfSubmittedRedeemReceipt},
    keystore::OrderSigner,
    relayer::{RelayerClient, RelayerTxOutcome},
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantResult, execution::ExecutionError, rpc::RpcError};
use quant_pivot_models::{
    config::RelayerConfig,
    constants::COLLATERAL_SCALE,
    domain::{
        ConfirmSettlementRedeem, CoreEvent, CoreEventPublisher, MarketInfo, NewSettlementRedeem,
        NewSettlementRedeemLot, OrderIntentInfo, PositionExit, PositionInfo, SettlementRedeemInfo,
        SettlementRedeemLifecycleEvent, SettlementRedeemLotWrite,
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        execution::{ExitReason, ExitState, PositionLedgerState, SettlementRedeemState},
        quant::{ExecutionWalletKind, ExitSettlementMode, OutcomeSide, RedeemPolicy},
    },
    types::{
        MarketId, SettlementBalanceEvidence, SettlementPayoutVector, SettlementRedeemId,
        SettlementRedeemIndexSets, SettlementRedeemLotId, SettlementTokenBalance, Shares, TokenId,
        Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, MarketRepository, OrderIntentRepository, PositionRepository,
    SettlementRedeemRepository,
};
use rust_decimal::Decimal;

use crate::{
    governance::{KillSwitchHandle, RuntimeModeHandle},
    observability::{
        capital_allocation_fact_writer::CapitalAllocationEventWriter,
        ledger_fact_projection::{project_capital_event, project_position_event},
        position_fact_writer::PositionEventWriter,
    },
    runtime_config::RuntimeConfigStore,
};

// Gnosis CTF binary partition: YES = 0b01, NO = 0b10. This matches the
// Polymarket SDK CTF example and Gamma's `clobTokenIds[0]=YES,[1]=NO` mapping.
const STANDARD_BINARY_YES_INDEX_SET: u8 = 1;
const STANDARD_BINARY_NO_INDEX_SET: u8 = 2;

/// Payout vector returned by the CTF facade in decimal-string form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementCtfPayoutVector {
    pub denominator: String,
    pub yes: String,
    pub no: String,
}

impl SettlementCtfPayoutVector {
    #[must_use]
    pub fn is_resolved(&self) -> bool {
        self.denominator != "0"
    }
}

/// YES/NO raw ERC-1155 balances returned by the CTF facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementCtfBalances {
    pub yes_raw: String,
    pub no_raw: String,
}

/// Confirmed standard binary redeem receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementCtfRedeemReceipt {
    pub tx_hash: String,
    pub gas_used: u64,
    pub effective_gas_price_wei: u128,
}

/// Confirmation status for an already-submitted standard binary redeem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementCtfSubmittedRedeemReceipt {
    Pending,
    Confirmed(SettlementCtfRedeemReceipt),
    Reverted { tx_hash: String },
}

/// Submitted standard binary redeem transaction.
#[async_trait]
pub trait SettlementRedeemTx: Send {
    fn tx_hash(&self) -> &str;

    async fn wait(
        self: Box<Self>,
        confirmations: u64,
    ) -> Result<SettlementCtfRedeemReceipt, RpcError>;
}

/// On-chain CTF facade consumed by the core settlement service.
#[async_trait]
pub trait SettlementCtfClient: Send + Sync {
    async fn binary_payout_vector(
        &self,
        market_id: &MarketId,
    ) -> Result<SettlementCtfPayoutVector, RpcError>;

    async fn binary_balances(
        &self,
        funder_address: &str,
        yes_token_id: &TokenId,
        no_token_id: &TokenId,
    ) -> Result<SettlementCtfBalances, RpcError>;

    async fn simulate_standard_binary_redeem(&self, market_id: &MarketId) -> Result<(), RpcError>;

    async fn submit_standard_binary_redeem(
        &self,
        market_id: &MarketId,
    ) -> Result<Box<dyn SettlementRedeemTx>, RpcError>;

    async fn submitted_redeem_receipt(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<SettlementCtfSubmittedRedeemReceipt, RpcError>;
}

#[async_trait]
impl SettlementRedeemTx for CtfPendingRedeem {
    fn tx_hash(&self) -> &str {
        self.tx_hash()
    }

    async fn wait(
        self: Box<Self>,
        confirmations: u64,
    ) -> Result<SettlementCtfRedeemReceipt, RpcError> {
        let receipt = (*self).wait(confirmations).await?;
        Ok(SettlementCtfRedeemReceipt {
            tx_hash: receipt.tx_hash,
            gas_used: receipt.gas_used,
            effective_gas_price_wei: receipt.effective_gas_price_wei,
        })
    }
}

#[async_trait]
impl SettlementCtfClient for CtfClient {
    async fn binary_payout_vector(
        &self,
        market_id: &MarketId,
    ) -> Result<SettlementCtfPayoutVector, RpcError> {
        let payout = self.binary_payout_vector(market_id).await?;
        Ok(SettlementCtfPayoutVector {
            denominator: payout.denominator.to_string(),
            yes: payout.yes.to_string(),
            no: payout.no.to_string(),
        })
    }

    async fn binary_balances(
        &self,
        funder_address: &str,
        yes_token_id: &TokenId,
        no_token_id: &TokenId,
    ) -> Result<SettlementCtfBalances, RpcError> {
        let balances = self
            .binary_balances_for_funder(funder_address, yes_token_id, no_token_id)
            .await?;
        Ok(SettlementCtfBalances {
            yes_raw: balances.yes.to_string(),
            no_raw: balances.no.to_string(),
        })
    }

    async fn simulate_standard_binary_redeem(&self, market_id: &MarketId) -> Result<(), RpcError> {
        self.simulate_standard_binary_redeem(market_id).await
    }

    async fn submit_standard_binary_redeem(
        &self,
        market_id: &MarketId,
    ) -> Result<Box<dyn SettlementRedeemTx>, RpcError> {
        self.submit_standard_binary_redeem(market_id)
            .await
            .map(|tx| Box::new(tx) as Box<dyn SettlementRedeemTx>)
    }

    async fn submitted_redeem_receipt(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<SettlementCtfSubmittedRedeemReceipt, RpcError> {
        self.submitted_redeem_receipt(tx_hash, confirmations)
            .await
            .map(|status| match status {
                CtfSubmittedRedeemReceipt::Pending => SettlementCtfSubmittedRedeemReceipt::Pending,
                CtfSubmittedRedeemReceipt::Confirmed(receipt) => {
                    SettlementCtfSubmittedRedeemReceipt::Confirmed(SettlementCtfRedeemReceipt {
                        tx_hash: receipt.tx_hash,
                        gas_used: receipt.gas_used,
                        effective_gas_price_wei: receipt.effective_gas_price_wei,
                    })
                }
                CtfSubmittedRedeemReceipt::Reverted { tx_hash } => {
                    SettlementCtfSubmittedRedeemReceipt::Reverted { tx_hash }
                }
            })
    }
}

/// Inputs to connect the gasless relayer, captured at boot but consumed lazily.
///
/// Held by [`RelayerSettlementClient`] so the relayer HTTP client (and its
/// mandatory API credentials) is only materialized when a redeem is first
/// attempted — never at boot.
pub struct RelayerConnectParams {
    /// EOA signer that signs the relayer `SafeTx` / Proxy relay payload.
    pub signer: Arc<OrderSigner>,
    /// Relayer endpoint + API credentials (validated at first use).
    pub relayer_config: RelayerConfig,
    /// Resolved wallet topology (selects Proxy vs Safe relay tx type + funder).
    pub wallet: WalletTopology,
    /// Polygon chain id for the relayer domain.
    pub chain_id: u64,
}

/// Relayer-backed settlement client for Proxy / Gnosis Safe topologies.
///
/// On-chain reads (payout vector, balances, simulate) stay on the Polygon RPC
/// via [`CtfClient`]; the money-moving `redeemPositions` is signed by the EOA and
/// broadcast gaslessly by the Polymarket relayer from the funder wallet. The
/// relayer transaction id is surfaced as the persisted `tx_hash` so the worker's
/// recovery path can re-poll it after a restart.
///
/// The relayer connection is **deferred**: reads work immediately, but the
/// relayer (which requires API credentials) is connected on the first redeem.
/// This lets `report_only` — which never redeems — boot without relayer creds,
/// while `semi_auto` / `auto_execution` still fail closed at redeem time when the
/// credentials are missing.
pub struct RelayerSettlementClient {
    reads: CtfClient,
    funder_address: String,
    relayer: OnceLock<RelayerClient>,
    connect: RelayerConnectParams,
}

impl RelayerSettlementClient {
    /// Build a settlement client that connects the relayer lazily on first use.
    #[must_use]
    pub const fn deferred(
        reads: CtfClient,
        funder_address: String,
        connect: RelayerConnectParams,
    ) -> Self {
        Self {
            reads,
            funder_address,
            relayer: OnceLock::new(),
            connect,
        }
    }

    /// Connect (once) and return the gasless relayer client.
    ///
    /// Fails closed with the relayer credential/connection error at the point of
    /// actual redemption when credentials are absent.
    fn relayer(&self) -> Result<&RelayerClient, RpcError> {
        if let Some(relayer) = self.relayer.get() {
            return Ok(relayer);
        }
        let relayer = RelayerClient::connect(
            self.connect.signer.as_ref(),
            &self.connect.relayer_config,
            &self.connect.wallet,
            self.connect.chain_id,
        )?;
        let _ = self.relayer.set(relayer);
        Ok(self.relayer.get().expect("relayer set above"))
    }
}

/// Bounded relayer poll budget per `wait` call before deferring to the worker's
/// durable recovery path (the relayer id stays persisted as the `tx_hash`).
const RELAYER_WAIT_MAX_POLLS: u32 = 30;
const RELAYER_WAIT_POLL_INTERVAL: StdDuration = StdDuration::from_secs(2);

/// Submitted relayer redeem awaiting finality (polls by relayer transaction id).
struct RelayerRedeemTx {
    relayer: RelayerClient,
    transaction_id: String,
}

#[async_trait]
impl SettlementRedeemTx for RelayerRedeemTx {
    fn tx_hash(&self) -> &str {
        &self.transaction_id
    }

    async fn wait(
        self: Box<Self>,
        _confirmations: u64,
    ) -> Result<SettlementCtfRedeemReceipt, RpcError> {
        for _ in 0..RELAYER_WAIT_MAX_POLLS {
            match self
                .relayer
                .transaction_outcome(&self.transaction_id)
                .await?
            {
                RelayerTxOutcome::Confirmed { tx_hash } => {
                    return Ok(SettlementCtfRedeemReceipt {
                        tx_hash,
                        gas_used: 0,
                        effective_gas_price_wei: 0,
                    });
                }
                RelayerTxOutcome::Failed { detail } => {
                    return Err(RpcError::CallFailed {
                        method: "relayer.redeem.wait".into(),
                        reason: detail,
                    });
                }
                RelayerTxOutcome::Pending => {
                    tokio::time::sleep(RELAYER_WAIT_POLL_INTERVAL).await;
                }
            }
        }
        let elapsed_ms = u64::from(RELAYER_WAIT_MAX_POLLS).saturating_mul(
            u64::try_from(RELAYER_WAIT_POLL_INTERVAL.as_millis()).unwrap_or(u64::MAX),
        );
        Err(RpcError::Timeout {
            method: "relayer.redeem.wait".into(),
            elapsed_ms,
        })
    }
}

#[async_trait]
impl SettlementCtfClient for RelayerSettlementClient {
    async fn binary_payout_vector(
        &self,
        market_id: &MarketId,
    ) -> Result<SettlementCtfPayoutVector, RpcError> {
        let payout = self.reads.binary_payout_vector(market_id).await?;
        Ok(SettlementCtfPayoutVector {
            denominator: payout.denominator.to_string(),
            yes: payout.yes.to_string(),
            no: payout.no.to_string(),
        })
    }

    async fn binary_balances(
        &self,
        funder_address: &str,
        yes_token_id: &TokenId,
        no_token_id: &TokenId,
    ) -> Result<SettlementCtfBalances, RpcError> {
        let balances = self
            .reads
            .binary_balances_for_funder(funder_address, yes_token_id, no_token_id)
            .await?;
        Ok(SettlementCtfBalances {
            yes_raw: balances.yes.to_string(),
            no_raw: balances.no.to_string(),
        })
    }

    async fn simulate_standard_binary_redeem(&self, market_id: &MarketId) -> Result<(), RpcError> {
        self.reads
            .simulate_standard_binary_redeem_from_funder(market_id, &self.funder_address)
            .await
    }

    async fn submit_standard_binary_redeem(
        &self,
        market_id: &MarketId,
    ) -> Result<Box<dyn SettlementRedeemTx>, RpcError> {
        let relayer = self.relayer()?;
        let submission = relayer
            .submit_standard_binary_redeem(market_id.as_str())
            .await?;
        Ok(Box::new(RelayerRedeemTx {
            relayer: relayer.clone(),
            transaction_id: submission.transaction_id,
        }) as Box<dyn SettlementRedeemTx>)
    }

    async fn submitted_redeem_receipt(
        &self,
        tx_hash: &str,
        _confirmations: u64,
    ) -> Result<SettlementCtfSubmittedRedeemReceipt, RpcError> {
        Ok(match self.relayer()?.transaction_outcome(tx_hash).await? {
            RelayerTxOutcome::Pending => SettlementCtfSubmittedRedeemReceipt::Pending,
            RelayerTxOutcome::Confirmed { tx_hash } => {
                SettlementCtfSubmittedRedeemReceipt::Confirmed(SettlementCtfRedeemReceipt {
                    tx_hash,
                    gas_used: 0,
                    effective_gas_price_wei: 0,
                })
            }
            RelayerTxOutcome::Failed { .. } => SettlementCtfSubmittedRedeemReceipt::Reverted {
                tx_hash: tx_hash.to_owned(),
            },
        })
    }
}

/// Collaborators for [`SettlementRedeemService`].
pub struct SettlementRedeemServiceDeps {
    pub positions: Arc<dyn PositionRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub markets: Arc<dyn MarketRepository>,
    pub settlement_redeems: Arc<dyn SettlementRedeemRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub ctf: Arc<dyn SettlementCtfClient>,
    pub runtime_mode: RuntimeModeHandle,
    pub kill_switch: KillSwitchHandle,
    pub config: Arc<RuntimeConfigStore>,
    pub funder_address: String,
    pub wallet_kind: ExecutionWalletKind,
    pub capital_events: Arc<CapitalAllocationEventWriter>,
    pub position_events: Arc<PositionEventWriter>,
    /// Fans out `quant.settlement` revision hints after a state transition.
    pub events: CoreEventPublisher,
}

/// One settlement redeem sweep summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SettlementRedeemPassSummary {
    pub candidates: usize,
    pub submitted: usize,
    pub confirmed: usize,
    pub manual_required: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Drives resolved standard binary CTF redemption and settlement ledger writes.
pub struct SettlementRedeemService {
    deps: SettlementRedeemServiceDeps,
}

impl SettlementRedeemService {
    #[must_use]
    pub const fn new(deps: SettlementRedeemServiceDeps) -> Self {
        Self { deps }
    }

    /// Scan auto-redeem hold-to-resolution lots and process at most the configured batch.
    pub async fn run_pass(&self, now: DateTime<Utc>) -> QuantResult<SettlementRedeemPassSummary> {
        let policy = self
            .deps
            .config
            .current()
            .execution
            .settlement_redeem
            .clone();
        if !policy.enabled || policy.batch_size == 0 {
            return Ok(SettlementRedeemPassSummary::default());
        }
        // Settlement redeem is fund recovery, not new exposure — independent of
        // CLOB runtime mode (`report_only` must still reclaim resolved CTF lots).
        if self.deps.kill_switch.requires_emergency_exit() && !policy.allow_during_emergency {
            return Ok(SettlementRedeemPassSummary::default());
        }

        let groups = self.candidate_groups().await?;
        let mut summary = SettlementRedeemPassSummary {
            candidates: groups.len(),
            ..SettlementRedeemPassSummary::default()
        };
        let batch_size = usize::try_from(policy.batch_size).unwrap_or(usize::MAX);
        for (market_id, lots) in groups.into_iter().take(batch_size) {
            match self.process_market(&market_id, lots, now).await {
                Ok(MarketRedeemOutcome::Skipped) => summary.skipped += 1,
                Ok(MarketRedeemOutcome::Submitted) => summary.submitted += 1,
                Ok(MarketRedeemOutcome::Confirmed) => summary.confirmed += 1,
                Ok(MarketRedeemOutcome::ManualRequired) => summary.manual_required += 1,
                Ok(MarketRedeemOutcome::Failed) => summary.failed += 1,
                Err(error) => {
                    summary.failed += 1;
                    tracing::warn!(
                        %error,
                        market_id = %market_id,
                        "settlement redeem market processing failed"
                    );
                }
            }
        }
        Ok(summary)
    }

    async fn candidate_groups(&self) -> QuantResult<BTreeMap<MarketId, Vec<CandidateLot>>> {
        let lots = self.deps.positions.find_open_lots().await?;
        let open_lots: Vec<PositionInfo> = lots
            .into_iter()
            .filter(|lot| lot.state == PositionLedgerState::Open)
            .collect();
        let intent_ids: Vec<_> = open_lots
            .iter()
            .map(|lot| lot.order_intent_id.clone())
            .collect();
        let intents = self.deps.intents.find_by_ids(&intent_ids).await?;
        let intent_map: HashMap<_, _> = intents
            .into_iter()
            .map(|intent| (intent.order_intent_id.clone(), intent))
            .collect();

        let mut groups: BTreeMap<MarketId, Vec<CandidateLot>> = BTreeMap::new();
        for lot in open_lots {
            let Some(intent) = intent_map.get(&lot.order_intent_id) else {
                tracing::warn!(
                    order_intent_id = %lot.order_intent_id,
                    position_id = %lot.position_id,
                    "settlement redeem skipped lot with missing intent"
                );
                continue;
            };
            if !is_auto_redeem_candidate(intent) {
                continue;
            }
            groups
                .entry(lot.market_id.clone())
                .or_default()
                .push(CandidateLot { lot });
        }
        Ok(groups)
    }

    async fn process_market(
        &self,
        market_id: &MarketId,
        lots: Vec<CandidateLot>,
        now: DateTime<Utc>,
    ) -> QuantResult<MarketRedeemOutcome> {
        let Some(market) = self.deps.markets.find_by_id(market_id).await? else {
            tracing::warn!(
                %market_id,
                "settlement redeem skipped market with missing catalog row"
            );
            return Ok(MarketRedeemOutcome::Skipped);
        };

        if let Some(existing) = self
            .deps
            .settlement_redeems
            .find_by_market_funder(market_id, &self.deps.funder_address)
            .await?
        {
            return match existing.state {
                SettlementRedeemState::Confirmed => Ok(MarketRedeemOutcome::Skipped),
                SettlementRedeemState::ManualRequired => Ok(MarketRedeemOutcome::ManualRequired),
                SettlementRedeemState::Submitted => {
                    self.recover_submitted(
                        existing.tx_hash.as_deref(),
                        &existing.settlement_redeem_id,
                        existing.attempt_count,
                        &market,
                        &lots,
                        now,
                    )
                    .await
                }
                SettlementRedeemState::Failed if !retry_due(&existing, now) => {
                    Ok(MarketRedeemOutcome::Skipped)
                }
                SettlementRedeemState::Failed
                    if attempts_exhausted(
                        existing.attempt_count,
                        self.deps
                            .config
                            .current()
                            .execution
                            .settlement_redeem
                            .max_attempts,
                    ) =>
                {
                    self.mark_failed(
                        &existing.settlement_redeem_id,
                        market_id,
                        existing.attempt_count,
                        "settlement redeem retry budget exhausted".to_owned(),
                        now,
                        true,
                    )
                    .await?;
                    Ok(MarketRedeemOutcome::ManualRequired)
                }
                SettlementRedeemState::Pending | SettlementRedeemState::Failed => {
                    self.prepare_and_submit(
                        &existing.settlement_redeem_id,
                        market.as_ref(),
                        &lots,
                        now,
                    )
                    .await
                }
            };
        }

        let settlement_redeem_id = SettlementRedeemId::from_v7();
        self.prepare_and_submit(&settlement_redeem_id, market.as_ref(), &lots, now)
            .await
    }

    async fn recover_submitted(
        &self,
        tx_hash: Option<&str>,
        settlement_redeem_id: &SettlementRedeemId,
        attempt_count: i32,
        market: &MarketInfo,
        lots: &[CandidateLot],
        now: DateTime<Utc>,
    ) -> QuantResult<MarketRedeemOutcome> {
        let Some(tx_hash) = tx_hash else {
            self.mark_failed(
                settlement_redeem_id,
                &market.market_id,
                attempt_count,
                "submitted settlement redeem row has no tx_hash".to_owned(),
                now,
                true,
            )
            .await?;
            return Ok(MarketRedeemOutcome::ManualRequired);
        };
        let confirmations = self
            .deps
            .config
            .current()
            .execution
            .settlement_redeem
            .confirmation_blocks;
        let status = self
            .deps
            .ctf
            .submitted_redeem_receipt(tx_hash, confirmations)
            .await?;
        let receipt = match status {
            SettlementCtfSubmittedRedeemReceipt::Pending => {
                return Ok(MarketRedeemOutcome::Submitted);
            }
            SettlementCtfSubmittedRedeemReceipt::Confirmed(receipt) => receipt,
            SettlementCtfSubmittedRedeemReceipt::Reverted { tx_hash } => {
                self.mark_failed(
                    settlement_redeem_id,
                    &market.market_id,
                    attempt_count,
                    format!("submitted settlement redeem transaction {tx_hash} reverted"),
                    now,
                    false,
                )
                .await?;
                return Ok(MarketRedeemOutcome::Failed);
            }
        };
        self.confirm_redeem(settlement_redeem_id, market, lots, receipt, now)
            .await
    }

    async fn prepare_and_submit(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        market: &MarketInfo,
        lots: &[CandidateLot],
        now: DateTime<Utc>,
    ) -> QuantResult<MarketRedeemOutcome> {
        let preflight = self
            .build_preflight(settlement_redeem_id, market, lots)
            .await?;
        if let PreflightDecision::ManualRequired { reason, record } = preflight {
            let redeem = self.deps.settlement_redeems.upsert_pending(record).await?;
            self.publish_manual_required(&redeem.settlement_redeem_id, &market.market_id);
            tracing::warn!(
                market_id = %market.market_id,
                reason,
                "settlement redeem requires manual handling"
            );
            return Ok(MarketRedeemOutcome::ManualRequired);
        }
        let PreflightDecision::Auto { record } = preflight else {
            return Ok(MarketRedeemOutcome::Skipped);
        };

        let redeem = self.deps.settlement_redeems.upsert_pending(record).await?;
        if redeem.state == SettlementRedeemState::ManualRequired {
            self.publish_manual_required(&redeem.settlement_redeem_id, &market.market_id);
            return Ok(MarketRedeemOutcome::ManualRequired);
        }
        if redeem.state == SettlementRedeemState::Confirmed {
            return Ok(MarketRedeemOutcome::Skipped);
        }

        if let Err(error) = self
            .deps
            .ctf
            .simulate_standard_binary_redeem(&market.market_id)
            .await
        {
            self.mark_failed(
                &redeem.settlement_redeem_id,
                &market.market_id,
                redeem.attempt_count,
                error.to_string(),
                now,
                false,
            )
            .await?;
            return Ok(MarketRedeemOutcome::Failed);
        }

        let pending = match self
            .deps
            .ctf
            .submit_standard_binary_redeem(&market.market_id)
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                self.mark_failed(
                    &redeem.settlement_redeem_id,
                    &market.market_id,
                    redeem.attempt_count,
                    error.to_string(),
                    now,
                    false,
                )
                .await?;
                return Ok(MarketRedeemOutcome::Failed);
            }
        };
        let tx_hash = pending.tx_hash().to_owned();
        self.deps
            .settlement_redeems
            .mark_submitted(&redeem.settlement_redeem_id, tx_hash.clone(), now)
            .await?;
        self.publish_settlement(
            &redeem.settlement_redeem_id,
            &market.market_id,
            SettlementRedeemState::Submitted,
        );
        tracing::info!(
            market_id = %market.market_id,
            tx_hash,
            "submitted standard binary settlement redeem"
        );

        let confirmations = self
            .deps
            .config
            .current()
            .execution
            .settlement_redeem
            .confirmation_blocks;
        let receipt = match pending.wait(confirmations).await {
            Ok(receipt) => receipt,
            Err(error) => {
                tracing::warn!(
                    market_id = %market.market_id,
                    tx_hash,
                    %error,
                    "submitted settlement redeem receipt wait failed; keeping submitted row for recovery"
                );
                return Ok(MarketRedeemOutcome::Submitted);
            }
        };
        self.confirm_redeem(&redeem.settlement_redeem_id, market, lots, receipt, now)
            .await
    }

    async fn build_preflight(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        market: &MarketInfo,
        lots: &[CandidateLot],
    ) -> QuantResult<PreflightDecision> {
        let zero_payout = SettlementCtfPayoutVector {
            denominator: "0".to_owned(),
            yes: "0".to_owned(),
            no: "0".to_owned(),
        };
        let zero_balances = SettlementCtfBalances {
            yes_raw: "0".to_owned(),
            no_raw: "0".to_owned(),
        };

        if market.neg_risk {
            return Ok(PreflightDecision::ManualRequired {
                reason: "neg-risk auto redeem is not implemented".to_owned(),
                record: self.new_redeem_record(NewRedeemRecordParams {
                    settlement_redeem_id: settlement_redeem_id.clone(),
                    market,
                    state: SettlementRedeemState::ManualRequired,
                    payout: &zero_payout,
                    balances: &zero_balances,
                    last_error: Some("neg-risk auto redeem is not implemented".to_owned()),
                    payout_usd: None,
                })?,
            });
        }

        let payout = self
            .deps
            .ctf
            .binary_payout_vector(&market.market_id)
            .await?;
        if !payout.is_resolved() {
            return Ok(PreflightDecision::NotResolved);
        }
        let balances = self
            .deps
            .ctf
            .binary_balances(
                &self.deps.funder_address,
                &market.yes_token_id,
                &market.no_token_id,
            )
            .await?;
        let validation = validate_standard_binary_lots(market, lots, &balances)?;
        if let Some(reason) = validation.manual_reason {
            return Ok(PreflightDecision::ManualRequired {
                reason: reason.clone(),
                record: self.new_redeem_record(NewRedeemRecordParams {
                    settlement_redeem_id: settlement_redeem_id.clone(),
                    market,
                    state: SettlementRedeemState::ManualRequired,
                    payout: &payout,
                    balances: &balances,
                    last_error: Some(reason),
                    payout_usd: None,
                })?,
            });
        }

        Ok(PreflightDecision::Auto {
            record: self.new_redeem_record(NewRedeemRecordParams {
                settlement_redeem_id: settlement_redeem_id.clone(),
                market,
                state: SettlementRedeemState::Pending,
                payout: &payout,
                balances: &balances,
                last_error: None,
                payout_usd: Some(Usd::ZERO),
            })?,
        })
    }

    fn new_redeem_record(
        &self,
        params: NewRedeemRecordParams<'_>,
    ) -> QuantResult<NewSettlementRedeem> {
        Ok(NewSettlementRedeem {
            settlement_redeem_id: params.settlement_redeem_id,
            market_id: params.market.market_id.clone(),
            funder_address: self.deps.funder_address.clone(),
            wallet_kind: self.deps.wallet_kind,
            state: params.state,
            tx_hash: None,
            index_sets_json: SettlementRedeemIndexSets {
                index_sets: vec![STANDARD_BINARY_YES_INDEX_SET, STANDARD_BINARY_NO_INDEX_SET],
            },
            payout_vector_json: SettlementPayoutVector {
                denominator: params.payout.denominator.clone(),
                yes: params.payout.yes.clone(),
                no: params.payout.no.clone(),
            },
            balance_before_json: balance_evidence(params.market, params.balances)?,
            balance_after_json: None,
            payout_usd: params.payout_usd.unwrap_or(Usd::ZERO),
            gas_fee_pol: None,
            attempt_count: 0,
            next_attempt_at: None,
            last_error: params.last_error,
            submitted_at: None,
            confirmed_at: None,
            failed_at: None,
        })
    }

    async fn confirm_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        market: &MarketInfo,
        lots: &[CandidateLot],
        receipt: SettlementCtfRedeemReceipt,
        now: DateTime<Utc>,
    ) -> QuantResult<MarketRedeemOutcome> {
        let payout = self
            .deps
            .ctf
            .binary_payout_vector(&market.market_id)
            .await?;
        let balances_after = self
            .deps
            .ctf
            .binary_balances(
                &self.deps.funder_address,
                &market.yes_token_id,
                &market.no_token_id,
            )
            .await?;
        let allocation = build_lot_writes(settlement_redeem_id, lots, &payout, now)?;
        let payout_usd = allocation.iter().map(|write| write.lot.payout_usd).sum();
        let balance_after_json = balance_evidence(market, &balances_after)?;
        let gas_fee_pol = gas_fee_pol(receipt.gas_used, receipt.effective_gas_price_wei)?;

        self.deps
            .settlement_redeems
            .confirm(ConfirmSettlementRedeem {
                settlement_redeem_id: settlement_redeem_id.clone(),
                balance_after_json,
                payout_usd,
                gas_fee_pol: Some(gas_fee_pol),
                confirmed_at: now,
                lots: allocation.clone(),
            })
            .await?;
        self.mirror_settlement_confirm(&allocation, now).await?;
        self.publish_settlement(
            settlement_redeem_id,
            &market.market_id,
            SettlementRedeemState::Confirmed,
        );
        tracing::info!(
            market_id = %market.market_id,
            tx_hash = receipt.tx_hash,
            payout_usd = %payout_usd,
            "confirmed standard binary settlement redeem"
        );
        Ok(MarketRedeemOutcome::Confirmed)
    }

    async fn mirror_settlement_confirm(
        &self,
        allocation: &[SettlementRedeemLotWrite],
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        for write in allocation {
            let intent_id = &write.lot.order_intent_id;
            if let Some(capital) = self.deps.capital.find_by_intent(intent_id).await? {
                self.deps.capital_events.write(project_capital_event(
                    &capital,
                    ChQuantLedgerEventKind::SettlementRedeemConfirmed,
                    now,
                ));
            }
            if let Some(position) = self.deps.positions.find_by_intent(intent_id).await? {
                self.deps.position_events.write(project_position_event(
                    &position,
                    ChQuantLedgerEventKind::SettlementRedeemConfirmed,
                    now,
                ));
            }
        }
        Ok(())
    }

    async fn mark_failed(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        market_id: &MarketId,
        prior_attempt_count: i32,
        error: String,
        now: DateTime<Utc>,
        manual_required: bool,
    ) -> QuantResult<()> {
        let policy = self
            .deps
            .config
            .current()
            .execution
            .settlement_redeem
            .clone();
        let manual_required = manual_required
            || attempts_exhausted(prior_attempt_count.saturating_add(1), policy.max_attempts);
        let next_attempt_at = if manual_required {
            None
        } else {
            let backoff = i64::try_from(policy.retry_backoff_secs).unwrap_or(i64::MAX);
            Some(now + Duration::seconds(backoff))
        };
        self.deps
            .settlement_redeems
            .mark_failed(
                settlement_redeem_id,
                error,
                next_attempt_at,
                now,
                manual_required,
            )
            .await?;
        let state = if manual_required {
            SettlementRedeemState::ManualRequired
        } else {
            SettlementRedeemState::Failed
        };
        self.publish_settlement(settlement_redeem_id, market_id, state);
        Ok(())
    }

    /// Fan out a `quant.settlement` revision hint after a state transition.
    /// Consumers re-fetch the settlement ledger over REST on any bump.
    fn publish_settlement(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        market_id: &MarketId,
        state: SettlementRedeemState,
    ) {
        self.deps
            .events
            .publish(CoreEvent::Settlement(SettlementRedeemLifecycleEvent {
                settlement_redeem_id: settlement_redeem_id.to_string(),
                market_id: market_id.clone(),
                state,
            }));
    }

    fn publish_manual_required(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        market_id: &MarketId,
    ) {
        self.publish_settlement(
            settlement_redeem_id,
            market_id,
            SettlementRedeemState::ManualRequired,
        );
    }
}

#[derive(Debug, Clone)]
struct CandidateLot {
    lot: PositionInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarketRedeemOutcome {
    Skipped,
    Submitted,
    Confirmed,
    ManualRequired,
    Failed,
}

enum PreflightDecision {
    NotResolved,
    Auto {
        record: NewSettlementRedeem,
    },
    ManualRequired {
        reason: String,
        record: NewSettlementRedeem,
    },
}

struct BalanceValidation {
    manual_reason: Option<String>,
}

/// Inputs for assembling a [`NewSettlementRedeem`] ledger row.
struct NewRedeemRecordParams<'a> {
    settlement_redeem_id: SettlementRedeemId,
    market: &'a MarketInfo,
    state: SettlementRedeemState,
    payout: &'a SettlementCtfPayoutVector,
    balances: &'a SettlementCtfBalances,
    last_error: Option<String>,
    payout_usd: Option<Usd>,
}

pub(crate) fn is_auto_redeem_candidate(intent: &OrderIntentInfo) -> bool {
    intent.exit_policy_json.settlement_mode == ExitSettlementMode::HoldToResolution
        && intent.exit_policy_json.redeem_policy == RedeemPolicy::Auto
        && !matches!(
            intent.exit_state,
            ExitState::OrderSubmitted
                | ExitState::PartiallyExited
                | ExitState::ManualRequired
                | ExitState::Exited
                | ExitState::Failed
        )
}

fn retry_due(redeem: &SettlementRedeemInfo, now: DateTime<Utc>) -> bool {
    redeem.next_attempt_at.is_none_or(|at| now >= at)
}

fn attempts_exhausted(attempt_count: i32, max_attempts: u32) -> bool {
    let max_attempts = i32::try_from(max_attempts.max(1)).unwrap_or(i32::MAX);
    attempt_count >= max_attempts
}

fn validate_standard_binary_lots(
    market: &MarketInfo,
    lots: &[CandidateLot],
    balances: &SettlementCtfBalances,
) -> QuantResult<BalanceValidation> {
    let mut yes_shares = Shares::ZERO;
    let mut no_shares = Shares::ZERO;
    for candidate in lots {
        match candidate.lot.side {
            OutcomeSide::Yes => {
                if candidate.lot.token_id != market.yes_token_id {
                    return Ok(BalanceValidation {
                        manual_reason: Some(format!(
                            "YES lot token {} does not match market YES token {}",
                            candidate.lot.token_id, market.yes_token_id
                        )),
                    });
                }
                yes_shares += candidate.lot.shares;
            }
            OutcomeSide::No => {
                if candidate.lot.token_id != market.no_token_id {
                    return Ok(BalanceValidation {
                        manual_reason: Some(format!(
                            "NO lot token {} does not match market NO token {}",
                            candidate.lot.token_id, market.no_token_id
                        )),
                    });
                }
                no_shares += candidate.lot.shares;
            }
        }
    }

    let chain_yes = raw_to_shares(&balances.yes_raw)?;
    let chain_no = raw_to_shares(&balances.no_raw)?;
    if chain_yes != yes_shares || chain_no != no_shares {
        return Ok(BalanceValidation {
            manual_reason: Some(format!(
                "funder balance mismatch: chain_yes={chain_yes}, system_yes={yes_shares}, chain_no={chain_no}, system_no={no_shares}"
            )),
        });
    }
    if yes_shares.is_zero() && no_shares.is_zero() {
        return Ok(BalanceValidation {
            manual_reason: Some("no positive shares to redeem".to_owned()),
        });
    }

    Ok(BalanceValidation {
        manual_reason: None,
    })
}

fn balance_evidence(
    market: &MarketInfo,
    balances: &SettlementCtfBalances,
) -> QuantResult<SettlementBalanceEvidence> {
    Ok(SettlementBalanceEvidence {
        yes: SettlementTokenBalance {
            token_id: market.yes_token_id.to_string(),
            index_set: STANDARD_BINARY_YES_INDEX_SET,
            raw_balance: balances.yes_raw.clone(),
            shares: raw_to_shares(&balances.yes_raw)?.to_string(),
        },
        no: SettlementTokenBalance {
            token_id: market.no_token_id.to_string(),
            index_set: STANDARD_BINARY_NO_INDEX_SET,
            raw_balance: balances.no_raw.clone(),
            shares: raw_to_shares(&balances.no_raw)?.to_string(),
        },
    })
}

fn build_lot_writes(
    settlement_redeem_id: &SettlementRedeemId,
    lots: &[CandidateLot],
    payout: &SettlementCtfPayoutVector,
    now: DateTime<Utc>,
) -> QuantResult<Vec<SettlementRedeemLotWrite>> {
    let denominator = decimal_from_str("payout.denominator", &payout.denominator)?;
    if denominator.is_zero() {
        return Err(ExecutionError::SettlementRedeemInvariant {
            reason: "cannot allocate redeem payout with zero denominator".to_owned(),
        }
        .into());
    }
    let yes_ratio = decimal_from_str("payout.yes", &payout.yes)? / denominator;
    let no_ratio = decimal_from_str("payout.no", &payout.no)? / denominator;
    let mut writes = Vec::with_capacity(lots.len());
    for candidate in lots {
        let ratio = match candidate.lot.side {
            OutcomeSide::Yes => yes_ratio,
            OutcomeSide::No => no_ratio,
        };
        let payout_usd = Usd::new(candidate.lot.shares.inner() * ratio);
        let realized_pnl_usd = payout_usd - candidate.lot.cost_usd;
        writes.push(SettlementRedeemLotWrite {
            lot: NewSettlementRedeemLot {
                settlement_redeem_lot_id: SettlementRedeemLotId::from_v7(),
                settlement_redeem_id: settlement_redeem_id.clone(),
                position_id: candidate.lot.position_id.clone(),
                order_intent_id: candidate.lot.order_intent_id.clone(),
                token_id: candidate.lot.token_id.clone(),
                side: candidate.lot.side,
                shares_redeemed: candidate.lot.shares,
                cost_basis_usd: candidate.lot.cost_usd,
                payout_usd,
                realized_pnl_usd,
            },
            position_exit: PositionExit {
                shares: candidate.lot.shares,
                avg_price: candidate.lot.avg_price,
                proceeds_usd: payout_usd,
                realized_pnl_usd,
                exited_at: now,
                reason: ExitReason::ResolutionRedeem,
            },
        });
    }
    Ok(writes)
}

fn raw_to_shares(raw: &str) -> QuantResult<Shares> {
    let raw = decimal_from_str("ctf.raw_balance", raw)?;
    Ok(Shares::new(raw / Decimal::from(COLLATERAL_SCALE)))
}

fn gas_fee_pol(gas_used: u64, effective_gas_price_wei: u128) -> QuantResult<Decimal> {
    let gas_used = Decimal::from(gas_used);
    let price_wei = decimal_from_str(
        "effective_gas_price_wei",
        &effective_gas_price_wei.to_string(),
    )?;
    Ok((gas_used * price_wei) / Decimal::from(1_000_000_000_000_000_000_u64))
}

fn decimal_from_str(field: &'static str, value: &str) -> QuantResult<Decimal> {
    Decimal::from_str(value).map_err(|e| {
        ExecutionError::SettlementRedeemInvariant {
            reason: format!("invalid decimal {field}='{value}': {e}"),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::{
        domain::OrderIntentInfo,
        enums::{
            common::{MarketCategory, OrderType, Side, TickSize},
            execution::{ExitState, OrderIntentKind},
            market::MarketStatus,
            quant::{
                AccountSource, ApprovalStatus, ExitSettlementMode, OrderIntentStatus,
                QuantRuntimeMode, RedeemPolicy,
            },
        },
        types::{
            Bps, ContentHash, EntryConditionInstanceId, EntryOrderSpec, EventId, ExitPolicySpec,
            ModelVersionId, OpportunisticExitPolicy, OrderAmount, OrderIntentId, PositionId, Price,
            Probability, RecommendationId, RuntimeConfigVersionId, ScaleOutState,
            ThesisInvalidationPolicy,
        },
    };
    use rust_decimal_macros::dec;

    fn test_content_hash(seed: u8) -> ContentHash {
        let hex: String = format!("{seed:02x}").chars().cycle().take(64).collect();
        ContentHash::parse(format!("blake3:{hex}")).expect("hash")
    }

    #[test]
    fn standard_binary_index_sets_match_yes_no_partition() {
        assert_eq!(STANDARD_BINARY_YES_INDEX_SET, 1);
        assert_eq!(STANDARD_BINARY_NO_INDEX_SET, 2);
    }

    #[test]
    fn validate_standard_binary_lots_requires_exact_chain_balances() -> QuantResult<()> {
        let market = market(false);
        let lots = vec![
            lot(&market, OutcomeSide::Yes, dec!(100), dec!(0.40)),
            lot(&market, OutcomeSide::No, dec!(50), dec!(0.60)),
        ];

        let validation = validate_standard_binary_lots(
            &market,
            &lots,
            &SettlementCtfBalances {
                yes_raw: "100000000".to_owned(),
                no_raw: "50000000".to_owned(),
            },
        )?;
        assert_eq!(validation.manual_reason, None);

        let validation = validate_standard_binary_lots(
            &market,
            &lots,
            &SettlementCtfBalances {
                yes_raw: "101000000".to_owned(),
                no_raw: "50000000".to_owned(),
            },
        )?;
        assert!(
            validation
                .manual_reason
                .is_some_and(|reason| reason.contains("funder balance mismatch"))
        );
        Ok(())
    }

    #[test]
    fn build_lot_writes_allocates_resolution_payout_by_side() -> QuantResult<()> {
        let now = Utc::now();
        let market = market(false);
        let lots = vec![
            lot(&market, OutcomeSide::Yes, dec!(100), dec!(0.40)),
            lot(&market, OutcomeSide::No, dec!(50), dec!(0.60)),
        ];

        let writes = build_lot_writes(
            &SettlementRedeemId::from_v7(),
            &lots,
            &SettlementCtfPayoutVector {
                denominator: "1".to_owned(),
                yes: "1".to_owned(),
                no: "0".to_owned(),
            },
            now,
        )?;

        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].lot.side, OutcomeSide::Yes);
        assert_eq!(writes[0].lot.payout_usd, Usd::new(dec!(100)));
        assert_eq!(writes[0].lot.realized_pnl_usd, Usd::new(dec!(60)));
        assert_eq!(writes[0].position_exit.reason, ExitReason::ResolutionRedeem);
        assert_eq!(writes[1].lot.side, OutcomeSide::No);
        assert_eq!(writes[1].lot.payout_usd, Usd::ZERO);
        assert_eq!(writes[1].lot.realized_pnl_usd, Usd::new(dec!(-30)));
        Ok(())
    }

    #[test]
    fn build_lot_writes_rejects_zero_payout_denominator() {
        let market = market(false);
        let lots = vec![lot(&market, OutcomeSide::Yes, dec!(1), dec!(0.50))];
        let result = build_lot_writes(
            &SettlementRedeemId::from_v7(),
            &lots,
            &SettlementCtfPayoutVector {
                denominator: "0".to_owned(),
                yes: "0".to_owned(),
                no: "0".to_owned(),
            },
            Utc::now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn raw_to_shares_uses_polymarket_collateral_scale() -> QuantResult<()> {
        assert_eq!(raw_to_shares("1234567")?, Shares::new(dec!(1.234567)));
        Ok(())
    }

    #[test]
    fn auto_redeem_candidate_excludes_in_flight_and_partial_exit_states() {
        let mut intent = candidate_intent();
        assert!(is_auto_redeem_candidate(&intent));

        intent.exit_state = ExitState::PartiallyExited;
        assert!(!is_auto_redeem_candidate(&intent));

        intent.exit_state = ExitState::OrderSubmitted;
        assert!(!is_auto_redeem_candidate(&intent));

        intent.exit_policy_json.redeem_policy = RedeemPolicy::Manual;
        intent.exit_state = ExitState::Monitoring;
        assert!(!is_auto_redeem_candidate(&intent));

        intent.exit_policy_json.redeem_policy = RedeemPolicy::Auto;
        intent.exit_policy_json.settlement_mode = ExitSettlementMode::ExitBeforeResolution;
        assert!(!is_auto_redeem_candidate(&intent));
    }

    fn candidate_intent() -> OrderIntentInfo {
        let now = Utc::now();
        OrderIntentInfo {
            order_intent_id: OrderIntentId::from_v7(),
            recommendation_id: RecommendationId::from_v7(),
            runtime_mode: QuantRuntimeMode::AutoExecution,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            intent_kind: OrderIntentKind::Buy,
            status: OrderIntentStatus::Filled,
            approval_status: ApprovalStatus::NotRequired,
            approved_by: None,
            approval_reason: None,
            approved_at: None,
            policy_id: None,
            policy_hash: None,
            status_reason: None,
            admission_trace_ref: None,
            condition_instance_id: EntryConditionInstanceId::from_v7(),
            entry_order_json: EntryOrderSpec {
                token_id: TokenId::new("111"),
                side: Side::Buy,
                order_type: OrderType::Gtc,
                post_only: false,
                limit_price: Price::new(dec!(0.5)),
                amount: OrderAmount::Shares(Shares::new(dec!(1))),
                max_slippage_bps: Bps::new(dec!(50)),
                valid_until: now + chrono::Duration::hours(1),
            },
            exit_policy_json: ExitPolicySpec {
                take_profit_price: None,
                take_profit_pct: None,
                stop_loss_price: Some(Price::new(dec!(0.4))),
                stop_loss_pct: None,
                time_exit_at: None,
                max_hold_secs: None,
                trailing_stop: None,
                thesis_invalidation: ThesisInvalidationPolicy {
                    min_score_retention: dec!(0.6),
                    min_expected_return_bps: Bps::ZERO,
                    require_execution_eligibility: true,
                },
                opportunistic_exit: OpportunisticExitPolicy {
                    min_confidence: Probability::new(dec!(0.65)),
                    min_expected_alpha_bps: Bps::new(dec!(50)),
                    min_p_exit_better: Probability::new(dec!(0.5)),
                    max_cumulative_exit_pct: dec!(1),
                    min_incremental_exit_pct: dec!(0.1),
                },
                scale_out_targets: Vec::new(),
                settlement_mode: ExitSettlementMode::HoldToResolution,
                redeem_policy: RedeemPolicy::Auto,
                manual_review_at: None,
                entry_reference_price: Price::new(dec!(0.5)),
                entry_composite_score: Probability::new(dec!(0.7)),
            },
            risk_envelope_hash: test_content_hash(b'e'),
            expires_at: now + chrono::Duration::hours(1),
            exit_state: ExitState::Monitoring,
            exit_reason: None,
            next_check_at: None,
            peak_mark_price: None,
            last_signal_recheck_at: None,
            latest_reinference_json: None,
            scale_out_state: ScaleOutState::default(),
            created_at: now,
            updated_at: now,
        }
    }

    fn market(neg_risk: bool) -> MarketInfo {
        let now = Utc::now();
        MarketInfo {
            market_id: MarketId::new(
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            ),
            event_id: EventId::new("event-1"),
            question: "Will it happen?".to_owned(),
            slug: "will-it-happen".to_owned(),
            description: None,
            categories: vec![MarketCategory::Politics],
            status: MarketStatus::Settled,
            outcome: Some("Yes".to_owned()),
            yes_token_id: TokenId::new("111"),
            no_token_id: TokenId::new("222"),
            tick_size: TickSize::Hundredth,
            neg_risk,
            start_date: None,
            end_date: None,
            resolved_at: Some(now),
            fees_enabled: false,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn lot(
        market: &MarketInfo,
        side: OutcomeSide,
        shares: Decimal,
        avg_price: Decimal,
    ) -> CandidateLot {
        let shares = Shares::new(shares);
        let avg_price = Price::new(avg_price);
        let token_id = match side {
            OutcomeSide::Yes => market.yes_token_id.clone(),
            OutcomeSide::No => market.no_token_id.clone(),
        };
        let now = Utc::now();
        CandidateLot {
            lot: PositionInfo {
                position_id: PositionId::from_v7(),
                order_intent_id: OrderIntentId::from_v7(),
                token_id,
                market_id: market.market_id.clone(),
                event_id: Some(market.event_id.clone()),
                category: MarketCategory::Politics,
                side,
                state: PositionLedgerState::Open,
                shares,
                avg_price,
                cost_usd: shares * avg_price,
                realized_pnl_usd: Usd::ZERO,
                source: AccountSource::Polymarket,
                opened_at: now,
                updated_at: now,
                closed_at: None,
            },
        }
    }
}
