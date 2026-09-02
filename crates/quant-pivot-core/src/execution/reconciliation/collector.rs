//! Venue evidence collection in deterministic reconciliation order.
//!
//! For one reconcilable order the collector gathers, in the immutable order
//! 1→5, one [`ReconciliationEvidence`] per source: CLOB order status → CLOB
//! trades → token balance → account balance → book context. (`OperatorNote`,
//! #6, is appended only on a human resolve, never by the machine.) The high-
//! confidence sources (status + trades) decide; balances corroborate.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::{
    clob::ClobTrade, exchange::constants::EXCHANGE_CONTRACTS, wallet::WalletTopology,
};
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    clickhouse::ExchangeEventRow,
    domain::{
        data_plane::DecisionClock,
        quant::{
            ExecutionIdentityEnrichment, ExecutionOrderIdentityRefs, ExecutionOrderInfo,
            ExecutionTradeObservation,
        },
    },
    enums::{
        clickhouse::{ChExchangeEventKind, ChExchangeSide, ChExchangeVersion},
        common::Side,
        execution::{ReconciliationEvidenceKind, VenueTradeStatus},
        fee::FeeLiquidityRole,
    },
    types::{
        EvmAddress, EvmTransactionHash, FeeMeasurement, MatchMakerRebateEvidence,
        MatchRebateUnavailableReason, OrderId, Price, ReconciliationEvidence, Shares, TokenId, Usd,
        VenueTradeId,
    },
};
use quant_pivot_repository::traits::{
    CatalogLedgerRepository, ClobMarketInfoRepository, QuantFactReadRepository,
};
use quant_pivot_research::execution_semantics::{
    LiquidityRole, PitFeeSchedule, PitMakerRebateEvidence, PitMarketExecutionEconomics,
};
use rust_decimal::Decimal;

use super::{ReconcileFacts, ShareSettlementBasis, VenuePresence, VenueReconciliationReader};
use crate::ingest::book_store::BookStore;

/// Evidence chain + structured facts produced for one reconcilable order.
pub struct CollectedReconciliation {
    /// Ordered evidence (kinds 1→5) recorded on the reconciliation summary row.
    pub evidence: Vec<ReconciliationEvidence>,
    /// Decision facts derived from the same observations.
    pub facts: ReconcileFacts,
    /// Exact identity/status/hash observations to persist before applying a
    /// business verdict.
    pub identity_enrichment: ExecutionIdentityEnrichment,
}

/// Collects the fixed-order venue evidence for one reconcilable order.
#[async_trait]
pub trait EvidenceCollector: Send + Sync {
    async fn collect(
        &self,
        order: &ExecutionOrderInfo,
        identity_refs: &ExecutionOrderIdentityRefs,
        share_basis: ShareSettlementBasis,
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<CollectedReconciliation>;
}

/// [`EvidenceCollector`] backed by the venue reader + the in-memory book store.
pub struct VenueEvidenceCollector {
    reader: Arc<dyn VenueReconciliationReader>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    wallet: WalletTopology,
    book_store: Arc<BookStore>,
    catalog_ledger: Arc<dyn CatalogLedgerRepository>,
    clob_market_info: Arc<dyn ClobMarketInfoRepository>,
}

struct ResolvedVenueIdentities {
    exact_order_id: Option<OrderId>,
    trades_by_id: BTreeMap<VenueTradeId, ClobTrade>,
    missing_trade_count: usize,
    attributable: bool,
    still_working: bool,
    used_account_discovery: bool,
}

struct MatchTermsContext {
    fee_schedule: Option<PitFeeSchedule>,
    rebate_evidence: MatchMakerRebateEvidence,
    resolved: Option<PitMarketExecutionEconomics>,
}

struct TradeEvidenceInput<'a> {
    order: &'a ExecutionOrderInfo,
    trades: &'a BTreeMap<VenueTradeId, ClobTrade>,
    settled_events: &'a [ExchangeEventRow],
    exact_order_id: Option<&'a OrderId>,
    missing_trade_count: usize,
    now: DateTime<Utc>,
}

struct CollectedTradeEvidence {
    evidence: Vec<ReconciliationEvidence>,
    filled_shares: Shares,
    avg_price: Option<Price>,
}

impl VenueEvidenceCollector {
    #[must_use]
    pub const fn new(
        reader: Arc<dyn VenueReconciliationReader>,
        fact_read: Arc<dyn QuantFactReadRepository>,
        wallet: WalletTopology,
        book_store: Arc<BookStore>,
        catalog_ledger: Arc<dyn CatalogLedgerRepository>,
        clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    ) -> Self {
        Self {
            reader,
            fact_read,
            wallet,
            book_store,
            catalog_ledger,
            clob_market_info,
        }
    }

    /// Evidence #5 — the current published book snapshot for price sanity
    /// (best effort: records version + timestamp, or that none is published).
    fn book_context_evidence(
        &self,
        token_id: &TokenId,
        now: DateTime<Utc>,
    ) -> ReconciliationEvidence {
        let last_known = self.book_store.load_known_book(token_id);
        let detail = last_known.snapshot.map_or_else(
            || format!("no book snapshot ({:?})", last_known.availability),
            |snapshot| {
                format!(
                    "book version={} ts_ms={} availability={:?}",
                    snapshot.version, snapshot.timestamp_ms, last_known.availability
                )
            },
        );
        ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::BookContext,
            observed_at: now,
            detail,
            venue_ref: Some(token_id.to_string()),
            shares: None,
            price: None,
            fee_evidence: None,
        }
    }

    async fn token_balance_evidence(
        &self,
        token_id: &TokenId,
        share_basis: ShareSettlementBasis,
        now: DateTime<Utc>,
    ) -> (Option<Shares>, ReconciliationEvidence) {
        let (balance, detail) = match self.reader.token_balance(token_id).await {
            Ok(balance) => (
                Some(balance),
                format!(
                    "token_balance={balance} (absolute diagnostic); share_basis={share_basis:?}"
                ),
            ),
            Err(error) => (
                None,
                format!(
                    "token_balance=unavailable ({error}); diagnostic only; share_basis={share_basis:?}"
                ),
            ),
        };
        let evidence = ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::TokenBalanceDelta,
            observed_at: now,
            detail,
            venue_ref: Some(token_id.to_string()),
            shares: balance,
            price: None,
            fee_evidence: None,
        };
        (balance, evidence)
    }

    async fn collect_trade_evidence(
        &self,
        input: TradeEvidenceInput<'_>,
    ) -> QuantResult<CollectedTradeEvidence> {
        let mut evidence = Vec::with_capacity(input.trades.len().max(1));
        let mut filled_shares = Shares::ZERO;
        let mut filled_cost = Usd::ZERO;
        for trade in input.trades.values() {
            let confirmed = trade_is_final_fill(trade.status);
            if confirmed {
                filled_shares += trade.size;
                filled_cost += trade.size * trade.price;
            }
            evidence.push(ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobTrades,
                observed_at: input.now,
                detail: format!(
                    "trade_id={}; status={:?}; role={:?}; matched_at={}; transaction_hash={}",
                    trade.trade_id,
                    trade.status,
                    trade.trader_side,
                    trade.matched_at,
                    trade
                        .transaction_hash
                        .as_ref()
                        .map_or("none", |hash| hash.as_str())
                ),
                venue_ref: Some(trade.order_id.to_string()),
                shares: confirmed.then_some(trade.size),
                price: confirmed.then_some(trade.price),
                fee_evidence: if confirmed {
                    Some(
                        authenticated_fee_evidence(
                            self.catalog_ledger.as_ref(),
                            self.clob_market_info.as_ref(),
                            input.order,
                            trade,
                            input.now,
                        )
                        .await?,
                    )
                } else {
                    None
                },
            });
            if confirmed
                && let Some(settled) = on_chain_fee_evidence(
                    input.order,
                    trade,
                    input.settled_events,
                    &self.wallet,
                    input.now,
                )?
            {
                evidence.push(settled);
            }
        }
        let avg_price = if filled_shares.is_positive() {
            Some(Price::new(filled_cost.inner() / filled_shares.inner()))
        } else {
            None
        };
        if input.trades.is_empty() {
            evidence.push(ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobTrades,
                observed_at: input.now,
                detail: format!(
                    "exact_trades=0; missing_trade_ids={}; filled_shares=0",
                    input.missing_trade_count
                ),
                venue_ref: input.exact_order_id.map(ToString::to_string),
                shares: Some(Shares::ZERO),
                price: None,
                fee_evidence: None,
            });
        }
        Ok(CollectedTradeEvidence {
            evidence,
            filled_shares,
            avg_price,
        })
    }

    async fn resolve_identities(
        &self,
        order: &ExecutionOrderInfo,
        identity_refs: &ExecutionOrderIdentityRefs,
        submitted_at: DateTime<Utc>,
    ) -> QuantResult<ResolvedVenueIdentities> {
        let mut exact_order_id = order.venue_order_id.clone();
        let mut trade_ids = identity_refs
            .trades
            .iter()
            .map(|trade| trade.venue_trade_id.clone())
            .collect::<BTreeSet<_>>();
        let mut trades_by_id = BTreeMap::new();
        let mut order_is_working = false;
        let mut exact_order_loaded = false;
        let mut used_account_discovery = false;
        let allow_account_discovery = account_discovery_allowed(
            exact_order_id.is_some(),
            !identity_refs.trades.is_empty(),
            !identity_refs.transactions.is_empty(),
        );

        if let Some(order_id) = exact_order_id.as_ref() {
            let exact_order = self.reader.order(order_id).await?;
            validate_exact_order_id(order_id, &exact_order.order_id)?;
            order_is_working = exact_order.is_working;
            trade_ids.extend(exact_order.associated_trade_ids);
            exact_order_loaded = true;
        } else if allow_account_discovery {
            let discovery_before =
                (order.updated_at + Duration::seconds(1)).max(submitted_at + Duration::seconds(1));
            let discovered = self
                .reader
                .discover_trades(&order.token_id, submitted_at, discovery_before)
                .await?;
            let candidates = discovered
                .into_iter()
                .filter(|trade| trade_matches_ambiguous_order(order, trade))
                .collect::<Vec<_>>();
            let candidate_order_ids = candidates
                .iter()
                .map(|trade| trade.order_id.clone())
                .collect::<HashSet<_>>();
            if candidate_order_ids.len() == 1 {
                exact_order_id = candidate_order_ids.into_iter().next();
                for trade in candidates {
                    trade_ids.insert(trade.trade_id.clone());
                    trades_by_id.insert(trade.trade_id.clone(), trade);
                }
                used_account_discovery = true;
            }
        }

        for trade_id in &trade_ids {
            if trades_by_id.contains_key(trade_id) {
                continue;
            }
            if let Some(trade) = self.reader.trade(trade_id).await? {
                validate_exact_trade_id(trade_id, &trade.trade_id)?;
                trades_by_id.insert(trade_id.clone(), trade);
            }
        }

        if exact_order_id.is_none() {
            let observed_order_ids = trades_by_id
                .values()
                .map(|trade| trade.order_id.clone())
                .collect::<HashSet<_>>();
            if observed_order_ids.len() == 1 {
                exact_order_id = observed_order_ids.into_iter().next();
            }
        }
        if let Some(order_id) = exact_order_id.as_ref() {
            if !exact_order_loaded {
                let exact_order = self.reader.order(order_id).await?;
                validate_exact_order_id(order_id, &exact_order.order_id)?;
                order_is_working = exact_order.is_working;
                for trade_id in exact_order.associated_trade_ids {
                    if trade_ids.insert(trade_id.clone())
                        && let Some(trade) = self.reader.trade(&trade_id).await?
                    {
                        validate_exact_trade_id(&trade_id, &trade.trade_id)?;
                        trades_by_id.insert(trade_id, trade);
                    }
                }
            }
            if trades_by_id
                .values()
                .any(|trade| &trade.order_id != order_id)
            {
                return Err(ExecutionError::ReconciliationUnresolvable {
                    reason: format!("trade identity set does not belong to exact order {order_id}"),
                }
                .into());
            }
        }

        let missing_trade_count = trade_ids.len().saturating_sub(trades_by_id.len());
        let pending_trade_count = trades_by_id
            .values()
            .filter(|trade| {
                !matches!(
                    trade.status,
                    VenueTradeStatus::Confirmed | VenueTradeStatus::Failed
                )
            })
            .count();
        Ok(ResolvedVenueIdentities {
            attributable: exact_order_id.is_some() || !identity_refs.trades.is_empty(),
            still_working: order_is_working || missing_trade_count > 0 || pending_trade_count > 0,
            exact_order_id,
            trades_by_id,
            missing_trade_count,
            used_account_discovery,
        })
    }
}

async fn match_terms_context(
    catalog_ledger: &dyn CatalogLedgerRepository,
    clob_market_info: &dyn ClobMarketInfoRepository,
    order: &ExecutionOrderInfo,
    trade: &ClobTrade,
    observed_at: DateTime<Utc>,
) -> QuantResult<MatchTermsContext> {
    let clob_at_match = clob_market_info
        .at(&order.market_id, trade.matched_at, observed_at)
        .await?;
    let fee_schedule = clob_at_match
        .as_ref()
        .map(|version| PitFeeSchedule::from_market_fee_schedule(&version.fee_schedule()))
        .transpose()
        .map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "authenticated trade {} match-time fee schedule is invalid: {error:?}",
                trade.trade_id
            ),
        })?;
    let catalog_boundary = DecisionClock::new(0).boundary(trade.matched_at)?;
    let catalog = catalog_ledger
        .market_at(&order.market_id, &catalog_boundary)
        .await?;
    let resolved = match (&fee_schedule, catalog.as_ref()) {
        (Some(_), Some(catalog)) => match catalog.verified_payload() {
            Ok(market) => Some(
                PitMarketExecutionEconomics::resolve(
                    &clob_at_match
                        .as_ref()
                        .ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
                            reason: "match-time CLOB market info disappeared".to_owned(),
                        })?
                        .fee_schedule(),
                    &market.maker_rebate_evidence,
                    catalog.available_at,
                    trade.matched_at,
                )
                .map_err(|error| ExecutionError::ReconciliationUnresolvable {
                    reason: format!("match-time execution economics are invalid: {error:?}"),
                })?,
            ),
            Err(_) => None,
        },
        (None, _) | (_, None) => None,
    };
    let rebate_evidence = if !order.prepared_order_json.post_only {
        MatchMakerRebateEvidence::NotApplicable
    } else if fee_schedule.is_none() {
        MatchMakerRebateEvidence::Unavailable {
            reason: MatchRebateUnavailableReason::MissingClobMarketInfo,
        }
    } else if catalog.is_none() {
        MatchMakerRebateEvidence::Unavailable {
            reason: MatchRebateUnavailableReason::MissingCatalog,
        }
    } else if resolved.is_none() {
        MatchMakerRebateEvidence::Unavailable {
            reason: MatchRebateUnavailableReason::InvalidCatalog,
        }
    } else {
        match &resolved
            .as_ref()
            .ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
                reason: "match-time economics disappeared".to_owned(),
            })?
            .maker_rebate_evidence
        {
            PitMakerRebateEvidence::NoProgram { terms_hash, .. } => {
                MatchMakerRebateEvidence::NoProgram {
                    terms_hash: *terms_hash,
                }
            }
            PitMakerRebateEvidence::Available { schedule } => MatchMakerRebateEvidence::Available {
                terms_hash: schedule.terms_hash,
            },
            PitMakerRebateEvidence::Unavailable { reason, .. } => {
                MatchMakerRebateEvidence::Unavailable {
                    reason: reason.match_reason(),
                }
            }
        }
    };
    Ok(MatchTermsContext {
        fee_schedule,
        rebate_evidence,
        resolved,
    })
}

async fn authenticated_fee_evidence(
    catalog_ledger: &dyn CatalogLedgerRepository,
    clob_market_info: &dyn ClobMarketInfoRepository,
    order: &ExecutionOrderInfo,
    trade: &ClobTrade,
    observed_at: DateTime<Utc>,
) -> QuantResult<FeeMeasurement> {
    let prepared = &order.prepared_order_json.fee_schedule;
    let role = match trade.trader_side {
        FeeLiquidityRole::Maker => LiquidityRole::Maker,
        FeeLiquidityRole::Taker => LiquidityRole::Taker,
    };
    let expected_schedule = PitFeeSchedule {
        schedule_hash: prepared.schedule_hash,
        effective_at: prepared.effective_at,
        available_at: prepared.available_at,
        platform_rate: prepared.platform_rate,
        exponent: prepared.exponent,
        taker_only: prepared.taker_only,
        builder_maker_fee_bps: prepared.builder_maker_fee_bps,
        builder_taker_fee_bps: prepared.builder_taker_fee_bps,
        builder_attribution: prepared.builder_attribution,
    };
    let expected_fee = expected_schedule
        .fee(role, trade.price, trade.size, trade.matched_at)
        .map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "authenticated trade {} expected fee reconstruction failed: {error:?}",
                trade.trade_id
            ),
        })?;
    let derived_schedule = PitFeeSchedule {
        platform_rate: trade.fee_rate_bps.to_fraction(),
        ..expected_schedule
    };
    let derived_fee = derived_schedule
        .fee(role, trade.price, trade.size, trade.matched_at)
        .map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "authenticated trade {} fee reconstruction failed: {error:?}",
                trade.trade_id
            ),
        })?;
    let match_terms =
        match_terms_context(catalog_ledger, clob_market_info, order, trade, observed_at).await?;
    let match_rebate_evidence = match_terms.rebate_evidence;
    let decision_rebate_terms_hash = order
        .prepared_order_json
        .maker_rebate_terms
        .passive_terms_hash();
    let terms_drifted = match_terms
        .fee_schedule
        .as_ref()
        .is_none_or(|schedule| schedule.schedule_hash != prepared.schedule_hash)
        || match (
            decision_rebate_terms_hash,
            match_rebate_evidence,
            order.prepared_order_json.post_only,
        ) {
            (None, MatchMakerRebateEvidence::NotApplicable, false) => false,
            (
                Some(decision),
                MatchMakerRebateEvidence::NoProgram { terms_hash }
                | MatchMakerRebateEvidence::Available { terms_hash },
                true,
            ) => decision != terms_hash,
            _ => true,
        };
    let expected_maker_rebate = if let Some(economics) = &match_terms.resolved {
        economics
            .maker_rebate_evidence
            .schedule()
            .map(|schedule| {
                schedule.expected_incentive(
                    &economics.fee_schedule,
                    role,
                    trade.price,
                    trade.size,
                    trade.matched_at,
                )
            })
            .transpose()
            .map_err(|error| ExecutionError::ReconciliationUnresolvable {
                reason: format!(
                    "authenticated trade {} maker-rebate accrual failed: {error:?}",
                    trade.trade_id
                ),
            })?
            .flatten()
    } else {
        None
    };
    Ok(FeeMeasurement::AuthenticatedTradeDerived {
        trade_id: trade.trade_id.clone(),
        bucket_index: trade.bucket_index,
        order_id: trade.order_id.clone(),
        liquidity_role: trade.trader_side,
        fee_rate_bps: trade.fee_rate_bps,
        expected_fee,
        derived_fee,
        decision_fee_hash: prepared.schedule_hash,
        match_fee_hash: match_terms
            .fee_schedule
            .map(|schedule| schedule.schedule_hash),
        decision_rebate_terms_hash,
        match_rebate_evidence,
        terms_drifted,
        expected_maker_rebate,
        transaction_hash: trade.transaction_hash.clone(),
        matched_at: trade.matched_at,
        maker_order_ids: trade
            .maker_orders
            .iter()
            .map(|maker| maker.order_id.clone())
            .collect(),
    })
}

fn on_chain_fee_evidence(
    order: &ExecutionOrderInfo,
    trade: &ClobTrade,
    events: &[ExchangeEventRow],
    wallet: &WalletTopology,
    now: DateTime<Utc>,
) -> QuantResult<Option<ReconciliationEvidence>> {
    let Some(authenticated_transaction_hash) = trade.transaction_hash.as_ref() else {
        return Ok(None);
    };
    let account_address = format!("{:#x}", wallet.funder);
    let matches = events
        .iter()
        .filter(|event| {
            event.order_hash == trade.order_id.as_str()
                && event.transaction_hash == authenticated_transaction_hash.as_str()
        })
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Ok(None);
    }
    if matches.len() != 1 {
        return Err(ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "order {} trade {} maps to {} finalized V2 OrderFilled events",
                trade.order_id,
                trade.trade_id,
                matches.len()
            ),
        }
        .into());
    }
    let event = matches[0];
    let expected_side = match order.side {
        Side::Buy => ChExchangeSide::Buy,
        Side::Sell => ChExchangeSide::Sell,
    };
    let allowed_contract = EXCHANGE_CONTRACTS
        .iter()
        .any(|contract| format!("{:#x}", contract.address) == event.contract_address);
    if event.chain_id != 137
        || event.event_kind != ChExchangeEventKind::OrderFilled
        || event.exchange_version != ChExchangeVersion::V2
        || event.schema_version != ExchangeEventRow::SCHEMA_VERSION
        || !allowed_contract
        || event.maker != account_address
        || event.token_id.as_deref() != Some(order.token_id.as_str())
        || event.side != expected_side
    {
        return Err(ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "finalized V2 OrderFilled identity disagrees with governed order {} and account {}",
                trade.order_id, account_address
            ),
        }
        .into());
    }
    let maker_amount = v2_asset_amount(&event.maker_amount, event)?;
    let taker_amount = v2_asset_amount(&event.taker_amount, event)?;
    let (principal, shares) = match order.side {
        Side::Buy => (maker_amount, taker_amount),
        Side::Sell => (taker_amount, maker_amount),
    };
    if shares <= Decimal::ZERO
        || Shares::new(shares) != trade.size
        || Price::new(principal / shares) != trade.price
    {
        return Err(ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "finalized V2 OrderFilled amounts disagree with authenticated trade {}",
                trade.trade_id
            ),
        }
        .into());
    }
    let available_at =
        DateTime::from_timestamp_millis(event.model_available_at).ok_or_else(|| {
            ExecutionError::ReconciliationUnresolvable {
                reason: format!(
                    "finalized V2 OrderFilled {:?} has invalid availability timestamp",
                    event.event_id
                ),
            }
        })?;
    if available_at > now {
        return Ok(None);
    }
    let matched_at = DateTime::from_timestamp_millis(event.block_timestamp).ok_or_else(|| {
        ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "finalized V2 OrderFilled {:?} has invalid block timestamp",
                event.event_id
            ),
        }
    })?;
    let raw_fee =
        event
            .fee_amount
            .as_deref()
            .ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
                reason: format!(
                    "finalized V2 OrderFilled {:?} has no fee amount",
                    event.event_id
                ),
            })?;
    let fee_raw =
        Decimal::from_str(raw_fee).map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "finalized V2 OrderFilled {:?} fee is not decimal: {error}",
                event.event_id
            ),
        })?;
    let exchange_address = EvmAddress::parse(event.contract_address.clone()).map_err(|error| {
        ExecutionError::ReconciliationUnresolvable {
            reason: format!("finalized exchange address is invalid: {error}"),
        }
    })?;
    let transaction_hash =
        EvmTransactionHash::parse(event.transaction_hash.clone()).map_err(|error| {
            ExecutionError::ReconciliationUnresolvable {
                reason: format!("finalized transaction hash is invalid: {error}"),
            }
        })?;
    let zero_builder = format!("0x{}", "0".repeat(64));
    Ok(Some(ReconciliationEvidence {
        kind: ReconciliationEvidenceKind::OnChainSettlement,
        observed_at: available_at,
        detail: format!(
            "V2 OrderFilled block={} log_index={} order_hash={}",
            event.block_number, event.log_index, event.order_hash
        ),
        venue_ref: Some(event.transaction_hash.clone()),
        shares: Some(trade.size),
        price: Some(trade.price),
        fee_evidence: Some(FeeMeasurement::OnChainSettled {
            venue_trade_id: trade.trade_id.clone(),
            chain_id: 137,
            protocol_version: 2,
            exchange_address,
            order_id: trade.order_id.clone(),
            liquidity_role: trade.trader_side,
            transaction_hash,
            log_index: event.log_index,
            matched_at,
            available_at,
            settled_fee: Usd::new(fee_raw / Decimal::from(1_000_000_u64)),
            builder_code: event
                .builder
                .clone()
                .filter(|builder| builder != &zero_builder),
        }),
    }))
}

fn v2_asset_amount(raw: &str, event: &ExchangeEventRow) -> QuantResult<Decimal> {
    Decimal::from_str(raw)
        .map(|value| value / Decimal::from(1_000_000_u64))
        .map_err(|error| {
            ExecutionError::ReconciliationUnresolvable {
                reason: format!(
                    "finalized V2 OrderFilled {:?} asset amount is not decimal: {error}",
                    event.event_id
                ),
            }
            .into()
        })
}

fn trade_matches_ambiguous_order(order: &ExecutionOrderInfo, trade: &ClobTrade) -> bool {
    if trade.token_id != order.token_id || trade.side != order.side {
        return false;
    }
    match order.side {
        Side::Buy => trade.price <= order.price,
        Side::Sell => trade.price >= order.price,
    }
}

fn validate_exact_order_id(requested: &OrderId, observed: &OrderId) -> QuantResult<()> {
    if requested == observed {
        Ok(())
    } else {
        Err(ExecutionError::ReconciliationUnresolvable {
            reason: format!("exact order lookup returned {observed} for requested {requested}"),
        }
        .into())
    }
}

fn validate_exact_trade_id(requested: &VenueTradeId, observed: &VenueTradeId) -> QuantResult<()> {
    if requested == observed {
        Ok(())
    } else {
        Err(ExecutionError::ReconciliationUnresolvable {
            reason: format!("exact trade lookup returned {observed} for requested {requested}"),
        }
        .into())
    }
}

fn trade_observation(trade: &ClobTrade) -> ExecutionTradeObservation {
    ExecutionTradeObservation {
        venue_trade_id: trade.trade_id.clone(),
        trade_status: trade.status,
        transaction_hash: trade.transaction_hash.clone(),
    }
}

const fn account_discovery_allowed(
    has_order_id: bool,
    has_trade_ids: bool,
    has_transaction_hashes: bool,
) -> bool {
    !has_order_id && !has_trade_ids && !has_transaction_hashes
}

const fn trade_is_final_fill(status: VenueTradeStatus) -> bool {
    matches!(status, VenueTradeStatus::Confirmed)
}

#[async_trait]
impl EvidenceCollector for VenueEvidenceCollector {
    async fn collect(
        &self,
        order: &ExecutionOrderInfo,
        identity_refs: &ExecutionOrderIdentityRefs,
        share_basis: ShareSettlementBasis,
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<CollectedReconciliation> {
        let token_id = &order.token_id;
        let submitted_at = order.submitted_at.unwrap_or(order.created_at);
        let past_stale_deadline = now - submitted_at > stale_after;
        let resolved = self
            .resolve_identities(order, identity_refs, submitted_at)
            .await?;
        let exact_order_id = resolved.exact_order_id;
        let trades_by_id = resolved.trades_by_id;
        let missing_trade_count = resolved.missing_trade_count;
        let attributable = resolved.attributable;
        let still_working = resolved.still_working;
        let used_account_discovery = resolved.used_account_discovery;
        let mut evidence = Vec::with_capacity(8 + trades_by_id.len());
        let exact_order_ids = trades_by_id
            .values()
            .filter(|trade| trade_is_final_fill(trade.status))
            .map(|trade| trade.order_id.clone())
            .collect::<Vec<_>>();
        let settled_events = self.fact_read.order_filled_events(exact_order_ids).await?;

        // 1 — exact CLOB order identity/status. No account-wide open-order scan.
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: now,
            detail: format!(
                "exact_order={}; still_working={still_working}; attributable={attributable}; \
                 account_discovery={used_account_discovery}",
                exact_order_id.as_ref().map_or("none", OrderId::as_str)
            ),
            venue_ref: exact_order_id.as_ref().map(ToString::to_string),
            shares: None,
            price: None,
            fee_evidence: None,
        });

        // 2 — only CONFIRMED trades are realized fill truth. MATCHED/MINED/
        // RETRYING keep the order pending; FAILED contributes no fill.
        let trade_evidence = self
            .collect_trade_evidence(TradeEvidenceInput {
                order,
                trades: &trades_by_id,
                settled_events: &settled_events,
                exact_order_id: exact_order_id.as_ref(),
                missing_trade_count,
                now,
            })
            .await?;
        let filled_shares = trade_evidence.filled_shares;
        let avg_price = trade_evidence.avg_price;
        evidence.extend(trade_evidence.evidence);

        // 3 — Account-wide token balance is diagnostic only. Exact Confirmed
        // trades and optional finalized OrderFilled facts prove receipt/debit;
        // fungible balance can change through other legitimate orders.
        let (token_balance, token_balance_evidence) = self
            .token_balance_evidence(token_id, share_basis, now)
            .await;
        evidence.push(token_balance_evidence);

        // 4 — Account balance: absolute corroboration that collateral was spent.
        let collateral = self.reader.collateral_balance().await?;
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::AccountBalanceDelta,
            observed_at: now,
            detail: format!("collateral_balance={collateral} (absolute)"),
            venue_ref: None,
            shares: None,
            price: None,
            fee_evidence: None,
        });

        // 5 — Book context: price sanity around the submission (best effort).
        evidence.push(self.book_context_evidence(token_id, now));

        let gtd_expired = order.gtd_expiration_at.is_some_and(|expiry| now >= expiry);
        let presence = if !attributable {
            VenuePresence::Unattributable
        } else if still_working {
            VenuePresence::Resting
        } else {
            VenuePresence::Settled
        };

        Ok(CollectedReconciliation {
            evidence,
            facts: ReconcileFacts {
                order_shares: order.shares,
                presence,
                filled_shares,
                avg_price,
                share_basis,
                observed_token_balance: token_balance,
                past_stale_deadline,
                gtd_expired,
            },
            identity_enrichment: ExecutionIdentityEnrichment {
                discovered_order_id: (order.venue_order_id != exact_order_id)
                    .then_some(exact_order_id)
                    .flatten(),
                trades: trades_by_id.values().map(trade_observation).collect(),
                observed_at: now,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::enums::execution::VenueTradeStatus;

    use super::{account_discovery_allowed, trade_is_final_fill};

    #[test]
    fn account_history_requires_absence() {
        assert!(account_discovery_allowed(false, false, false));
        assert!(!account_discovery_allowed(true, false, false));
        assert!(!account_discovery_allowed(false, true, false));
        assert!(!account_discovery_allowed(false, false, true));
    }

    #[test]
    fn only_confirmed_trade_truth() {
        assert!(trade_is_final_fill(VenueTradeStatus::Confirmed));
        for status in [
            VenueTradeStatus::Matched,
            VenueTradeStatus::Mined,
            VenueTradeStatus::Retrying,
            VenueTradeStatus::Failed,
        ] {
            assert!(!trade_is_final_fill(status));
        }
    }
}
