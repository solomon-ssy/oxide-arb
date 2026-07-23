//! Venue evidence collection in deterministic reconciliation order.
//!
//! For one reconcilable order the collector gathers, in the immutable order
//! 1→5, one [`ReconciliationEvidence`] per source: CLOB order status → CLOB
//! trades → token balance → account balance → book context. (`OperatorNote`,
//! #6, is appended only on a human resolve, never by the machine.) The high-
//! confidence sources (status + trades) decide; balances corroborate.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::clob::ClobTrade;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::quant::{
        ExecutionIdentityEnrichment, ExecutionOrderIdentityRefs, ExecutionOrderInfo,
        ExecutionTradeObservation,
    },
    enums::{
        common::Side,
        execution::{ReconciliationEvidenceKind, VenueTradeStatus},
        fee::FeeLiquidityRole,
    },
    types::{
        FeeEvidence, OrderId, Price, ReconciliationEvidence, Shares, TokenId, Usd, VenueTradeId,
    },
};
use quant_pivot_research::execution_semantics::{LiquidityRole, PitFeeSchedule};

use super::{ReconcileFacts, VenuePresence, VenueReconciliationReader};
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
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<CollectedReconciliation>;
}

/// [`EvidenceCollector`] backed by the venue reader + the in-memory book store.
pub struct VenueEvidenceCollector {
    reader: Arc<dyn VenueReconciliationReader>,
    book_store: Arc<BookStore>,
}

struct ResolvedVenueIdentities {
    exact_order_id: Option<OrderId>,
    trades_by_id: BTreeMap<VenueTradeId, ClobTrade>,
    missing_trade_count: usize,
    attributable: bool,
    still_working: bool,
    used_account_discovery: bool,
}

impl VenueEvidenceCollector {
    #[must_use]
    pub const fn new(
        reader: Arc<dyn VenueReconciliationReader>,
        book_store: Arc<BookStore>,
    ) -> Self {
        Self { reader, book_store }
    }

    /// Evidence #5 — the current published book snapshot for price sanity
    /// (best effort: records version + timestamp, or that none is published).
    fn book_context_evidence(
        &self,
        token_id: &TokenId,
        now: DateTime<Utc>,
    ) -> ReconciliationEvidence {
        let last_known = self.book_store.load_last_known_by_id(token_id);
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

fn authenticated_fee_evidence(
    order: &ExecutionOrderInfo,
    trade: &ClobTrade,
) -> QuantResult<FeeEvidence> {
    let prepared = &order.prepared_order_json.fee_schedule;
    let role = match trade.trader_side {
        FeeLiquidityRole::Maker => LiquidityRole::Maker,
        FeeLiquidityRole::Taker => LiquidityRole::Taker,
    };
    let schedule = PitFeeSchedule {
        schedule_hash: prepared.schedule_hash,
        effective_at: prepared.effective_at,
        available_at: prepared.available_at,
        platform_rate: trade.fee_rate_bps.to_fraction(),
        exponent: prepared.exponent,
        taker_only: prepared.taker_only,
        builder_maker_fee_bps: prepared.builder_maker_fee_bps,
        builder_taker_fee_bps: prepared.builder_taker_fee_bps,
        builder_attribution: prepared.builder_attribution,
    };
    let reconstructed_fee = schedule
        .fee(role, trade.price, trade.size, trade.matched_at)
        .map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "authenticated trade {} fee reconstruction failed: {error:?}",
                trade.trade_id
            ),
        })?;
    Ok(FeeEvidence::AuthenticatedTradeReconstructed {
        trade_id: trade.trade_id.clone(),
        order_id: trade.order_id.clone(),
        liquidity_role: trade.trader_side,
        fee_rate_bps: trade.fee_rate_bps,
        reconstructed_fee,
        transaction_hash: trade.transaction_hash.clone(),
        matched_at: trade.matched_at,
        maker_order_ids: trade
            .maker_orders
            .iter()
            .map(|maker| maker.order_id.clone())
            .collect(),
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
        let mut filled_shares = Shares::ZERO;
        let mut filled_cost = Usd::ZERO;
        for trade in trades_by_id.values() {
            let confirmed = trade_is_final_fill(trade.status);
            if confirmed {
                filled_shares += trade.size;
                filled_cost += trade.size * trade.price;
            }
            evidence.push(ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobTrades,
                observed_at: now,
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
                    Some(authenticated_fee_evidence(order, trade)?)
                } else {
                    None
                },
            });
        }
        let avg_price = if filled_shares.is_positive() {
            Some(Price::new(filled_cost.inner() / filled_shares.inner()))
        } else {
            None
        };
        if trades_by_id.is_empty() {
            evidence.push(ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobTrades,
                observed_at: now,
                detail: format!(
                    "exact_trades=0; missing_trade_ids={missing_trade_count}; filled_shares=0"
                ),
                venue_ref: exact_order_id.as_ref().map(ToString::to_string),
                shares: Some(Shares::ZERO),
                price: None,
                fee_evidence: None,
            });
        }

        // 3 — Token balance: absolute corroboration that shares were received.
        let token_balance = self.reader.token_balance(token_id).await?;
        evidence.push(ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::TokenBalanceDelta,
            observed_at: now,
            detail: format!("token_balance={token_balance} (absolute)"),
            venue_ref: Some(token_id.to_string()),
            shares: Some(token_balance),
            price: None,
            fee_evidence: None,
        });

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
                token_balance,
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
    fn account_history_discovery_requires_complete_identity_absence() {
        assert!(account_discovery_allowed(false, false, false));
        assert!(!account_discovery_allowed(true, false, false));
        assert!(!account_discovery_allowed(false, true, false));
        assert!(!account_discovery_allowed(false, false, true));
    }

    #[test]
    fn only_confirmed_trade_status_is_realized_fill_truth() {
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
