//! Compiled, read-optimized indexes over an active publication's factors.
//!
//! Indexes are built once by [`super::snapshot::ControlFactorSnapshot::compile`]
//! and only read on the hot path. Bucket / execution-quality indexes use hash
//! maps with a deterministic specificity fallback; portfolio / reconciliation
//! resolve to a single conservative worst-of state at compile time.

use super::applied::{
    AppliedControlFactor, MarketAnomalyDecision, PortfolioRiskDecision,
    ReconciliationHealthDecision,
};
use crate::{
    domain::control_factor::{
        BucketRiskDimensions, BucketRiskPayload, ExecutionQualityDimensions,
        ExecutionQualityPayload, LatencyBucket, MarketAnomalyPayload, PortfolioRiskPayload,
        ReconciliationHealthPayload,
    },
    enums::{common::MarketCategory, control_factor::ControlFactorType},
    types::{ControlFactorId, EventId, FactorPublicationId, MarketId},
};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// A factor payload paired with the id that produced it, for audit attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedFactor<P> {
    pub factor_id: ControlFactorId,
    pub payload: P,
}

/// Bucket-risk index keyed by typed dimensions with specificity fallback.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BucketRiskIndex {
    entries: HashMap<BucketRiskDimensions, IndexedFactor<BucketRiskPayload>>,
}

impl BucketRiskIndex {
    pub(super) fn insert(
        &mut self,
        dims: BucketRiskDimensions,
        factor_id: ControlFactorId,
        payload: BucketRiskPayload,
    ) {
        self.entries
            .insert(dims, IndexedFactor { factor_id, payload });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Resolve the most specific matching bucket factor.
    ///
    /// Tries the fully-specified key first, then relaxes optional dimensions in
    /// a fixed order (fee profile → neg risk → hours-to-settlement) so a coarse
    /// factor still applies when a fine-grained one was not materialized.
    #[must_use]
    pub fn lookup(&self, dims: &BucketRiskDimensions) -> Option<&IndexedFactor<BucketRiskPayload>> {
        for candidate in Self::fallback_keys(dims) {
            if let Some(found) = self.entries.get(&candidate) {
                return Some(found);
            }
        }
        None
    }

    fn fallback_keys(dims: &BucketRiskDimensions) -> [BucketRiskDimensions; 4] {
        [
            dims.clone(),
            BucketRiskDimensions {
                fee_profile: None,
                ..dims.clone()
            },
            BucketRiskDimensions {
                fee_profile: None,
                neg_risk: None,
                ..dims.clone()
            },
            BucketRiskDimensions {
                fee_profile: None,
                neg_risk: None,
                hours_to_settlement_bucket: None,
                ..dims.clone()
            },
        ]
    }
}

/// Execution-quality index keyed by typed dimensions with latency fallback.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionQualityIndex {
    entries: HashMap<ExecutionQualityDimensions, IndexedFactor<ExecutionQualityPayload>>,
}

impl ExecutionQualityIndex {
    pub(super) fn insert(
        &mut self,
        dims: ExecutionQualityDimensions,
        factor_id: ControlFactorId,
        payload: ExecutionQualityPayload,
    ) {
        self.entries
            .insert(dims, IndexedFactor { factor_id, payload });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Resolve the execution-quality factor for these dimensions.
    ///
    /// Live latency classification is best-effort, so an exact match is tried
    /// first and then the same key with `LatencyBucket::Unknown`.
    #[must_use]
    pub fn lookup(
        &self,
        dims: &ExecutionQualityDimensions,
    ) -> Option<&IndexedFactor<ExecutionQualityPayload>> {
        if let Some(found) = self.entries.get(dims) {
            return Some(found);
        }
        if dims.latency_bucket != LatencyBucket::Unknown {
            let relaxed = ExecutionQualityDimensions {
                latency_bucket: LatencyBucket::Unknown,
                ..dims.clone()
            };
            return self.entries.get(&relaxed);
        }
        None
    }
}

/// Market-anomaly index for market/event blocks and category cooldowns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketAnomalyIndex {
    blocked_markets: HashMap<MarketId, IndexedFactor<MarketAnomalyPayload>>,
    blocked_events: HashMap<EventId, IndexedFactor<MarketAnomalyPayload>>,
    category_cooldowns: HashMap<MarketCategory, IndexedFactor<MarketAnomalyPayload>>,
}

impl MarketAnomalyIndex {
    pub(super) fn insert_market(
        &mut self,
        market_id: MarketId,
        factor_id: ControlFactorId,
        payload: MarketAnomalyPayload,
    ) {
        self.blocked_markets
            .insert(market_id, IndexedFactor { factor_id, payload });
    }

    pub(super) fn insert_event(
        &mut self,
        event_id: EventId,
        factor_id: ControlFactorId,
        payload: MarketAnomalyPayload,
    ) {
        self.blocked_events
            .insert(event_id, IndexedFactor { factor_id, payload });
    }

    pub(super) fn insert_category(
        &mut self,
        category: MarketCategory,
        factor_id: ControlFactorId,
        payload: MarketAnomalyPayload,
    ) {
        self.category_cooldowns
            .insert(category, IndexedFactor { factor_id, payload });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.blocked_markets.is_empty()
            && self.blocked_events.is_empty()
            && self.category_cooldowns.is_empty()
    }

    /// Block entry on `market_id`, if a `block_market` anomaly is active.
    #[must_use]
    pub fn market_block(
        &self,
        market_id: &MarketId,
    ) -> Option<&IndexedFactor<MarketAnomalyPayload>> {
        self.blocked_markets
            .get(market_id)
            .filter(|entry| entry.payload.block_market)
    }

    /// Block entry on `event_id`, if a `block_event` anomaly is active.
    #[must_use]
    pub fn event_block(&self, event_id: &EventId) -> Option<&IndexedFactor<MarketAnomalyPayload>> {
        self.blocked_events
            .get(event_id)
            .filter(|entry| entry.payload.block_event)
    }

    /// Active category cooldown window in seconds, if any.
    #[must_use]
    pub fn category_cooldown_secs(&self, category: MarketCategory) -> Option<u64> {
        self.category_cooldowns
            .get(&category)
            .and_then(|entry| entry.payload.category_cooldown_secs)
    }

    /// Resolve the execution-time anomaly decision for a market/event.
    #[must_use]
    pub fn decision(
        &self,
        publication_id: &FactorPublicationId,
        market_id: &MarketId,
        event_id: &EventId,
    ) -> MarketAnomalyDecision {
        let market = self.market_block(market_id);
        let event = self.event_block(event_id);
        let source = market.or(event);
        let market_entry = self.blocked_markets.get(market_id);
        let event_entry = self.blocked_events.get(event_id);
        let manual_ack_required = market_entry
            .is_some_and(|entry| entry.payload.manual_ack_required)
            || event_entry.is_some_and(|entry| entry.payload.manual_ack_required);
        MarketAnomalyDecision {
            block_market: market.is_some(),
            block_event: event.is_some(),
            manual_ack_required,
            reason_code: source.map(|entry| entry.payload.reason_code.clone()),
            source: source.map(|entry| {
                AppliedControlFactor::new(
                    entry.factor_id.clone(),
                    ControlFactorType::MarketAnomaly,
                    publication_id.clone(),
                    Decimal::ZERO,
                    Decimal::ONE,
                    format!("market anomaly block: {}", entry.payload.reason_code),
                )
            }),
        }
    }
}

/// Conservative worst-of portfolio-risk state resolved at compile time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioRiskState {
    global_size_multiplier: Decimal,
    daily_budget_multiplier: Decimal,
    kelly_fraction_multiplier: Decimal,
    max_open_positions: Option<u32>,
    category_multipliers: HashMap<MarketCategory, Decimal>,
    binding_factor_id: Option<ControlFactorId>,
}

impl Default for PortfolioRiskState {
    fn default() -> Self {
        Self {
            global_size_multiplier: Decimal::ONE,
            daily_budget_multiplier: Decimal::ONE,
            kelly_fraction_multiplier: Decimal::ONE,
            max_open_positions: None,
            category_multipliers: HashMap::new(),
            binding_factor_id: None,
        }
    }
}

impl PortfolioRiskState {
    /// Fold one portfolio factor into the worst-of aggregate.
    pub(super) fn absorb(&mut self, factor_id: &ControlFactorId, payload: &PortfolioRiskPayload) {
        if payload.global_size_multiplier < self.global_size_multiplier {
            self.global_size_multiplier = payload.global_size_multiplier;
            self.binding_factor_id = Some(factor_id.clone());
        }
        self.daily_budget_multiplier = self
            .daily_budget_multiplier
            .min(payload.daily_budget_multiplier);
        self.kelly_fraction_multiplier = self
            .kelly_fraction_multiplier
            .min(payload.kelly_fraction_multiplier);
        self.max_open_positions = min_optional(self.max_open_positions, payload.max_open_positions);
    }

    pub(super) fn absorb_category(&mut self, category: MarketCategory, multiplier: Decimal) {
        let entry = self
            .category_multipliers
            .entry(category)
            .or_insert(Decimal::ONE);
        *entry = (*entry).min(multiplier);
    }

    #[must_use]
    pub fn is_neutral(&self) -> bool {
        self.global_size_multiplier == Decimal::ONE
            && self.daily_budget_multiplier == Decimal::ONE
            && self.kelly_fraction_multiplier == Decimal::ONE
            && self.max_open_positions.is_none()
            && self.category_multipliers.is_empty()
    }

    /// Resolve the execution-time decision for an optional category.
    #[must_use]
    pub fn decision(
        &self,
        publication_id: &FactorPublicationId,
        category: MarketCategory,
    ) -> PortfolioRiskDecision {
        let category_size_multiplier = self.category_multipliers.get(&category).copied();
        let source = self.binding_factor_id.as_ref().map(|factor_id| {
            AppliedControlFactor::new(
                factor_id.clone(),
                ControlFactorType::PortfolioRisk,
                publication_id.clone(),
                Decimal::ONE,
                self.global_size_multiplier,
                "portfolio risk worst-of size multiplier",
            )
        });
        PortfolioRiskDecision {
            global_size_multiplier: self.global_size_multiplier,
            category_size_multiplier,
            daily_budget_multiplier: self.daily_budget_multiplier,
            kelly_fraction_multiplier: self.kelly_fraction_multiplier,
            max_open_positions: self.max_open_positions,
            category: Some(category),
            source,
        }
    }
}

/// Conservative worst-of reconciliation-health state resolved at compile time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationHealthState {
    force_maintenance_mode: bool,
    size_multiplier: Decimal,
    require_manual_ack: bool,
    binding_factor_id: Option<ControlFactorId>,
}

impl Default for ReconciliationHealthState {
    fn default() -> Self {
        Self {
            force_maintenance_mode: false,
            size_multiplier: Decimal::ONE,
            require_manual_ack: false,
            binding_factor_id: None,
        }
    }
}

impl ReconciliationHealthState {
    /// Fold one reconciliation factor into the worst-of aggregate.
    pub(super) fn absorb(
        &mut self,
        factor_id: &ControlFactorId,
        payload: &ReconciliationHealthPayload,
    ) {
        if payload.force_maintenance_mode && !self.force_maintenance_mode {
            self.force_maintenance_mode = true;
            self.binding_factor_id = Some(factor_id.clone());
        }
        if payload.size_multiplier < self.size_multiplier {
            self.size_multiplier = payload.size_multiplier;
            if self.binding_factor_id.is_none() || !self.force_maintenance_mode {
                self.binding_factor_id = Some(factor_id.clone());
            }
        }
        self.require_manual_ack = self.require_manual_ack || payload.require_manual_ack;
    }

    #[must_use]
    pub fn is_neutral(&self) -> bool {
        !self.force_maintenance_mode
            && self.size_multiplier == Decimal::ONE
            && !self.require_manual_ack
    }

    /// Resolve the execution-time decision.
    #[must_use]
    pub fn decision(&self, publication_id: &FactorPublicationId) -> ReconciliationHealthDecision {
        let source = self.binding_factor_id.as_ref().map(|factor_id| {
            AppliedControlFactor::new(
                factor_id.clone(),
                ControlFactorType::ReconciliationHealth,
                publication_id.clone(),
                Decimal::ONE,
                self.size_multiplier,
                if self.force_maintenance_mode {
                    "reconciliation maintenance mode"
                } else {
                    "reconciliation size throttle"
                },
            )
        });
        ReconciliationHealthDecision {
            force_maintenance_mode: self.force_maintenance_mode,
            size_multiplier: self.size_multiplier,
            require_manual_ack: self.require_manual_ack,
            source,
        }
    }
}

fn min_optional(current: Option<u32>, candidate: Option<u32>) -> Option<u32> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}
