//! Auditable factor-application trace and the per-trade factor decision context.
//!
//! Every place where a control factor changes a live decision must emit an
//! [`AppliedControlFactor`] so the effect is recoverable from the detection /
//! execution audit, never hidden in logs. [`FactorDecisionContext`] is the
//! execution-time bundle threaded into the risk engine: it carries the named
//! safety decisions (reconciliation / market anomaly / portfolio) evaluated
//! against the *current* published snapshot.

use crate::{
    enums::{common::MarketCategory, control_factor::ControlFactorType},
    types::{ControlFactorId, FactorPublicationId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// One auditable application of a control factor to a live decision.
///
/// `input_value` / `output_value` capture the governed quantity before and
/// after the factor effect (e.g. base vs haircut resolution probability) so the
/// adjustment is fully reconstructable from the audit trail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedControlFactor {
    pub factor_id: ControlFactorId,
    pub factor_type: ControlFactorType,
    pub publication_id: FactorPublicationId,
    pub input_value: Decimal,
    pub output_value: Decimal,
    pub reason: String,
}

impl AppliedControlFactor {
    #[must_use]
    pub fn new(
        factor_id: ControlFactorId,
        factor_type: ControlFactorType,
        publication_id: FactorPublicationId,
        input_value: Decimal,
        output_value: Decimal,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            factor_id,
            factor_type,
            publication_id,
            input_value,
            output_value,
            reason: reason.into(),
        }
    }
}

/// Reconciliation-health decision resolved at execution time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationHealthDecision {
    /// Hard reject all new entries (maintenance mode) when true.
    pub force_maintenance_mode: bool,
    /// Conservative size multiplier in `0..=1` applied to the sized bet.
    pub size_multiplier: Decimal,
    /// Whether the active reconciliation factor requires manual acknowledgement.
    pub require_manual_ack: bool,
    /// The factor backing this decision, if any matched.
    pub source: Option<AppliedControlFactor>,
}

impl Default for ReconciliationHealthDecision {
    fn default() -> Self {
        Self {
            force_maintenance_mode: false,
            size_multiplier: Decimal::ONE,
            require_manual_ack: false,
            source: None,
        }
    }
}

/// Market-anomaly decision resolved at execution time for a single market/event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MarketAnomalyDecision {
    /// Hard block this market.
    pub block_market: bool,
    /// Hard block this market's event.
    pub block_event: bool,
    /// Reason code carried from the matched anomaly factor.
    pub reason_code: Option<String>,
    /// The factor backing this decision, if any matched.
    pub source: Option<AppliedControlFactor>,
}

impl MarketAnomalyDecision {
    /// Whether this decision hard-rejects the trade.
    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        self.block_market || self.block_event
    }
}

/// Portfolio-risk decision resolved at execution time from the active regime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskDecision {
    pub global_size_multiplier: Decimal,
    pub category_size_multiplier: Option<Decimal>,
    pub daily_budget_multiplier: Decimal,
    pub kelly_fraction_multiplier: Decimal,
    pub max_open_positions: Option<u32>,
    pub category: Option<MarketCategory>,
    pub source: Option<AppliedControlFactor>,
}

impl Default for PortfolioRiskDecision {
    fn default() -> Self {
        Self {
            global_size_multiplier: Decimal::ONE,
            category_size_multiplier: None,
            daily_budget_multiplier: Decimal::ONE,
            kelly_fraction_multiplier: Decimal::ONE,
            max_open_positions: None,
            category: None,
            source: None,
        }
    }
}

/// Execution-time factor decision bundle threaded into the risk engine.
///
/// Built from the *current* published snapshot at validation time (not frozen
/// at detection), so safety factors act on the freshest information. The
/// `applied_factors` vector aggregates every source factor for audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorDecisionContext {
    pub publication_id: Option<FactorPublicationId>,
    pub reconciliation_health: ReconciliationHealthDecision,
    pub market_anomaly: MarketAnomalyDecision,
    pub portfolio_risk: PortfolioRiskDecision,
    /// Bucket-risk size multiplier resolved for this opportunity's bucket
    /// (`1` when neutral). Threaded to the sizer as an explicit size cap.
    pub bucket_size_multiplier: Decimal,
    pub applied_factors: Vec<AppliedControlFactor>,
}

impl Default for FactorDecisionContext {
    fn default() -> Self {
        Self::neutral()
    }
}

impl FactorDecisionContext {
    /// A no-op context: no publication active, every factor neutral.
    #[must_use]
    pub fn neutral() -> Self {
        Self {
            publication_id: None,
            reconciliation_health: ReconciliationHealthDecision::default(),
            market_anomaly: MarketAnomalyDecision::default(),
            portfolio_risk: PortfolioRiskDecision::default(),
            bucket_size_multiplier: Decimal::ONE,
            applied_factors: Vec::new(),
        }
    }

    /// Whether any named safety factor hard-rejects this trade.
    #[must_use]
    pub const fn is_hard_rejected(&self) -> bool {
        self.reconciliation_health.force_maintenance_mode || self.market_anomaly.is_blocking()
    }
}
