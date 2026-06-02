//! Typed control-factor payloads and safety validation.

use super::{
    evidence::ManualApproval,
    safety::{ensure_block_monotonic, ensure_multiplier, ensure_non_negative},
};
use crate::enums::control_factor::{ControlFactorType, FactorSeverity};
use oxide_arb_error::control::PayloadSafetyError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Versioned typed payload for the five Phase 5 control-factor families.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "factor_type", content = "payload", rename_all = "snake_case")]
pub enum FactorPayload {
    BucketRisk(BucketRiskPayload),
    ExecutionQuality(ExecutionQualityPayload),
    PortfolioRisk(PortfolioRiskPayload),
    ReconciliationHealth(ReconciliationHealthPayload),
    MarketAnomaly(MarketAnomalyPayload),
}

impl FactorPayload {
    #[must_use]
    pub const fn factor_type(&self) -> ControlFactorType {
        match self {
            Self::BucketRisk(_) => ControlFactorType::BucketRisk,
            Self::ExecutionQuality(_) => ControlFactorType::ExecutionQuality,
            Self::PortfolioRisk(_) => ControlFactorType::PortfolioRisk,
            Self::ReconciliationHealth(_) => ControlFactorType::ReconciliationHealth,
            Self::MarketAnomaly(_) => ControlFactorType::MarketAnomaly,
        }
    }

    pub fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        match self {
            Self::BucketRisk(payload) => payload.validate_safety(),
            Self::ExecutionQuality(payload) => payload.validate_safety(),
            Self::PortfolioRisk(payload) => payload.validate_safety(),
            Self::ReconciliationHealth(payload) => payload.validate_safety(),
            Self::MarketAnomaly(payload) => payload.validate_safety(),
        }
    }

    /// Validates automatic tightening rules that require a previous payload snapshot.
    pub fn validate_safety_transition(
        &self,
        previous: Option<&Self>,
    ) -> Result<(), PayloadSafetyError> {
        self.validate_safety()?;
        let Some(previous) = previous else {
            return Ok(());
        };
        if self.factor_type() != previous.factor_type() {
            return Ok(());
        }
        match (previous, self) {
            (Self::ExecutionQuality(prev), Self::ExecutionQuality(next)) => ensure_block_monotonic(
                "block_stale_books",
                prev.block_stale_books,
                next.block_stale_books,
                next.manual_approval.is_some(),
            ),
            (Self::PortfolioRisk(prev), Self::PortfolioRisk(next)) => ensure_block_monotonic(
                "block_new_positions",
                prev.block_new_positions,
                next.block_new_positions,
                next.manual_approval.is_some(),
            ),
            (Self::ReconciliationHealth(prev), Self::ReconciliationHealth(next)) => {
                ensure_block_monotonic(
                    "block_trading",
                    prev.block_trading,
                    next.block_trading,
                    next.manual_approval.is_some(),
                )
            }
            (Self::MarketAnomaly(prev), Self::MarketAnomaly(next)) => ensure_block_monotonic(
                "block_market",
                prev.block_market,
                next.block_market,
                next.manual_approval.is_some(),
            ),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub const fn manual_approval(&self) -> Option<&ManualApproval> {
        match self {
            Self::BucketRisk(payload) => payload.manual_approval.as_ref(),
            Self::ExecutionQuality(payload) => payload.manual_approval.as_ref(),
            Self::PortfolioRisk(payload) => payload.manual_approval.as_ref(),
            Self::ReconciliationHealth(payload) => payload.manual_approval.as_ref(),
            Self::MarketAnomaly(payload) => payload.manual_approval.as_ref(),
        }
    }

    #[must_use]
    pub const fn severity(&self) -> Option<FactorSeverity> {
        match self {
            Self::ReconciliationHealth(payload) => Some(payload.severity),
            Self::MarketAnomaly(payload) => Some(payload.severity),
            _ => None,
        }
    }
}

/// Bucket-level risk tightening from historical resolution evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRiskPayload {
    pub resolution_haircut_factor: Decimal,
    pub size_multiplier: Decimal,
    pub min_edge_bps_addon: Decimal,
    pub kelly_fraction_multiplier: Decimal,
    pub max_open_positions: Option<u32>,
    pub active_config_max_open_positions: Option<u32>,
    pub manual_approval: Option<ManualApproval>,
}

impl BucketRiskPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(self.resolution_haircut_factor, "resolution_haircut_factor")?;
        ensure_multiplier(self.size_multiplier, "size_multiplier")?;
        ensure_multiplier(self.kelly_fraction_multiplier, "kelly_fraction_multiplier")?;
        ensure_non_negative(self.min_edge_bps_addon, "min_edge_bps_addon")?;
        if let (Some(limit), Some(active)) = (
            self.max_open_positions,
            self.active_config_max_open_positions,
        ) {
            if limit > active && self.manual_approval.is_none() {
                return Err(PayloadSafetyError::RiskExpandingWithoutApproval {
                    field: "max_open_positions",
                });
            }
        }
        Ok(())
    }
}

/// Execution-quality tightening from FOK fill, depth, and staleness evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionQualityPayload {
    pub fill_probability_multiplier: Decimal,
    pub size_multiplier: Decimal,
    pub slippage_bps_addon: Decimal,
    pub block_stale_books: bool,
    pub manual_approval: Option<ManualApproval>,
}

impl ExecutionQualityPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(
            self.fill_probability_multiplier,
            "fill_probability_multiplier",
        )?;
        ensure_multiplier(self.size_multiplier, "size_multiplier")?;
        ensure_non_negative(self.slippage_bps_addon, "slippage_bps_addon")
    }
}

/// Portfolio-level throttles from exposure, drawdown, and liquidity evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskPayload {
    pub daily_budget_multiplier: Decimal,
    pub kelly_fraction_multiplier: Decimal,
    pub size_multiplier: Decimal,
    pub block_new_positions: bool,
    pub manual_approval: Option<ManualApproval>,
}

impl PortfolioRiskPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(self.daily_budget_multiplier, "daily_budget_multiplier")?;
        ensure_multiplier(self.kelly_fraction_multiplier, "kelly_fraction_multiplier")?;
        ensure_multiplier(self.size_multiplier, "size_multiplier")
    }
}

/// Reconciliation health factor that may fail closed when critical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationHealthPayload {
    pub severity: FactorSeverity,
    pub block_trading: bool,
    pub size_multiplier: Decimal,
    pub manual_approval: Option<ManualApproval>,
}

impl ReconciliationHealthPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(self.size_multiplier, "size_multiplier")
    }
}

/// Market anomaly factor for market/category blocking and throttling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnomalyPayload {
    pub severity: FactorSeverity,
    pub block_market: bool,
    pub size_multiplier: Decimal,
    pub min_edge_bps_addon: Decimal,
    pub manual_approval: Option<ManualApproval>,
}

impl MarketAnomalyPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(self.size_multiplier, "size_multiplier")?;
        ensure_non_negative(self.min_edge_bps_addon, "min_edge_bps_addon")
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionQualityPayload, FactorPayload};
    use oxide_arb_error::control::PayloadSafetyError;
    use rust_decimal_macros::dec;

    #[test]
    fn rejects_risk_expanding_multiplier() {
        let payload = FactorPayload::BucketRisk(super::BucketRiskPayload {
            resolution_haircut_factor: dec!(1.1),
            size_multiplier: dec!(1),
            min_edge_bps_addon: dec!(0),
            kelly_fraction_multiplier: dec!(1),
            max_open_positions: None,
            active_config_max_open_positions: None,
            manual_approval: None,
        });

        assert_eq!(
            payload.validate_safety(),
            Err(PayloadSafetyError::MultiplierOutOfRange {
                field: "resolution_haircut_factor"
            })
        );
    }

    #[test]
    fn accepts_tightening_bucket_payload() {
        let payload = FactorPayload::BucketRisk(super::BucketRiskPayload {
            resolution_haircut_factor: dec!(0.9),
            size_multiplier: dec!(0.8),
            min_edge_bps_addon: dec!(25),
            kelly_fraction_multiplier: dec!(0.5),
            max_open_positions: Some(2),
            active_config_max_open_positions: Some(3),
            manual_approval: None,
        });

        assert!(payload.validate_safety().is_ok());
    }

    #[test]
    fn rejects_relaxing_block_flag_without_approval() {
        let previous = FactorPayload::ExecutionQuality(ExecutionQualityPayload {
            fill_probability_multiplier: dec!(1),
            size_multiplier: dec!(1),
            slippage_bps_addon: dec!(0),
            block_stale_books: true,
            manual_approval: None,
        });
        let next = FactorPayload::ExecutionQuality(ExecutionQualityPayload {
            fill_probability_multiplier: dec!(1),
            size_multiplier: dec!(1),
            slippage_bps_addon: dec!(0),
            block_stale_books: false,
            manual_approval: None,
        });

        assert_eq!(
            next.validate_safety_transition(Some(&previous)),
            Err(PayloadSafetyError::BlockFlagRelaxed {
                field: "block_stale_books"
            })
        );
    }
}
