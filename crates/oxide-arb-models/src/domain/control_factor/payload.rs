//! Typed control-factor payloads and safety validation.

use super::safety::{ensure_multiplier, ensure_non_negative};
use crate::enums::control_factor::{ControlFactorType, FactorSeverity, TradingHealth};
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

    #[must_use]
    pub const fn severity(&self) -> Option<FactorSeverity> {
        match self {
            Self::ReconciliationHealth(payload) => {
                if payload.force_maintenance_mode {
                    Some(FactorSeverity::Critical)
                } else {
                    Some(FactorSeverity::Warning)
                }
            }
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
    pub block_new_entries: bool,
}

impl BucketRiskPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(self.resolution_haircut_factor, "resolution_haircut_factor")?;
        ensure_multiplier(self.size_multiplier, "size_multiplier")?;
        ensure_non_negative(self.min_edge_bps_addon, "min_edge_bps_addon")
    }
}

/// Execution-quality tightening from FOK fill, depth, and staleness evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionQualityPayload {
    pub fill_probability_multiplier: Decimal,
    pub max_depth_usage_pct: Option<Decimal>,
    pub slippage_bps_addon: Decimal,
    pub min_liquidity_score: Option<Decimal>,
}

impl ExecutionQualityPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(
            self.fill_probability_multiplier,
            "fill_probability_multiplier",
        )?;
        if let Some(max_depth_usage_pct) = self.max_depth_usage_pct {
            ensure_multiplier(max_depth_usage_pct, "max_depth_usage_pct")?;
        }
        if let Some(min_liquidity_score) = self.min_liquidity_score {
            ensure_multiplier(min_liquidity_score, "min_liquidity_score")?;
        }
        ensure_non_negative(self.slippage_bps_addon, "slippage_bps_addon")
    }
}

/// Portfolio-level throttles from exposure, drawdown, and liquidity evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskPayload {
    pub global_size_multiplier: Decimal,
    pub category_size_multiplier: Option<Decimal>,
    pub daily_budget_multiplier: Decimal,
    pub max_open_positions: Option<u32>,
    pub kelly_fraction_multiplier: Decimal,
}

impl PortfolioRiskPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        ensure_multiplier(self.global_size_multiplier, "global_size_multiplier")?;
        if let Some(category_size_multiplier) = self.category_size_multiplier {
            ensure_multiplier(category_size_multiplier, "category_size_multiplier")?;
        }
        ensure_multiplier(self.daily_budget_multiplier, "daily_budget_multiplier")?;
        ensure_multiplier(self.kelly_fraction_multiplier, "kelly_fraction_multiplier")
    }
}

/// Reconciliation health factor that may fail closed when critical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationHealthPayload {
    pub trading_health: TradingHealth,
    pub size_multiplier: Decimal,
    pub require_manual_ack: bool,
    pub force_maintenance_mode: bool,
    pub fail_closed_after_secs: Option<u64>,
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
    pub block_event: bool,
    pub category_cooldown_secs: Option<u64>,
    pub reason_code: String,
    pub manual_ack_required: bool,
}

impl MarketAnomalyPayload {
    fn validate_safety(&self) -> Result<(), PayloadSafetyError> {
        if self.reason_code.trim().is_empty() {
            return Err(PayloadSafetyError::RiskExpandingWithoutApproval {
                field: "reason_code",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FactorPayload;
    use oxide_arb_error::control::PayloadSafetyError;
    use rust_decimal_macros::dec;

    #[test]
    fn rejects_risk_expanding_multiplier() {
        let payload = FactorPayload::BucketRisk(super::BucketRiskPayload {
            resolution_haircut_factor: dec!(1.1),
            size_multiplier: dec!(1),
            min_edge_bps_addon: dec!(0),
            block_new_entries: false,
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
            block_new_entries: true,
        });

        assert!(payload.validate_safety().is_ok());
    }
}
