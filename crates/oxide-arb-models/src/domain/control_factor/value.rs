//! Governed control-factor value and transition validation.

use super::{evidence::FactorEvidence, lifecycle::FactorLifecycle, payload::FactorPayload};
use crate::{
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
        control_factor::{ControlFactorType, FactorStatus},
    },
    types::{ControlFactorId, EventId, MarketId, TokenId},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::control::FactorValueError;
use serde::{Deserialize, Serialize};

/// Typed dimensional key describing where a factor applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FactorDimensions {
    pub market_id: Option<MarketId>,
    pub event_id: Option<EventId>,
    pub token_id: Option<TokenId>,
    pub category: Option<MarketCategory>,
    pub price_zone: Option<PriceZone>,
    pub duration_bucket: Option<DurationBucket>,
}

/// Governed factor artifact. This is the canonical business representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFactorValue {
    pub factor_id: ControlFactorId,
    pub factor_type: ControlFactorType,
    pub dimensions: FactorDimensions,
    pub payload: FactorPayload,
    pub evidence: FactorEvidence,
    pub status: FactorStatus,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub owner: String,
    pub schema_version: u32,
}

impl ControlFactorValue {
    pub fn validate_for_transition(
        &self,
        target: FactorStatus,
        previous_payload: Option<&FactorPayload>,
    ) -> Result<(), FactorValueError> {
        FactorLifecycle::assert_transition(self.status, target)?;
        FactorLifecycle::assert_not_report_only_promotion(self.status, target)?;

        if self.factor_type != self.payload.factor_type() {
            return Err(FactorValueError::PayloadTypeMismatch {
                factor_type: self.factor_type.to_string(),
                payload_type: self.payload.factor_type().to_string(),
            });
        }
        if self.generated_at >= self.expires_at {
            return Err(FactorValueError::InvalidExpiry);
        }

        self.payload
            .validate_safety_transition(previous_payload)
            .map_err(FactorValueError::from)?;

        if FactorLifecycle::requires_governed_evidence(target)
            && !self.evidence.is_sufficient_for_candidate()
        {
            return Err(FactorValueError::InsufficientEvidence);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlFactorValue, FactorDimensions};
    use crate::{
        domain::control_factor::{
            ConfidenceInterval, DataCoverageReport, FactorEvidence, FactorPayload,
            PointInTimeInputManifest, TailRiskEvidence,
        },
        enums::control_factor::{ControlFactorType, FactorStatus},
        types::{ControlFactorId, MaterializationRunId, RuntimeConfigVersionId, StageReportId},
    };
    use chrono::Utc;
    use oxide_arb_error::control::FactorValueError;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn minimal_factor(status: FactorStatus) -> ControlFactorValue {
        ControlFactorValue {
            factor_id: ControlFactorId::new_v7(),
            factor_type: ControlFactorType::BucketRisk,
            dimensions: FactorDimensions::default(),
            payload: FactorPayload::BucketRisk(crate::domain::control_factor::BucketRiskPayload {
                resolution_haircut_factor: dec!(0.9),
                size_multiplier: dec!(0.9),
                min_edge_bps_addon: dec!(0),
                kelly_fraction_multiplier: dec!(1),
                max_open_positions: None,
                active_config_max_open_positions: None,
                manual_approval: None,
            }),
            evidence: FactorEvidence {
                materialization_run_id: MaterializationRunId::new_v7(),
                stage_report_ids: vec![StageReportId::new_v7()],
                window_from: Utc::now() - chrono::Duration::hours(1),
                window_to: Utc::now(),
                source_delay_secs: 60,
                market_count: 1,
                event_count: 1,
                opportunity_count: 1,
                settlement_count: 0,
                sample_count: 1,
                data_coverage: DataCoverageReport {
                    expected_rows: 1,
                    observed_rows: 1,
                    missing_rows: 0,
                    coverage_ratio: Decimal::ONE,
                    insufficient_reasons: Vec::new(),
                },
                point_in_time_inputs: PointInTimeInputManifest {
                    market_metadata_version: "v1".into(),
                    token_mapping_version: "v1".into(),
                    fee_schedule_version: "v1".into(),
                    calibration_snapshot_version: "v1".into(),
                    runtime_config_version_id: RuntimeConfigVersionId::new("cfg-1"),
                    risk_state_snapshot_version: "v1".into(),
                    balance_snapshot_version: "v1".into(),
                    settlement_truth_version: "v1".into(),
                },
                baseline_config_hash: "hash".into(),
                code_git_sha: "sha".into(),
                query_fingerprint: "fp".into(),
                confidence_interval: ConfidenceInterval {
                    lower: dec!(0),
                    point_estimate: dec!(0),
                    upper: dec!(0),
                    confidence_level: dec!(0.95),
                },
                tail_risk: TailRiskEvidence {
                    p95_loss: dec!(0),
                    p99_loss: dec!(0),
                    max_loss: dec!(0),
                    expected_shortfall: dec!(0),
                },
                warnings: Vec::new(),
            },
            status,
            generated_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::days(1),
            owner: "test".into(),
            schema_version: 1,
        }
    }

    #[test]
    fn report_only_cannot_become_candidate() {
        let factor = minimal_factor(FactorStatus::ReportOnly);
        assert!(matches!(
            factor.validate_for_transition(FactorStatus::Candidate, None),
            Err(FactorValueError::IllegalTransition { .. })
        ));
    }

    #[test]
    fn candidate_requires_sufficient_evidence() {
        let mut factor = minimal_factor(FactorStatus::Draft);
        factor.evidence.sample_count = 0;
        assert!(matches!(
            factor.validate_for_transition(FactorStatus::Candidate, None),
            Err(FactorValueError::InsufficientEvidence)
        ));
    }
}
