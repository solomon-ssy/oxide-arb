//! Governance services for factor transitions and publication hashing.

mod hash;

use oxide_arb_error::control::GovernanceError;
use oxide_arb_models::{
    domain::control_factor::{
        ControlFactorPublication, ControlFactorValue, FactorLifecycle, FactorPayload,
    },
    enums::control_factor::{FactorStatus, PublicationMode},
};

pub use hash::PublicationHasher;

pub struct ControlFactorGovernor;

impl ControlFactorGovernor {
    pub fn validate_factor_transition(
        factor: &ControlFactorValue,
        target: FactorStatus,
        previous_payload: Option<&FactorPayload>,
    ) -> Result<(), GovernanceError> {
        factor
            .validate_for_transition(target, previous_payload)
            .map_err(GovernanceError::from)
    }

    pub fn validate_publication(
        publication: &ControlFactorPublication,
        factors: &[ControlFactorValue],
    ) -> Result<(), GovernanceError> {
        if !publication.publication_hash.is_empty() {
            PublicationHasher::verify(publication)?;
        }
        if publication.factor_ids.is_empty() {
            return Err(GovernanceError::EmptyPublication);
        }
        if publication.effective_from >= publication.expires_at {
            return Err(GovernanceError::InvalidPublicationWindow);
        }
        if publication.factor_ids.len() != factors.len() {
            return Err(GovernanceError::FactorSetMismatch);
        }

        let (target_status, required_current) = match publication.mode {
            PublicationMode::Shadow => (FactorStatus::Shadow, FactorStatus::Candidate),
            PublicationMode::Published => (FactorStatus::Published, FactorStatus::Shadow),
        };

        for factor in factors {
            if !publication
                .factor_ids
                .iter()
                .any(|factor_id| factor_id == &factor.factor_id)
            {
                return Err(GovernanceError::FactorSetMismatch);
            }
            if factor.status != required_current {
                return Err(GovernanceError::FactorNotReadyForPublication {
                    factor_id: factor.factor_id.as_str().to_owned(),
                    mode: publication.mode.as_str().to_owned(),
                    expected: required_current.to_string(),
                    actual: factor.status.to_string(),
                });
            }
            Self::validate_factor_transition(factor, target_status, None)?;
        }
        Ok(())
    }

    /// Validates a materialization output row before persistence.
    pub fn validate_materialization_output(
        factor: &ControlFactorValue,
    ) -> Result<(), GovernanceError> {
        if !FactorLifecycle::is_materialization_output(factor.status) {
            return Err(GovernanceError::FactorValue(
                oxide_arb_error::control::FactorValueError::IllegalTransition {
                    from: factor.status.to_string(),
                    to: "materialization_output_only".to_string(),
                },
            ));
        }
        factor
            .validate_for_transition(factor.status, None)
            .map_err(GovernanceError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::ControlFactorGovernor;
    use chrono::Utc;
    use oxide_arb_models::{
        domain::control_factor::{
            BucketRiskDimensions, BucketRiskPayload, ConfidenceInterval, ControlFactorPublication,
            ControlFactorValue, DataCoverageReport, FactorDimensions, FactorEvidence,
            FactorPayload, PointInTimeInputManifest, TailRiskEvidence,
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::MarketCategory,
            control_factor::{
                ControlFactorType, FactorMaturity, FactorStatus, PublicationMode, PublicationStatus,
            },
        },
        types::{ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn candidate_factor() -> ControlFactorValue {
        ControlFactorValue {
            factor_id: ControlFactorId::new_v7(),
            factor_type: ControlFactorType::BucketRisk,
            dimensions: FactorDimensions::BucketRisk(BucketRiskDimensions {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z99,
                duration_bucket: DurationBucket::Short,
                hours_to_settlement_bucket: None,
                neg_risk: Some(false),
                fee_profile: None,
            }),
            payload: FactorPayload::BucketRisk(BucketRiskPayload {
                resolution_haircut_factor: dec!(0.9),
                size_multiplier: dec!(0.9),
                min_edge_bps_addon: dec!(0),
                block_new_entries: false,
            }),
            evidence: FactorEvidence {
                materialization_run_id: MaterializationRunId::new_v7(),
                stage_report_ids: vec![StageReportId::new_v7()],
                window_from: Utc::now() - chrono::Duration::hours(1),
                window_to: Utc::now(),
                source_delay_secs: 30,
                market_count: 1,
                event_count: 1,
                opportunity_count: 1,
                settlement_count: 0,
                sample_count: 10,
                data_coverage: DataCoverageReport {
                    expected_rows: 10,
                    observed_rows: 10,
                    missing_rows: 0,
                    coverage_ratio: Decimal::ONE,
                    insufficient_reasons: Vec::new(),
                },
                point_in_time_inputs: PointInTimeInputManifest {
                    inputs: Vec::new(),
                    production_eligible: true,
                    missing_inputs: Vec::new(),
                    fatal_errors: Vec::new(),
                    warnings: Vec::new(),
                    manifest_hash: "pit-hash".into(),
                },
                baseline_config_hash: "cfg".into(),
                code_git_sha: "sha".into(),
                dataset_hash: "dataset".into(),
                feature_schema_hash: "features".into(),
                label_schema_hash: "labels".into(),
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
                maturity: FactorMaturity::RuleSeeded,
                source_refs: Vec::new(),
                warnings: Vec::new(),
            },
            status: FactorStatus::Candidate,
            generated_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::days(7),
            owner: "mat".into(),
            schema_version: 1,
        }
    }

    #[test]
    fn shadow_publication_requires_candidate_factors() {
        let factor = candidate_factor();
        let publication = ControlFactorPublication {
            publication_id: FactorPublicationId::new_v7(),
            mode: PublicationMode::Shadow,
            factor_ids: vec![factor.factor_id.clone()],
            previous_publication_id: None,
            status: PublicationStatus::Pending,
            effective_from: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::days(1),
            approved_by: Some("op".into()),
            approval_reason: "shadow".into(),
            publication_hash: String::new(),
        };
        assert!(ControlFactorGovernor::validate_publication(&publication, &[factor]).is_ok());
    }
}
