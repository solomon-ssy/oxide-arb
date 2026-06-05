//! Publication assembly and governance gates (rollback target, risk expansion).

use chrono::{DateTime, Utc};
use oxide_arb_error::control::GovernanceError;
use oxide_arb_models::{
    domain::control_factor::{
        ControlFactorPublication, ControlFactorValue, NewControlFactorPublication,
    },
    enums::control_factor::{PublicationMode, PublicationStatus},
    types::{ControlFactorId, FactorPublicationId},
};

use crate::governance::PublicationHasher;

/// Inputs for assembling a sealed publication insert payload.
pub struct PublicationDraft {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub factor_ids: Vec<ControlFactorId>,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: String,
    pub approval_reason: String,
    pub idempotency_key: String,
}

/// Stateless publication assembly + governance gate helpers.
pub struct PublicationManager;

impl PublicationManager {
    /// Builds a sealed publication insert payload (status `Pending`, hash sealed).
    pub fn seal(draft: PublicationDraft) -> Result<NewControlFactorPublication, GovernanceError> {
        let mut domain = ControlFactorPublication {
            publication_id: draft.publication_id,
            mode: draft.mode,
            factor_ids: draft.factor_ids,
            previous_publication_id: draft.previous_publication_id,
            status: PublicationStatus::Pending,
            effective_from: draft.effective_from,
            expires_at: draft.expires_at,
            approved_by: Some(draft.approved_by),
            approval_reason: draft.approval_reason,
            publication_hash: String::new(),
        };
        PublicationHasher::seal(&mut domain)?;
        Ok(NewControlFactorPublication {
            publication_id: domain.publication_id,
            mode: domain.mode,
            factor_ids: domain.factor_ids,
            previous_publication_id: domain.previous_publication_id,
            status: domain.status,
            effective_from: domain.effective_from,
            expires_at: domain.expires_at,
            approved_by: domain.approved_by,
            approval_reason: domain.approval_reason,
            idempotency_key: draft.idempotency_key,
            publication_hash: domain.publication_hash,
        })
    }

    /// `RollbackGate`: a `Published` publication that supersedes a live one must
    /// point at a known-good rollback target. Genesis (no active `Published`) is
    /// allowed without a target.
    pub const fn check_rollback_target(
        mode: PublicationMode,
        previous_publication_id: Option<&FactorPublicationId>,
        has_active: bool,
    ) -> Result<(), GovernanceError> {
        if matches!(mode, PublicationMode::Published)
            && has_active
            && previous_publication_id.is_none()
        {
            return Err(GovernanceError::RollbackTargetMissing);
        }
        Ok(())
    }

    /// Risk-expansion gate: if any new factor relaxes the matching active factor
    /// (same dimensions) without explicit approval, reject. Only conservative
    /// (risk-tightening) publications activate automatically.
    pub fn check_risk_expansion(
        new_factors: &[ControlFactorValue],
        active_factors: &[ControlFactorValue],
        approved: bool,
    ) -> Result<(), GovernanceError> {
        if approved {
            return Ok(());
        }
        for new in new_factors {
            for prior in active_factors {
                if new.dimensions == prior.dimensions && new.payload.relaxes(&prior.payload) {
                    return Err(GovernanceError::RiskExpansionNotApproved);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PublicationManager;
    use chrono::Utc;
    use oxide_arb_error::control::GovernanceError;
    use oxide_arb_models::{
        domain::control_factor::{
            BucketRiskDimensions, BucketRiskPayload, ConfidenceInterval, ControlFactorValue,
            DataCoverageReport, FactorDimensions, FactorEvidence, FactorPayload,
            PointInTimeInputManifest, TailRiskEvidence,
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::MarketCategory,
            control_factor::{ControlFactorType, FactorMaturity, FactorStatus, PublicationMode},
        },
        types::{ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn bucket_factor(size_multiplier: Decimal) -> ControlFactorValue {
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
                size_multiplier,
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
                    manifest_hash: "pit".into(),
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
                maturity: FactorMaturity::StatisticallyMaterialized,
                source_refs: Vec::new(),
                warnings: Vec::new(),
            },
            status: FactorStatus::Shadow,
            generated_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::days(1),
            owner: "test".into(),
            schema_version: 1,
        }
    }

    #[test]
    fn rollback_gate_requires_target_for_live_published() {
        let previous = FactorPublicationId::new_v7();
        // Published superseding a live publication without a target is rejected.
        assert!(matches!(
            PublicationManager::check_rollback_target(PublicationMode::Published, None, true),
            Err(GovernanceError::RollbackTargetMissing)
        ));
        // With a known-good target it is allowed.
        assert!(
            PublicationManager::check_rollback_target(
                PublicationMode::Published,
                Some(&previous),
                true
            )
            .is_ok()
        );
        // Genesis (no active Published) is allowed without a target.
        assert!(
            PublicationManager::check_rollback_target(PublicationMode::Published, None, false)
                .is_ok()
        );
        // Shadow never requires a rollback target.
        assert!(
            PublicationManager::check_rollback_target(PublicationMode::Shadow, None, true).is_ok()
        );
    }

    #[test]
    fn risk_expansion_gate_blocks_unapproved_relaxation() {
        let active = vec![bucket_factor(dec!(0.5))];
        let relaxed = vec![bucket_factor(dec!(0.9))]; // higher size multiplier = more permissive

        assert!(matches!(
            PublicationManager::check_risk_expansion(&relaxed, &active, false),
            Err(GovernanceError::RiskExpansionNotApproved)
        ));
        // Explicit approval allows it.
        assert!(PublicationManager::check_risk_expansion(&relaxed, &active, true).is_ok());
        // A tighter publication needs no approval.
        let tighter = vec![bucket_factor(dec!(0.3))];
        assert!(PublicationManager::check_risk_expansion(&tighter, &active, false).is_ok());
    }
}
