//! Governed control-factor value, typed dimensions, and transition validation.

use super::{evidence::FactorEvidence, lifecycle::FactorLifecycle, payload::FactorPayload};
use crate::{
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{MarketCategory, StalenessLevel},
        control_factor::{ControlFactorType, FactorSeverity, FactorStatus},
    },
    types::{ControlFactorId, EventId, MarketId},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::control::FactorValueError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Typed dimensional key describing where a factor applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "factor_type", content = "dimensions", rename_all = "snake_case")]
pub enum FactorDimensions {
    BucketRisk(BucketRiskDimensions),
    ExecutionQuality(ExecutionQualityDimensions),
    PortfolioRisk(PortfolioRiskDimensions),
    ReconciliationHealth(ReconciliationHealthDimensions),
    MarketAnomaly(MarketAnomalyDimensions),
    #[default]
    Unspecified,
}

impl FactorDimensions {
    #[must_use]
    pub const fn factor_type(&self) -> Option<ControlFactorType> {
        match self {
            Self::BucketRisk(_) => Some(ControlFactorType::BucketRisk),
            Self::ExecutionQuality(_) => Some(ControlFactorType::ExecutionQuality),
            Self::PortfolioRisk(_) => Some(ControlFactorType::PortfolioRisk),
            Self::ReconciliationHealth(_) => Some(ControlFactorType::ReconciliationHealth),
            Self::MarketAnomaly(_) => Some(ControlFactorType::MarketAnomaly),
            Self::Unspecified => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketRiskDimensions {
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub hours_to_settlement_bucket: Option<TimeToSettlementBucket>,
    pub neg_risk: Option<bool>,
    pub fee_profile: Option<FeeProfileBucket>,
}

impl BucketRiskDimensions {
    /// Coarse live-lookup key built from an opportunity's endgame metadata.
    ///
    /// Optional dimensions are left unset; [`super::BucketRiskIndex::lookup`]
    /// relaxes them so a coarse factor still matches.
    #[must_use]
    pub const fn coarse(
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Self {
        Self {
            category,
            price_zone,
            duration_bucket,
            hours_to_settlement_bucket: None,
            neg_risk: None,
            fee_profile: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionQualityDimensions {
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub spread_bucket: SpreadBucket,
    pub depth_bucket: DepthBucket,
    pub book_age_bucket: BookAgeBucket,
    pub latency_bucket: LatencyBucket,
    pub staleness_level: StalenessLevel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskDimensions {
    pub portfolio_regime: PortfolioRegime,
    pub category: Option<MarketCategory>,
    pub open_position_bucket: CountBucket,
    pub potential_loss_bucket: UsdExposureBucket,
    pub drawdown_bucket: DrawdownBucket,
    pub settlement_backlog_bucket: CountBucket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationHealthDimensions {
    pub account_scope: AccountScope,
    pub asset_scope: AssetScope,
    pub drift_severity: FactorSeverity,
    pub metrics_freshness_bucket: MetricsFreshnessBucket,
    pub redeem_status_bucket: RedeemStatusBucket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnomalyDimensions {
    pub market_id: Option<MarketId>,
    pub event_id: Option<EventId>,
    pub category: Option<MarketCategory>,
    pub anomaly_type: AnomalyType,
    pub severity: FactorSeverity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeToSettlementBucket {
    UnderOneHour,
    OneToSixHours,
    SixToTwentyFourHours,
    OverTwentyFourHours,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeProfileBucket {
    Zero,
    Low,
    Standard,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpreadBucket {
    Tight,
    Normal,
    Wide,
    VeryWide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepthBucket {
    Thin,
    Normal,
    Deep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookAgeBucket {
    Fresh,
    Recent,
    Stale,
    VeryStale,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyBucket {
    Named(String),
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioRegime {
    Normal,
    CapitalPressure,
    Drawdown,
    SettlementBacklog,
    Incident,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CountBucket {
    Zero,
    One,
    TwoToFive,
    SixToTen,
    OverTen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsdExposureBucket {
    None,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrawdownBucket {
    None,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "scope", content = "value")]
pub enum AccountScope {
    Global,
    Holder(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "scope", content = "value")]
pub enum AssetScope {
    All,
    Collateral,
    ConditionalTokens,
    Market(MarketId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsFreshnessBucket {
    Fresh,
    Delayed,
    Stale,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedeemStatusBucket {
    NotRedeemable,
    Redeemable,
    Delayed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyType {
    PriceReversal,
    AbnormalBook,
    OracleMismatch,
    SettlementDelay,
    ManualIncident,
    CategorySpike,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecimalRange {
    pub min: Decimal,
    pub max: Decimal,
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
        _previous_payload: Option<&FactorPayload>,
    ) -> Result<(), FactorValueError> {
        FactorLifecycle::assert_transition(self.status, target)?;
        FactorLifecycle::assert_not_report_only_promotion(self.status, target)?;

        if self.factor_type != self.payload.factor_type() {
            return Err(FactorValueError::PayloadTypeMismatch {
                factor_type: self.factor_type.to_string(),
                payload_type: self.payload.factor_type().to_string(),
            });
        }
        if self
            .dimensions
            .factor_type()
            .is_some_and(|factor_type| factor_type != self.factor_type)
        {
            return Err(FactorValueError::PayloadTypeMismatch {
                factor_type: self.factor_type.to_string(),
                payload_type: self.dimensions.factor_type().map_or_else(
                    || "unspecified".to_owned(),
                    |factor_type| factor_type.to_string(),
                ),
            });
        }
        if self.generated_at >= self.expires_at {
            return Err(FactorValueError::InvalidExpiry);
        }

        self.payload
            .validate_safety()
            .map_err(FactorValueError::from)?;

        if FactorLifecycle::requires_governed_evidence(target)
            && !self.evidence.is_sufficient_for_candidate()
        {
            return Err(FactorValueError::InsufficientEvidence);
        }

        Ok(())
    }

    /// Validates a freshly materialized row before it is persisted as a draft output.
    ///
    /// Materialization may only emit `Draft` / `Rejected` / `ReportOnly`; promotion
    /// into the governed funnel happens later through the registry service.
    pub fn validate_as_materialization_output(&self) -> Result<(), FactorValueError> {
        if !FactorLifecycle::is_materialization_output(self.status) {
            return Err(FactorValueError::IllegalTransition {
                from: self.status.to_string(),
                to: "materialization_output_only".to_owned(),
            });
        }
        self.validate_for_transition(self.status, None)
    }
}

#[cfg(test)]
mod tests {
    use super::{BucketRiskDimensions, ControlFactorValue, FactorDimensions};
    use crate::{
        domain::{
            BucketRiskPayload,
            control_factor::{
                ConfidenceInterval, DataCoverageReport, FactorEvidence, FactorPayload,
                PointInTimeInputManifest, TailRiskEvidence, TimeToSettlementBucket,
            },
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::MarketCategory,
            control_factor::{ControlFactorType, FactorMaturity, FactorStatus},
        },
        types::{ControlFactorId, MaterializationRunId, StageReportId},
    };
    use chrono::Utc;
    use oxide_arb_error::control::FactorValueError;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn minimal_factor(status: FactorStatus) -> ControlFactorValue {
        ControlFactorValue {
            factor_id: ControlFactorId::from_v7(),
            factor_type: ControlFactorType::BucketRisk,
            dimensions: FactorDimensions::BucketRisk(BucketRiskDimensions {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z99,
                duration_bucket: DurationBucket::Short,
                hours_to_settlement_bucket: Some(TimeToSettlementBucket::UnderOneHour),
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
                materialization_run_id: MaterializationRunId::from_v7(),
                stage_report_ids: vec![StageReportId::from_v7()],
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
                    inputs: Vec::new(),
                    production_eligible: true,
                    missing_inputs: Vec::new(),
                    fatal_errors: Vec::new(),
                    warnings: Vec::new(),
                    manifest_hash: "pit-hash".into(),
                },
                baseline_config_hash: "hash".into(),
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

    #[test]
    fn materialization_output_accepts_run_writable_statuses() {
        for status in [
            FactorStatus::Draft,
            FactorStatus::Candidate,
            FactorStatus::Rejected,
            FactorStatus::ReportOnly,
        ] {
            let factor = minimal_factor(status);
            assert!(
                factor.validate_as_materialization_output().is_ok(),
                "status {status} should be a valid materialization output"
            );
        }
    }

    #[test]
    fn materialization_output_rejects_governed_statuses() {
        let factor = minimal_factor(FactorStatus::Shadow);
        assert!(matches!(
            factor.validate_as_materialization_output(),
            Err(FactorValueError::IllegalTransition { .. })
        ));
    }

    #[test]
    fn shadow_publication_requires_candidate_members() {
        use crate::{
            domain::control_factor::ControlFactorPublication,
            enums::control_factor::{PublicationMode, PublicationStatus},
            types::FactorPublicationId,
        };

        let factor = minimal_factor(FactorStatus::Candidate);
        let publication = ControlFactorPublication {
            publication_id: FactorPublicationId::from_v7(),
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
        assert!(publication.validate_for_activation(&[factor]).is_ok());

        // A draft member cannot back a Shadow publication.
        let draft = minimal_factor(FactorStatus::Draft);
        let publication = ControlFactorPublication {
            factor_ids: vec![draft.factor_id.clone()],
            ..publication
        };
        assert!(publication.validate_for_activation(&[draft]).is_err());
    }
}
