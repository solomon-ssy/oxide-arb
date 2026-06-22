//! Immutable, compiled control-factor snapshot read on the live hot path.
//!
//! [`ControlFactorSnapshot::compile`] turns an active publication plus its
//! resolved factor values into read-optimized typed indexes. The hot path only
//! reads this struct; it never queries `ClickHouse` or Postgres. Compilation
//! validates schema version, dimension/payload consistency, conservative payload
//! safety, and per-factor TTL (critical safety factors fail closed on expiry
//! when policy requires it; non-safety factors fail neutral by being dropped).

use super::index::{
    BucketRiskIndex, ExecutionQualityIndex, MarketAnomalyIndex, PortfolioRiskState,
    ReconciliationHealthState,
};
use crate::{
    domain::control_factor::{
        ControlFactorPublication, ControlFactorValue, FactorDimensions, FactorPayload,
    },
    enums::control_factor::FactorExpiryBehavior,
    types::FactorPublicationId,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::control::SnapshotBuildError;

/// Schema version this build understands. A factor row carrying any other
/// version is rejected rather than silently mis-decoded.
pub const LIVE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Compiled, immutable view of the active control-factor publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlFactorSnapshot {
    pub publication_id: Option<FactorPublicationId>,
    pub loaded_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub source_version: String,
    pub schema_version: u32,
    pub bucket_risk: BucketRiskIndex,
    pub execution_quality: ExecutionQualityIndex,
    pub portfolio_risk: PortfolioRiskState,
    pub reconciliation_health: ReconciliationHealthState,
    pub market_anomalies: MarketAnomalyIndex,
}

impl ControlFactorSnapshot {
    /// The empty fail-neutral snapshot used when no publication is active.
    #[must_use]
    pub fn neutral(loaded_at: DateTime<Utc>) -> Self {
        Self {
            publication_id: None,
            loaded_at,
            expires_at: None,
            source_version: String::new(),
            schema_version: LIVE_SNAPSHOT_SCHEMA_VERSION,
            bucket_risk: BucketRiskIndex::default(),
            execution_quality: ExecutionQualityIndex::default(),
            portfolio_risk: PortfolioRiskState::default(),
            reconciliation_health: ReconciliationHealthState::default(),
            market_anomalies: MarketAnomalyIndex::default(),
        }
    }

    /// Compile an active publication and its resolved factors into typed indexes.
    ///
    /// `fail_closed` governs the response to an expired critical safety factor:
    /// when true, compilation aborts so Live can refuse to trade on stale safety
    /// information; when false, the expired factor is dropped (fail neutral).
    pub fn compile(
        publication: &ControlFactorPublication,
        factors: &[ControlFactorValue],
        now: DateTime<Utc>,
        fail_closed: bool,
    ) -> Result<Self, SnapshotBuildError> {
        let mut snapshot = Self::neutral(now);
        snapshot.publication_id = Some(publication.publication_id.clone());
        snapshot.expires_at = Some(publication.expires_at);
        snapshot
            .source_version
            .clone_from(&publication.publication_hash);

        for factor in factors {
            let factor_id = factor.factor_id.to_string();

            if factor.schema_version != LIVE_SNAPSHOT_SCHEMA_VERSION {
                return Err(SnapshotBuildError::SchemaMismatch {
                    factor_id,
                    actual: factor.schema_version,
                    supported: LIVE_SNAPSHOT_SCHEMA_VERSION,
                });
            }
            if factor.factor_type != factor.payload.factor_type() {
                return Err(SnapshotBuildError::DimensionPayloadMismatch { factor_id });
            }
            if factor
                .dimensions
                .factor_type()
                .is_some_and(|dim_type| dim_type != factor.factor_type)
            {
                return Err(SnapshotBuildError::DimensionPayloadMismatch { factor_id });
            }
            factor.payload.validate_safety().map_err(|source| {
                SnapshotBuildError::PayloadConstraint {
                    factor_id: factor_id.clone(),
                    source,
                }
            })?;

            if factor.expires_at <= now {
                let severity = factor.payload.severity();
                let behavior = severity.map_or_else(
                    || factor.factor_type.expiry_behavior(),
                    |severity| factor.factor_type.effective_expiry_behavior(severity),
                );
                if matches!(behavior, FactorExpiryBehavior::FailClosedIfCritical) && fail_closed {
                    return Err(SnapshotBuildError::ExpiredSafetyFactor {
                        factor_id,
                        expires_at: factor.expires_at.to_rfc3339(),
                        now: now.to_rfc3339(),
                    });
                }
                // Fail neutral: drop the expired non-critical factor.
                continue;
            }

            snapshot.route(factor)?;
        }

        Ok(snapshot)
    }

    /// Whether the active publication's TTL has elapsed.
    #[must_use]
    pub fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    /// Whether any safety factor (reconciliation maintenance / anomaly block) is
    /// present, used by callers deciding fail-closed-on-stale behavior.
    #[must_use]
    pub fn has_active_safety_factor(&self) -> bool {
        !self.reconciliation_health.is_neutral() || !self.market_anomalies.is_empty()
    }

    fn route(&mut self, factor: &ControlFactorValue) -> Result<(), SnapshotBuildError> {
        let factor_id = factor.factor_id.clone();
        match (&factor.dimensions, &factor.payload) {
            (FactorDimensions::BucketRisk(dims), FactorPayload::BucketRisk(payload)) => {
                self.bucket_risk
                    .insert(dims.clone(), factor_id, payload.clone());
            }
            (
                FactorDimensions::ExecutionQuality(dims),
                FactorPayload::ExecutionQuality(payload),
            ) => {
                self.execution_quality
                    .insert(dims.clone(), factor_id, payload.clone());
            }
            (FactorDimensions::PortfolioRisk(dims), FactorPayload::PortfolioRisk(payload)) => {
                self.portfolio_risk.absorb(&factor_id, payload);
                if let (Some(category), Some(multiplier)) =
                    (dims.category, payload.category_size_multiplier)
                {
                    self.portfolio_risk.absorb_category(category, multiplier);
                }
            }
            (
                FactorDimensions::ReconciliationHealth(_dims),
                FactorPayload::ReconciliationHealth(payload),
            ) => {
                self.reconciliation_health.absorb(&factor_id, payload);
            }
            (FactorDimensions::MarketAnomaly(dims), FactorPayload::MarketAnomaly(payload)) => {
                if payload.block_market {
                    if let Some(market_id) = &dims.market_id {
                        self.market_anomalies.insert_market(
                            market_id.clone(),
                            factor_id.clone(),
                            payload.clone(),
                        );
                    }
                }
                if payload.block_event {
                    if let Some(event_id) = &dims.event_id {
                        self.market_anomalies.insert_event(
                            event_id.clone(),
                            factor_id.clone(),
                            payload.clone(),
                        );
                    }
                }
                if payload.category_cooldown_secs.is_some() {
                    if let Some(category) = dims.category {
                        self.market_anomalies.insert_category(
                            category,
                            factor_id.clone(),
                            payload.clone(),
                        );
                    }
                }
            }
            _ => {
                return Err(SnapshotBuildError::DimensionPayloadMismatch {
                    factor_id: factor.factor_id.to_string(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlFactorSnapshot, LIVE_SNAPSHOT_SCHEMA_VERSION};
    use crate::{
        domain::control_factor::{
            AccountScope, AnomalyType, AssetScope, BucketRiskDimensions, BucketRiskPayload,
            ConfidenceInterval, ControlFactorPublication, ControlFactorValue, DataCoverageReport,
            FactorDimensions, FactorEvidence, FactorPayload, MarketAnomalyDimensions,
            MarketAnomalyPayload, MetricsFreshnessBucket, PointInTimeInputManifest,
            ReconciliationHealthDimensions, ReconciliationHealthPayload, RedeemStatusBucket,
            TailRiskEvidence,
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::MarketCategory,
            control_factor::{
                ControlFactorType, FactorMaturity, FactorSeverity, FactorStatus, PublicationMode,
                PublicationStatus, TradingHealth,
            },
        },
        types::{
            ControlFactorId, FactorPublicationId, MarketId, MaterializationRunId, StageReportId,
        },
    };
    use chrono::{Duration, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn evidence() -> FactorEvidence {
        let now = Utc::now();
        FactorEvidence {
            materialization_run_id: MaterializationRunId::from_v7(),
            stage_report_ids: vec![StageReportId::from_v7()],
            window_from: now - Duration::hours(1),
            window_to: now,
            source_delay_secs: 60,
            market_count: 1,
            event_count: 1,
            opportunity_count: 1,
            settlement_count: 1,
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
                manifest_hash: "pit".into(),
            },
            baseline_config_hash: "cfg".into(),
            code_git_sha: "sha".into(),
            dataset_hash: "ds".into(),
            feature_schema_hash: "fs".into(),
            label_schema_hash: "ls".into(),
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
        }
    }

    fn factor(
        factor_type: ControlFactorType,
        dimensions: FactorDimensions,
        payload: FactorPayload,
        expires_at: chrono::DateTime<Utc>,
    ) -> ControlFactorValue {
        ControlFactorValue {
            factor_id: ControlFactorId::from_v7(),
            factor_type,
            dimensions,
            payload,
            evidence: evidence(),
            status: FactorStatus::Published,
            generated_at: Utc::now() - Duration::hours(2),
            expires_at,
            owner: "test".into(),
            schema_version: LIVE_SNAPSHOT_SCHEMA_VERSION,
        }
    }

    fn bucket_factor(expires_at: chrono::DateTime<Utc>) -> ControlFactorValue {
        factor(
            ControlFactorType::BucketRisk,
            FactorDimensions::BucketRisk(BucketRiskDimensions::coarse(
                MarketCategory::Politics,
                PriceZone::Z97,
                DurationBucket::Short,
            )),
            FactorPayload::BucketRisk(BucketRiskPayload {
                resolution_haircut_factor: dec!(0.8),
                size_multiplier: dec!(0.5),
                min_edge_bps_addon: dec!(10),
                block_new_entries: false,
            }),
            expires_at,
        )
    }

    fn reconciliation_factor(
        force_maintenance_mode: bool,
        expires_at: chrono::DateTime<Utc>,
    ) -> ControlFactorValue {
        let severity = if force_maintenance_mode {
            FactorSeverity::Critical
        } else {
            FactorSeverity::Warning
        };
        factor(
            ControlFactorType::ReconciliationHealth,
            FactorDimensions::ReconciliationHealth(ReconciliationHealthDimensions {
                account_scope: AccountScope::Global,
                asset_scope: AssetScope::All,
                drift_severity: severity,
                metrics_freshness_bucket: MetricsFreshnessBucket::Fresh,
                redeem_status_bucket: RedeemStatusBucket::NotRedeemable,
            }),
            FactorPayload::ReconciliationHealth(ReconciliationHealthPayload {
                trading_health: TradingHealth::Degraded,
                size_multiplier: dec!(0.25),
                require_manual_ack: false,
                force_maintenance_mode,
                fail_closed_after_secs: None,
            }),
            expires_at,
        )
    }

    fn anomaly_factor(market: &str) -> ControlFactorValue {
        factor(
            ControlFactorType::MarketAnomaly,
            FactorDimensions::MarketAnomaly(MarketAnomalyDimensions {
                market_id: Some(MarketId::new(market)),
                event_id: None,
                category: None,
                anomaly_type: AnomalyType::OracleMismatch,
                severity: FactorSeverity::Critical,
            }),
            FactorPayload::MarketAnomaly(MarketAnomalyPayload {
                severity: FactorSeverity::Critical,
                block_market: true,
                block_event: false,
                category_cooldown_secs: None,
                reason_code: "oracle_mismatch".into(),
                manual_ack_required: true,
            }),
            Utc::now() + Duration::hours(1),
        )
    }

    fn publication(factors: &[ControlFactorValue]) -> ControlFactorPublication {
        ControlFactorPublication {
            publication_id: FactorPublicationId::from_v7(),
            mode: PublicationMode::Published,
            factor_ids: factors.iter().map(|f| f.factor_id.clone()).collect(),
            previous_publication_id: None,
            status: PublicationStatus::Active,
            effective_from: Utc::now() - Duration::hours(1),
            expires_at: Utc::now() + Duration::days(1),
            approved_by: Some("op".into()),
            approval_reason: "test".into(),
            publication_hash: "hash-v1".into(),
        }
    }

    #[test]
    fn neutral_snapshot_is_empty() {
        let snapshot = ControlFactorSnapshot::neutral(Utc::now());
        assert!(snapshot.publication_id.is_none());
        assert!(snapshot.bucket_risk.is_empty());
        assert!(!snapshot.has_active_safety_factor());
    }

    #[test]
    fn compile_builds_typed_indexes() {
        let factors = vec![
            bucket_factor(Utc::now() + Duration::days(1)),
            anomaly_factor("0xmkt"),
        ];
        let pubn = publication(&factors);
        let snapshot =
            ControlFactorSnapshot::compile(&pubn, &factors, Utc::now(), true).expect("compile");
        assert_eq!(snapshot.bucket_risk.len(), 1);
        assert!(
            snapshot
                .market_anomalies
                .market_block(&MarketId::new("0xmkt"))
                .is_some()
        );
        assert!(snapshot.has_active_safety_factor());
        let dims = BucketRiskDimensions::coarse(
            MarketCategory::Politics,
            PriceZone::Z97,
            DurationBucket::Short,
        );
        let found = snapshot.bucket_risk.lookup(&dims).expect("bucket factor");
        assert_eq!(found.payload.resolution_haircut_factor, dec!(0.8));
    }

    #[test]
    fn compile_rejects_schema_mismatch() {
        let mut factors = vec![bucket_factor(Utc::now() + Duration::days(1))];
        factors[0].schema_version = LIVE_SNAPSHOT_SCHEMA_VERSION + 1;
        let pubn = publication(&factors);
        let result = ControlFactorSnapshot::compile(&pubn, &factors, Utc::now(), true);
        assert!(matches!(
            result,
            Err(quant_pivot_error::control::SnapshotBuildError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn compile_fails_closed_on_expired_critical_safety_factor() {
        let factors = vec![reconciliation_factor(
            true,
            Utc::now() - Duration::minutes(1),
        )];
        let pubn = publication(&factors);
        let result = ControlFactorSnapshot::compile(&pubn, &factors, Utc::now(), true);
        assert!(matches!(
            result,
            Err(quant_pivot_error::control::SnapshotBuildError::ExpiredSafetyFactor { .. })
        ));
    }

    #[test]
    fn compile_drops_expired_non_safety_factor_when_not_fail_closed() {
        let factors = vec![reconciliation_factor(
            true,
            Utc::now() - Duration::minutes(1),
        )];
        let pubn = publication(&factors);
        // fail_closed = false → expired critical factor is dropped (fail neutral).
        let snapshot =
            ControlFactorSnapshot::compile(&pubn, &factors, Utc::now(), false).expect("compile");
        assert!(snapshot.reconciliation_health.is_neutral());
    }

    #[test]
    fn compile_reconciliation_worst_of_forces_maintenance() {
        let factors = vec![reconciliation_factor(true, Utc::now() + Duration::hours(1))];
        let pubn = publication(&factors);
        let snapshot =
            ControlFactorSnapshot::compile(&pubn, &factors, Utc::now(), true).expect("compile");
        let decision = snapshot
            .reconciliation_health
            .decision(&pubn.publication_id);
        assert!(decision.force_maintenance_mode);
        assert!(decision.source.is_some());
    }
}
