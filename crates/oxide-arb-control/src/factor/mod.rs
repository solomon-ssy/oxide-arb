//! Typed control-factor builders for Phase 5.4 statistical materialization.

mod bucket;
pub mod stats;

use chrono::{Duration, Utc};
use oxide_arb_models::{
    domain::{
        BucketRiskPayload, EvidenceMetric, ExecutionQualityPayload, MarketAnomalyPayload,
        PointInTimeInputManifest, PortfolioRiskPayload, ReconciliationHealthPayload,
        RuntimeConfigRef,
        control_factor::{
            ConfidenceInterval, ControlFactorValue, DataCoverageReport, EvidenceWarning,
            FactorBuildArtifact, FactorDimensions, FactorEvidence, FactorPayload,
            MaterializationRunManifest, StageReportBody, TailRiskEvidence,
        },
        evidence::FactorTrainingExample,
    },
    enums::control_factor::{
        ControlFactorType, FactorMaturity, FactorSeverity, FactorStatus, TradingHealth,
    },
    types::ControlFactorId,
};
use rust_decimal::Decimal;

use crate::{
    evidence::{
        execution::ExecutionEvidenceArtifact, portfolio::PortfolioRiskEvidenceArtifact,
        settlement::SettlementReconciliationEvidenceArtifact, training::TrainingExampleArtifact,
    },
    factor::{
        bucket::{BucketBuildGroup, bucket_build_groups},
        stats::{
            conservative_ratio, observed_rate_lower_bound, parent_bucket_required,
            unestimated_confidence_interval,
        },
    },
};

pub struct FactorBuildContext<'a> {
    pub manifest: &'a MaterializationRunManifest,
    pub pit_manifest: &'a PointInTimeInputManifest,
    pub stage_reports: &'a [StageReportBody],
    pub training: &'a TrainingExampleArtifact,
    pub execution: &'a ExecutionEvidenceArtifact,
    pub portfolio: &'a PortfolioRiskEvidenceArtifact,
    pub settlement: &'a SettlementReconciliationEvidenceArtifact,
}

pub struct FactorBuilderRegistry {
    builders: Vec<Box<dyn FactorBuilder>>,
}

impl Default for FactorBuilderRegistry {
    fn default() -> Self {
        Self {
            builders: vec![
                Box::new(BucketRiskBuilder),
                Box::new(ExecutionQualityBuilder),
                Box::new(PortfolioRiskBuilder),
                Box::new(ReconciliationHealthBuilder),
                Box::new(MarketAnomalyBuilder),
            ],
        }
    }
}

impl FactorBuilderRegistry {
    pub fn build(&self, context: &FactorBuildContext<'_>) -> FactorBuildArtifact {
        let mut built_factors = Vec::new();
        let mut report_only_factors = Vec::new();
        let rejected_factors = Vec::new();
        for builder in &self.builders {
            if !context
                .manifest
                .requested_factor_types
                .contains(&builder.factor_type())
            {
                continue;
            }
            for output in builder.build(context) {
                match output {
                    BuilderOutput::Draft(factor) => built_factors.push(factor),
                    BuilderOutput::ReportOnly(factor) => report_only_factors.push(factor),
                }
            }
        }
        FactorBuildArtifact {
            run_id: context.manifest.run_id.clone(),
            built_factors,
            report_only_factors,
            rejected_factors,
            warnings: Vec::new(),
        }
    }
}

trait FactorBuilder: Send + Sync {
    fn factor_type(&self) -> ControlFactorType;
    fn build(&self, context: &FactorBuildContext<'_>) -> Vec<BuilderOutput>;
}

enum BuilderOutput {
    Draft(ControlFactorValue),
    ReportOnly(ControlFactorValue),
}

struct BucketRiskBuilder;
struct ExecutionQualityBuilder;
struct PortfolioRiskBuilder;
struct ReconciliationHealthBuilder;
struct MarketAnomalyBuilder;

impl FactorBuilder for BucketRiskBuilder {
    fn factor_type(&self) -> ControlFactorType {
        ControlFactorType::BucketRisk
    }

    fn build(&self, context: &FactorBuildContext<'_>) -> Vec<BuilderOutput> {
        let thresholds = context
            .manifest
            .quality_gate_policy
            .thresholds_for(self.factor_type());
        let groups = bucket_build_groups(context.training, thresholds.min_opportunities);
        if groups.is_empty() {
            return vec![BuilderOutput::ReportOnly(factor_value(
                context,
                self.factor_type(),
                FactorDimensions::default(),
                neutral_bucket_payload(),
                FactorStatus::ReportOnly,
                unestimated_confidence_interval(),
            ))];
        }
        groups
            .into_iter()
            .map(|group| build_bucket_group(context, group))
            .collect()
    }
}

fn build_bucket_group(context: &FactorBuildContext<'_>, group: BucketBuildGroup) -> BuilderOutput {
    let wins = context.settlement.report.won_count;
    let estimate = observed_rate_lower_bound(wins, group.sample_count);
    let sample_sufficient = !parent_bucket_required(
        group.sample_count,
        context
            .manifest
            .quality_gate_policy
            .thresholds_for(ControlFactorType::BucketRisk)
            .min_opportunities,
    );
    let estimate_ok = estimate.is_ok() && sample_sufficient;
    let confidence = estimate.as_ref().map_or_else(
        |_| unestimated_confidence_interval(),
        stats::RatioEstimate::to_confidence_interval,
    );
    let payload = estimate.map_or_else(
        |_| neutral_bucket_payload(),
        |estimate| {
            FactorPayload::BucketRisk(BucketRiskPayload {
                resolution_haircut_factor: estimate.lower,
                size_multiplier: estimate.lower,
                min_edge_bps_addon: Decimal::ZERO,
                block_new_entries: estimate.lower == Decimal::ZERO,
            })
        },
    );
    let status = if estimate_ok {
        FactorStatus::Draft
    } else {
        FactorStatus::ReportOnly
    };
    let mut factor = factor_value(
        context,
        ControlFactorType::BucketRisk,
        FactorDimensions::BucketRisk(group.dimensions),
        payload,
        status,
        confidence,
    );
    if group.parent_shrunk {
        factor.evidence.warnings.push(EvidenceWarning {
            code: "bucket.parent_shrink".to_owned(),
            message:
                "bucket estimate shrunk to parent dimensions due to insufficient child samples"
                    .to_owned(),
        });
    }
    if estimate_ok {
        BuilderOutput::Draft(factor)
    } else {
        BuilderOutput::ReportOnly(factor)
    }
}

impl FactorBuilder for ExecutionQualityBuilder {
    fn factor_type(&self) -> ControlFactorType {
        ControlFactorType::ExecutionQuality
    }

    fn build(&self, context: &FactorBuildContext<'_>) -> Vec<BuilderOutput> {
        let samples = examples_for(context.training, self.factor_type());
        let fill_multiplier = conservative_ratio(
            Decimal::from(context.execution.report.true_fill_count),
            Decimal::from(samples.len().try_into().unwrap_or(u64::MAX)),
        );
        let fill_multiplier_ok = fill_multiplier.is_ok();
        let sample_count = u64::try_from(samples.len()).unwrap_or(u64::MAX);
        let confidence =
            observed_rate_lower_bound(context.execution.report.true_fill_count, sample_count)
                .ok()
                .as_ref()
                .map_or_else(
                    unestimated_confidence_interval,
                    stats::RatioEstimate::to_confidence_interval,
                );
        let payload = fill_multiplier.map_or(
            FactorPayload::ExecutionQuality(ExecutionQualityPayload {
                fill_probability_multiplier: Decimal::ONE,
                max_depth_usage_pct: None,
                slippage_bps_addon: Decimal::ZERO,
                min_liquidity_score: None,
            }),
            |fill_multiplier| {
                FactorPayload::ExecutionQuality(ExecutionQualityPayload {
                    fill_probability_multiplier: fill_multiplier,
                    max_depth_usage_pct: Some(Decimal::new(9, 1)),
                    slippage_bps_addon: Decimal::ZERO,
                    min_liquidity_score: None,
                })
            },
        );
        let status = if fill_multiplier_ok {
            FactorStatus::Draft
        } else {
            FactorStatus::ReportOnly
        };
        let factor = factor_value(
            context,
            self.factor_type(),
            first_dimensions(&samples).unwrap_or_default(),
            payload,
            status,
            confidence,
        );
        vec![if fill_multiplier_ok {
            BuilderOutput::Draft(factor)
        } else {
            BuilderOutput::ReportOnly(factor)
        }]
    }
}

impl FactorBuilder for PortfolioRiskBuilder {
    fn factor_type(&self) -> ControlFactorType {
        ControlFactorType::PortfolioRisk
    }

    fn build(&self, context: &FactorBuildContext<'_>) -> Vec<BuilderOutput> {
        let samples = examples_for(context.training, self.factor_type());
        let production_ready = context.portfolio.sequence_complete
            && context.portfolio.report.max_drawdown_pct_bps.is_available()
            && context
                .portfolio
                .report
                .settlement_backlog_max
                .is_available();
        let status = if production_ready {
            FactorStatus::Draft
        } else {
            FactorStatus::ReportOnly
        };
        let payload = FactorPayload::PortfolioRisk(PortfolioRiskPayload {
            global_size_multiplier: if production_ready {
                Decimal::new(9, 1)
            } else {
                Decimal::ONE
            },
            category_size_multiplier: None,
            daily_budget_multiplier: Decimal::ONE,
            max_open_positions: None,
            kelly_fraction_multiplier: Decimal::ONE,
        });
        let factor = factor_value(
            context,
            self.factor_type(),
            first_dimensions(&samples).unwrap_or_default(),
            payload,
            status,
            portfolio_confidence_interval(context),
        );
        vec![if production_ready {
            BuilderOutput::Draft(factor)
        } else {
            BuilderOutput::ReportOnly(factor)
        }]
    }
}

impl FactorBuilder for ReconciliationHealthBuilder {
    fn factor_type(&self) -> ControlFactorType {
        ControlFactorType::ReconciliationHealth
    }

    fn build(&self, context: &FactorBuildContext<'_>) -> Vec<BuilderOutput> {
        let samples = examples_for(context.training, self.factor_type());
        let critical = context
            .settlement
            .report
            .critical_drift_count
            .as_ref()
            .is_some_and(|count| *count > 0);
        let payload = FactorPayload::ReconciliationHealth(ReconciliationHealthPayload {
            trading_health: if critical {
                TradingHealth::Halted
            } else {
                TradingHealth::Healthy
            },
            size_multiplier: if critical {
                Decimal::ZERO
            } else {
                Decimal::ONE
            },
            require_manual_ack: critical,
            force_maintenance_mode: critical,
            fail_closed_after_secs: critical.then_some(3_600),
        });
        vec![BuilderOutput::Draft(factor_value(
            context,
            self.factor_type(),
            first_dimensions(&samples).unwrap_or_default(),
            payload,
            FactorStatus::Draft,
            unestimated_confidence_interval(),
        ))]
    }
}

impl FactorBuilder for MarketAnomalyBuilder {
    fn factor_type(&self) -> ControlFactorType {
        ControlFactorType::MarketAnomaly
    }

    fn build(&self, context: &FactorBuildContext<'_>) -> Vec<BuilderOutput> {
        let samples = examples_for(context.training, self.factor_type());
        let payload = FactorPayload::MarketAnomaly(MarketAnomalyPayload {
            severity: FactorSeverity::Warning,
            block_market: false,
            block_event: false,
            category_cooldown_secs: None,
            reason_code: "market_anomaly.report_only_no_incident".to_owned(),
            manual_ack_required: false,
        });
        vec![BuilderOutput::ReportOnly(factor_value(
            context,
            self.factor_type(),
            first_dimensions(&samples).unwrap_or_default(),
            payload,
            FactorStatus::ReportOnly,
            unestimated_confidence_interval(),
        ))]
    }
}

fn examples_for(
    training: &TrainingExampleArtifact,
    factor_type: ControlFactorType,
) -> Vec<&FactorTrainingExample> {
    training
        .examples
        .iter()
        .filter(|example| example.factor_type == factor_type)
        .collect()
}

fn first_dimensions(examples: &[&FactorTrainingExample]) -> Option<FactorDimensions> {
    examples.first().map(|example| example.entity_key.clone())
}

fn portfolio_confidence_interval(context: &FactorBuildContext<'_>) -> ConfidenceInterval {
    let EvidenceMetric::Available {
        value: drawdown_bps,
    } = &context.portfolio.report.max_drawdown_pct_bps
    else {
        return unestimated_confidence_interval();
    };
    let drawdown = Decimal::from(*drawdown_bps) / Decimal::from(10_000);
    ConfidenceInterval {
        lower: Decimal::ZERO,
        point_estimate: Decimal::ONE - drawdown,
        upper: Decimal::ONE,
        confidence_level: Decimal::new(95, 2),
    }
}

const fn neutral_bucket_payload() -> FactorPayload {
    FactorPayload::BucketRisk(BucketRiskPayload {
        resolution_haircut_factor: Decimal::ONE,
        size_multiplier: Decimal::ONE,
        min_edge_bps_addon: Decimal::ZERO,
        block_new_entries: false,
    })
}

fn factor_value(
    context: &FactorBuildContext<'_>,
    factor_type: ControlFactorType,
    dimensions: FactorDimensions,
    payload: FactorPayload,
    status: FactorStatus,
    confidence: ConfidenceInterval,
) -> ControlFactorValue {
    let thresholds = context
        .manifest
        .quality_gate_policy
        .thresholds_for(factor_type);
    ControlFactorValue {
        factor_id: ControlFactorId::new_v7(),
        factor_type,
        dimensions,
        payload,
        evidence: factor_evidence(context, factor_type, confidence),
        status,
        generated_at: Utc::now(),
        expires_at: Utc::now()
            + Duration::seconds(i64::try_from(thresholds.default_ttl_secs).unwrap_or(i64::MAX)),
        owner: context.manifest.created_by.clone(),
        schema_version: 1,
    }
}

fn factor_evidence(
    context: &FactorBuildContext<'_>,
    factor_type: ControlFactorType,
    confidence: ConfidenceInterval,
) -> FactorEvidence {
    let examples = examples_for(context.training, factor_type);
    let market_count = examples
        .iter()
        .map(|example| example.market_id.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let opportunity_count = u32::try_from(examples.len()).unwrap_or(u32::MAX);
    let sample_count = opportunity_count;
    let settlement_count =
        u32::try_from(context.settlement.report.settled_trade_count).unwrap_or(u32::MAX);
    FactorEvidence {
        materialization_run_id: context.manifest.run_id.clone(),
        stage_report_ids: context
            .stage_reports
            .iter()
            .map(|stage| stage.stage_report_id.clone())
            .collect(),
        window_from: context.manifest.window.from,
        window_to: context.manifest.window.to,
        source_delay_secs: context.manifest.source_delay_secs,
        market_count: u32::try_from(market_count).unwrap_or(u32::MAX),
        event_count: u32::try_from(context.training.report.entity_count).unwrap_or(u32::MAX),
        opportunity_count,
        settlement_count,
        sample_count,
        data_coverage: DataCoverageReport {
            expected_rows: context.training.report.example_count,
            observed_rows: context.training.report.example_count,
            missing_rows: 0,
            coverage_ratio: Decimal::ONE,
            insufficient_reasons: Vec::new(),
        },
        point_in_time_inputs: context.pit_manifest.clone(),
        baseline_config_hash: config_hash(context),
        code_git_sha: context.manifest.code_git_sha.clone(),
        dataset_hash: context.training.report.dataset_hash.clone(),
        feature_schema_hash: context.training.report.feature_schema_hash.clone(),
        label_schema_hash: context.training.report.label_schema_hash.clone(),
        query_fingerprint: context
            .training
            .report
            .query_fingerprints
            .first()
            .map_or_else(|| "none".to_owned(), |fingerprint| fingerprint.0.clone()),
        confidence_interval: confidence,
        tail_risk: tail_risk_from_portfolio(context),
        maturity: FactorMaturity::StatisticallyMaterialized,
        source_refs: examples
            .first()
            .map_or_else(Vec::new, |example| vec![example.source_refs.clone()]),
        warnings: Vec::new(),
    }
}

fn tail_risk_from_portfolio(context: &FactorBuildContext<'_>) -> TailRiskEvidence {
    let drawdown_bps = context
        .portfolio
        .report
        .max_drawdown_pct_bps
        .as_ref()
        .copied()
        .unwrap_or(0);
    let drawdown = Decimal::from(drawdown_bps);
    let peak_loss = portfolio_metric_usd(&context.portfolio.report.peak_potential_loss_usd);
    TailRiskEvidence {
        p95_loss: drawdown.max(peak_loss),
        p99_loss: drawdown.saturating_add(peak_loss / Decimal::from(2)),
        max_loss: drawdown.max(peak_loss),
        expected_shortfall: drawdown,
    }
}

fn config_hash(context: &FactorBuildContext<'_>) -> String {
    match &context.manifest.runtime_config_ref {
        RuntimeConfigRef::Version { config_hash, .. } | RuntimeConfigRef::Hash { config_hash } => {
            config_hash.clone()
        }
        RuntimeConfigRef::ActiveAt { .. } => "active_at".to_owned(),
    }
}

fn portfolio_metric_usd(metric: &EvidenceMetric<String>) -> Decimal {
    match metric {
        EvidenceMetric::Available { value } => value.parse().unwrap_or(Decimal::ZERO),
        EvidenceMetric::Unavailable { .. } => Decimal::ZERO,
    }
}

trait EvidenceMetricExt<T> {
    fn as_ref(&self) -> Option<&T>;
}

impl<T> EvidenceMetricExt<T> for EvidenceMetric<T> {
    fn as_ref(&self) -> Option<&T> {
        match self {
            Self::Available { value } => Some(value),
            Self::Unavailable { .. } => None,
        }
    }
}
