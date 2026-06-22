//! Shadow consumption: compute what the Shadow publication *would* do versus the
//! live Published baseline, and record the delta without ever touching the real
//! order path.
//!
//! [`ShadowEvaluator`] is a pure factor-effect diff over the two compiled
//! snapshots. [`ShadowDecisionWriter`] owns a bounded channel and a background
//! drain task: under backpressure or write failure it drops the shadow record
//! and increments a metric — the live decision is never affected.

use crate::observability::metrics_hub::MetricsHub;
use chrono::Utc;
use oxide_arb_models::{
    domain::{
        control_factor::{
            AppliedControlFactor, BucketRiskDimensions, ControlFactorSnapshot,
            ExecutionQualityDimensions, bucket_resolution_trace, effective_resolution_prob,
            execution_quality_fill_trace, size_cap,
        },
        facts::NewControlFactorShadowDecision,
        opportunity::Opportunity,
    },
    enums::fact::ShadowDecisionType,
    types::{EventId, FactorPublicationId, MarketId, OpportunityId, ShadowDecisionId, Usd},
};
use oxide_arb_repository::traits::ControlFactorShadowDecisionRepository;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Default capacity of the shadow-decision channel; full channel drops records.
const SHADOW_CHANNEL_CAPACITY: usize = 4_096;

/// Summary of the factor effects a snapshot would impose on one opportunity.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FactorEffect {
    resolution_multiplier: Decimal,
    fill_multiplier: Decimal,
    size_multiplier: Decimal,
    would_reject: bool,
    applied: Vec<AppliedControlFactor>,
}

impl FactorEffect {
    const fn neutral() -> Self {
        Self {
            resolution_multiplier: Decimal::ONE,
            fill_multiplier: Decimal::ONE,
            size_multiplier: Decimal::ONE,
            would_reject: false,
            applied: Vec::new(),
        }
    }

    /// Compute the effect of `snapshot`'s factors on a single opportunity.
    fn compute(
        snapshot: &ControlFactorSnapshot,
        opp: &Opportunity,
        bucket_dims: &BucketRiskDimensions,
        eq_dims: &ExecutionQualityDimensions,
    ) -> Self {
        let Some(publication_id) = snapshot.publication_id.clone() else {
            return Self::neutral();
        };
        let mut effect = Self::neutral();

        if let Some(found) = snapshot.bucket_risk.lookup(bucket_dims) {
            effect.resolution_multiplier = found.payload.resolution_haircut_factor;
            effect.size_multiplier *= found.payload.size_multiplier;
            effect.would_reject |= found.payload.block_new_entries;
            let effective = effective_resolution_prob(
                opp.resolution_adjust,
                found.payload.resolution_haircut_factor,
            );
            effect.applied.push(bucket_resolution_trace(
                found.factor_id.clone(),
                publication_id.clone(),
                opp.resolution_adjust,
                effective,
            ));
        }

        if let Some(found) = snapshot.execution_quality.lookup(eq_dims) {
            effect.fill_multiplier = found.payload.fill_probability_multiplier;
            effect.applied.push(execution_quality_fill_trace(
                found.factor_id.clone(),
                publication_id.clone(),
                Decimal::ONE,
                found.payload.fill_probability_multiplier,
            ));
        }

        let anomaly =
            snapshot
                .market_anomalies
                .decision(&publication_id, &opp.market_id, &opp.event_id);
        if anomaly.is_blocking() {
            effect.would_reject = true;
            if let Some(source) = anomaly.source {
                effect.applied.push(source);
            }
        }

        let recon = snapshot.reconciliation_health.decision(&publication_id);
        if recon.force_maintenance_mode {
            effect.would_reject = true;
        }
        effect.size_multiplier *= recon.size_multiplier;
        if let Some(source) = recon.source {
            effect.applied.push(source);
        }

        let portfolio = snapshot
            .portfolio_risk
            .decision(&publication_id, opp.category);
        effect.size_multiplier *= portfolio.global_size_multiplier;
        if let Some(category_mult) = portfolio.category_size_multiplier {
            effect.size_multiplier *= category_mult;
        }
        if let Some(source) = portfolio.source {
            effect.applied.push(source);
        }

        effect
    }
}

/// Pure baseline-vs-shadow factor-effect diff.
pub struct ShadowEvaluator;

impl ShadowEvaluator {
    /// Build a shadow-decision record, or `None` when no Shadow publication is
    /// active (nothing to compare).
    #[must_use]
    pub fn evaluate(
        published: &ControlFactorSnapshot,
        shadow: &ControlFactorSnapshot,
        opp: &Opportunity,
        bucket_dims: &BucketRiskDimensions,
        eq_dims: &ExecutionQualityDimensions,
        baseline_size_usd: Usd,
    ) -> Option<NewShadowDecision> {
        let publication_id = shadow.publication_id.clone()?;
        let baseline = FactorEffect::compute(published, opp, bucket_dims, eq_dims);
        let candidate = FactorEffect::compute(shadow, opp, bucket_dims, eq_dims);

        let baseline_size = size_cap(baseline_size_usd, baseline.size_multiplier);
        let shadow_size = size_cap(baseline_size_usd, candidate.size_multiplier);
        let size_delta = Usd::new(shadow_size.inner() - baseline_size.inner());

        let decision_type = if candidate.would_reject && !baseline.would_reject {
            ShadowDecisionType::WouldReject
        } else if shadow_size != baseline_size {
            ShadowDecisionType::WouldSize
        } else if candidate.fill_multiplier != baseline.fill_multiplier
            || candidate.resolution_multiplier != baseline.resolution_multiplier
        {
            ShadowDecisionType::WouldScore
        } else {
            ShadowDecisionType::NoEffect
        };

        let affected: Vec<&AppliedControlFactor> = candidate.applied.iter().collect();
        let affected_factor_ids: Vec<String> = affected
            .iter()
            .map(|factor| factor.factor_id.to_string())
            .collect();

        Some(NewShadowDecision {
            publication_id,
            opportunity_id: opp.opportunity_id.clone(),
            event_id: opp.event_id.clone(),
            market_id: opp.market_id.clone(),
            decision_type,
            baseline: serde_json::json!({
                "size_usd": baseline_size.inner().to_string(),
                "resolution_multiplier": baseline.resolution_multiplier.to_string(),
                "fill_multiplier": baseline.fill_multiplier.to_string(),
                "would_reject": baseline.would_reject,
            }),
            shadow: serde_json::json!({
                "size_usd": shadow_size.inner().to_string(),
                "resolution_multiplier": candidate.resolution_multiplier.to_string(),
                "fill_multiplier": candidate.fill_multiplier.to_string(),
                "would_reject": candidate.would_reject,
            }),
            delta: serde_json::json!({
                "size_delta_usd": size_delta.inner().to_string(),
                "would_reject_delta": i8::from(candidate.would_reject) - i8::from(baseline.would_reject),
            }),
            affected_factor_ids: serde_json::json!(affected_factor_ids),
        })
    }
}

/// Owned, serializable shadow-decision payload queued for the async writer.
#[derive(Debug, Clone)]
pub struct NewShadowDecision {
    pub publication_id: FactorPublicationId,
    pub opportunity_id: OpportunityId,
    pub event_id: EventId,
    pub market_id: MarketId,
    pub decision_type: ShadowDecisionType,
    pub baseline: serde_json::Value,
    pub shadow: serde_json::Value,
    pub delta: serde_json::Value,
    pub affected_factor_ids: serde_json::Value,
}

/// Backpressure-safe writer for shadow decisions. Cloneable handle that feeds a
/// bounded channel drained by [`ShadowDecisionWriter::run`].
#[derive(Clone)]
pub struct ShadowDecisionWriter {
    tx: mpsc::Sender<NewShadowDecision>,
    metrics: Arc<MetricsHub>,
}

impl ShadowDecisionWriter {
    /// Create a writer handle and its background drain task.
    #[must_use]
    pub fn new(
        repo: Arc<dyn ControlFactorShadowDecisionRepository>,
        metrics: Arc<MetricsHub>,
    ) -> (Self, ShadowWriterTask) {
        let (tx, rx) = mpsc::channel(SHADOW_CHANNEL_CAPACITY);
        (
            Self {
                tx,
                metrics: Arc::clone(&metrics),
            },
            ShadowWriterTask { repo, rx, metrics },
        )
    }

    /// Non-blocking enqueue. Drops the record (and counts it) if the channel is
    /// full — the live order path must never block on shadow persistence.
    pub fn record(&self, decision: NewShadowDecision) {
        match self.tx.try_send(decision) {
            Ok(()) => self.metrics.control_factor_shadow_decisions.inc(),
            Err(_) => self.metrics.control_factor_shadow_dropped.inc(),
        }
    }
}

/// Background drain task that persists queued shadow decisions.
pub struct ShadowWriterTask {
    repo: Arc<dyn ControlFactorShadowDecisionRepository>,
    rx: mpsc::Receiver<NewShadowDecision>,
    metrics: Arc<MetricsHub>,
}

impl ShadowWriterTask {
    /// Drain and persist shadow decisions until shutdown.
    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                maybe = self.rx.recv() => {
                    let Some(decision) = maybe else { return };
                    self.persist(decision).await;
                }
            }
        }
    }

    async fn persist(&self, decision: NewShadowDecision) {
        let row = NewControlFactorShadowDecision {
            shadow_decision_id: ShadowDecisionId::from_v7(),
            publication_id: decision.publication_id,
            opportunity_id: decision.opportunity_id,
            event_id: decision.event_id,
            market_id: decision.market_id,
            decision_type: decision.decision_type,
            baseline_decision: decision.baseline,
            shadow_decision: decision.shadow,
            delta: decision.delta,
            affected_factor_ids: decision.affected_factor_ids,
            decided_at: Utc::now(),
        };
        if let Err(error) = self.repo.append_shadow_decision(row).await {
            self.metrics.control_factor_shadow_dropped.inc();
            tracing::warn!(%error, "shadow decision write failed — dropped");
        }
    }
}
