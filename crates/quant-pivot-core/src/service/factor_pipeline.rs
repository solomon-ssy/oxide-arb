//! Factor-plane orchestration: feature vectors → factor engine → partition →
//! persist (definitions + values) → emit.
//!
//! Consumes the accepted [`FeatureVector`]s of one online round (paired with
//! their persisted ids) and a minted `model_run_id`, builds a
//! [`FactorEngine`] from frozen config, computes the cross-sectional factor
//! batch, partitions markets by [`FactorEligibility`], persists the governed
//! definitions (idempotent) and the eligible markets' factor values, and emits
//! present-only long-format facts. Rejected markets are observable but never
//! reach persistence, facts, or the downstream model plane.
//!
//! `model_run_id` is minted up front by the online round and threaded through;
//! the `quant_model_run` row lifecycle (create / finalize) lands in 3.4, which
//! also adds the factor-value foreign key. This service is the callable unit the
//! Phase 4 / 3.4 `ModelRunner` reuses.

use crate::{
    governance::BiasTableApplicator, observability::factor_fact_writer::FactorEventWriter,
};
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, report::ReportError};
use quant_pivot_models::{
    domain::{FactorValueInfo, NewFactorValue},
    enums::quant::PublicationStatus,
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig, SmallCrossSectionPolicy},
    types::{FactorDefinitionId, FeatureVectorId, MarketId, ModelRunId},
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::{
    factors::{
        FactorEligibility, FactorEngine, FactorHistory, FactorValueInsertContext,
        MarketFactorOutcome, factor_definition_id, factor_events,
    },
    features::FeatureVector,
};
use rust_decimal::Decimal;
use std::{collections::HashMap, sync::Arc};

/// Frozen inputs for one factor-plane round.
pub struct FactorPipelineRequest<'a> {
    /// The owning online round (minted up front, threaded through the round).
    pub model_run_id: &'a ModelRunId,
    /// Accepted feature vectors (data-quality bar already passed at 3.2).
    pub vectors: &'a [FeatureVector],
    /// Persisted feature-vector ids, aligned 1:1 with `vectors`.
    pub feature_vector_ids: &'a [FeatureVectorId],
    /// Frozen factor config (enabled families, confidence floor, missing policy).
    pub factors: &'a FactorsConfig,
    /// Frozen feature config (resolves windowed factor inputs).
    pub features: &'a FeaturesConfig,
    /// Frozen domain config (category-routed domain factor registration).
    pub domain: &'a DomainConfig,
}

/// A market excluded by `RejectCandidate` (a required factor was missing or below
/// the confidence floor). Observable, but it produces no factor rows.
pub struct RejectedFactorMarket {
    /// The excluded market.
    pub market_id: MarketId,
    /// Why the market was rejected.
    pub reason: String,
}

/// Outcome of one factor-plane round.
pub struct FactorPipelineResult {
    /// Per-market factor outcomes (eligible + rejected), with transient scoring
    /// flags — the input the 3.4 model runtime consumes.
    pub outcomes: Vec<MarketFactorOutcome>,
    /// Markets excluded under `RejectCandidate` (not persisted, not emitted).
    pub rejected: Vec<RejectedFactorMarket>,
    /// Persisted factor-value rows for the eligible markets.
    pub persisted: Vec<FactorValueInfo>,
}

/// Orchestrates the online factor build loop for one round of feature vectors.
pub struct FactorPipelineService {
    factor_repo: Arc<dyn FactorRepository>,
    event_writer: Arc<FactorEventWriter>,
    bias_table: Arc<BiasTableApplicator>,
}

impl FactorPipelineService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub fn new(
        factor_repo: Arc<dyn FactorRepository>,
        event_writer: Arc<FactorEventWriter>,
        bias_table: Arc<BiasTableApplicator>,
    ) -> Self {
        Self {
            factor_repo,
            event_writer,
            bias_table,
        }
    }

    /// Run one factor round: compute the cross-sectional batch, partition by
    /// eligibility, persist definitions + eligible values, emit present-only facts.
    ///
    /// # Errors
    ///
    /// Returns an error when `vectors` / `feature_vector_ids` lengths differ, or
    /// on factor-computation, mapping, or persistence failures.
    pub async fn run(
        &self,
        request: FactorPipelineRequest<'_>,
    ) -> QuantResult<FactorPipelineResult> {
        if request.vectors.len() != request.feature_vector_ids.len() {
            return Err(ReportError::ContractViolation {
                detail: format!(
                    "factor pipeline: {} vectors but {} feature-vector ids",
                    request.vectors.len(),
                    request.feature_vector_ids.len()
                ),
            }
            .into());
        }

        // Bind the currently-activated favorite-longshot bias table (content-hash
        // verified at activation). `None` keeps `struct.favorite_longshot` inert.
        let bias_table = self.bias_table.current();
        let engine = FactorEngine::new(
            request.factors,
            request.features,
            request.domain,
            bias_table,
        );
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }

        FactorEngine::validate_batch_invariants(request.vectors)?;

        // Definitions are registered out-of-band by the explicit factor-register
        // governance action (not on this money-facing hot path). Fail closed if
        // any enabled definition is not `Published` (05.7).
        self.require_published_definitions(&engine).await?;

        // Pre-fetch the rolling history for the small-cross-section HistoricalQuantile
        // policy (a no-op empty history under the default Indeterminate policy).
        let history = self.build_history(&engine, &request).await?;

        // Factor compute is pure CPU work; run it on the blocking pool so a large
        // cross-sectional batch never stalls the async runtime. The engine moves
        // in and back out so its registry is reused for definition persistence.
        let config = request.factors.clone();
        let vectors = request.vectors.to_vec();
        let (_engine, outcomes) = tokio::task::spawn_blocking(move || {
            let outcomes = engine.compute_all_batch_with_history(&vectors, &config, &history);
            (engine, outcomes)
        })
        .await
        .map_err(|err| InfraError::BlockingTaskJoin {
            detail: err.to_string(),
        })?;
        let outcomes = outcomes?;

        // Build rows for eligible markets, tagging each factor with its source
        // feature-vector id (aligned by index), and collect rejections.
        let mut rows: Vec<NewFactorValue> = Vec::new();
        let mut rejected = Vec::new();
        for (index, outcome) in outcomes.iter().enumerate() {
            match &outcome.eligibility {
                FactorEligibility::RejectCandidate { reason }
                | FactorEligibility::NotApplicable { reason } => {
                    rejected.push(RejectedFactorMarket {
                        market_id: outcome.market_id.clone(),
                        reason: reason.clone(),
                    });
                }
                FactorEligibility::Eligible => {
                    let feature_vector_id = &request.feature_vector_ids[index];
                    let ctx = FactorValueInsertContext {
                        model_run_id: request.model_run_id,
                        feature_vector_id,
                        market_id: &outcome.market_id,
                        as_of: outcome.as_of,
                    };
                    for scored in &outcome.factors {
                        rows.push(scored.value.try_to_new(&ctx)?);
                    }
                }
            }
        }

        let persisted = self
            .factor_repo
            .create_values(rows)
            .await
            .map_err(QuantError::from)?;

        // Emit present-only facts (eligible markets only) to ClickHouse.
        let ingestion_time = Utc::now().timestamp_millis();
        self.event_writer.write_batch(factor_events(
            &outcomes,
            request.model_run_id,
            ingestion_time,
        ));

        Ok(FactorPipelineResult {
            outcomes,
            rejected,
            persisted,
        })
    }

    /// Build the rolling historical raw-value distribution for the
    /// `HistoricalQuantile` small-cross-section policy (empty under `Indeterminate`).
    async fn build_history(
        &self,
        engine: &FactorEngine,
        request: &FactorPipelineRequest<'_>,
    ) -> QuantResult<FactorHistory> {
        if request.factors.cross_section.small_cross_section_policy
            != SmallCrossSectionPolicy::HistoricalQuantile
        {
            return Ok(FactorHistory::empty());
        }
        let Some(as_of) = request.vectors.first().map(|vector| vector.as_of) else {
            return Ok(FactorHistory::empty());
        };
        let lookback = Duration::seconds(
            i64::try_from(request.factors.cross_section.historical_lookback_secs)
                .unwrap_or(i64::MAX),
        );
        let specs = engine.factor_set().definitions;
        let ids: Vec<_> = specs
            .iter()
            .map(|spec| factor_definition_id(spec.name.as_str()))
            .collect();
        let rows = self
            .factor_repo
            .recent_values(&ids, as_of - lookback, as_of)
            .await
            .map_err(QuantError::from)?;
        let mut by_definition: HashMap<FactorDefinitionId, Vec<Decimal>> = HashMap::new();
        for row in rows {
            if let Some(raw) = row.raw_value {
                by_definition
                    .entry(row.factor_definition_id)
                    .or_default()
                    .push(raw);
            }
        }
        let mut history = FactorHistory::empty();
        for spec in &specs {
            let id = factor_definition_id(spec.name.as_str());
            if let Some(values) = by_definition.remove(&id) {
                history.insert(spec.name.clone(), values);
            }
        }
        Ok(history)
    }

    /// Fail closed when any enabled definition is missing (never registered) or
    /// not `Published`. Definitions are registered out-of-band by the explicit
    /// factor-register governance action, so a fresh system with an unregistered
    /// factor set is a hard block here (never a silent pass).
    async fn require_published_definitions(&self, engine: &FactorEngine) -> QuantResult<()> {
        let specs = engine.factor_set().definitions;
        let ids: Vec<_> = specs
            .iter()
            .map(|spec| factor_definition_id(spec.name.as_str()))
            .collect();
        let rows = self
            .factor_repo
            .find_definitions_by_ids(&ids)
            .await
            .map_err(QuantError::from)?;
        let status_by_id: HashMap<FactorDefinitionId, PublicationStatus> = rows
            .into_iter()
            .map(|row| (row.factor_definition_id, row.status))
            .collect();
        let mut violations = Vec::new();
        for spec in &specs {
            let id = factor_definition_id(spec.name.as_str());
            match status_by_id.get(&id) {
                Some(PublicationStatus::Published) => {}
                Some(status) => {
                    violations.push(format!("{}={}", spec.name.as_str(), status.as_str()));
                }
                None => violations.push(format!("{}=unregistered", spec.name.as_str())),
            }
        }
        if violations.is_empty() {
            return Ok(());
        }
        Err(ReportError::ContractViolation {
            detail: format!(
                "factor pipeline blocked: enabled definitions must be Published before compute \
                 (unregistered definitions must first be registered via \
                 POST /research/factors/register, then published) ({})",
                violations.join(", ")
            ),
        }
        .into())
    }
}
