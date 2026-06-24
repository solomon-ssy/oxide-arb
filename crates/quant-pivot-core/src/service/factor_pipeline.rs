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

use crate::observability::factor_fact_writer::FactorEventWriter;
use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{FactorValueInfo, NewFactorValue},
    runtime_config::{FactorsConfig, FeaturesConfig},
    types::{FeatureVectorId, MarketId, ModelRunId},
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::{
    factors::{
        FactorEligibility, FactorEngine, FactorValueInsertContext, MarketFactorOutcome,
        factor_events,
    },
    features::FeatureVector,
};
use std::sync::Arc;

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
}

impl FactorPipelineService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub fn new(
        factor_repo: Arc<dyn FactorRepository>,
        event_writer: Arc<FactorEventWriter>,
    ) -> Self {
        Self {
            factor_repo,
            event_writer,
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
            return Err(QuantError::Internal(format!(
                "factor pipeline: {} vectors but {} feature-vector ids",
                request.vectors.len(),
                request.feature_vector_ids.len()
            )));
        }

        let engine = FactorEngine::new(request.factors, request.features);
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }

        FactorEngine::validate_batch_invariants(request.vectors)?;

        // Factor compute is pure CPU work; run it on the blocking pool so a large
        // cross-sectional batch never stalls the async runtime. The engine moves
        // in and back out so its registry is reused for definition persistence.
        let config = request.factors.clone();
        let vectors = request.vectors.to_vec();
        let (engine, outcomes) = tokio::task::spawn_blocking(move || {
            let outcomes = engine.compute_all_batch(&vectors, &config);
            (engine, outcomes)
        })
        .await
        .map_err(|err| QuantError::Internal(format!("factor compute task panicked: {err}")))?;
        let outcomes = outcomes?;

        // Persist the governed factor definitions first (idempotent on their
        // deterministic ids), satisfying the factor-value → definition FK.
        self.persist_definitions(&engine, request.features).await?;

        // Build rows for eligible markets, tagging each factor with its source
        // feature-vector id (aligned by index), and collect rejections.
        let mut rows: Vec<NewFactorValue> = Vec::new();
        let mut rejected = Vec::new();
        for (index, outcome) in outcomes.iter().enumerate() {
            match &outcome.eligibility {
                FactorEligibility::RejectCandidate { reason } => {
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

    /// Upsert every enabled factor's governed definition (idempotent).
    async fn persist_definitions(
        &self,
        engine: &FactorEngine,
        features: &FeaturesConfig,
    ) -> QuantResult<()> {
        for spec in &engine.factor_set().definitions {
            let definition = spec.try_to_new(features.feature_schema_version)?;
            self.factor_repo
                .create_definition(definition)
                .await
                .map_err(QuantError::from)?;
        }
        Ok(())
    }
}
