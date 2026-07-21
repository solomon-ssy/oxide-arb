//! Factor-plane orchestration: feature vectors → factor engine → partition →
//! validate immutable definitions → persist values → emit.
//!
//! Consumes the accepted [`FeatureVector`]s of one online round (paired with
//! their persisted ids) and a minted `model_run_id`, builds a
//! [`FactorEngine`] from frozen config, computes the cross-sectional factor
//! batch, partitions markets by [`FactorEligibility`], validates the governed
//! revision identities, persists the eligible markets' factor values, and emits
//! present-only long-format facts. Rejected markets are observable but never
//! reach persistence, facts, or the downstream model plane.
//!
//! `model_run_id` is minted up front by the online round and threaded through
//! the `quant_model_run` create/finalize lifecycle and factor-value foreign key.
//! This service is the callable unit reused by `ModelRunner`.

use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, report::ReportError};
use quant_pivot_models::{
    domain::quant::{FactorValueInfo, NewFactorValue},
    enums::quant::PublicationStatus,
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig, SmallCrossSectionPolicy},
    types::{FactorDefinitionId, FeatureVectorId, MarketId, ModelRunId},
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::{
    factors::{
        FactorEligibility, FactorEngine, FactorValueInsertContext, FrozenReferenceQuantiles,
        MarketFactorOutcome, factor_events,
    },
    features::FeatureVector,
};

use crate::{
    governance::BiasTableApplicator, observability::factor_fact_writer::FactorEventWriter,
};

/// Frozen inputs for one factor-plane round.
pub struct FactorPipelineRequest<'a> {
    /// The owning online round (minted up front, threaded through the round).
    pub model_run_id: &'a ModelRunId,
    /// Accepted feature vectors whose data-quality bar already passed.
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
    /// flags — the input consumed by the model runtime.
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
        self.run_with_references(request, &FrozenReferenceQuantiles::empty())
            .await
    }

    /// Run one factor round with the training CDFs frozen in the already-loaded
    /// weighted-model artifact. Serving must use this entrypoint whenever the
    /// configured small-cross-section policy is `FrozenReferenceQuantile`.
    ///
    /// # Errors
    ///
    /// In addition to [`Self::run`] errors, rejects a frozen-reference policy
    /// without a validated artifact reference collection.
    pub async fn run_with_references(
        &self,
        request: FactorPipelineRequest<'_>,
        references: &FrozenReferenceQuantiles,
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
        references.validate()?;
        if request.factors.cross_section.small_cross_section_policy
            == SmallCrossSectionPolicy::FrozenReferenceQuantile
            && references.is_empty()
        {
            return Err(ReportError::ContractViolation {
                detail: "factor pipeline: FrozenReferenceQuantile requires reference CDFs from \
                         the loaded weighted-model artifact"
                    .to_owned(),
            }
            .into());
        }

        // Definitions are registered out-of-band by the explicit factor-register
        // governance action (not on this money-facing hot path). Fail closed if
        // any enabled definition is not `Published`.
        self.require_published_definitions(&engine).await?;

        // Factor compute is pure CPU work; run it on the blocking pool so a large
        // cross-sectional batch never stalls the async runtime. The engine moves
        // in and back out so its registry is reused for definition persistence.
        let config = request.factors.clone();
        let vectors = request.vectors.to_vec();
        let references = references.clone();
        let (_engine, outcomes) = tokio::task::spawn_blocking(move || {
            let outcomes = engine.compute_all_batch_with_references(&vectors, &config, &references);
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
                        decision_at: outcome.decision_at,
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

    /// Fail closed when any enabled definition is missing (never registered) or
    /// not `Published`. Definitions are registered out-of-band by the explicit
    /// factor-register governance action, so a fresh system with an unregistered
    /// factor set is a hard block here (never a silent pass).
    async fn require_published_definitions(&self, engine: &FactorEngine) -> QuantResult<()> {
        let specs = engine.factor_set().definitions;
        let identities = engine.definition_identities()?;
        let ids: Vec<_> = identities
            .iter()
            .map(|identity| identity.factor_definition_id.clone())
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
        for (spec, identity) in specs.iter().zip(identities) {
            let id = identity.factor_definition_id;
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
