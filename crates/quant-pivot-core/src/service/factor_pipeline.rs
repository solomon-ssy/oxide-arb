//! Factor-plane orchestration: feature vectors → factor engine → partition →
//! validate immutable definitions → persist values → emit.
//!
//! Consumes the accepted [`FeatureVector`]s of one online round (paired with
//! their persisted ids), a minted `model_run_id`, and an already-built
//! [`FactorExecutionPlane`]. It computes the cross-sectional factor batch,
//! partitions markets by [`FactorEligibility`], validates the governed revision
//! identities, persists the eligible markets' factor values, and emits
//! present-only long-format facts. Rejected markets are observable but never
//! reach persistence, facts, or the downstream model plane.
//!
//! `model_run_id` is minted up front by the online round and threaded through
//! the `quant_model_run` create/finalize lifecycle and factor-value foreign key.
//! This service is the callable unit reused by `ModelRunner`.

use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::quant::{FactorDefinitionInfo, FactorValueInfo, NewFactorValue},
    enums::common::MarketCategory,
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig, SmallCrossSectionPolicy},
    types::{
        ContentHash, FactorDefinitionId, FeatureVectorId, MarketId, ModelRunId,
        factor::FactorDefinitionRef,
    },
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::{
    factors::{
        FactorEligibility, FactorEngine, FactorValueInsertContext, FrozenReferenceQuantiles,
        MarketFactorOutcome, factor_events,
    },
    features::FeatureVector,
    model::FavoriteLongshotBiasTable,
};

use crate::observability::factor_fact_writer::FactorEventWriter;

/// One immutable, reusable factor engine plus its exact computation policy.
///
/// Model serving constructs this value once per verified serving contract and
/// then binds its derived [`quant_pivot_models::types::factor::FactorServingPlane`]
/// and optional bias-table hash to that contract before publication. Offline
/// factor workflows may use the same construction boundary without pretending
/// to own a model-serving contract.
pub struct FactorExecutionPlane {
    engine: Arc<FactorEngine>,
    config: FactorsConfig,
    bias_table_hash: Option<ContentHash>,
}

impl FactorExecutionPlane {
    /// Build one execution plane from frozen policy/profile inputs.
    ///
    /// # Errors
    ///
    /// Rejects an invalid derived serving plane.
    pub fn try_new(
        factors: &FactorsConfig,
        features: &FeaturesConfig,
        domain: &DomainConfig,
        category_scope: Option<MarketCategory>,
        bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ) -> QuantResult<Self> {
        let bias_table_hash = bias_table.as_ref().map(|table| table.content_hash);
        let engine =
            FactorEngine::for_model_scope(factors, features, domain, category_scope, bias_table);
        engine.serving_plane()?;
        Ok(Self {
            engine: Arc::new(engine),
            config: factors.clone(),
            bias_table_hash,
        })
    }

    #[must_use]
    pub fn engine(&self) -> Arc<FactorEngine> {
        Arc::clone(&self.engine)
    }

    #[must_use]
    pub const fn config(&self) -> &FactorsConfig {
        &self.config
    }

    #[must_use]
    pub const fn bias_table_hash(&self) -> Option<ContentHash> {
        self.bias_table_hash
    }
}

/// Frozen inputs for one factor-plane round.
pub struct FactorPipelineRequest<'a> {
    /// The owning online round (minted up front, threaded through the round).
    pub model_run_id: &'a ModelRunId,
    /// Accepted feature vectors whose data-quality bar already passed.
    pub vectors: Arc<[FeatureVector]>,
    /// Persisted feature-vector ids, aligned 1:1 with `vectors`.
    pub feature_vector_ids: &'a [FeatureVectorId],
    /// Fully validated engine/config pair owned by the serving-contract registry.
    pub factor_execution: &'a FactorExecutionPlane,
}

/// A market excluded because a required factor was missing, indeterminate, or
/// below the confidence floor. Observable, but it produces no factor rows.
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
    /// Markets excluded by the sealed required-factor contract (not persisted,
    /// not emitted).
    pub rejected: Vec<RejectedFactorMarket>,
    /// Persisted factor-value rows for the eligible markets.
    pub persisted: Vec<FactorValueInfo>,
}

/// Orchestrates the online factor build loop for one round of feature vectors.
pub struct FactorPipelineService {
    factor_repo: Arc<dyn FactorRepository>,
    event_writer: Arc<FactorEventWriter>,
    compute: Arc<ComputeExecutor>,
}

impl FactorPipelineService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub fn new(
        factor_repo: Arc<dyn FactorRepository>,
        event_writer: Arc<FactorEventWriter>,
        compute: Arc<ComputeExecutor>,
    ) -> Self {
        Self {
            factor_repo,
            event_writer,
            compute,
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

        let engine = request.factor_execution.engine();
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }

        FactorEngine::validate_batch_invariants(request.vectors.as_ref())?;
        references.validate()?;
        let factors = request.factor_execution.config();
        if factors.cross_section.small_cross_section_policy
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

        // Training seals and registers immutable definitions before a model
        // contract can become serving-eligible. The money-facing path is
        // read-only and fails closed unless every exact revision is present.
        self.require_registered_definitions(engine.as_ref()).await?;

        // Factor compute is pure CPU work; run it on the blocking pool so a large
        // cross-sectional batch never stalls the async runtime. The engine moves
        // in and back out so its registry is reused for definition persistence.
        let config = factors.clone();
        let vectors = request.vectors;
        let references = references.clone();
        let outcomes = self
            .compute
            .run_serving(move || engine.compute_batch_with_refs(&vectors, &config, &references))
            .await?;

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

    /// Fail closed when any exact, content-addressed serving definition is
    /// absent or does not reconstruct the expected revision.
    async fn require_registered_definitions(&self, engine: &FactorEngine) -> QuantResult<()> {
        let plane = engine.serving_plane()?;
        let ids: Vec<_> = plane
            .definitions()
            .iter()
            .map(FactorDefinitionRef::factor_definition_id)
            .collect();
        let rows = self
            .factor_repo
            .find_definitions_by_ids(&ids)
            .await
            .map_err(QuantError::from)?;
        let mut violations = Vec::new();
        let mut rows_by_id: HashMap<FactorDefinitionId, FactorDefinitionInfo> = HashMap::new();
        for row in rows {
            let id = row.factor_definition_id;
            if rows_by_id.insert(id, row).is_some() {
                violations.push(format!("{id}=duplicate_persisted_row"));
            }
        }
        for expected in plane.definitions() {
            let id = expected.factor_definition_id();
            let name = expected.factor_name();
            match rows_by_id.remove(&id) {
                None => violations.push(format!("{name}=unregistered")),
                Some(row) => match FactorDefinitionRef::try_from(&row) {
                    Err(error) => {
                        violations.push(format!("{name}=invalid_persisted_revision({error})"));
                    }
                    Ok(actual) if actual != *expected => {
                        violations.push(format!("{name}=serving_plane_mismatch"));
                    }
                    Ok(_) => {}
                },
            }
        }
        violations.extend(
            rows_by_id
                .into_keys()
                .map(|id| format!("{id}=unexpected_persisted_row")),
        );
        if violations.is_empty() {
            return Ok(());
        }
        Err(ReportError::ContractViolation {
            detail: format!(
                "factor pipeline blocked: serving contract references definitions that were not \
                 registered immutably during training ({})",
                violations.join(", ")
            ),
        }
        .into())
    }
}
