//! Feature-plane orchestration: selection → window prefetch → resolve → build →
//! partition → persist → emit.
//!
//! Wires the research [`ConfiguredFeatureBuilder`] with the online
//! [`FeatureWindowProvider`], Postgres persistence, and the `ClickHouse` feature
//! event writer. PIT inputs are resolved per market (the only async step), then
//! vectors are built in parallel from those frozen inputs. Vectors whose data
//! quality is [`DataQualityStatus::Insufficient`] are durably retained as audit
//! evidence but partitioned out before the factor/model plane. The serving
//! completion commits every selected vector and separately binds the admitted
//! model-input subset, so parity can replay rejections without treating them as
//! inference inputs.

use crate::{
    ingest::{
        market_registry::MarketRegistry,
        trade_tape_health::{cursors_by_contract_address, trade_tape_market_ingest_available},
    },
    observability::{
        feature_fact_writer::FeatureEventWriter, serving_evidence::FeatureEvidenceCommitment,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::basis_alert::detect_basis_alerts,
};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::future::join_all;
use quant_pivot_error::{QuantError, QuantResult, report::ReportError, research::ResearchError};
use quant_pivot_models::{
    config::TradeTapeOnChainConfig,
    domain::{
        DecisionBoundary, FeatureVectorInfo, MarketLinkage, NewFeatureVector, TradeTapeSourceKind,
        quant::NewReportDataQualitySnapshot,
    },
    enums::{domain::DomainFamily, quant::DataQualityStatus},
    runtime_config::{DataQualityConfig, DomainConfig, FeaturesConfig},
    types::{DomainInstrumentKey, MarketId, RuntimeConfigVersionId, TokenId, Usd},
};
use quant_pivot_repository::traits::{
    BasisAlertRepository, FeatureRepository, MarketLinkageRepository,
    TradeTapeBlockCursorRepository,
};
use quant_pivot_research::domain::{
    build_domain_slice_inputs, crypto_lookback_secs, oracle_instrument,
};
use quant_pivot_research::{
    features::{
        ConfiguredFeatureBuilder, DomainSliceInputs, FeatureName, FeatureSchema,
        FeatureSourceWindows, FeatureVector, MarketDecisionCapture, MarketWindowSnapshot,
        NullReason, RejectedMarketDraft, ResolvedMarketBundle, TradeTapeWindowSnapshot,
        draft_data_quality_snapshot, feature_events,
    },
    pit::PointInTimeSnapshotSource,
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

/// Frozen inputs for one feature-plane round.
pub struct FeaturePipelineRequest<'a> {
    /// Markets selected for this round.
    pub included: &'a [SelectedMarket],
    /// Decision time and the single, already-derived source visibility cutoff.
    pub boundary: DecisionBoundary,
    /// Frozen feature config.
    pub features: &'a FeaturesConfig,
    /// Frozen domain-plane config (Phase 11.2.2).
    pub domain: &'a DomainConfig,
    /// Frozen data-quality config.
    pub data_quality: &'a DataQualityConfig,
    /// Model-required features (drives fail-closed missing-input rejection).
    pub model_requirements: &'a ModelFeatureRequirements,
    /// Durable point-in-time snapshot source.
    pub pit: &'a dyn PointInTimeSnapshotSource,
    /// Config version governing this round (DQ snapshot header).
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Liquidity cap used to normalize capture liquidity scores.
    pub liquidity_cap_usd: Usd,
}

/// A market whose feature vector failed the data-quality bar and was excluded.
///
/// Rejected markets are observable (so operators can see *why* a market dropped
/// out). Their vectors/cells are persisted for audit but never enter the factor
/// or model plane.
pub struct RejectedMarket {
    /// The excluded market.
    pub market_id: MarketId,
    /// The primary outcome token, when scoped.
    pub token_id: Option<TokenId>,
    /// Required features that were missing, with their reasons.
    pub missing_required: Vec<(FeatureName, NullReason)>,
}

/// Outcome of one feature-plane round.
pub struct FeaturePipelineResult {
    /// Vectors that passed the data-quality bar (persisted + emitted).
    pub accepted: Vec<FeatureVector>,
    /// Markets excluded for insufficient data quality.
    pub rejected: Vec<RejectedMarket>,
    /// Postgres persistence rows, aligned with `accepted`.
    pub persisted: Vec<FeatureVectorInfo>,
    /// Canonical commitment returned only after every selected feature cell is
    /// durably acknowledged and the admitted subset is bound. `None` means no
    /// vector passed the DQ gate, although rejected audit facts were persisted.
    pub feature_evidence: Option<FeatureEvidenceCommitment>,
    /// Decision captures keyed by market id (accepted + rejected).
    pub captures: HashMap<MarketId, MarketDecisionCapture>,
    /// Draft report-level DQ snapshot for the transaction composer.
    pub data_quality_snapshot: NewReportDataQualitySnapshot,
}

/// Orchestrates the online feature build loop for a selection snapshot.
///
/// Holds only process-lifetime dependencies (window read port, persistence,
/// fact writer). Each [`Self::run`] builds a [`ConfiguredFeatureBuilder`] from
/// the request's frozen [`FeaturesConfig`], so runtime-config activations never
/// require rebootstrap.
pub struct FeaturePipelineService {
    window_provider: FeatureWindowProvider,
    feature_repo: Arc<dyn FeatureRepository>,
    event_writer: Arc<FeatureEventWriter>,
    market_registry: Arc<MarketRegistry>,
    block_cursor_repo: Arc<dyn TradeTapeBlockCursorRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    basis_alert_repo: Arc<dyn BasisAlertRepository>,
    trade_tape_on_chain: TradeTapeOnChainConfig,
}

/// Boot-time dependencies for [`FeaturePipelineService::new`].
pub struct FeaturePipelineDeps {
    pub window_provider: FeatureWindowProvider,
    pub feature_repo: Arc<dyn FeatureRepository>,
    pub event_writer: Arc<FeatureEventWriter>,
    pub market_registry: Arc<MarketRegistry>,
    pub block_cursor_repo: Arc<dyn TradeTapeBlockCursorRepository>,
    pub linkage_repo: Arc<dyn MarketLinkageRepository>,
    pub basis_alert_repo: Arc<dyn BasisAlertRepository>,
    pub trade_tape_on_chain: TradeTapeOnChainConfig,
}

impl FeaturePipelineService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub fn new(deps: FeaturePipelineDeps) -> Self {
        Self {
            window_provider: deps.window_provider,
            feature_repo: deps.feature_repo,
            event_writer: deps.event_writer,
            market_registry: deps.market_registry,
            block_cursor_repo: deps.block_cursor_repo,
            linkage_repo: deps.linkage_repo,
            basis_alert_repo: deps.basis_alert_repo,
            trade_tape_on_chain: deps.trade_tape_on_chain,
        }
    }

    /// Run one feature round: prefetch windows, resolve PIT inputs, build vectors
    /// in parallel, retain complete audit evidence, then expose only accepted
    /// vectors to the model plane.
    ///
    /// # Errors
    ///
    /// Propagates window read, PIT resolution, persistence, or mapping failures.
    pub async fn run(
        &self,
        request: FeaturePipelineRequest<'_>,
    ) -> QuantResult<FeaturePipelineResult> {
        let builder = ConfiguredFeatureBuilder::new(request.features, request.domain)?;
        let windows = self.load_windows(&builder, &request).await?;

        let max_concurrent = usize::try_from(request.features.max_concurrent_market_resolves)
            .map_err(|error| ReportError::NumericOverflow {
                field: "features.max_concurrent_market_resolves",
                detail: error.to_string(),
            })?
            .max(1);
        let resolve_jobs = request
            .included
            .iter()
            .enumerate()
            .map(|(index, market)| {
                let window = windows
                    .microstructure
                    .get(&market.primary_token_id)
                    .ok_or_else(|| {
                        QuantError::from(ReportError::InvariantViolation {
                            stage: "feature_pipeline",
                            detail: format!(
                                "missing prefetched window for token {}",
                                market.primary_token_id.as_str()
                            ),
                        })
                    })?;
                let trade_tape = windows.trade_tape.get(&market.market_id).ok_or_else(|| {
                    QuantError::from(ReportError::InvariantViolation {
                        stage: "feature_pipeline",
                        detail: format!(
                            "missing prefetched trade-tape window for market {}",
                            market.market_id.as_str()
                        ),
                    })
                })?;
                let domain = windows.domain.get(&market.market_id);
                Ok((index, market, window, trade_tape, domain))
            })
            .collect::<QuantResult<Vec<_>>>()?;

        let bundles =
            Self::resolve_bundles(&builder, &request, &resolve_jobs, max_concurrent).await?;

        // The union across every route (generic + every category-specific
        // model): the null-policy / required-input gate is a per-feature
        // decision the builder only ever consults for a feature that is
        // structurally applicable to the market being built (a domain
        // feature is only evaluated when that market's domain slice exists
        // at all — see `compute_domain_slice`), so folding in every
        // category's requirement here never falsely gates a market outside
        // that category.
        let required = request.model_requirements.union_all();
        let vectors =
            builder.build_batch(&bundles, &required, request.features, request.data_quality)?;

        let required_names: HashSet<FeatureName> = required.iter().cloned().collect();
        let partition = partition_feature_vectors(&bundles, &vectors, &required_names);
        let persistence = self
            .persist_vectors(&vectors, &partition.captures, builder.schema(), &request)
            .await?;
        let data_quality_snapshot = draft_data_quality_snapshot(
            request.boundary.decision_at(),
            request.runtime_config_version_id.clone(),
            &bundles,
            &vectors,
            &persistence.all,
            &partition.rejected_drafts,
        )?;
        self.persist_basis_alerts(&partition.accepted, &windows.domain, request.domain)
            .await?;

        Ok(FeaturePipelineResult {
            accepted: partition.accepted,
            rejected: partition.rejected,
            persisted: persistence.accepted,
            feature_evidence: persistence.evidence,
            captures: partition.captures,
            data_quality_snapshot,
        })
    }

    /// Persist every resolved vector and commit every selected market's feature
    /// cells as serving evidence. The commitment separately binds the subset
    /// admitted to model input, so rejected vectors remain replayable without
    /// pretending that they reached inference.
    async fn persist_vectors(
        &self,
        vectors: &[FeatureVector],
        captures: &HashMap<MarketId, MarketDecisionCapture>,
        schema: &FeatureSchema,
        request: &FeaturePipelineRequest<'_>,
    ) -> QuantResult<PersistedFeatureVectors> {
        let rows = vectors
            .iter()
            .map(|vector| {
                let capture = captures.get(&vector.market_id).ok_or_else(|| {
                    ReportError::InvariantViolation {
                        stage: "feature_pipeline",
                        detail: format!(
                            "market {} has no decision capture before persistence",
                            vector.market_id
                        ),
                    }
                })?;
                if capture.token_id
                    != vector
                        .token_id
                        .clone()
                        .ok_or_else(|| ReportError::InvariantViolation {
                            stage: "feature_pipeline",
                            detail: format!(
                                "market {} feature vector has no token id",
                                vector.market_id
                            ),
                        })?
                    || capture.data_quality != vector.data_quality
                    || capture.snapshot.boundary != request.boundary
                {
                    return Err(ReportError::InvariantViolation {
                        stage: "feature_pipeline",
                        detail: format!(
                            "market {} decision capture is not aligned with its vector",
                            vector.market_id
                        ),
                    }
                    .into());
                }
                let mut row = vector.try_to_new(&request.boundary)?;
                row.decision_capture_hash = Some(capture.evidence_hash()?);
                row.decision_capture =
                    Some(serde_json::to_value(capture.evidence()).map_err(|error| {
                        ResearchError::Serialization {
                            detail: format!(
                                "serialize decision capture for market {}: {error}",
                                vector.market_id
                            ),
                        }
                    })?);
                Ok(row)
            })
            .collect::<QuantResult<Vec<NewFeatureVector>>>()?;
        let all_persisted = self
            .feature_repo
            .create_batch(rows)
            .await
            .map_err(QuantError::from)?;
        ensure_persistence_alignment(vectors, &all_persisted)?;

        let ingestion_time = Utc::now().timestamp_millis();
        let projected = vectors
            .iter()
            .zip(&all_persisted)
            .map(|(vector, persisted)| {
                feature_events(
                    vector,
                    persisted,
                    &request.boundary,
                    &request.runtime_config_version_id,
                    schema,
                    ingestion_time,
                )
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let mut accepted = Vec::new();
        let mut admitted_vector_ids = Vec::new();
        let mut all_events = Vec::new();
        for ((vector, info), events) in vectors.iter().zip(&all_persisted).zip(projected) {
            if vector.data_quality != DataQualityStatus::Insufficient {
                accepted.push(info.clone());
                admitted_vector_ids.push(info.feature_vector_id.clone());
            }
            all_events.extend(events);
        }
        let evidence = if admitted_vector_ids.is_empty() {
            if !all_events.is_empty() {
                self.event_writer.write_batch(all_events).await?;
            }
            None
        } else {
            Some(
                self.event_writer
                    .write_batch(all_events)
                    .await?
                    .bind_model_vectors(&admitted_vector_ids)?,
            )
        };
        Ok(PersistedFeatureVectors {
            accepted,
            all: all_persisted,
            evidence,
        })
    }

    /// Basis cross-check closed loop (11.2.2 remediation R6): every accepted
    /// vector whose `domain.crypto.basis_vs_resolution_source` exceeds the
    /// governed threshold is durably recorded — never a
    /// computed-but-unconsumed feature value. A per-market cooldown keeps a
    /// persistent divergence from flooding the feed with one row per report
    /// round.
    async fn persist_basis_alerts(
        &self,
        accepted: &[FeatureVector],
        domain_inputs: &HashMap<MarketId, DomainSliceInputs>,
        domain: &DomainConfig,
    ) -> QuantResult<()> {
        let cooldown_secs =
            i64::try_from(domain.crypto.cross_check.alert_cooldown_secs).map_err(|error| {
                ReportError::NumericOverflow {
                    field: "domain.crypto.cross_check.alert_cooldown_secs",
                    detail: error.to_string(),
                }
            })?;
        let cooldown = ChronoDuration::seconds(cooldown_secs);
        for alert in detect_basis_alerts(accepted, domain_inputs, domain) {
            let recent = self
                .basis_alert_repo
                .latest_for_market(&alert.market_id)
                .await
                .map_err(QuantError::from)?;
            let cooled_down =
                recent.is_none_or(|previous| alert.as_of - previous.as_of >= cooldown);
            if cooled_down {
                self.basis_alert_repo
                    .record(alert)
                    .await
                    .map_err(QuantError::from)?;
            }
        }
        Ok(())
    }

    /// Prefetch the microstructure windows, skipping the `ClickHouse` read entirely
    /// when no enabled feature consumes a window (book / metadata-only schemas).
    async fn load_windows(
        &self,
        builder: &ConfiguredFeatureBuilder,
        request: &FeaturePipelineRequest<'_>,
    ) -> QuantResult<FeaturePrefetchWindows> {
        let lookback = Duration::from_secs(request.features.max_microstructure_lookback_secs());
        let microstructure = if builder.schema().needs_window() {
            self.window_provider
                .load_windows(request.included, &request.boundary, lookback)
                .await?
        } else {
            empty_windows(request.included, &request.boundary)
        };
        let trade_tape = if builder.needs_trade_tape() && self.trade_tape_on_chain.enabled {
            let cursors = self
                .block_cursor_repo
                .list_by_source(TradeTapeSourceKind::OnChain.as_str())
                .await?;
            let cursors_by_address = cursors_by_contract_address(&cursors);
            let trade_lookback =
                Duration::from_secs(request.features.structural.trade_tape_window_secs);
            let mut windows = self
                .window_provider
                .load_trade_tape_windows(request.included, &request.boundary, trade_lookback)
                .await?;
            for market in request.included {
                let neg_risk = self
                    .market_registry
                    .get_market(&market.market_id)
                    .is_some_and(|info| info.neg_risk);
                let available = trade_tape_market_ingest_available(
                    &self.trade_tape_on_chain,
                    &cursors_by_address,
                    neg_risk,
                );
                if let Some(window) = windows.get_mut(&market.market_id)
                    && !available
                {
                    *window = window.clone().with_source_available(false);
                }
            }
            windows
        } else {
            empty_trade_tape_windows(request.included, &request.boundary)
        };
        let domain = if builder.needs_domain() {
            self.load_domain_inputs(request).await?
        } else {
            HashMap::new()
        };
        Ok(FeaturePrefetchWindows {
            microstructure,
            trade_tape,
            domain,
        })
    }

    /// Prefetch the frozen linkage records + PIT domain observations for every
    /// category-mapped market, then assemble per-market domain-slice inputs via
    /// the SAME pure function the offline replay uses (zero train-serve skew).
    ///
    /// The observation fetch is bounded by the **domain** source cutoff
    /// (`domain.crypto.availability_lag_secs`), not merely the global knowledge
    /// cutoff: the pure assembly re-slices to that cutoff, so fetching under a different
    /// bound would silently truncate the window.
    async fn load_domain_inputs(
        &self,
        request: &FeaturePipelineRequest<'_>,
    ) -> QuantResult<HashMap<MarketId, DomainSliceInputs>> {
        let mapped: Vec<&SelectedMarket> = request
            .included
            .iter()
            .filter(|market| {
                DomainFamily::for_category(market.category)
                    .is_some_and(|family| request.domain.family_enabled(family))
            })
            .collect();
        if mapped.is_empty() {
            return Ok(HashMap::new());
        }
        let market_ids: Vec<MarketId> = mapped
            .iter()
            .map(|market| market.market_id.clone())
            .collect();
        let ledger_rows = self
            .linkage_repo
            .ledger_for_markets(&market_ids, &request.boundary)
            .await
            .map_err(QuantError::from)?;
        let mut linkages: HashMap<MarketId, Vec<MarketLinkage>> = HashMap::new();
        let mut instruments: HashSet<DomainInstrumentKey> = HashSet::new();
        for info in ledger_rows {
            let market_id = info.market_id.clone();
            let linkage = info.into_domain().map_err(|error| {
                QuantError::config(format!(
                    "linkage ledger row for market {market_id} has an undecodable outcome \
                     payload: {error}"
                ))
            })?;
            if let Some(binding) = linkage.binding() {
                instruments.insert(binding.instrument_key.clone());
                if let Some(oracle_key) = oracle_instrument(binding) {
                    instruments.insert(oracle_key);
                }
            }
            linkages.entry(market_id).or_default().push(linkage);
        }
        let lookback = Duration::from_secs(crypto_lookback_secs(request.domain));
        let observations = self
            .window_provider
            .load_domain_observations(
                instruments.into_iter().collect(),
                &request.boundary,
                lookback,
            )
            .await?;
        let mut inputs = HashMap::new();
        for market in mapped {
            if let Some(slice_inputs) = build_domain_slice_inputs(
                market.category,
                linkages
                    .get(&market.market_id)
                    .map_or(&[][..], Vec::as_slice),
                &request.boundary,
                request.domain,
                &observations,
            )? {
                inputs.insert(market.market_id.clone(), slice_inputs);
            }
        }
        Ok(inputs)
    }

    /// Resolve one PIT bundle per market with bounded concurrency, preserving
    /// input order.
    ///
    /// Neg-risk membership and every sibling leg are projected from the same
    /// immutable catalog snapshot inside [`ConfiguredFeatureBuilder::resolve_inputs`].
    async fn resolve_bundles<'a>(
        builder: &ConfiguredFeatureBuilder,
        request: &FeaturePipelineRequest<'a>,
        resolve_jobs: &[(
            usize,
            &'a SelectedMarket,
            &'a MarketWindowSnapshot,
            &'a TradeTapeWindowSnapshot,
            Option<&'a DomainSliceInputs>,
        )],
        max_concurrent: usize,
    ) -> QuantResult<Vec<ResolvedMarketBundle<'a>>> {
        let mut bundles = Vec::with_capacity(resolve_jobs.len());
        for chunk in resolve_jobs.chunks(max_concurrent) {
            let chunk_results =
                join_all(chunk.iter().map(|(_, market, window, trade_tape, domain)| {
                    builder.resolve_inputs(
                        market,
                        &request.boundary,
                        request.pit,
                        FeatureSourceWindows {
                            microstructure: window,
                            trade_tape,
                            domain: *domain,
                        },
                        request.liquidity_cap_usd,
                    )
                }))
                .await;
            for ((index, _, _, _, _), bundle) in chunk.iter().zip(chunk_results) {
                bundles.push((*index, bundle?));
            }
        }
        bundles.sort_by_key(|(index, _)| *index);
        Ok(bundles.into_iter().map(|(_, bundle)| bundle).collect())
    }
}

struct FeaturePrefetchWindows {
    microstructure: HashMap<TokenId, MarketWindowSnapshot>,
    trade_tape: HashMap<MarketId, TradeTapeWindowSnapshot>,
    domain: HashMap<MarketId, DomainSliceInputs>,
}

struct FeatureVectorPartition {
    accepted: Vec<FeatureVector>,
    rejected: Vec<RejectedMarket>,
    captures: HashMap<MarketId, MarketDecisionCapture>,
    rejected_drafts: Vec<RejectedMarketDraft>,
}

struct PersistedFeatureVectors {
    /// Accepted rows only, aligned 1:1 with `FeatureVectorPartition::accepted`.
    accepted: Vec<FeatureVectorInfo>,
    /// Every selected vector, including DQ-rejected rows, in deterministic
    /// selection order. The report DQ snapshot freezes these exact ids.
    all: Vec<FeatureVectorInfo>,
    /// Serving commitment over all selected vectors, bound to the accepted
    /// model-input subset.
    evidence: Option<FeatureEvidenceCommitment>,
}

fn partition_feature_vectors(
    bundles: &[ResolvedMarketBundle<'_>],
    vectors: &[FeatureVector],
    required_names: &HashSet<FeatureName>,
) -> FeatureVectorPartition {
    let mut accepted = Vec::with_capacity(vectors.len());
    let mut rejected = Vec::new();
    let mut rejected_drafts = Vec::new();
    for vector in vectors {
        if vector.data_quality == DataQualityStatus::Insufficient {
            let rejected_market = reject_market(vector, required_names);
            rejected_drafts.push(RejectedMarketDraft {
                market_id: rejected_market.market_id.clone(),
                missing_required: rejected_market.missing_required.clone(),
            });
            rejected.push(rejected_market);
        } else {
            accepted.push(vector.clone());
        }
    }
    let captures = finalize_captures(bundles, vectors);
    FeatureVectorPartition {
        accepted,
        rejected,
        captures,
        rejected_drafts,
    }
}

fn ensure_persistence_alignment(
    vectors: &[FeatureVector],
    persisted: &[FeatureVectorInfo],
) -> QuantResult<()> {
    if persisted.len() == vectors.len() {
        return Ok(());
    }
    Err(ReportError::InvariantViolation {
        stage: "feature_pipeline",
        detail: format!(
            "feature repository returned {} rows for {} resolved vectors",
            persisted.len(),
            vectors.len()
        ),
    }
    .into())
}

/// Merge post-build data quality into frozen captures for every market.
fn finalize_captures(
    bundles: &[ResolvedMarketBundle<'_>],
    vectors: &[FeatureVector],
) -> HashMap<MarketId, MarketDecisionCapture> {
    bundles
        .iter()
        .zip(vectors)
        .map(|(bundle, vector)| {
            let mut capture = bundle.capture.clone();
            capture.data_quality = vector.data_quality;
            (capture.market_id.clone(), capture)
        })
        .collect()
}

/// Summarize why a market was rejected: the required features that
/// were missing, with their reasons.
fn reject_market(vector: &FeatureVector, required_names: &HashSet<FeatureName>) -> RejectedMarket {
    let missing_required = vector
        .iter_cells()
        .filter_map(|(name, cell)| {
            let reason = cell.reason?;
            required_names
                .contains(name)
                .then_some((name.clone(), reason))
        })
        .collect();
    RejectedMarket {
        market_id: vector.market_id.clone(),
        token_id: vector.token_id.clone(),
        missing_required,
    }
}

/// Empty (PIT-correct) windows for every market, used when the active schema
/// needs no windowed feature — avoids an unnecessary `ClickHouse` round-trip.
fn empty_windows(
    markets: &[SelectedMarket],
    boundary: &DecisionBoundary,
) -> HashMap<TokenId, MarketWindowSnapshot> {
    markets
        .iter()
        .map(|market| {
            let token = market.primary_token_id.clone();
            (
                token.clone(),
                MarketWindowSnapshot::empty(
                    token,
                    boundary.decision_at(),
                    boundary.knowledge_cutoff(),
                ),
            )
        })
        .collect()
}

fn empty_trade_tape_windows(
    markets: &[SelectedMarket],
    boundary: &DecisionBoundary,
) -> HashMap<MarketId, TradeTapeWindowSnapshot> {
    markets
        .iter()
        .map(|market| {
            let market_id = market.market_id.clone();
            (
                market_id.clone(),
                TradeTapeWindowSnapshot::empty(
                    market_id,
                    boundary.decision_at(),
                    boundary.knowledge_cutoff(),
                ),
            )
        })
        .collect()
}
