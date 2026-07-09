//! Bias-table fit orchestration (`kind = market_price_bias`, Phase 11.2.1).
//!
//! The fit reuses the **offline point-in-time spine**
//!   (`HistoricalWindowLoader` + the in-memory `MaterializedPitEngine`) that the
//!   training-dataset build and backtest replay share.
//!
//! Entry mids are resolved through the *same* `resolve_book` semantics the online
//! factor plane serves — no training-serving skew, no bespoke fact-read spine.
//!
//! It samples the entry mid across each settled market's whole lifecycle (a
//! `fit_sample_stride_secs` grid), not a single pre-resolution lead, and pairs
//! every sample with its residual time to resolution — so the fitted bias is
//! conditioned on `(category, ttr_bucket, price_bucket)` and measured on the
//! same distribution the factor is served on.
//!
//! Steps:
//!
//! 1. Enumerate markets observed in the fit window (`observed_markets_between`).
//! 2. Load their settlements and reduce each market to its terminal resolution.
//! 3. Batch-prefetch every settled market's YES-leg books into the PIT engine.
//! 4. For each market, resolve the PIT YES-leg mid at each grid instant and pair
//!    it with the realized `settled_yes` truth and residual ttr.
//! 5. Fit the per-`(category, ttr_bucket)` empirical-bias curves (Wilson bucket
//!    gate + IC significance).
//! 6. Persist the content-addressed artifact — **only** when a curve qualifies;
//!    otherwise succeed with no artifact (fail-closed greenfield).

use std::{collections::BTreeMap, sync::Arc, time::Duration as StdDuration};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        BiasTableFitJobParams, BiasTableFitOutcome, CalibrationArtifactFitPort,
        CalibrationArtifactInfo, CalibrationArtifactListQuery, JobProgressSink, Paginated,
        WindowBoundsError, query::TimeWindow,
    },
    enums::{common::MarketCategory, quant::CalibrationKind},
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorsConfig, FavoriteLongshotConfig, RuntimeConfig,
    },
    types::{CalibrationArtifactId, MarketId, ResearchJobProgress, TokenId},
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, EventRepository, MarketLinkageRepository, MarketRepository,
    QuantFactReadRepository, RuntimeConfigVersionRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    features::PitView,
    model::favorite_longshot::{
        BiasFitConfig, BiasSample, CategoryBiasCurve, FavoriteLongshotBiasTable,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    governance::ModelScoreCalibrationPayload,
    prefetch::historical_window::{HistoricalWindowLoader, ReplaySample, WindowSpec},
    service::calibration_shared::{
        assert_disjoint_from_all_training_datasets, calibration_split_hash,
    },
};

/// Validate that `info.payload_json` actually deserializes into the shape its
/// declared `kind` promises, **before** serving it to a read-only detail view
/// (Phase 11.3 closed-loop hardening — closes the "kind says X, payload is
/// shaped like Y" drift that a raw `payload_json` pass-through could
/// otherwise surface as a broken frontend render rather than a clear server
/// error). Deliberately **does not** re-verify content hash or `active`: an
/// operator must still be able to inspect a superseded / historical artifact's
/// full detail — those two invariants are enforced at the *consumption* points
/// (`CoreCalibrationArtifactLoader::load`, `BiasTableApplicator::reload`), not
/// at read-only display.
///
/// # Errors
///
/// Returns an error when the payload cannot deserialize into the shape its
/// `kind` promises.
fn validate_payload_shape(info: &CalibrationArtifactInfo) -> QuantResult<()> {
    match info.kind {
        CalibrationKind::ModelScore => {
            serde_json::from_value::<ModelScoreCalibrationPayload>(info.payload_json.clone())
                .map_err(|error| {
                    QuantError::from(ResearchError::DatasetBuild {
                        detail: format!(
                            "calibration artifact `{}` declares kind `model_score` but its \
                             payload does not deserialize as one: {error}",
                            info.artifact_id
                        ),
                    })
                })?;
        }
        CalibrationKind::MarketPriceBias => {
            serde_json::from_value::<BTreeMap<MarketCategory, CategoryBiasCurve>>(
                info.payload_json.clone(),
            )
            .map_err(|error| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "calibration artifact `{}` declares kind `market_price_bias` but its \
                         payload does not deserialize as one: {error}",
                        info.artifact_id
                    ),
                })
            })?;
        }
    }
    Ok(())
}

/// Parse a runtime-config decimal string (must have passed validation on write).
fn config_decimal(raw: &str, field: &'static str) -> QuantResult<Decimal> {
    raw.trim().parse().map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("{field} `{raw}` invalid despite config validation: {error}"),
        })
    })
}

/// Resolve the favorite-longshot bias table pinned by a **frozen** factor config.
///
/// Content-hash verified — the offline (training / backtest / replay) counterpart
/// to the online [`BiasTableApplicator`](crate::governance::BiasTableApplicator).
///
/// `bias_table_ref = None` yields `None` (the factor stays inert).
///
/// Both online and offline resolve the same artifact bytes for a given ref, so the
/// scored `struct.favorite_longshot` is byte-identical across serve and train.
///
/// # Errors
///
/// Fails closed when the ref is malformed, the table is absent, or its recomputed
/// content hash does not match — never a silent skip that would train the model
/// on a different (or absent) bias than serving uses.
pub async fn resolve_frozen_bias_table(
    repo: &dyn CalibrationArtifactRepository,
    factors: &FactorsConfig,
) -> QuantResult<Option<Arc<FavoriteLongshotBiasTable>>> {
    let Some(raw) = factors.structural.favorite_longshot.bias_table_ref.as_ref() else {
        return Ok(None);
    };
    let id: CalibrationArtifactId = raw.trim().parse().map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("favorite_longshot.bias_table_ref `{raw}` is not a valid id: {error}"),
        })
    })?;
    let info = repo
        .find_by_id(&id)
        .await
        .map_err(QuantError::from)?
        .ok_or_else(|| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("frozen bias_table_ref `{id}` not found (train-serve parity)"),
            })
        })?;
    let table = FavoriteLongshotBiasTable::from_persisted(&info)?;
    Ok(Some(Arc::new(table)))
}

/// Frozen fit parameters resolved from the pinned runtime-config version.
struct FrozenFit {
    favorite_longshot: FavoriteLongshotConfig,
    max_book_staleness: StdDuration,
}

/// One market's terminal settlement, keyed to its YES leg.
struct SettledMarket {
    market_id: MarketId,
    yes_token_id: TokenId,
    category: MarketCategory,
    resolved_at: DateTime<Utc>,
    settled_yes: bool,
}

/// Core bias-table fitter + calibration-artifact read port.
pub struct BiasTableFitService {
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    event_repo: Arc<dyn EventRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
}

impl BiasTableFitService {
    /// Wire the fitter from its persistence ports.
    #[must_use]
    pub const fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        market_repo: Arc<dyn MarketRepository>,
        event_repo: Arc<dyn EventRepository>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        calibration_repo: Arc<dyn CalibrationArtifactRepository>,
        runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
        training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    ) -> Self {
        Self {
            fact_read,
            market_repo,
            event_repo,
            linkage_repo,
            calibration_repo,
            runtime_config_repo,
            training_dataset_repo,
        }
    }

    /// Load the frozen fit parameters from the runtime-config version pinned at
    /// enqueue (deterministic on replay).
    async fn frozen_fit(&self, params: &BiasTableFitJobParams) -> QuantResult<FrozenFit> {
        let version = self
            .runtime_config_repo
            .load_version(&params.runtime_config_version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: "runtime config version for bias-table fit not found".to_owned(),
                })
            })?;
        let config = RuntimeConfig::from_json(&version.config_json).map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("frozen runtime config parse failed: {error}"),
            })
        })?;
        let max_book_staleness = book_staleness(&config.data_quality);
        Ok(FrozenFit {
            favorite_longshot: config.factors.structural.favorite_longshot,
            max_book_staleness,
        })
    }

    /// Reduce the window's settlements to one terminal record per market, joined
    /// with the market catalog (category + YES token). Deduplicates the multiple
    /// resolution observations a market may carry to its terminal outcome.
    async fn settled_markets(&self, window: &TimeWindow) -> QuantResult<Vec<SettledMarket>> {
        let from_ms = window.from.timestamp_millis();
        let to_ms = window.to.timestamp_millis();
        let markets = self
            .fact_read
            .observed_markets_between(from_ms, to_ms)
            .await
            .map_err(QuantError::from)?;
        if markets.is_empty() {
            return Ok(Vec::new());
        }
        let resolutions = self
            .fact_read
            .resolutions_between(markets, from_ms, to_ms)
            .await
            .map_err(QuantError::from)?;

        // Reduce to the terminal resolution per market (latest resolved/observed).
        let mut terminal: BTreeMap<MarketId, (i64, TokenId)> = BTreeMap::new();
        let mut ordering: BTreeMap<MarketId, (i64, i64)> = BTreeMap::new();
        for row in resolutions {
            let key = (row.resolved_at, row.observed_at);
            let entry = ordering
                .entry(row.market_id.clone())
                .or_insert((i64::MIN, i64::MIN));
            if key > *entry {
                *entry = key;
                terminal.insert(
                    row.market_id.clone(),
                    (row.resolved_at, row.winning_token_id),
                );
            }
        }
        if terminal.is_empty() {
            return Ok(Vec::new());
        }

        let market_ids: Vec<MarketId> = terminal.keys().cloned().collect();
        let infos = self
            .market_repo
            .find_by_ids(&market_ids)
            .await
            .map_err(QuantError::from)?;

        let mut settled = Vec::with_capacity(infos.len());
        for info in &infos {
            let Some((resolved_at_ms, winning_token_id)) = terminal.get(&info.market_id) else {
                continue;
            };
            let Some(resolved_at) = DateTime::from_timestamp_millis(*resolved_at_ms) else {
                continue;
            };
            settled.push(SettledMarket {
                market_id: info.market_id.clone(),
                yes_token_id: info.yes_token_id.clone(),
                category: info.fee_category(),
                resolved_at,
                settled_yes: *winning_token_id == info.yes_token_id,
            });
        }
        Ok(settled)
    }

    /// Collect the `(category, entry_mid, ttr_secs, settled_yes)` spine by
    /// resolving the PIT YES-leg mid across each settled market's lifecycle.
    async fn collect_samples(
        &self,
        window: &TimeWindow,
        stride_secs: u64,
        max_book_staleness: StdDuration,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<Vec<BiasSample>> {
        let settled = self.settled_markets(window).await?;
        if settled.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-prefetch every settled market's YES-leg books into an in-memory
        // PIT engine — the fit then resolves mids with zero further DB round-trips
        // (kills the per-sample N+1) through the shared `resolve_book` semantics.
        let samples_spec: Vec<ReplaySample> = settled
            .iter()
            .map(|m| ReplaySample {
                market_id: m.market_id.clone(),
                token_id: m.yes_token_id.clone(),
            })
            .collect();
        let spec = WindowSpec {
            window_start: window.from,
            window_end: window.to,
            samples: samples_spec,
            lookback: StdDuration::ZERO,
            source_delay: StdDuration::ZERO,
            max_horizon_secs: 0,
            // The bias fit reads only settlement mids — no domain data.
            domain: DomainConfig::disabled(),
        };
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.fact_read),
            Arc::clone(&self.market_repo),
            Arc::clone(&self.event_repo),
            Arc::clone(&self.linkage_repo),
            max_book_staleness,
        );
        let historical = loader.load(&spec).await?;
        let pit = PitView::Historical(&historical.pit);

        let stride = Duration::seconds(i64::try_from(stride_secs.max(1)).unwrap_or(i64::MAX));
        let total = settled.len() as u64;
        let mut samples = Vec::new();
        for (index, market) in settled.iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(QuantError::from(ResearchError::Cancelled {
                    detail: "bias-table fit cancelled".to_owned(),
                }));
            }
            if index % 256 == 0 {
                progress.report(ResearchJobProgress::with_total(
                    "sampling",
                    index as u64,
                    total,
                ));
            }
            // Grid of decision instants over the market's observable life within
            // the fit window, strictly before resolution.
            let upper = market.resolved_at.min(window.to);
            let mut as_of = window.from;
            while as_of < upper {
                let ttr = upper.max(market.resolved_at) - as_of;
                let ttr_secs = ttr.num_seconds();
                if ttr_secs > 0
                    && let Some(mid) = pit
                        .resolve_book(&market.yes_token_id, as_of)
                        .await?
                        .and_then(|book| book.mid())
                {
                    let mid_inner = mid.inner();
                    if mid_inner > Decimal::ZERO && mid_inner < Decimal::ONE {
                        samples.push(BiasSample {
                            market_id: market.market_id.clone(),
                            sampled_at: as_of,
                            category: market.category,
                            entry_mid: mid,
                            ttr_secs: ttr_secs.to_u64().unwrap_or(u64::MAX),
                            settled_yes: market.settled_yes,
                        });
                    }
                }
                as_of += stride;
            }
        }
        Ok(samples)
    }
}

#[async_trait]
impl CalibrationArtifactFitPort for BiasTableFitService {
    async fn fit(
        &self,
        params: BiasTableFitJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BiasTableFitOutcome> {
        // Reject an inverted / empty window up front (fail-closed: never a
        // silently-empty fit masquerading as success on a degenerate window).
        // HTTP enqueue validates via [`TimeWindow::try_half_open`]; replay keeps
        // the same rule through [`TimeWindow::try_half_open`].
        let window =
            TimeWindow::try_half_open(params.request.window_start, params.request.window_end)
                .map_err(|_| {
                    QuantError::from(ResearchError::DatasetBuild {
                        detail: WindowBoundsError::MESSAGE.to_owned(),
                    })
                })?;
        let frozen = self.frozen_fit(&params).await?;
        let config = &frozen.favorite_longshot;
        assert_disjoint_from_all_training_datasets(
            self.training_dataset_repo.as_ref(),
            &window,
            "bias-table fit",
        )
        .await?;

        progress.report(ResearchJobProgress::with_total("resolutions", 0, 1));
        let samples = self
            .collect_samples(
                &window,
                config.fit_sample_stride_secs,
                frozen.max_book_staleness,
                &progress,
                &cancel,
            )
            .await?;
        let total_sample_count = samples.len() as u64;

        let fit_config = BiasFitConfig {
            bins: config.bins,
            ttr_bucket_bounds_secs: config.ttr_bucket_bounds_secs.clone(),
            min_bin_samples: config.min_bin_samples,
            min_curve_samples: config.min_curve_samples,
            ci_confidence: config_decimal(
                &config.ci_confidence.value,
                "factors.structural.favorite_longshot.ci_confidence",
            )?,
            ic_significance_min: config_decimal(
                &config.ic_significance_min.value,
                "factors.structural.favorite_longshot.ic_significance_min",
            )?,
        };
        // The split hash anchors the artifact to the *exact* fit sample set
        // (sorted market keys + window), not just a coarse window/count — a real
        // provenance / leakage anchor (full purged/embargo CPCV is Phase 11.5).
        let split_hash = calibration_split_hash(
            &window,
            samples
                .iter()
                .map(|s| (s.market_id.to_string(), s.sampled_at)),
        )?;

        progress.report(ResearchJobProgress::with_total("fit", 0, 1));
        let Some(table) =
            FavoriteLongshotBiasTable::fit(&samples, window, split_hash, &fit_config)?
        else {
            // Fail-closed: no category/ttr curve qualified, so no artifact is minted.
            return Ok(BiasTableFitOutcome {
                artifact_id: None,
                category_count: 0,
                total_sample_count,
            });
        };

        progress.report(ResearchJobProgress::with_total("persist", 0, 1));
        let persisted = self.persist(table).await?;
        let category_count = persisted
            .payload_json
            .as_object()
            .map_or(0, |obj| obj.len() as u64);
        Ok(BiasTableFitOutcome {
            artifact_id: Some(persisted.artifact_id),
            category_count,
            total_sample_count,
        })
    }

    async fn find(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> QuantResult<Option<CalibrationArtifactInfo>> {
        let found = self
            .calibration_repo
            .find_by_id(artifact_id)
            .await
            .map_err(QuantError::from)?;
        if let Some(info) = &found {
            validate_payload_shape(info)?;
        }
        Ok(found)
    }

    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> QuantResult<Paginated<CalibrationArtifactInfo>> {
        self.calibration_repo
            .page(query)
            .await
            .map_err(QuantError::from)
    }

    async fn mark_active(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> QuantResult<CalibrationArtifactInfo> {
        self.calibration_repo
            .mark_active(artifact_id)
            .await
            .map_err(QuantError::from)
    }
}

impl BiasTableFitService {
    /// Persist a fitted table as a content-addressed, unified `CalibrationArtifact` row.
    async fn persist(
        &self,
        table: FavoriteLongshotBiasTable,
    ) -> QuantResult<CalibrationArtifactInfo> {
        self.calibration_repo
            .create(table.into())
            .await
            .map_err(QuantError::from)
    }
}

/// Max book staleness for the PIT engine, from the frozen data-quality config.
const fn book_staleness(data_quality: &DataQualityConfig) -> StdDuration {
    StdDuration::from_millis(data_quality.max_book_age_ms)
}

#[cfg(test)]
mod tests {
    use super::validate_payload_shape;
    use chrono::Utc;
    use quant_pivot_models::{
        domain::CalibrationArtifactInfo,
        enums::quant::CalibrationKind,
        types::{CalibrationArtifactId, ContentHash},
    };

    fn base_info(
        kind: CalibrationKind,
        payload_json: serde_json::Value,
    ) -> CalibrationArtifactInfo {
        let hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
        CalibrationArtifactInfo {
            artifact_id: CalibrationArtifactId::from_v7(),
            kind,
            content_hash: hash.clone(),
            fit_window_start: Utc::now(),
            fit_window_end: Utc::now(),
            calibration_split_hash: hash,
            sample_count: 100,
            payload_json,
            active: true,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn rejects_model_score_kind_with_market_price_bias_shaped_payload() {
        // The exact "kind says X, payload is shaped like Y" drift this guard
        // exists to catch before serving a broken payload to the detail view.
        let info = base_info(CalibrationKind::ModelScore, serde_json::json!({}));
        assert!(
            validate_payload_shape(&info).is_err(),
            "an empty object is not a valid ModelScoreCalibrationPayload"
        );
    }

    #[test]
    fn rejects_market_price_bias_kind_with_non_map_payload() {
        let info = base_info(
            CalibrationKind::MarketPriceBias,
            serde_json::json!("not-a-map"),
        );
        assert!(
            validate_payload_shape(&info).is_err(),
            "a bare string is not a valid by-category bias curve map"
        );
    }

    #[test]
    fn accepts_well_formed_market_price_bias_payload() {
        // An empty map is a structurally valid (if practically inert) payload.
        let info = base_info(CalibrationKind::MarketPriceBias, serde_json::json!({}));
        assert!(validate_payload_shape(&info).is_ok());
    }
}
