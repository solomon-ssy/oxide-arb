//! Favorite-longshot bias-table fit orchestration (Phase 11.2.1).
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

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        BiasTableFitJobParams, BiasTableFitOutcome, BiasTableListQuery,
        FavoriteLongshotBiasTableInfo, FavoriteLongshotFitPort, JobProgressSink,
        NewFavoriteLongshotBiasTable, Paginated, TrainingDatasetListQuery, WindowBoundsError,
        query::TimeWindow,
    },
    enums::common::MarketCategory,
    enums::quant::TrainingDatasetStatus,
    hashing::CanonicalDigest,
    runtime_config::{
        DataQualityConfig, DomainConfig, FactorsConfig, FavoriteLongshotConfig, RuntimeConfig,
    },
    types::{ContentHash, FavoriteLongshotBiasTableId, MarketId, ResearchJobProgress, TokenId},
};
use quant_pivot_repository::traits::{
    EventRepository, FavoriteLongshotBiasTableRepository, MarketLinkageRepository,
    MarketRepository, QuantFactReadRepository, RuntimeConfigVersionRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    features::PitView,
    model::favorite_longshot::{BiasFitConfig, BiasSample, FavoriteLongshotBiasTable},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::json;

use crate::pipeline::historical_window::{HistoricalWindowLoader, ReplaySample, WindowSpec};

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
    repo: &dyn FavoriteLongshotBiasTableRepository,
    factors: &FactorsConfig,
) -> QuantResult<Option<Arc<FavoriteLongshotBiasTable>>> {
    let Some(raw) = factors.structural.favorite_longshot.bias_table_ref.as_ref() else {
        return Ok(None);
    };
    let id: FavoriteLongshotBiasTableId = raw.trim().parse().map_err(|error| {
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

/// Core favorite-longshot bias-table fitter + read port.
pub struct FavoriteLongshotFitService {
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    event_repo: Arc<dyn EventRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    bias_table_repo: Arc<dyn FavoriteLongshotBiasTableRepository>,
    runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
}

impl FavoriteLongshotFitService {
    /// Wire the fitter from its persistence ports.
    #[must_use]
    pub const fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        market_repo: Arc<dyn MarketRepository>,
        event_repo: Arc<dyn EventRepository>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        bias_table_repo: Arc<dyn FavoriteLongshotBiasTableRepository>,
        runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
        training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    ) -> Self {
        Self {
            fact_read,
            market_repo,
            event_repo,
            linkage_repo,
            bias_table_repo,
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
impl FavoriteLongshotFitPort for FavoriteLongshotFitService {
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
        self.assert_disjoint_from_training_datasets(&window).await?;

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
        let calibration_split_hash = split_hash(&window, &samples)?;

        progress.report(ResearchJobProgress::with_total("fit", 0, 1));
        let Some(table) =
            FavoriteLongshotBiasTable::fit(&samples, window, calibration_split_hash, &fit_config)?
        else {
            // Fail-closed: no category/ttr curve qualified, so no artifact is minted.
            return Ok(BiasTableFitOutcome {
                bias_table_id: None,
                category_count: 0,
                total_sample_count,
            });
        };

        progress.report(ResearchJobProgress::with_total("persist", 0, 1));
        let persisted = self.persist(&table, total_sample_count).await?;
        Ok(BiasTableFitOutcome {
            bias_table_id: Some(persisted.bias_table_id),
            category_count: persisted.category_count.cast_unsigned(),
            total_sample_count,
        })
    }

    async fn find(
        &self,
        bias_table_id: &FavoriteLongshotBiasTableId,
    ) -> QuantResult<Option<FavoriteLongshotBiasTableInfo>> {
        self.bias_table_repo
            .find_by_id(bias_table_id)
            .await
            .map_err(QuantError::from)
    }

    async fn page(
        &self,
        query: BiasTableListQuery,
    ) -> QuantResult<Paginated<FavoriteLongshotBiasTableInfo>> {
        self.bias_table_repo
            .page(query)
            .await
            .map_err(QuantError::from)
    }
}

impl FavoriteLongshotFitService {
    /// Persist a fitted table as a content-addressed ledger row.
    async fn persist(
        &self,
        table: &FavoriteLongshotBiasTable,
        total_sample_count: u64,
    ) -> QuantResult<FavoriteLongshotBiasTableInfo> {
        let by_category = serde_json::to_value(&table.by_category).map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("bias-table payload serialization failed: {error}"),
            })
        })?;
        let category_count = i64::try_from(table.by_category.len()).unwrap_or(i64::MAX);
        let row = NewFavoriteLongshotBiasTable {
            bias_table_id: table.table_id.clone(),
            content_hash: table.content_hash.clone(),
            fit_window_start: table.fit_window.from,
            fit_window_end: table.fit_window.to,
            calibration_split_hash: table.calibration_split_hash.clone(),
            category_count,
            total_sample_count: i64::try_from(total_sample_count).unwrap_or(i64::MAX),
            by_category,
        };
        self.bias_table_repo
            .create(row)
            .await
            .map_err(QuantError::from)
    }
}

/// Max book staleness for the PIT engine, from the frozen data-quality config.
const fn book_staleness(data_quality: &DataQualityConfig) -> StdDuration {
    StdDuration::from_millis(data_quality.max_book_age_ms)
}

/// Content hash anchoring the fit's calibration split to the exact sample set.
///
/// Covers the window and the sorted, distinct `(market_id, sampled_at)` keys
/// that fed the fit — a deterministic provenance anchor for leakage audits.
/// Full purged/embargoed CPCV splits are Phase 11.5.
fn split_hash(window: &TimeWindow, samples: &[BiasSample]) -> QuantResult<ContentHash> {
    let mut keys: Vec<(String, DateTime<Utc>)> = samples
        .iter()
        .map(|s| (s.market_id.to_string(), s.sampled_at))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let sample_keys: Vec<_> = keys
        .iter()
        .map(|(market_id, sampled_at)| {
            json!({
                "market_id": market_id,
                "sampled_at": sampled_at,
            })
        })
        .collect();
    CanonicalDigest::content_hash_json(&json!({
        "window_start": window.from,
        "window_end": window.to,
        "sample_count": samples.len() as u64,
        "sample_keys": sample_keys,
    }))
    .map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("calibration split hash failed: {error}"),
        })
    })
}

impl FavoriteLongshotFitService {
    /// Fail closed when the fit window overlaps a `built` or `ready` training
    /// dataset — the bias table must not be fit on the same spine the model
    /// trains on.
    async fn assert_disjoint_from_training_datasets(&self, window: &TimeWindow) -> QuantResult<()> {
        let mut query = TrainingDatasetListQuery::default();
        query.page.size = 100;
        let mut page = 1_u64;
        loop {
            query.page.page = page;
            let batch = self
                .training_dataset_repo
                .page(query.clone())
                .await
                .map_err(QuantError::from)?;
            for dataset in &batch.items {
                if !matches!(
                    dataset.status,
                    TrainingDatasetStatus::Built | TrainingDatasetStatus::Ready
                ) {
                    continue;
                }
                if half_open_windows_overlap(
                    window.from,
                    window.to,
                    dataset.window_start,
                    dataset.window_end,
                ) {
                    return Err(QuantError::from(ResearchError::DatasetBuild {
                        detail: format!(
                            "bias-table fit window [{}, {}) overlaps training dataset {} \
                             [{}, {}) in status `{}` — fit and train windows must be disjoint",
                            window.from,
                            window.to,
                            dataset.training_dataset_id,
                            dataset.window_start,
                            dataset.window_end,
                            dataset.status,
                        ),
                    }));
                }
            }
            if page.saturating_mul(query.page.size) >= batch.total {
                break;
            }
            page += 1;
        }
        Ok(())
    }
}

/// True when half-open intervals `[start, end)` intersect.
fn half_open_windows_overlap(
    a_start: DateTime<Utc>,
    a_end: DateTime<Utc>,
    b_start: DateTime<Utc>,
    b_end: DateTime<Utc>,
) -> bool {
    a_start < b_end && b_start < a_end
}
