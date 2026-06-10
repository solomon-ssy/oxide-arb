//! Background calibration updater with Gamma/CTF cross-check.
//!
//! The `CalibrationDataSource` trait is implemented by the core layer to
//! bridge between the algorithm crate (pure computation) and I/O (DB, APIs).

use super::{calibrator::ResolutionCalibrator, prior::estimate_mom_prior, types::CalibrationEntry};
use arc_swap::ArcSwap;
use oxide_arb_error::algorithm::AlgoError;
use oxide_arb_models::{
    clickhouse::{CalibrationSnapshotRow, ChDecimal64, ChProbability, ChSchemaVersion},
    domain::calibration::{BucketKey, UpsertCalibration},
    enums::clickhouse::{ChDurationBucket, ChFactSource, ChMarketCategory, ChPriceZone},
    runtime_config::CalibrationConfig,
    types::MarketId,
};
use sha2::{Digest, Sha256};
use std::{fmt::Write, sync::Arc};

/// External data source for calibration — injected by `oxide-arb-core`.
///
/// All methods are async because they involve DB queries or API calls.
#[async_trait::async_trait]
pub trait CalibrationDataSource: Send + Sync + 'static {
    /// Fetch all unresolved calibration outcomes from the database.
    async fn get_unresolved_outcomes(&self) -> Result<Vec<UnresolvedOutcome>, AlgoError>;

    /// Query Gamma API for market resolution status.
    async fn check_gamma_resolution(&self, market_id: &MarketId)
    -> Result<Option<bool>, AlgoError>;

    /// Query CTF on-chain oracle for market resolution status.
    async fn check_ctf_resolution(&self, market_id: &MarketId) -> Result<Option<bool>, AlgoError>;

    /// Persist updated calibration bucket entries to the database.
    async fn upsert_buckets(&self, entries: &[UpsertCalibration]) -> Result<(), AlgoError>;

    /// Persist point-in-time calibration snapshots for evidence materialization.
    async fn write_calibration_snapshots(
        &self,
        _snapshots: &[CalibrationSnapshotRow],
    ) -> Result<(), AlgoError> {
        Ok(())
    }

    /// Mark an outcome as resolved in the database.
    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), AlgoError>;
}

/// An outcome awaiting resolution confirmation.
#[derive(Debug, Clone)]
pub struct UnresolvedOutcome {
    pub outcome_id: i64,
    pub market_id: MarketId,
    pub bucket_key: BucketKey,
    pub predicted_yes: bool,
}

/// Statistics from a single calibration update tick.
#[derive(Debug, Clone, Default)]
pub struct UpdateStats {
    pub total_unresolved: u32,
    pub resolved: u32,
    pub gamma_miss: u32,
}

/// Orchestrates periodic calibration reconciliation.
///
/// On each `tick()`:
/// 1. Fetch unresolved outcomes from DB.
/// 2. Check Gamma API for resolution.
/// 3. Cross-check with CTF on-chain oracle (best-effort).
/// 4. Update in-memory calibrator with confirmed outcomes.
/// 5. Re-estimate `MoM` priors if any new data.
/// 6. Persist updated buckets.
pub struct CalibrationUpdater {
    calibrator: Arc<ResolutionCalibrator>,
    data_source: Arc<dyn CalibrationDataSource>,
    config: ArcSwap<CalibrationConfig>,
}

impl CalibrationUpdater {
    /// Create a new updater.
    #[must_use]
    pub fn new(
        calibrator: Arc<ResolutionCalibrator>,
        data_source: Arc<dyn CalibrationDataSource>,
        config: CalibrationConfig,
    ) -> Self {
        Self {
            calibrator,
            data_source,
            config: ArcSwap::from_pointee(config),
        }
    }

    /// Hot-reload the calibration configuration (runtime-config activation).
    ///
    /// Also pushes the new config into the shared calibrator so lookups and
    /// the next prior re-estimation observe consistent parameters. The new
    /// `refresh_interval_secs` is read dynamically by the periodic tick.
    pub fn reload(&self, config: CalibrationConfig) {
        self.calibrator.reload(config.clone());
        self.config.store(Arc::new(config));
    }

    /// Active refresh cadence (seconds); read per tick by the periodic task so
    /// interval changes apply without a restart.
    #[must_use]
    pub fn refresh_interval_secs(&self) -> u64 {
        self.config.load().refresh_interval_secs
    }

    /// Execute one reconciliation cycle.
    pub async fn tick(&self) -> Result<UpdateStats, AlgoError> {
        let unresolved = self.data_source.get_unresolved_outcomes().await?;
        let mut stats = UpdateStats {
            total_unresolved: u32::try_from(unresolved.len()).unwrap_or(u32::MAX),
            ..Default::default()
        };

        for outcome in &unresolved {
            let Ok(Some(gamma_yes)) = self
                .data_source
                .check_gamma_resolution(&outcome.market_id)
                .await
            else {
                stats.gamma_miss += 1;
                continue;
            };

            let ctf_result = self
                .data_source
                .check_ctf_resolution(&outcome.market_id)
                .await;

            let confirmed = match ctf_result {
                Ok(Some(ctf_yes)) => {
                    if ctf_yes != gamma_yes {
                        tracing::warn!(
                            market_id = %outcome.market_id,
                            gamma = gamma_yes,
                            ctf = ctf_yes,
                            "Gamma/CTF disagree — skipping"
                        );
                        continue;
                    }
                    gamma_yes
                }
                _ => gamma_yes,
            };

            let was_correct = confirmed == outcome.predicted_yes;
            self.calibrator
                .record_outcome(&outcome.bucket_key, was_correct);

            self.data_source
                .resolve_outcome(outcome.outcome_id, confirmed)
                .await?;
            stats.resolved += 1;
        }

        if stats.resolved > 0 {
            self.update_priors().await?;
        }

        Ok(stats)
    }

    /// Re-estimate `MoM` priors and update sparse buckets.
    async fn update_priors(&self) -> Result<(), AlgoError> {
        let config = self.config.load_full();
        let all_entries = self.calibrator.all_entries();

        let (alpha, beta) = estimate_mom_prior(
            &all_entries,
            config.min_sample_size,
            config.bootstrap_alpha,
            config.bootstrap_beta,
        );

        for mut entry in self.calibrator.buckets().iter_mut() {
            if entry.total_count < config.min_sample_size {
                entry.alpha_prior = alpha;
                entry.beta_prior = beta;
            }
        }

        let upserts: Vec<UpsertCalibration> = all_entries
            .iter()
            .map(CalibrationEntry::to_upsert)
            .collect();
        self.data_source.upsert_buckets(&upserts).await?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let snapshots = upserts
            .iter()
            .enumerate()
            .map(|(idx, entry)| CalibrationSnapshotRow {
                category: ChMarketCategory::from(entry.category),
                price_zone: ChPriceZone::from(entry.price_zone),
                duration_bucket: ChDurationBucket::from(entry.duration_bucket),
                total_count: u32::try_from(entry.total_count).unwrap_or(0),
                correct_count: u32::try_from(entry.correct_count).unwrap_or(0),
                alpha_prior: ChDecimal64::from(entry.alpha_prior.inner()),
                beta_prior: ChDecimal64::from(entry.beta_prior.inner()),
                posterior_mean: entry
                    .posterior_mean
                    .map(|value| ChProbability::from(value.inner())),
                fallback_tier: 1,
                config_hash: calibration_config_hash(&config),
                snapshot_hash: calibration_snapshot_hash(entry),
                event_time: now_ms,
                ingestion_time: now_ms,
                sequence: u64::try_from(idx).unwrap_or(u64::MAX),
                source: ChFactSource::CalibrationUpdater,
                schema_version: ChSchemaVersion(2),
            })
            .collect::<Vec<_>>();
        self.data_source
            .write_calibration_snapshots(&snapshots)
            .await?;
        Ok(())
    }
}

fn calibration_snapshot_hash(entry: &UpsertCalibration) -> String {
    let mut canonical = String::new();
    write!(
        canonical,
        "{}|{}|{}|{}|{}|{}|{}|{}",
        entry.category,
        entry.price_zone,
        entry.duration_bucket,
        entry.total_count,
        entry.correct_count,
        entry.alpha_prior,
        entry.beta_prior,
        entry
            .posterior_mean
            .map_or_else(|| "none".to_owned(), |value| value.to_string())
    )
    .expect("writing to String cannot fail");
    let digest = Sha256::digest(canonical.as_bytes());
    format!("{digest:x}")
}

fn calibration_config_hash(config: &CalibrationConfig) -> String {
    let mut canonical = String::new();
    write!(
        canonical,
        "{}|{}|{}|{}|{}|{}|{}",
        config.min_sample_size,
        config.refresh_interval_secs,
        config.fusion_prior_strength,
        config.fused_p_floor,
        config.fused_p_ceiling,
        config.bootstrap_alpha,
        config.bootstrap_beta
    )
    .expect("writing to String cannot fail");
    let digest = Sha256::digest(canonical.as_bytes());
    format!("{digest:x}")
}
