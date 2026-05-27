//! Background calibration updater with Gamma/CTF cross-check.
//!
//! The `CalibrationDataSource` trait is implemented by the core layer to
//! bridge between the algorithm crate (pure computation) and I/O (DB, APIs).

use super::{calibrator::ResolutionCalibrator, prior::estimate_mom_prior, types::CalibrationEntry};
use num_traits::ToPrimitive;
use oxide_arb_error::algorithm::AlgoError;
use oxide_arb_models::{
    config::CalibrationConfig,
    domain::calibration::{BucketKey, UpsertCalibration},
    types::MarketId,
};
use std::sync::Arc;

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
    config: CalibrationConfig,
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
            config,
        }
    }

    /// Execute one reconciliation cycle.
    pub async fn tick(&self) -> Result<UpdateStats, AlgoError> {
        let unresolved = self.data_source.get_unresolved_outcomes().await?;
        let mut stats = UpdateStats {
            total_unresolved: ToPrimitive::to_u32(&unresolved.len()).unwrap_or(u32::MAX),
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
        let all_entries = self.calibrator.all_entries();

        let (alpha, beta) = estimate_mom_prior(
            &all_entries,
            self.config.min_sample_size,
            self.config.bootstrap_alpha,
            self.config.bootstrap_beta,
        );

        for mut entry in self.calibrator.buckets().iter_mut() {
            if entry.total_count < self.config.min_sample_size {
                entry.alpha_prior = alpha;
                entry.beta_prior = beta;
            }
        }

        let upserts: Vec<UpsertCalibration> = all_entries
            .iter()
            .map(CalibrationEntry::to_upsert)
            .collect();
        self.data_source.upsert_buckets(&upserts).await?;
        Ok(())
    }
}
