use crate::infra::oracle_health_tracker::OracleHealthTracker;
use oxide_arb_algorithm::calibration::updater::{CalibrationDataSource, UnresolvedOutcome};
use oxide_arb_api::{
    gamma::GammaClient,
    oracle::{VotingOracle, types::ResolutionVerdict},
};
use oxide_arb_error::algorithm::AlgoError;
use oxide_arb_models::{
    domain::calibration::{BucketKey, UpsertCalibration},
    types::MarketId,
};
use oxide_arb_repository::{postgres::PgCalibrationRepository, traits::CalibrationRepository};
use std::sync::Arc;

pub struct CoreCalibrationDataSource {
    calibration_repo: Arc<PgCalibrationRepository>,
    gamma_client: Arc<GammaClient>,
    voting_oracle: Arc<VotingOracle>,
    oracle_health_tracker: Arc<OracleHealthTracker>,
}

impl CoreCalibrationDataSource {
    pub const fn new(
        calibration_repo: Arc<PgCalibrationRepository>,
        gamma_client: Arc<GammaClient>,
        voting_oracle: Arc<VotingOracle>,
        oracle_health_tracker: Arc<OracleHealthTracker>,
    ) -> Self {
        Self {
            calibration_repo,
            gamma_client,
            voting_oracle,
            oracle_health_tracker,
        }
    }
}

#[async_trait::async_trait]
impl CalibrationDataSource for CoreCalibrationDataSource {
    async fn get_unresolved_outcomes(&self) -> Result<Vec<UnresolvedOutcome>, AlgoError> {
        let outcomes = self
            .calibration_repo
            .get_unresolved_outcomes()
            .await
            .map_err(|e| AlgoError::DataSource(e.to_string()))?;

        Ok(outcomes
            .into_iter()
            .map(|m| UnresolvedOutcome {
                outcome_id: m.id,
                market_id: m.market_id,
                bucket_key: BucketKey {
                    category: m.category,
                    price_zone: m.price_zone,
                    duration_bucket: m.duration_bucket,
                },
                predicted_yes: m.predicted_yes,
            })
            .collect())
    }

    async fn check_gamma_resolution(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<bool>, AlgoError> {
        let result = self
            .gamma_client
            .get_resolution_status(market_id)
            .await
            .map_err(|e| AlgoError::DataSource(e.to_string()))?;

        let success = result.is_some();
        self.oracle_health_tracker.record("gamma", success);

        Ok(result.and_then(|r| r.winning_outcome.as_deref().map(|o| o == "Yes" || o == "1")))
    }

    async fn check_ctf_resolution(&self, market_id: &MarketId) -> Result<Option<bool>, AlgoError> {
        let verdict = self
            .voting_oracle
            .resolve(market_id, market_id.as_str())
            .await
            .map_err(|e| AlgoError::DataSource(e.to_string()))?;

        match verdict {
            ResolutionVerdict::Resolved { actual_yes, .. } => {
                self.oracle_health_tracker.record("ctf", true);
                Ok(Some(actual_yes))
            }
            ResolutionVerdict::Disputed { .. } => {
                self.oracle_health_tracker.record("ctf", true);
                Ok(None)
            }
            ResolutionVerdict::Unresolved { .. } => {
                self.oracle_health_tracker.record("ctf", false);
                Ok(None)
            }
        }
    }

    async fn upsert_buckets(&self, entries: &[UpsertCalibration]) -> Result<(), AlgoError> {
        for entry in entries {
            self.calibration_repo
                .upsert(entry.clone())
                .await
                .map_err(|e| AlgoError::DataSource(e.to_string()))?;
        }
        Ok(())
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), AlgoError> {
        self.calibration_repo
            .resolve_outcome(outcome_id, actual_yes)
            .await
            .map_err(|e| AlgoError::DataSource(e.to_string()))
    }
}
