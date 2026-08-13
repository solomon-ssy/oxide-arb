//! Promoted scenario-generation models and report-specific joint scenarios.

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::quant::OutcomeSide,
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, MarketId, ModelVersionId,
        PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId, SchemaVersion, Shares,
        TokenId, Usd,
    },
};

/// Provenance class of one joint market-outcome scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioScenarioKind {
    PitBootstrap,
    CalibrationUncertainty,
    StructuralStress,
}

/// Statistical visibility under which one concrete scenario artifact was materialized.
///
/// Production/report artifacts must be point-in-time visible. Purged cross-validation may use a
/// fold-local estimator fitted on a disjoint population that is not chronologically prior to every
/// held-out decision; the exact fit and held-out populations are therefore content-bound here and
/// the artifact is restricted to a historical replay account by the portfolio planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum PortfolioScenarioVisibility {
    PointInTime,
    /// Historical decision replay under a policy/scenario binding governed
    /// after the decision window. Scenario data must still predate every
    /// replayed decision; only the governance clock is evaluated against this
    /// independently frozen boundary.
    HistoricalReplay {
        governance_frozen_at: DateTime<Utc>,
    },
    PurgedCrossValidation {
        fit_evidence_hash: ContentHash,
        test_groups_hash: ContentHash,
    },
}

/// Governed resampling method used to fit the joint scenario-state catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum PortfolioScenarioResamplingMethod {
    /// Politis-Romano stationary bootstrap over aligned multivariate PIT residual vectors.
    ///
    /// `expected_block_length` controls the geometric restart probability. The
    /// independently governed `scenario_horizon_buckets` controls the number
    /// of common-time joint observations compounded into one scenario. Keeping
    /// these parameters separate is required: block dependence length is not a
    /// forecast or capital-occupancy horizon.
    StationaryBootstrap {
        expected_block_length: u32,
        scenario_horizon_buckets: u32,
    },
    /// Outer-CPCV fold-local scenario catalog derived from allocation-
    /// independent residual ranks on a purge/embargo-isolated calibration
    /// holdout. This method is ephemeral validation evidence and is never
    /// eligible for production Route promotion.
    CrossFittedResidualQuantiles { minimum_observations: u32 },
}

/// One route factor in a long-lived joint scenario state.
///
/// Probability ranks and all scale factors use integer basis points. The
/// generator combines the systematic rank with a content-addressed
/// market-specific rank, then compares it with the candidate's calibrated
/// probability after the signed uncertainty shift. Cash recovery and release
/// multipliers are fitted from PIT Trade Policy replay, never from raw score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioRouteFactor {
    pub route: BuyModelRoute,
    pub systematic_quantile_bps: u32,
    pub systematic_weight_bps: u32,
    pub calibrated_probability_shift_bps: i32,
    /// Quantile within the Route calibration artifact's Wilson interval used
    /// for split-resolution mass in uncertainty/stress states. PIT bootstrap
    /// states use the empirical split rate directly.
    pub split_probability_quantile_bps: u32,
    pub win_cash_recovery_bps: u32,
    pub split_cash_recovery_bps: u32,
    pub loss_cash_recovery_bps: u32,
    pub executable_share_bps: u32,
    pub capital_release_multiplier_bps: u32,
    pub factor_lineage_hash: ContentHash,
}

/// Explicit terminal payout state selected for one scenario leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioPayoutState {
    Loss,
    Split,
    Win,
}

/// One reusable joint latent state produced from PIT residual blocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioModelState {
    pub scenario_index: u32,
    pub kind: PortfolioScenarioKind,
    pub label: String,
    pub scenario_state_hash: ContentHash,
    pub route_factors: Vec<PortfolioScenarioRouteFactor>,
}

/// Exact statistical source of one Route residual panel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum PortfolioScenarioFitEvidence {
    /// Complete reconstructed outer-CPCV path used for final promotion refit.
    CpcvPath {
        backtest_path_set_id: BacktestPathSetId,
        backtest_path_set_hash: ContentHash,
        representative_path_index: u32,
    },
    /// Inner purge/embargo-isolated holdout used by one ephemeral outer fold.
    NestedFold {
        estimator_identity_hash: ContentHash,
        model_fit_groups_hash: ContentHash,
        calibration_fit_groups_hash: ContentHash,
        scenario_fit_groups_hash: ContentHash,
    },
}

/// Exact model identities on both sides of one probability-calibration edge.
///
/// `evaluated_*` identifies the model whose calibrated residuals were consumed
/// by the scenario fit. `calibration_source_*` identifies the immutable parent
/// estimator against which the probability calibrator was fitted. Production
/// promotion requires those identities to be distinct; a nested CPCV fold may
/// use the same ephemeral estimator on both sides because its calibrator is
/// fitted and consumed inside the fold rather than sealed as a model version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioRouteModelLineage {
    pub evaluated_model_version_id: ModelVersionId,
    pub evaluated_model_artifact_hash: ContentHash,
    pub evaluated_serving_contract_hash: ContentHash,
    pub calibration_source_model_version_id: ModelVersionId,
    pub calibration_source_model_artifact_hash: ContentHash,
    pub calibration_source_serving_contract_hash: ContentHash,
}

/// Route-owned OOS and calibration evidence consumed by one joint model fit.
///
/// A promoted scenario model is invalid if this ordered vector does not cover
/// its exact Route set. Concrete market identities are deliberately absent so
/// the model can remain valid across future report universes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioRouteFitLineage {
    pub route: BuyModelRoute,
    pub model_lineage: PortfolioScenarioRouteModelLineage,
    pub fit_evidence: PortfolioScenarioFitEvidence,
    pub calibration_artifact_id: CalibrationArtifactId,
    pub calibration_artifact_hash: ContentHash,
    pub trade_policy_contract_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
}

impl PortfolioScenarioModelState {
    /// Recompute the content identity of the reusable joint latent state.
    pub fn recomputed_state_hash(&self) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            scenario_index: u32,
            kind: PortfolioScenarioKind,
            label: &'a str,
            route_factors: &'a [PortfolioScenarioRouteFactor],
        }
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-model-state",
            1,
            &Preimage {
                scenario_index: self.scenario_index,
                kind: self.kind,
                label: &self.label,
                route_factors: &self.route_factors,
            },
        )?)
    }
}

/// One immutable joint outcome state in artifact order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenario {
    pub scenario_index: u32,
    pub kind: PortfolioScenarioKind,
    pub label: String,
    /// Content identity of the promoted latent state expanded by this concrete scenario.
    pub scenario_model_state_hash: ContentHash,
    /// Content identity of the complete joint route/market shock state.
    pub scenario_state_hash: ContentHash,
    /// Trade-policy-evaluated exit states for every executable market leg.
    ///
    /// The artifact generator walks PIT exit books and applies the exact
    /// promoted trade-policy contract before publication. Runtime planning only
    /// performs an exact lookup and scales the already discounted per-share
    /// cash value; it never invents a price path or assumes independence.
    pub market_outcomes: Vec<ScenarioMarketOutcome>,
}

impl PortfolioScenario {
    /// Recompute the Merkle root of the complete joint state.
    ///
    /// Every ordered outcome leaf must be verified independently before this
    /// root is trusted. Binding only the ordered leaf hashes prevents parent
    /// hashing from repeatedly serializing the same financial payload.
    pub fn recomputed_state_hash(&self) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            scenario_index: u32,
            kind: PortfolioScenarioKind,
            label: &'a str,
            scenario_model_state_hash: ContentHash,
            outcome_hashes: &'a [ContentHash],
        }
        let outcome_hashes = self
            .market_outcomes
            .iter()
            .map(|outcome| outcome.outcome_lineage_hash)
            .collect::<Vec<_>>();
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-state",
            1,
            &Preimage {
                scenario_index: self.scenario_index,
                kind: self.kind,
                label: &self.label,
                scenario_model_state_hash: self.scenario_model_state_hash,
                outcome_hashes: &outcome_hashes,
            },
        )?)
    }
}

/// One trade-policy-evaluated market leg inside a joint scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioMarketOutcome {
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    /// Loss, canonical 50/50 split, or winner-take-all payout. This state is
    /// persisted explicitly so split resolution can never be inferred from a
    /// rounded cash value or silently collapsed into a binary outcome.
    pub payout_state: ScenarioPayoutState,
    /// Maximum shares for which the PIT exit-book walk proved this cash value.
    pub max_executable_exit_shares: Shares,
    /// Discounted account cash received per exited or settled share, net of
    /// scenario-specific exit fees and capital cost.
    pub discounted_exit_cash_per_share_usd: Usd,
    /// Elapsed seconds until the trade policy releases the tier's capital.
    pub capital_release_secs: u64,
    /// Hash of the PIT path, executable exit walk, settlement state, and policy
    /// decision supplied to the scenario materializer.
    pub source_lineage_hash: ContentHash,
    /// Hash of the fitted Route factor used to transform the source evidence.
    pub scenario_factor_lineage_hash: ContentHash,
    /// Domain-separated content hash of this complete outcome leaf.
    pub outcome_lineage_hash: ContentHash,
}

impl ScenarioMarketOutcome {
    /// Recompute the complete outcome leaf hash under its exact model state.
    pub fn recomputed_lineage_hash(
        &self,
        scenario_model_content_hash: ContentHash,
        scenario_model_state_hash: ContentHash,
    ) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            scenario_model_content_hash: ContentHash,
            scenario_model_state_hash: ContentHash,
            route: BuyModelRoute,
            market_id: &'a MarketId,
            token_id: &'a TokenId,
            outcome_side: OutcomeSide,
            payout_state: ScenarioPayoutState,
            max_executable_exit_shares: Shares,
            discounted_exit_cash_per_share_usd: Usd,
            capital_release_secs: u64,
            source_lineage_hash: ContentHash,
            scenario_factor_lineage_hash: ContentHash,
        }
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/report-scenario-market-outcome",
            1,
            &Preimage {
                scenario_model_content_hash,
                scenario_model_state_hash,
                route: self.route,
                market_id: &self.market_id,
                token_id: &self.token_id,
                outcome_side: self.outcome_side,
                payout_state: self.payout_state,
                max_executable_exit_shares: self.max_executable_exit_shares,
                discounted_exit_cash_per_share_usd: self.discounted_exit_cash_per_share_usd,
                capital_release_secs: self.capital_release_secs,
                source_lineage_hash: self.source_lineage_hash,
                scenario_factor_lineage_hash: self.scenario_factor_lineage_hash,
            },
        )?)
    }
}

/// Integer probability weight for one scenario inside one allowed distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioWeight {
    pub scenario_index: u32,
    /// Probability mass in basis points. Every distribution sums to exactly 10,000.
    pub probability_bps: u32,
}

/// One probability distribution admitted by distributional robustness governance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioDistribution {
    pub distribution_id: String,
    pub nominal: bool,
    pub weights: Vec<ScenarioWeight>,
}

/// Capital cost / discount point frozen into the scenario contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscountCurvePoint {
    pub end_secs: u64,
    pub annualized_cost_bps: u32,
}

/// Long-lived, promoted generator compatible with one exact ordered Route contract set.
///
/// This artifact deliberately contains no concrete market or token identity. It
/// can therefore remain valid across report universes while every report still
/// materializes and persists a separate concrete scenario artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioModelArtifact {
    pub portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId,
    pub schema_version: SchemaVersion,
    /// Latest source observation admitted by the PIT model fit.
    pub as_of: DateTime<Utc>,
    /// Inclusive beginning of the common OOS window shared by every Route.
    pub fit_window_start: DateTime<Utc>,
    /// Canonical aggregation bucket used before joint vector resampling.
    pub time_bucket_secs: u64,
    pub ordered_routes: Vec<BuyModelRoute>,
    pub route_set_digest: ContentHash,
    pub serving_contract_digest: ContentHash,
    pub calibration_contract_digest: ContentHash,
    pub trade_policy_contract_digest: ContentHash,
    /// Canonical ordered capital-time boundary grid shared with the discount curve.
    /// This is distinct from the statistical resampling interval in `time_bucket_secs`.
    pub capital_time_bucket_contract_digest: ContentHash,
    /// Precommitted random stream shared by every estimator evaluated on the
    /// same PIT population. It is derived from observation identities and the
    /// sampling contract, never from model outputs, residual values, artifact
    /// ids, or audit lineage. Scenario generation uses this stream for both
    /// block-resampling indices and market-level idiosyncratic draws.
    pub scenario_random_stream_hash: ContentHash,
    pub pit_residual_panel_hash: ContentHash,
    pub calibration_uncertainty_model_hash: ContentHash,
    pub stress_catalog_hash: ContentHash,
    pub resampling_method: PortfolioScenarioResamplingMethod,
    pub route_fit_lineage: Vec<PortfolioScenarioRouteFitLineage>,
    pub states: Vec<PortfolioScenarioModelState>,
    pub distributions: Vec<ScenarioDistribution>,
    pub discount_curve: Vec<DiscountCurvePoint>,
    pub content_hash: ContentHash,
}

impl PortfolioScenarioModelArtifact {
    /// Recompute the canonical model hash, excluding its derived id and stored hash.
    pub fn recomputed_hash(&self) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            schema_version: SchemaVersion,
            as_of: DateTime<Utc>,
            fit_window_start: DateTime<Utc>,
            time_bucket_secs: u64,
            ordered_routes: &'a [BuyModelRoute],
            route_set_digest: ContentHash,
            serving_contract_digest: ContentHash,
            calibration_contract_digest: ContentHash,
            trade_policy_contract_digest: ContentHash,
            capital_time_bucket_contract_digest: ContentHash,
            scenario_random_stream_hash: ContentHash,
            pit_residual_panel_hash: ContentHash,
            calibration_uncertainty_model_hash: ContentHash,
            stress_catalog_hash: ContentHash,
            resampling_method: PortfolioScenarioResamplingMethod,
            route_fit_lineage: &'a [PortfolioScenarioRouteFitLineage],
            state_hashes: &'a [ContentHash],
            distributions: &'a [ScenarioDistribution],
            discount_curve: &'a [DiscountCurvePoint],
        }
        let state_hashes = self
            .states
            .iter()
            .map(|state| state.scenario_state_hash)
            .collect::<Vec<_>>();
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-model-artifact",
            2,
            &Preimage {
                schema_version: self.schema_version,
                as_of: self.as_of,
                fit_window_start: self.fit_window_start,
                time_bucket_secs: self.time_bucket_secs,
                ordered_routes: &self.ordered_routes,
                route_set_digest: self.route_set_digest,
                serving_contract_digest: self.serving_contract_digest,
                calibration_contract_digest: self.calibration_contract_digest,
                trade_policy_contract_digest: self.trade_policy_contract_digest,
                capital_time_bucket_contract_digest: self.capital_time_bucket_contract_digest,
                scenario_random_stream_hash: self.scenario_random_stream_hash,
                pit_residual_panel_hash: self.pit_residual_panel_hash,
                calibration_uncertainty_model_hash: self.calibration_uncertainty_model_hash,
                stress_catalog_hash: self.stress_catalog_hash,
                resampling_method: self.resampling_method,
                route_fit_lineage: &self.route_fit_lineage,
                state_hashes: &state_hashes,
                distributions: &self.distributions,
                discount_curve: &self.discount_curve,
            },
        )?)
    }
}

/// One market/outcome member of an artifact-governed structural exclusivity set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StructuralOutcomeRef {
    pub market_id: MarketId,
    pub outcome_side: OutcomeSide,
}

/// At most one member may receive new capital in a global plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StructuralExclusivityGroup {
    pub group_id: String,
    pub members: Vec<StructuralOutcomeRef>,
}

/// Report-specific concrete scenario artifact for one frozen market universe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioArtifact {
    pub portfolio_scenario_artifact_id: PortfolioScenarioArtifactId,
    pub portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId,
    pub scenario_model_content_hash: ContentHash,
    pub schema_version: SchemaVersion,
    /// Decision boundary at which the concrete market universe was frozen.
    pub decision_at: DateTime<Utc>,
    /// Statistical visibility contract used to materialize this artifact.
    pub visibility: PortfolioScenarioVisibility,
    /// Canonical identity of candidates, held positions, books, and policy inputs.
    pub input_universe_hash: ContentHash,
    pub ordered_routes: Vec<BuyModelRoute>,
    pub route_set_digest: ContentHash,
    pub serving_contract_digest: ContentHash,
    pub calibration_contract_digest: ContentHash,
    pub trade_policy_contract_digest: ContentHash,
    /// Canonical ordered capital-time boundary grid inherited from the promoted model.
    pub capital_time_bucket_contract_digest: ContentHash,
    pub scenarios: Vec<PortfolioScenario>,
    pub distributions: Vec<ScenarioDistribution>,
    pub structural_exclusivity: Vec<StructuralExclusivityGroup>,
    pub discount_curve: Vec<DiscountCurvePoint>,
    pub content_hash: ContentHash,
}

impl PortfolioScenarioArtifact {
    #[must_use]
    pub fn nominal_distribution(&self) -> Option<&ScenarioDistribution> {
        let mut nominal = self
            .distributions
            .iter()
            .filter(|distribution| distribution.nominal);
        let first = nominal.next()?;
        nominal.next().is_none().then_some(first)
    }

    /// Recompute the canonical artifact Merkle root, excluding its derived id and stored hash.
    ///
    /// Callers accepting a deserialized artifact must verify every outcome and
    /// scenario leaf before trusting this root.
    pub fn recomputed_hash(&self) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId,
            scenario_model_content_hash: ContentHash,
            schema_version: SchemaVersion,
            decision_at: DateTime<Utc>,
            visibility: PortfolioScenarioVisibility,
            input_universe_hash: ContentHash,
            ordered_routes: &'a [BuyModelRoute],
            route_set_digest: ContentHash,
            serving_contract_digest: ContentHash,
            calibration_contract_digest: ContentHash,
            trade_policy_contract_digest: ContentHash,
            capital_time_bucket_contract_digest: ContentHash,
            scenario_hashes: &'a [ContentHash],
            distributions: &'a [ScenarioDistribution],
            structural_exclusivity: &'a [StructuralExclusivityGroup],
            discount_curve: &'a [DiscountCurvePoint],
        }
        let scenario_hashes = self
            .scenarios
            .iter()
            .map(|scenario| scenario.scenario_state_hash)
            .collect::<Vec<_>>();
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-artifact",
            1,
            &Preimage {
                portfolio_scenario_model_artifact_id: self.portfolio_scenario_model_artifact_id,
                scenario_model_content_hash: self.scenario_model_content_hash,
                schema_version: self.schema_version,
                decision_at: self.decision_at,
                visibility: self.visibility,
                input_universe_hash: self.input_universe_hash,
                ordered_routes: &self.ordered_routes,
                route_set_digest: self.route_set_digest,
                serving_contract_digest: self.serving_contract_digest,
                calibration_contract_digest: self.calibration_contract_digest,
                trade_policy_contract_digest: self.trade_policy_contract_digest,
                capital_time_bucket_contract_digest: self.capital_time_bucket_contract_digest,
                scenario_hashes: &scenario_hashes,
                distributions: &self.distributions,
                structural_exclusivity: &self.structural_exclusivity,
                discount_curve: &self.discount_curve,
            },
        )?)
    }
}
