//! Deterministic joint scenario-model refit for an atomic Route promotion.

use std::collections::BTreeMap;

use crate::model::ResolvedCalibration;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::{
        BacktestPathSetInfo, CalibrationArtifactInfo, DiscountCurvePoint,
        PortfolioScenarioFitEvidence, PortfolioScenarioKind, PortfolioScenarioModelArtifact,
        PortfolioScenarioModelState, PortfolioScenarioResamplingMethod,
        PortfolioScenarioRouteFactor, PortfolioScenarioRouteFitLineage,
        PortfolioScenarioRouteModelLineage, RepresentedRouteSet, RouteCompatibilityDigests,
        RouteContractHash, ScenarioDistribution,
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioScenarioModelArtifactBinding},
    types::{
        CalibrationArtifactId, ContentHash, MarketId, ModelVersionId,
        PortfolioScenarioModelArtifactId, Probability, SchemaVersion, TokenId,
        backtest::BacktestPath,
        calibration::{ModelScoreCalibrationPayload, ReliabilityBin, ReliabilityReport},
    },
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde::Serialize;

use super::CapitalTimeBucketContract;

const BASIS_POINTS: u32 = 10_000;
const MINIMUM_FOLD_OBSERVATIONS: u32 = 10;

/// Exact Route-owned evidence consumed by one joint refit.
pub struct PortfolioScenarioRouteFitInput<'a> {
    pub route: BuyModelRoute,
    pub model_lineage: PortfolioScenarioRouteModelLineage,
    pub calibration_artifact_id: CalibrationArtifactId,
    pub calibration_artifact_hash: ContentHash,
    pub trade_policy_contract_hash: ContentHash,
    pub prediction_horizon_secs: u64,
    pub path_set: &'a BacktestPathSetInfo,
    pub calibration: &'a CalibrationArtifactInfo,
}

/// Complete immutable preimage for one promoted joint scenario model.
pub struct PortfolioScenarioModelFitInput<'a> {
    pub methodology: &'a PortfolioScenarioMethodology,
    pub represented_routes: &'a RepresentedRouteSet,
    pub compatibility: RouteCompatibilityDigests,
    pub routes: Vec<PortfolioScenarioRouteFitInput<'a>>,
    pub bound_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ScenarioRouteFactorMethodology {
    route: BuyModelRoute,
    systematic_weight_bps: u32,
    split_probability_quantile_bps: u32,
    win_cash_recovery_bps: u32,
    split_cash_recovery_bps: u32,
    loss_cash_recovery_bps: u32,
    executable_share_bps: u32,
    capital_release_multiplier_bps: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ScenarioStateMethodology {
    scenario_index: u32,
    kind: PortfolioScenarioKind,
    label: String,
    route_factors: Vec<ScenarioRouteFactorMethodology>,
}

/// Data-free scenario methodology extracted from a verified promoted model.
///
/// Learned systematic ranks, calibration shifts, lineage hashes, and fit
/// clocks are deliberately absent, so a CPCV fold or prospective promotion
/// cannot reuse them by type. Model, calibration, and Trade Policy contracts
/// belong exclusively to the fit input and the newly sealed artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioScenarioMethodology {
    schema_version: SchemaVersion,
    ordered_routes: Vec<BuyModelRoute>,
    route_set_digest: ContentHash,
    resampling_method: PortfolioScenarioResamplingMethod,
    states: Vec<ScenarioStateMethodology>,
    distributions: Vec<ScenarioDistribution>,
    discount_curve: Vec<DiscountCurvePoint>,
    capital_time_bucket_contract_digest: ContentHash,
    methodology_hash: ContentHash,
}

impl PortfolioScenarioMethodology {
    /// Project only governed, data-free scenario semantics from a verified
    /// promoted artifact.
    pub fn from_promoted(model: &PortfolioScenarioModelArtifact) -> QuantResult<Self> {
        let model_hash = model.recomputed_hash()?;
        let capital_bucket_digest =
            CapitalTimeBucketContract::try_from(model.discount_curve.as_slice())
                .map_err(|error| {
                    methodology_error(format!(
                        "promoted scenario model has an invalid capital-time grid: {error}"
                    ))
                })?
                .content_hash()?;
        if model_hash != model.content_hash
            || PortfolioScenarioModelArtifactId::from_content_hash(&model_hash)
                != model.portfolio_scenario_model_artifact_id
            || model.schema_version != SchemaVersion::FIRST
            || model.states.is_empty()
            || model.distributions.is_empty()
            || model.discount_curve.is_empty()
            || model.capital_time_bucket_contract_digest != capital_bucket_digest
        {
            return Err(methodology(
                "cannot extract fold scenario methodology from an invalid promoted model",
            ));
        }
        let states = model
            .states
            .iter()
            .map(|state| ScenarioStateMethodology {
                scenario_index: state.scenario_index,
                kind: state.kind,
                label: state.label.clone(),
                route_factors: state
                    .route_factors
                    .iter()
                    .map(|factor| ScenarioRouteFactorMethodology {
                        route: factor.route,
                        systematic_weight_bps: factor.systematic_weight_bps,
                        split_probability_quantile_bps: factor.split_probability_quantile_bps,
                        win_cash_recovery_bps: factor.win_cash_recovery_bps,
                        split_cash_recovery_bps: factor.split_cash_recovery_bps,
                        loss_cash_recovery_bps: factor.loss_cash_recovery_bps,
                        executable_share_bps: factor.executable_share_bps,
                        capital_release_multiplier_bps: factor.capital_release_multiplier_bps,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let mut methodology = Self {
            schema_version: model.schema_version,
            ordered_routes: model.ordered_routes.clone(),
            route_set_digest: model.route_set_digest,
            resampling_method: model.resampling_method,
            states,
            distributions: model.distributions.clone(),
            discount_curve: model.discount_curve.clone(),
            capital_time_bucket_contract_digest: model.capital_time_bucket_contract_digest,
            methodology_hash: ContentHash::from_bytes([0_u8; 32]),
        };
        methodology.methodology_hash = methodology.recomputed_hash()?;
        methodology.verify()?;
        Ok(methodology)
    }

    fn recomputed_hash(&self) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-methodology",
            2,
            &(
                self.schema_version,
                &self.ordered_routes,
                self.route_set_digest,
                self.resampling_method,
                &self.states,
                &self.distributions,
                &self.discount_curve,
                self.capital_time_bucket_contract_digest,
            ),
        )
        .map_err(QuantError::from)
    }

    fn verify(&self) -> QuantResult<()> {
        let capital_bucket_digest =
            CapitalTimeBucketContract::try_from(self.discount_curve.as_slice())
                .map_err(|error| {
                    methodology_error(format!(
                        "scenario methodology has an invalid capital-time grid: {error}"
                    ))
                })?
                .content_hash()?;
        if self.schema_version != SchemaVersion::FIRST
            || self.ordered_routes.is_empty()
            || self.states.is_empty()
            || self.distributions.is_empty()
            || self.discount_curve.is_empty()
            || self.capital_time_bucket_contract_digest != capital_bucket_digest
            || self.methodology_hash != self.recomputed_hash()?
            || self.states.iter().any(|state| {
                state
                    .route_factors
                    .iter()
                    .map(|factor| factor.route)
                    .collect::<Vec<_>>()
                    != self.ordered_routes
            })
        {
            return Err(methodology(
                "scenario methodology is empty, unordered, unsupported, or has invalid self-lineage",
            ));
        }
        let represented = RepresentedRouteSet::from_routes(self.ordered_routes.iter().copied())?;
        if represented.routes != self.ordered_routes || represented.digest != self.route_set_digest
        {
            return Err(methodology(
                "scenario methodology Route-set identity is non-canonical",
            ));
        }
        Ok(())
    }
}

/// Allocation-independent calibrated payout residual at one frozen decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioScenarioResidualObservation {
    pub decision_at: DateTime<Utc>,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub economic_residual: Decimal,
}

/// Complete preimage for one ephemeral outer-CPCV scenario estimator.
pub struct PortfolioScenarioFoldFitInput<'a> {
    pub methodology: &'a PortfolioScenarioMethodology,
    pub represented_routes: &'a RepresentedRouteSet,
    pub compatibility: RouteCompatibilityDigests,
    pub route: BuyModelRoute,
    pub model_version_id: ModelVersionId,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub calibration_artifact_hash: ContentHash,
    pub calibration: &'a ResolvedCalibration,
    pub trade_policy_contract_hash: ContentHash,
    pub prediction_horizon_secs: u64,
    pub observations: &'a [PortfolioScenarioResidualObservation],
    pub estimator_identity_hash: ContentHash,
    pub model_fit_groups_hash: ContentHash,
    pub calibration_fit_groups_hash: ContentHash,
    pub scenario_fit_groups_hash: ContentHash,
    pub bound_at: DateTime<Utc>,
}

/// Canonical artifact and the exact Runtime binding installed with a champion.
#[derive(Debug)]
pub struct FittedPortfolioScenarioModel {
    pub artifact: PortfolioScenarioModelArtifact,
    pub binding: PortfolioScenarioModelArtifactBinding,
}

impl FittedPortfolioScenarioModel {
    /// Commit the complete scenario-generation function without audit lineage.
    ///
    /// A CPCV subject fold and its governed base-trial replay carry different
    /// estimator, calibration-artifact, and scenario-artifact identities. None
    /// of those identities may change sampled quantiles, cash recovery,
    /// capital release, distribution weights, or discounting. This projection
    /// captures every field that can affect those economics and deliberately
    /// excludes content ids and lineage hashes.
    pub fn economic_function_hash(&self) -> QuantResult<ContentHash> {
        scenario_economic_function_hash(&self.artifact)
    }
}

/// Commit every scenario field consumed by portfolio economics while excluding
/// audit-only artifact and fit-lineage identities.
pub fn scenario_economic_function_hash(
    artifact: &PortfolioScenarioModelArtifact,
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct FactorFunction {
        route: BuyModelRoute,
        systematic_quantile_bps: u32,
        systematic_weight_bps: u32,
        calibrated_probability_shift_bps: i32,
        split_probability_quantile_bps: u32,
        win_cash_recovery_bps: u32,
        split_cash_recovery_bps: u32,
        loss_cash_recovery_bps: u32,
        executable_share_bps: u32,
        capital_release_multiplier_bps: u32,
    }

    #[derive(Serialize)]
    struct StateFunction<'a> {
        scenario_index: u32,
        kind: PortfolioScenarioKind,
        label: &'a str,
        route_factors: Vec<FactorFunction>,
    }

    #[derive(Serialize)]
    struct ScenarioFunction<'a> {
        schema_version: SchemaVersion,
        as_of: DateTime<Utc>,
        fit_window_start: DateTime<Utc>,
        time_bucket_secs: u64,
        ordered_routes: &'a [BuyModelRoute],
        route_set_digest: ContentHash,
        trade_policy_contract_digest: ContentHash,
        capital_time_bucket_contract_digest: ContentHash,
        scenario_random_stream_hash: ContentHash,
        resampling_method: PortfolioScenarioResamplingMethod,
        states: Vec<StateFunction<'a>>,
        distributions: &'a [ScenarioDistribution],
        discount_curve: &'a [DiscountCurvePoint],
    }

    let states = artifact
        .states
        .iter()
        .map(|state| StateFunction {
            scenario_index: state.scenario_index,
            kind: state.kind,
            label: &state.label,
            route_factors: state
                .route_factors
                .iter()
                .map(|factor| FactorFunction {
                    route: factor.route,
                    systematic_quantile_bps: factor.systematic_quantile_bps,
                    systematic_weight_bps: factor.systematic_weight_bps,
                    calibrated_probability_shift_bps: factor.calibrated_probability_shift_bps,
                    split_probability_quantile_bps: factor.split_probability_quantile_bps,
                    win_cash_recovery_bps: factor.win_cash_recovery_bps,
                    split_cash_recovery_bps: factor.split_cash_recovery_bps,
                    loss_cash_recovery_bps: factor.loss_cash_recovery_bps,
                    executable_share_bps: factor.executable_share_bps,
                    capital_release_multiplier_bps: factor.capital_release_multiplier_bps,
                })
                .collect(),
        })
        .collect();
    CanonicalDigest::content_hash_typed(
        "quant-pivot/portfolio-scenario-economic-function",
        1,
        &ScenarioFunction {
            schema_version: artifact.schema_version,
            as_of: artifact.as_of,
            fit_window_start: artifact.fit_window_start,
            time_bucket_secs: artifact.time_bucket_secs,
            ordered_routes: &artifact.ordered_routes,
            route_set_digest: artifact.route_set_digest,
            trade_policy_contract_digest: artifact.trade_policy_contract_digest,
            capital_time_bucket_contract_digest: artifact.capital_time_bucket_contract_digest,
            scenario_random_stream_hash: artifact.scenario_random_stream_hash,
            resampling_method: artifact.resampling_method,
            states,
            distributions: &artifact.distributions,
            discount_curve: &artifact.discount_curve,
        },
    )
    .map_err(QuantError::from)
}

#[derive(Debug, Clone, Serialize)]
struct JointResidualRow {
    bucket_start_epoch_secs: i64,
    route_residuals: Vec<RouteResidual>,
}

#[derive(Debug, Clone, Serialize)]
struct RouteResidual {
    route: BuyModelRoute,
    economic_residual: Decimal,
}

struct VerifiedRouteFit<'a> {
    input: &'a PortfolioScenarioRouteFitInput<'a>,
    calibration: &'a ModelScoreCalibrationPayload,
    representative_path: &'a BacktestPath,
}

struct VerifiedFoldScenarioFit<'input, 'evidence> {
    input: &'input PortfolioScenarioFoldFitInput<'evidence>,
    observations: Vec<PortfolioScenarioResidualObservation>,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    resampling_method: PortfolioScenarioResamplingMethod,
    resampling_seed_hash: ContentHash,
    panel_hash: ContentHash,
    capital_time_bucket_contract_digest: ContentHash,
    calibration_uncertainty_model_hash: ContentHash,
}

#[derive(Serialize)]
struct FoldResidualIdentity<'a> {
    decision_at: DateTime<Utc>,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
}

#[derive(Serialize)]
struct JointResidualIdentity {
    bucket_start_epoch_secs: i64,
    ordered_routes: Vec<BuyModelRoute>,
}

fn fold_resampling_seed(
    methodology_hash: ContentHash,
    route: BuyModelRoute,
    model_fit_groups_hash: ContentHash,
    calibration_fit_groups_hash: ContentHash,
    scenario_fit_groups_hash: ContentHash,
    observations: &[PortfolioScenarioResidualObservation],
) -> QuantResult<ContentHash> {
    let identities = observations
        .iter()
        .map(|observation| FoldResidualIdentity {
            decision_at: observation.decision_at,
            market_id: &observation.market_id,
            token_id: &observation.token_id,
        })
        .collect::<Vec<_>>();
    CanonicalDigest::content_hash_typed(
        "quant-pivot/cpcv-fold-scenario-common-random-stream",
        1,
        &(
            methodology_hash,
            route,
            model_fit_groups_hash,
            calibration_fit_groups_hash,
            scenario_fit_groups_hash,
            identities,
        ),
    )
    .map_err(QuantError::from)
}

fn fold_panel_hash(
    model_fit_groups_hash: ContentHash,
    calibration_fit_groups_hash: ContentHash,
    scenario_fit_groups_hash: ContentHash,
    resampling_method: PortfolioScenarioResamplingMethod,
    observations: &[PortfolioScenarioResidualObservation],
) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_typed(
        "quant-pivot/cpcv-fold-scenario-residual-panel",
        2,
        &(
            model_fit_groups_hash,
            calibration_fit_groups_hash,
            scenario_fit_groups_hash,
            resampling_method,
            observations,
        ),
    )
    .map_err(QuantError::from)
}

fn joint_resampling_seed(
    methodology_hash: ContentHash,
    route_set_digest: ContentHash,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    time_bucket_secs: u64,
    resampling: StationaryBootstrapContract,
    panel: &[JointResidualRow],
) -> QuantResult<ContentHash> {
    let identities = panel
        .iter()
        .map(|row| JointResidualIdentity {
            bucket_start_epoch_secs: row.bucket_start_epoch_secs,
            ordered_routes: row
                .route_residuals
                .iter()
                .map(|residual| residual.route)
                .collect(),
        })
        .collect::<Vec<_>>();
    CanonicalDigest::content_hash_typed(
        "quant-pivot/portfolio-scenario-common-random-stream",
        1,
        &(
            methodology_hash,
            route_set_digest,
            fit_window_start,
            fit_window_end,
            time_bucket_secs,
            resampling,
            identities,
        ),
    )
    .map_err(QuantError::from)
}

impl<'input, 'evidence> VerifiedFoldScenarioFit<'input, 'evidence> {
    fn try_new(input: &'input PortfolioScenarioFoldFitInput<'evidence>) -> QuantResult<Self> {
        let minimum_observations = usize::try_from(MINIMUM_FOLD_OBSERVATIONS).map_err(|error| {
            methodology_error(format!(
                "fold scenario observation floor does not fit usize: {error}"
            ))
        })?;
        if input.represented_routes.routes.as_slice() != [input.route] {
            return Err(methodology(
                "fold scenario must represent exactly the estimator's Route",
            ));
        }
        input.methodology.verify()?;
        if input.methodology.ordered_routes != input.represented_routes.routes
            || input.methodology.route_set_digest != input.represented_routes.digest
        {
            return Err(methodology(
                "fold scenario methodology Route identity differs from the replay Route set",
            ));
        }
        if input.prediction_horizon_secs == 0 {
            return Err(methodology(
                "fold scenario estimator has a zero prediction horizon",
            ));
        }
        if input.observations.len() < minimum_observations {
            return Err(methodology_error(format!(
                "fold scenario residual population has {} observations; at least {minimum_observations} are required",
                input.observations.len()
            ))
            .into());
        }
        let expected_compatibility = RouteCompatibilityDigests::try_new(
            input.represented_routes,
            &[RouteContractHash {
                route: input.route,
                content_hash: input.serving_contract_hash,
            }],
            &[RouteContractHash {
                route: input.route,
                content_hash: input.calibration_artifact_hash,
            }],
            &[RouteContractHash {
                route: input.route,
                content_hash: input.trade_policy_contract_hash,
            }],
        )
        .map_err(|error| methodology_error(format!("fold compatibility failed: {error}")))?;
        if expected_compatibility != input.compatibility {
            return Err(methodology(
                "fold scenario compatibility differs from its exact estimator contracts",
            ));
        }
        let mut observations = input.observations.to_vec();
        observations.sort_by_key(|observation| observation.decision_at);
        let fit_window_start = observations
            .first()
            .map(|observation| observation.decision_at)
            .ok_or_else(|| methodology_error("fold scenario residual population is empty"))?;
        let fit_window_end = observations
            .last()
            .map(|observation| observation.decision_at)
            .ok_or_else(|| methodology_error("fold scenario residual population is empty"))?;
        if fit_window_start >= fit_window_end || fit_window_end > input.bound_at {
            return Err(methodology(
                "fold scenario residual clock is degenerate or crosses its governance boundary",
            ));
        }
        let resampling_method = PortfolioScenarioResamplingMethod::CrossFittedResidualQuantiles {
            minimum_observations: MINIMUM_FOLD_OBSERVATIONS,
        };
        // Candidate configurations on the same exact fold use one common
        // random stream. The stream is bound to immutable observation
        // identities and partition contracts, never to trial role, model
        // performance, or residual values. This preserves paired comparisons
        // while `panel_hash` separately binds the complete economic residuals.
        let resampling_seed_hash = fold_resampling_seed(
            input.methodology.methodology_hash,
            input.route,
            input.model_fit_groups_hash,
            input.calibration_fit_groups_hash,
            input.scenario_fit_groups_hash,
            &observations,
        )?;
        let panel_hash = fold_panel_hash(
            input.model_fit_groups_hash,
            input.calibration_fit_groups_hash,
            input.scenario_fit_groups_hash,
            resampling_method,
            &observations,
        )?;
        let capital_time_bucket_contract_digest =
            input.methodology.capital_time_bucket_contract_digest;
        let calibration_uncertainty_model_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-calibration-uncertainty",
            1,
            &(
                input.calibration_artifact_hash,
                &input.calibration.reliability,
                input.calibration.split_payout_rate,
            ),
        )?;
        Ok(Self {
            input,
            observations,
            fit_window_start,
            fit_window_end,
            resampling_method,
            resampling_seed_hash,
            panel_hash,
            capital_time_bucket_contract_digest,
            calibration_uncertainty_model_hash,
        })
    }

    fn states(&self) -> QuantResult<Vec<PortfolioScenarioModelState>> {
        let residual_values = self
            .observations
            .iter()
            .map(|observation| observation.economic_residual)
            .collect::<Vec<_>>();
        let mut states = Vec::with_capacity(self.input.methodology.states.len());
        for template_state in &self.input.methodology.states {
            let sampled_index = draw_index(
                self.resampling_seed_hash,
                template_state.scenario_index,
                usize::try_from(template_state.scenario_index).map_err(|error| {
                    methodology_error(format!("fold scenario index does not fit usize: {error}"))
                })?,
                residual_values.len(),
            )?;
            let systematic_quantile_bps = empirical_quantile_bps(
                residual_values[sampled_index],
                residual_values.iter().copied(),
            )?;
            let calibration_shift = calibration_shift_bps(
                &self.input.calibration.reliability,
                systematic_quantile_bps,
                template_state.kind,
            )?;
            let template_factor = template_state
                .route_factors
                .iter()
                .find(|factor| factor.route == self.input.route)
                .ok_or_else(|| {
                    methodology_error(format!(
                        "fold scenario state {} lost Route {:?}",
                        template_state.scenario_index, self.input.route
                    ))
                })?;
            let factor_lineage_hash = CanonicalDigest::content_hash_typed(
                "quant-pivot/cpcv-fold-scenario-route-factor",
                1,
                &(
                    self.panel_hash,
                    self.resampling_seed_hash,
                    template_state.scenario_index,
                    self.input.route,
                    self.input.calibration_artifact_hash,
                    self.input.trade_policy_contract_hash,
                    systematic_quantile_bps,
                    calibration_shift,
                    template_factor.split_probability_quantile_bps,
                    self.input.calibration.split_payout_rate,
                ),
            )?;
            let mut state = PortfolioScenarioModelState {
                scenario_index: template_state.scenario_index,
                kind: template_state.kind,
                label: template_state.label.clone(),
                scenario_state_hash: self.panel_hash,
                route_factors: vec![PortfolioScenarioRouteFactor {
                    route: self.input.route,
                    systematic_quantile_bps,
                    systematic_weight_bps: template_factor.systematic_weight_bps,
                    calibrated_probability_shift_bps: calibration_shift,
                    split_probability_quantile_bps: template_factor.split_probability_quantile_bps,
                    win_cash_recovery_bps: template_factor.win_cash_recovery_bps,
                    split_cash_recovery_bps: template_factor.split_cash_recovery_bps,
                    loss_cash_recovery_bps: template_factor.loss_cash_recovery_bps,
                    executable_share_bps: template_factor.executable_share_bps,
                    capital_release_multiplier_bps: template_factor.capital_release_multiplier_bps,
                    factor_lineage_hash,
                }],
            };
            state.scenario_state_hash = state.recomputed_state_hash()?;
            states.push(state);
        }
        Ok(states)
    }

    fn fit(&self) -> QuantResult<FittedPortfolioScenarioModel> {
        let states = self.states()?;
        let stress_catalog_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-scenario-stress-catalog",
            1,
            &(
                self.input.methodology.methodology_hash,
                self.input.trade_policy_contract_hash,
                self.panel_hash,
                &states,
            ),
        )?;
        let route_fit_lineage = vec![PortfolioScenarioRouteFitLineage {
            route: self.input.route,
            model_lineage: PortfolioScenarioRouteModelLineage {
                evaluated_model_version_id: self.input.model_version_id,
                evaluated_model_artifact_hash: self.input.model_artifact_hash,
                evaluated_serving_contract_hash: self.input.serving_contract_hash,
                calibration_source_model_version_id: self.input.model_version_id,
                calibration_source_model_artifact_hash: self.input.model_artifact_hash,
                calibration_source_serving_contract_hash: self.input.serving_contract_hash,
            },
            fit_evidence: PortfolioScenarioFitEvidence::NestedFold {
                estimator_identity_hash: self.input.estimator_identity_hash,
                model_fit_groups_hash: self.input.model_fit_groups_hash,
                calibration_fit_groups_hash: self.input.calibration_fit_groups_hash,
                scenario_fit_groups_hash: self.input.scenario_fit_groups_hash,
            },
            calibration_artifact_id: self.input.calibration.artifact_id,
            calibration_artifact_hash: self.input.calibration_artifact_hash,
            trade_policy_contract_hash: self.input.trade_policy_contract_hash,
            fit_window_start: self.fit_window_start,
            fit_window_end: self.fit_window_end,
        }];
        let mut artifact = PortfolioScenarioModelArtifact {
            portfolio_scenario_model_artifact_id:
                PortfolioScenarioModelArtifactId::from_content_hash(&self.panel_hash),
            schema_version: self.input.methodology.schema_version,
            as_of: self.fit_window_end,
            fit_window_start: self.fit_window_start,
            time_bucket_secs: self.input.prediction_horizon_secs,
            ordered_routes: self.input.represented_routes.routes.clone(),
            route_set_digest: self.input.represented_routes.digest,
            serving_contract_digest: self.input.compatibility.serving_contract_digest,
            calibration_contract_digest: self.input.compatibility.calibration_contract_digest,
            trade_policy_contract_digest: self.input.compatibility.trade_policy_contract_digest,
            capital_time_bucket_contract_digest: self.capital_time_bucket_contract_digest,
            scenario_random_stream_hash: self.resampling_seed_hash,
            pit_residual_panel_hash: self.panel_hash,
            calibration_uncertainty_model_hash: self.calibration_uncertainty_model_hash,
            stress_catalog_hash,
            resampling_method: self.resampling_method,
            route_fit_lineage,
            states,
            distributions: self.input.methodology.distributions.clone(),
            discount_curve: self.input.methodology.discount_curve.clone(),
            content_hash: self.panel_hash,
        };
        artifact.content_hash = artifact.recomputed_hash()?;
        artifact.portfolio_scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&artifact.content_hash);
        let binding = binding_for(&artifact, self.input.bound_at);
        Ok(FittedPortfolioScenarioModel { artifact, binding })
    }
}

/// Fits synchronized Route factors from aligned OOS economic returns.
pub struct PortfolioScenarioModelFitter;

impl PortfolioScenarioModelFitter {
    /// Return the fail-closed complete-bucket floor for one production
    /// resampling contract.
    ///
    /// This is a computational identifiability floor, not a claim that every
    /// panel clearing it is statistically adequate. Promotion still requires
    /// the governed held-out stability and stress gates.
    pub fn minimum_complete_buckets(
        method: PortfolioScenarioResamplingMethod,
    ) -> QuantResult<usize> {
        StationaryBootstrapContract::try_from(method)?.minimum_complete_buckets()
    }

    /// Refit the complete represented Route set and derive a content-addressed binding.
    pub fn fit(
        input: &PortfolioScenarioModelFitInput<'_>,
    ) -> QuantResult<FittedPortfolioScenarioModel> {
        let resampling = validate_fit_input(input)?;
        let verified = input
            .routes
            .iter()
            .map(verify_route)
            .collect::<QuantResult<Vec<_>>>()?;
        if verified.iter().any(|route| {
            route.input.path_set.created_at > input.bound_at
                || route.input.calibration.created_at > input.bound_at
                || route.input.calibration.fit_window_end > input.bound_at
        }) {
            return Err(methodology(
                "Route scenario evidence was not observable at the atomic bind boundary",
            ));
        }
        let fit_window_start = verified
            .iter()
            .map(|route| route.input.path_set.window_start)
            .max()
            .ok_or_else(|| methodology_error("scenario refit has no Route window"))?;
        let fit_window_end = verified
            .iter()
            .map(|route| route.input.path_set.window_end)
            .min()
            .ok_or_else(|| methodology_error("scenario refit has no Route window"))?;
        if fit_window_start >= fit_window_end || fit_window_end > input.bound_at {
            return Err(methodology(
                "Route CPCV windows have no common PIT interval or cross the bind clock",
            ));
        }
        let time_bucket_secs = verified
            .iter()
            .map(|route| route.input.prediction_horizon_secs)
            .max()
            .filter(|value| *value > 0)
            .ok_or_else(|| methodology_error("scenario refit has a zero prediction horizon"))?;
        let panel = aligned_panel(
            &verified,
            fit_window_start,
            fit_window_end,
            time_bucket_secs,
            resampling,
        )?;
        let panel_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-fit-panel",
            1,
            &(
                fit_window_start,
                fit_window_end,
                time_bucket_secs,
                resampling,
                &panel,
            ),
        )?;
        let scenario_random_stream_hash = joint_resampling_seed(
            input.methodology.methodology_hash,
            input.represented_routes.digest,
            fit_window_start,
            fit_window_end,
            time_bucket_secs,
            resampling,
            &panel,
        )?;
        let capital_time_bucket_contract_digest =
            input.methodology.capital_time_bucket_contract_digest;
        let calibration_uncertainty_model_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-calibration-uncertainty-model",
            1,
            &verified
                .iter()
                .map(|route| {
                    (
                        route.input.route,
                        route.input.calibration_artifact_id,
                        route.input.calibration_artifact_hash,
                        &route.calibration.reliability,
                        route.calibration.split_payout_rate,
                    )
                })
                .collect::<Vec<_>>(),
        )?;
        let states = fit_states(
            input.methodology,
            input.represented_routes,
            &verified,
            &panel,
            panel_hash,
            scenario_random_stream_hash,
            resampling,
        )?;
        let stress_catalog_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/portfolio-scenario-stress-catalog",
            1,
            &(
                input.methodology.methodology_hash,
                input.compatibility.trade_policy_contract_digest,
                panel_hash,
                &states,
            ),
        )?;
        let route_fit_lineage = verified
            .iter()
            .map(|route| PortfolioScenarioRouteFitLineage {
                route: route.input.route,
                model_lineage: route.input.model_lineage,
                fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
                    backtest_path_set_id: route.input.path_set.path_set_id,
                    backtest_path_set_hash: route.input.path_set.path_set_hash,
                    representative_path_index: route.representative_path.path_index,
                },
                calibration_artifact_id: route.input.calibration_artifact_id,
                calibration_artifact_hash: route.input.calibration_artifact_hash,
                trade_policy_contract_hash: route.input.trade_policy_contract_hash,
                fit_window_start,
                fit_window_end,
            })
            .collect::<Vec<_>>();
        let mut artifact = PortfolioScenarioModelArtifact {
            portfolio_scenario_model_artifact_id:
                PortfolioScenarioModelArtifactId::from_content_hash(&panel_hash),
            schema_version: input.methodology.schema_version,
            as_of: fit_window_end,
            fit_window_start,
            time_bucket_secs,
            ordered_routes: input.represented_routes.routes.clone(),
            route_set_digest: input.represented_routes.digest,
            serving_contract_digest: input.compatibility.serving_contract_digest,
            calibration_contract_digest: input.compatibility.calibration_contract_digest,
            trade_policy_contract_digest: input.compatibility.trade_policy_contract_digest,
            capital_time_bucket_contract_digest,
            scenario_random_stream_hash,
            pit_residual_panel_hash: panel_hash,
            calibration_uncertainty_model_hash,
            stress_catalog_hash,
            resampling_method: input.methodology.resampling_method,
            route_fit_lineage,
            states,
            distributions: input.methodology.distributions.clone(),
            discount_curve: input.methodology.discount_curve.clone(),
            content_hash: panel_hash,
        };
        artifact.content_hash = artifact.recomputed_hash()?;
        artifact.portfolio_scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&artifact.content_hash);
        let binding = binding_for(&artifact, input.bound_at);
        Ok(FittedPortfolioScenarioModel { artifact, binding })
    }

    /// Fit one ephemeral outer-CPCV scenario estimator from nested holdout
    /// residuals. The promoted template contributes only data-free stress,
    /// cash-recovery, distribution, and discount semantics.
    pub fn fit_fold(
        input: &PortfolioScenarioFoldFitInput<'_>,
    ) -> QuantResult<FittedPortfolioScenarioModel> {
        VerifiedFoldScenarioFit::try_new(input)?.fit()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct StationaryBootstrapContract {
    expected_block_length: u32,
    scenario_horizon_buckets: u32,
}

impl StationaryBootstrapContract {
    fn minimum_complete_buckets(self) -> QuantResult<usize> {
        let dependence = usize::try_from(self.expected_block_length).map_err(|error| {
            methodology_error(format!("scenario block length does not fit usize: {error}"))
        })?;
        let horizon = usize::try_from(self.scenario_horizon_buckets).map_err(|error| {
            methodology_error(format!("scenario horizon does not fit usize: {error}"))
        })?;
        dependence.max(horizon).checked_mul(2).ok_or_else(|| {
            methodology_error("scenario minimum observation count overflowed").into()
        })
    }
}

impl TryFrom<PortfolioScenarioResamplingMethod> for StationaryBootstrapContract {
    type Error = QuantError;

    fn try_from(method: PortfolioScenarioResamplingMethod) -> Result<Self, Self::Error> {
        match method {
            PortfolioScenarioResamplingMethod::StationaryBootstrap {
                expected_block_length,
                scenario_horizon_buckets,
            } if expected_block_length > 0 && scenario_horizon_buckets > 0 => Ok(Self {
                expected_block_length,
                scenario_horizon_buckets,
            }),
            PortfolioScenarioResamplingMethod::StationaryBootstrap { .. } => Err(methodology(
                "scenario methodology has a zero stationary-bootstrap block length or scenario horizon",
            )),
            PortfolioScenarioResamplingMethod::CrossFittedResidualQuantiles { .. } => {
                Err(methodology(
                    "a fold-local residual-quantile model cannot serve as a production promotion template",
                ))
            }
        }
    }
}

fn validate_fit_input(
    input: &PortfolioScenarioModelFitInput<'_>,
) -> QuantResult<StationaryBootstrapContract> {
    input.methodology.verify()?;
    let expected_compatibility = RouteCompatibilityDigests::try_new(
        input.represented_routes,
        &input
            .routes
            .iter()
            .map(|route| RouteContractHash {
                route: route.route,
                content_hash: route.model_lineage.evaluated_serving_contract_hash,
            })
            .collect::<Vec<_>>(),
        &input
            .routes
            .iter()
            .map(|route| RouteContractHash {
                route: route.route,
                content_hash: route.calibration_artifact_hash,
            })
            .collect::<Vec<_>>(),
        &input
            .routes
            .iter()
            .map(|route| RouteContractHash {
                route: route.route,
                content_hash: route.trade_policy_contract_hash,
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| methodology_error(format!("scenario compatibility digest failed: {error}")))?;
    let resampling = StationaryBootstrapContract::try_from(input.methodology.resampling_method)?;
    if input.routes.len() != input.represented_routes.routes.len()
        || input.methodology.ordered_routes != input.represented_routes.routes
        || input.methodology.route_set_digest != input.represented_routes.digest
        || input.compatibility != expected_compatibility
        || input
            .routes
            .iter()
            .zip(&input.represented_routes.routes)
            .any(|(route, expected)| route.route != *expected)
    {
        return Err(methodology(
            "scenario refit input or methodology identity is incomplete, unordered, unsupported, or incompatible",
        ));
    }
    Ok(resampling)
}

fn binding_for(
    artifact: &PortfolioScenarioModelArtifact,
    bound_at: DateTime<Utc>,
) -> PortfolioScenarioModelArtifactBinding {
    PortfolioScenarioModelArtifactBinding {
        portfolio_scenario_model_artifact_id: artifact.portfolio_scenario_model_artifact_id,
        ordered_routes: artifact.ordered_routes.clone(),
        route_set_digest: artifact.route_set_digest,
        serving_contract_digest: artifact.serving_contract_digest,
        calibration_contract_digest: artifact.calibration_contract_digest,
        trade_policy_contract_digest: artifact.trade_policy_contract_digest,
        scenario_model_schema_version: artifact.schema_version,
        capital_time_bucket_contract_digest: artifact.capital_time_bucket_contract_digest,
        model_content_hash: artifact.content_hash,
        bound_at,
    }
}

fn verify_route<'a>(
    input: &'a PortfolioScenarioRouteFitInput<'a>,
) -> QuantResult<VerifiedRouteFit<'a>> {
    input.path_set.verify_hash().map_err(|error| {
        methodology_error(format!(
            "Route {:?} CPCV path-set hash is invalid: {error}",
            input.route
        ))
    })?;
    let calibration = input.calibration.verify_model_score().map_err(|error| {
        methodology_error(format!(
            "Route {:?} calibration artifact is invalid: {error}",
            input.route
        ))
    })?;
    let calibrated_model = &calibration.fit_contract.model;
    let model_lineage = input.model_lineage;
    let mismatch = |detail: &str| {
        methodology(format!(
            "Route scenario evidence differs from its exact model or calibration contract: {detail}"
        ))
    };
    if input.path_set.model_version_id != model_lineage.evaluated_model_version_id
        || input.path_set.subject.model_artifact_hash != model_lineage.evaluated_model_artifact_hash
        || input.path_set.subject.serving_contract_hash
            != model_lineage.evaluated_serving_contract_hash
    {
        return Err(mismatch(
            "CPCV path set does not identify the evaluated serving model",
        ));
    }
    if input.calibration.artifact_id != input.calibration_artifact_id
        || input.calibration.content_hash != input.calibration_artifact_hash
    {
        return Err(mismatch(
            "calibration artifact id or content hash differs from the serving binding",
        ));
    }
    if calibrated_model.model_version_id != model_lineage.calibration_source_model_version_id
        || calibrated_model.artifact_hash != model_lineage.calibration_source_model_artifact_hash
        || calibrated_model.serving_contract_hash
            != model_lineage.calibration_source_serving_contract_hash
    {
        return Err(mismatch(
            "calibration fit does not identify the committed source estimator",
        ));
    }
    if model_lineage.evaluated_model_version_id == model_lineage.calibration_source_model_version_id
        || model_lineage.evaluated_model_artifact_hash
            == model_lineage.calibration_source_model_artifact_hash
        || model_lineage.evaluated_serving_contract_hash
            == model_lineage.calibration_source_serving_contract_hash
    {
        return Err(mismatch(
            "calibration source aliases the evaluated calibrated child",
        ));
    }
    if calibrated_model.prediction_horizon_secs != input.prediction_horizon_secs
        || calibrated_model.category_scope != input.route.category()
        || input.prediction_horizon_secs == 0
    {
        return Err(mismatch(
            "Route or prediction-horizon semantics differ from the calibration source",
        ));
    }
    if calibrated_model.training_dataset_id != input.path_set.training_dataset_id
        || calibrated_model.training_dataset_hash != input.path_set.subject.training_dataset_hash
    {
        return Err(mismatch(
            "calibration source and evaluated path set do not share one training Dataset",
        ));
    }
    let representative_path = input
        .path_set
        .paths
        .iter()
        .min_by_key(|path| {
            (
                (path.sharpe - input.path_set.sharpe_distribution.median).abs(),
                path.path_index,
            )
        })
        .ok_or_else(|| methodology_error("Route CPCV path set has no reconstructed path"))?;
    if representative_path.decision_times.len() != representative_path.group_returns.len()
        || representative_path.scenario_residuals.len() != representative_path.decision_times.len()
        || representative_path.decision_times.is_empty()
        || representative_path
            .scenario_residuals
            .iter()
            .any(Option::is_none)
    {
        return Err(methodology(
            "Route CPCV representative path lost its PIT decision clock or calibrated residual evidence",
        ));
    }
    Ok(VerifiedRouteFit {
        input,
        calibration,
        representative_path,
    })
}

fn aligned_panel(
    routes: &[VerifiedRouteFit<'_>],
    fit_start: DateTime<Utc>,
    fit_end: DateTime<Utc>,
    bucket_secs: u64,
    resampling: StationaryBootstrapContract,
) -> QuantResult<Vec<JointResidualRow>> {
    let bucket_secs = i64::try_from(bucket_secs).map_err(|error| {
        methodology_error(format!("scenario bucket seconds do not fit i64: {error}"))
    })?;
    let mut route_maps = Vec::with_capacity(routes.len());
    for route in routes {
        let mut accumulators = BTreeMap::<i64, (Decimal, u64)>::new();
        for (&decision_at, economic_residual) in route
            .representative_path
            .decision_times
            .iter()
            .zip(&route.representative_path.scenario_residuals)
        {
            let economic_residual = economic_residual.ok_or_else(|| {
                methodology_error("Buy scenario path contains no calibrated residual evidence")
            })?;
            if decision_at < fit_start || decision_at >= fit_end {
                continue;
            }
            let bucket = decision_at.timestamp().div_euclid(bucket_secs) * bucket_secs;
            let (sum, count) = accumulators.entry(bucket).or_insert((Decimal::ZERO, 0));
            *sum += economic_residual;
            *count = count.checked_add(1).ok_or_else(|| {
                methodology_error("scenario residual bucket count overflowed u64")
            })?;
        }
        let buckets = accumulators
            .into_iter()
            .map(|(bucket, (sum, count))| {
                Ok((
                    bucket,
                    sum.checked_div(Decimal::from(count))
                        .ok_or_else(|| methodology_error("scenario residual bucket is empty"))?
                        .normalize(),
                ))
            })
            .collect::<QuantResult<BTreeMap<_, _>>>()?;
        let minimum = resampling.minimum_complete_buckets()?;
        if buckets.len() < minimum {
            return Err(methodology(format!(
                "Route {:?} has {} OOS buckets; at least {minimum} are required",
                route.input.route,
                buckets.len()
            )));
        }
        route_maps.push(buckets);
    }
    let first_route = route_maps
        .first()
        .ok_or_else(|| methodology_error("scenario refit has no Route observations"))?;
    let panel = first_route
        .keys()
        .filter_map(|bucket| {
            route_maps
                .iter()
                .map(|values| values.get(bucket).copied())
                .collect::<Option<Vec<_>>>()
                .map(|residuals| JointResidualRow {
                    bucket_start_epoch_secs: *bucket,
                    route_residuals: routes
                        .iter()
                        .zip(residuals)
                        .map(|(route, economic_residual)| RouteResidual {
                            route: route.input.route,
                            economic_residual,
                        })
                        .collect(),
                })
        })
        .collect::<Vec<_>>();
    let minimum_joint = resampling.minimum_complete_buckets()?;
    if panel.len() < minimum_joint {
        return Err(methodology(format!(
            "joint scenario fit has {} complete contemporaneous Route buckets; at least {minimum_joint} are required",
            panel.len()
        )));
    }
    verify_panel_clock(&panel, bucket_secs)?;
    Ok(panel)
}

fn verify_panel_clock(panel: &[JointResidualRow], bucket_secs: i64) -> QuantResult<()> {
    for pair in panel.windows(2) {
        let expected = pair[0]
            .bucket_start_epoch_secs
            .checked_add(bucket_secs)
            .ok_or_else(|| methodology_error("scenario panel clock overflowed i64"))?;
        if pair[1].bucket_start_epoch_secs != expected {
            return Err(methodology(format!(
                "joint scenario panel is missing canonical time bucket {expected} between {} and {}; sparse time cannot be collapsed into adjacent observations",
                pair[0].bucket_start_epoch_secs, pair[1].bucket_start_epoch_secs
            )));
        }
    }
    Ok(())
}

fn fit_states(
    methodology: &PortfolioScenarioMethodology,
    routes: &RepresentedRouteSet,
    evidence: &[VerifiedRouteFit<'_>],
    panel: &[JointResidualRow],
    panel_hash: ContentHash,
    scenario_random_stream_hash: ContentHash,
    resampling: StationaryBootstrapContract,
) -> QuantResult<Vec<PortfolioScenarioModelState>> {
    let rolling = rolling_horizon_returns(panel, resampling.scenario_horizon_buckets)?;
    let mut states = Vec::with_capacity(methodology.states.len());
    for template_state in &methodology.states {
        let sampled = stationary_bootstrap_returns(
            panel,
            resampling,
            scenario_random_stream_hash,
            template_state.scenario_index,
        )?;
        let mut route_factors = Vec::with_capacity(routes.routes.len());
        for (route_index, route) in routes.routes.iter().copied().enumerate() {
            let template_factor = template_state
                .route_factors
                .iter()
                .find(|factor| factor.route == route)
                .ok_or_else(|| {
                    methodology_error(format!(
                        "scenario template state {} has no Route {:?} factor",
                        template_state.scenario_index, route
                    ))
                })?;
            let systematic_quantile_bps = empirical_quantile_bps(
                sampled[route_index],
                rolling.iter().map(|row| row[route_index]),
            )?;
            let calibration_shift = calibration_shift_bps(
                &evidence[route_index].calibration.reliability,
                systematic_quantile_bps,
                template_state.kind,
            )?;
            let factor_lineage_hash = CanonicalDigest::content_hash_typed(
                "quant-pivot/portfolio-scenario-route-factor",
                1,
                &(
                    panel_hash,
                    template_state.scenario_index,
                    route,
                    evidence[route_index].input.path_set.path_set_hash,
                    evidence[route_index].input.calibration_artifact_hash,
                    evidence[route_index].input.trade_policy_contract_hash,
                    systematic_quantile_bps,
                    calibration_shift,
                    template_factor.split_probability_quantile_bps,
                    evidence[route_index].calibration.split_payout_rate,
                ),
            )?;
            route_factors.push(PortfolioScenarioRouteFactor {
                route,
                systematic_quantile_bps,
                systematic_weight_bps: template_factor.systematic_weight_bps,
                calibrated_probability_shift_bps: calibration_shift,
                split_probability_quantile_bps: template_factor.split_probability_quantile_bps,
                win_cash_recovery_bps: template_factor.win_cash_recovery_bps,
                split_cash_recovery_bps: template_factor.split_cash_recovery_bps,
                loss_cash_recovery_bps: template_factor.loss_cash_recovery_bps,
                executable_share_bps: template_factor.executable_share_bps,
                capital_release_multiplier_bps: template_factor.capital_release_multiplier_bps,
                factor_lineage_hash,
            });
        }
        let mut state = PortfolioScenarioModelState {
            scenario_index: template_state.scenario_index,
            kind: template_state.kind,
            label: template_state.label.clone(),
            scenario_state_hash: panel_hash,
            route_factors,
        };
        state.scenario_state_hash = state.recomputed_state_hash()?;
        states.push(state);
    }
    Ok(states)
}

fn rolling_horizon_returns(
    panel: &[JointResidualRow],
    scenario_horizon_buckets: u32,
) -> QuantResult<Vec<Vec<Decimal>>> {
    let length = usize::try_from(scenario_horizon_buckets).map_err(|error| {
        methodology_error(format!("scenario horizon does not fit usize: {error}"))
    })?;
    if length == 0 || panel.len() < length {
        return Err(methodology(
            "scenario panel is shorter than its governed scenario horizon",
        ));
    }
    (0..panel.len())
        .map(|start| {
            (0..length).try_fold(
                vec![Decimal::ZERO; panel[0].route_residuals.len()],
                |mut accumulated, offset| {
                    let row = &panel[(start + offset) % panel.len()];
                    for (value, route) in accumulated.iter_mut().zip(&row.route_residuals) {
                        *value = (*value + route.economic_residual).normalize();
                    }
                    Ok(accumulated)
                },
            )
        })
        .collect()
}

fn stationary_bootstrap_returns(
    panel: &[JointResidualRow],
    contract: StationaryBootstrapContract,
    random_stream_hash: ContentHash,
    state_index: u32,
) -> QuantResult<Vec<Decimal>> {
    let path = bootstrap_path_indices(panel.len(), contract, random_stream_hash, state_index)?;
    let mut accumulated = vec![Decimal::ZERO; panel[0].route_residuals.len()];
    for index in path {
        for (value, route) in accumulated.iter_mut().zip(&panel[index].route_residuals) {
            *value = (*value + route.economic_residual).normalize();
        }
    }
    Ok(accumulated)
}

fn bootstrap_path_indices(
    panel_length: usize,
    contract: StationaryBootstrapContract,
    random_stream_hash: ContentHash,
    state_index: u32,
) -> QuantResult<Vec<usize>> {
    let expected_block_length =
        usize::try_from(contract.expected_block_length).map_err(|error| {
            methodology_error(format!("scenario block length does not fit usize: {error}"))
        })?;
    let horizon = usize::try_from(contract.scenario_horizon_buckets).map_err(|error| {
        methodology_error(format!("scenario horizon does not fit usize: {error}"))
    })?;
    if panel_length == 0 || expected_block_length == 0 || horizon == 0 {
        return Err(methodology(
            "stationary-bootstrap panel, block length, and scenario horizon must be positive",
        ));
    }
    let mut path = Vec::with_capacity(horizon);
    let mut index = draw_index(random_stream_hash, state_index, 0, panel_length)?;
    path.push(index);
    for draw in 1..horizon {
        let restart =
            draw_index(random_stream_hash, state_index, draw, expected_block_length)? == 0;
        index = if restart {
            draw_index(
                random_stream_hash,
                state_index,
                draw.saturating_add(horizon),
                panel_length,
            )?
        } else {
            (index + 1) % panel_length
        };
        path.push(index);
    }
    Ok(path)
}

fn draw_index(
    random_stream_hash: ContentHash,
    state_index: u32,
    draw: usize,
    modulus: usize,
) -> QuantResult<usize> {
    if modulus == 0 {
        return Err(methodology("scenario bootstrap draw has a zero modulus"));
    }
    let hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/portfolio-stationary-bootstrap-draw",
        1,
        &(random_stream_hash, state_index, draw),
    )?;
    let bytes = hash.as_bytes();
    let value = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let modulus = u64::try_from(modulus).map_err(|error| {
        methodology_error(format!(
            "scenario bootstrap modulus does not fit u64: {error}"
        ))
    })?;
    Ok(usize::try_from(value % modulus).map_err(|error| {
        methodology_error(format!(
            "scenario bootstrap index does not fit usize: {error}"
        ))
    })?)
}

fn empirical_quantile_bps(
    value: Decimal,
    population: impl Iterator<Item = Decimal>,
) -> QuantResult<u32> {
    let values = population.collect::<Vec<_>>();
    if values.is_empty() {
        return Err(methodology("scenario empirical distribution is empty"));
    }
    let less = values
        .iter()
        .filter(|candidate| **candidate < value)
        .count();
    let equal = values
        .iter()
        .filter(|candidate| **candidate == value)
        .count();
    let numerator = less
        .checked_mul(2)
        .and_then(|twice| twice.checked_add(equal))
        .and_then(|rank| rank.checked_mul(usize::try_from(BASIS_POINTS).ok()?))
        .ok_or_else(|| methodology_error("scenario empirical rank overflowed"))?;
    let denominator = values
        .len()
        .checked_mul(2)
        .ok_or_else(|| methodology_error("scenario empirical denominator overflowed"))?;
    let quantile = numerator / denominator;
    Ok(u32::try_from(quantile.min(9_999)).map_err(|error| {
        methodology_error(format!(
            "scenario empirical quantile does not fit u32: {error}"
        ))
    })?)
}

fn calibration_shift_bps(
    reliability: &ReliabilityReport,
    quantile_bps: u32,
    kind: PortfolioScenarioKind,
) -> QuantResult<i32> {
    let bin = reliability_bin(&reliability.bins, quantile_bps)?;
    let reference = match kind {
        PortfolioScenarioKind::PitBootstrap => bin.empirical_frequency,
        PortfolioScenarioKind::CalibrationUncertainty | PortfolioScenarioKind::StructuralStress => {
            bin.wilson_ci.0
        }
    };
    probability_delta_bps(reference, bin.mean_predicted)
}

fn reliability_bin(bins: &[ReliabilityBin], quantile_bps: u32) -> QuantResult<&ReliabilityBin> {
    let total = bins.iter().try_fold(0_u64, |sum, bin| {
        sum.checked_add(bin.sample_count)
            .ok_or_else(|| methodology_error("calibration reliability sample count overflowed"))
    })?;
    if total == 0 {
        return Err(methodology("calibration reliability distribution is empty"));
    }
    let target = u128::from(quantile_bps)
        .checked_mul(u128::from(total))
        .ok_or_else(|| methodology_error("calibration quantile target overflowed"))?
        / u128::from(BASIS_POINTS);
    let mut cumulative = 0_u128;
    for bin in bins {
        cumulative = cumulative
            .checked_add(u128::from(bin.sample_count))
            .ok_or_else(|| methodology_error("calibration quantile cumulative overflowed"))?;
        if cumulative > target {
            return Ok(bin);
        }
    }
    Ok(bins
        .last()
        .ok_or_else(|| methodology_error("calibration reliability bins are empty"))?)
}

fn probability_delta_bps(left: Probability, right: Probability) -> QuantResult<i32> {
    let scaled = ((left.inner() - right.inner()) * Decimal::from(BASIS_POINTS))
        .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven);
    Ok(scaled.to_i32().ok_or_else(|| {
        methodology_error(format!(
            "calibration probability shift {scaled} does not fit i32"
        ))
    })?)
}

fn methodology(detail: impl Into<String>) -> QuantError {
    methodology_error(detail).into()
}

fn methodology_error(detail: impl Into<String>) -> ResearchError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::quant::{
            BacktestPathSetInfo, CalibrationArtifactInfo, CalibrationArtifactPayload,
            DiscountCurvePoint, PortfolioScenarioFitEvidence, PortfolioScenarioKind,
            PortfolioScenarioModelArtifact, PortfolioScenarioModelState,
            PortfolioScenarioResamplingMethod, PortfolioScenarioRouteFactor,
            PortfolioScenarioRouteFitLineage, PortfolioScenarioRouteModelLineage,
            RepresentedRouteSet, RouteCompatibilityDigests, RouteContractHash,
            ScenarioDistribution, ScenarioWeight,
        },
        enums::{model::ModelFamily, quant::CalibrationKind},
        runtime_config::BuyModelRoute,
        types::{
            BacktestPathSetId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
            MarketId, ModelRunId, ModelSpecId, ModelVersionId, PayoutRatio,
            PortfolioScenarioModelArtifactId, Probability, ResearchProfileId, ResearchProfileRef,
            SchemaVersion, TokenId, TrainingDatasetId,
            backtest::{
                BacktestPath, BacktestPaths, CpcvEstimatorIdentity, CpcvFoldArtifact,
                CpcvFoldArtifacts, CpcvFoldCalibrationPolicy, CpcvMethodologyBinding,
                CpcvPathSetSubject, CpcvTrialPathBinding, CscvSelectionEvidence,
                CscvTrialDescriptor, CscvTrialGridBinding, SharpeDistribution,
            },
            calibration::{
                IsotonicKnot, MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
                ModelScoreCalibrationDatasetBinding, ModelScoreCalibrationFitContract,
                ModelScoreCalibrationModelBinding, ModelScoreCalibrationPayload,
                ModelScoreCalibrationPolicyBinding, MonotoneMapping, ReliabilityBin,
                ReliabilityReport, SplitPayoutRateEvidence,
            },
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use crate::validation::{TrialPerformanceMatrix, analyze_selection_bias};

    use super::{
        CapitalTimeBucketContract, FittedPortfolioScenarioModel, JointResidualRow,
        PortfolioScenarioMethodology, PortfolioScenarioModelFitInput, PortfolioScenarioModelFitter,
        PortfolioScenarioResidualObservation, PortfolioScenarioRouteFitInput, RouteResidual,
        StationaryBootstrapContract, bootstrap_path_indices, empirical_quantile_bps,
        fold_panel_hash, fold_resampling_seed, joint_resampling_seed,
        scenario_economic_function_hash,
    };

    struct RouteEvidence {
        route: BuyModelRoute,
        model_version_id: ModelVersionId,
        model_artifact_hash: ContentHash,
        serving_contract_hash: ContentHash,
        calibration_source_model_version_id: ModelVersionId,
        calibration_source_model_artifact_hash: ContentHash,
        calibration_source_serving_contract_hash: ContentHash,
        trade_policy_contract_hash: ContentHash,
        prediction_horizon_secs: u64,
        path_set: BacktestPathSetInfo,
        calibration: CalibrationArtifactInfo,
    }

    struct RouteModelContract {
        route: BuyModelRoute,
        seed: u128,
        bound_at: DateTime<Utc>,
        model_version_id: ModelVersionId,
        serving_contract_hash: ContentHash,
        model_artifact_hash: ContentHash,
        calibration_source_model_version_id: ModelVersionId,
        calibration_source_model_artifact_hash: ContentHash,
        calibration_source_serving_contract_hash: ContentHash,
        training_dataset_id: TrainingDatasetId,
        training_dataset_hash: ContentHash,
        prediction_horizon_secs: u64,
    }

    impl RouteEvidence {
        const fn model_lineage(&self) -> PortfolioScenarioRouteModelLineage {
            PortfolioScenarioRouteModelLineage {
                evaluated_model_version_id: self.model_version_id,
                evaluated_model_artifact_hash: self.model_artifact_hash,
                evaluated_serving_contract_hash: self.serving_contract_hash,
                calibration_source_model_version_id: self.calibration_source_model_version_id,
                calibration_source_model_artifact_hash: self.calibration_source_model_artifact_hash,
                calibration_source_serving_contract_hash: self
                    .calibration_source_serving_contract_hash,
            }
        }

        fn fit_input(&self) -> PortfolioScenarioRouteFitInput<'_> {
            PortfolioScenarioRouteFitInput {
                route: self.route,
                model_lineage: self.model_lineage(),
                calibration_artifact_id: self.calibration.artifact_id,
                calibration_artifact_hash: self.calibration.content_hash,
                trade_policy_contract_hash: self.trade_policy_contract_hash,
                prediction_horizon_secs: self.prediction_horizon_secs,
                path_set: &self.path_set,
                calibration: &self.calibration,
            }
        }
    }

    struct ScenarioFitFixture {
        bound_at: DateTime<Utc>,
        represented_routes: RepresentedRouteSet,
        compatibility: RouteCompatibilityDigests,
        methodology: PortfolioScenarioMethodology,
        routes: Vec<RouteEvidence>,
    }

    impl ScenarioFitFixture {
        fn build() -> Self {
            let bound_at = Utc
                .with_ymd_and_hms(2025, 1, 31, 0, 0, 0)
                .single()
                .expect("fixture timestamp");
            let represented_routes =
                RepresentedRouteSet::from_routes([BuyModelRoute::Weather, BuyModelRoute::Crypto])
                    .expect("represented Routes");
            let routes = represented_routes
                .routes
                .iter()
                .copied()
                .map(|route| route_evidence(route, bound_at))
                .collect::<Vec<_>>();
            let compatibility = compatibility(&represented_routes, &routes);
            let promoted = scenario_template(
                bound_at,
                &represented_routes,
                compatibility.trade_policy_contract_digest,
                &routes,
            );
            let methodology = PortfolioScenarioMethodology::from_promoted(&promoted)
                .expect("data-free scenario methodology");
            Self {
                bound_at,
                represented_routes,
                compatibility,
                methodology,
                routes,
            }
        }

        fn fit(&self) -> QuantResult<FittedPortfolioScenarioModel> {
            PortfolioScenarioModelFitter::fit(&PortfolioScenarioModelFitInput {
                methodology: &self.methodology,
                represented_routes: &self.represented_routes,
                compatibility: self.compatibility,
                routes: self.routes.iter().map(RouteEvidence::fit_input).collect(),
                bound_at: self.bound_at,
            })
        }

        fn refresh_compatibility(&mut self) {
            self.compatibility = compatibility(&self.represented_routes, &self.routes);
        }
    }

    #[test]
    fn empirical_quantile_uses_midrank() {
        assert_eq!(
            empirical_quantile_bps(dec!(2), [dec!(1), dec!(2), dec!(2), dec!(4)].into_iter())
                .expect("midrank"),
            5_000
        );
    }

    #[test]
    fn fold_seed_ignores_performance() {
        let decision_at = Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .single()
            .expect("fixture timestamp");
        let observations = vec![PortfolioScenarioResidualObservation {
            decision_at,
            market_id: MarketId::new("market-a"),
            token_id: TokenId::new("yes"),
            economic_residual: dec!(0.1),
        }];
        let seed = fold_resampling_seed(
            hash(1),
            BuyModelRoute::Weather,
            hash(2),
            hash(3),
            hash(4),
            &observations,
        )
        .expect("common random stream");
        let panel = fold_panel_hash(
            hash(2),
            hash(3),
            hash(4),
            PortfolioScenarioResamplingMethod::CrossFittedResidualQuantiles {
                minimum_observations: 10,
            },
            &observations,
        )
        .expect("residual panel");
        let mut changed_performance = observations.clone();
        changed_performance[0].economic_residual = dec!(-0.8);

        assert_eq!(
            seed,
            fold_resampling_seed(
                hash(1),
                BuyModelRoute::Weather,
                hash(2),
                hash(3),
                hash(4),
                &changed_performance,
            )
            .expect("performance-independent stream")
        );
        assert_ne!(
            panel,
            fold_panel_hash(
                hash(2),
                hash(3),
                hash(4),
                PortfolioScenarioResamplingMethod::CrossFittedResidualQuantiles {
                    minimum_observations: 10,
                },
                &changed_performance,
            )
            .expect("performance-bound panel")
        );
        let mut changed_identity = observations;
        changed_identity[0].market_id = MarketId::new("market-b");
        assert_ne!(
            seed,
            fold_resampling_seed(
                hash(1),
                BuyModelRoute::Weather,
                hash(2),
                hash(3),
                hash(4),
                &changed_identity,
            )
            .expect("identity-bound stream")
        );
    }

    #[test]
    fn joint_seed_ignores_residuals() {
        let contract = StationaryBootstrapContract {
            expected_block_length: 2,
            scenario_horizon_buckets: 4,
        };
        let panel = vec![JointResidualRow {
            bucket_start_epoch_secs: 1_735_689_600,
            route_residuals: vec![
                RouteResidual {
                    route: BuyModelRoute::Crypto,
                    economic_residual: dec!(0.1),
                },
                RouteResidual {
                    route: BuyModelRoute::Weather,
                    economic_residual: dec!(-0.2),
                },
            ],
        }];
        let seed = joint_resampling_seed(
            hash(1),
            hash(2),
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                .single()
                .expect("fit start"),
            Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0)
                .single()
                .expect("fit end"),
            3_600,
            contract,
            &panel,
        )
        .expect("joint common random stream");
        let mut changed_values = panel.clone();
        changed_values[0].route_residuals[0].economic_residual = dec!(0.9);
        assert_eq!(
            seed,
            joint_resampling_seed(
                hash(1),
                hash(2),
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                    .single()
                    .expect("fit start"),
                Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0)
                    .single()
                    .expect("fit end"),
                3_600,
                contract,
                &changed_values,
            )
            .expect("performance-independent joint stream")
        );
        let mut changed_identity = panel;
        changed_identity[0].bucket_start_epoch_secs += 3_600;
        assert_ne!(
            seed,
            joint_resampling_seed(
                hash(1),
                hash(2),
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                    .single()
                    .expect("fit start"),
                Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0)
                    .single()
                    .expect("fit end"),
                3_600,
                contract,
                &changed_identity,
            )
            .expect("identity-bound joint stream")
        );
    }

    #[test]
    fn bootstrap_decouples_horizon_block() {
        let panel_hash = hash(231);
        let contract = StationaryBootstrapContract {
            expected_block_length: 2,
            scenario_horizon_buckets: 7,
        };
        let first = bootstrap_path_indices(12, contract, panel_hash, 9).expect("bootstrap path");
        let second =
            bootstrap_path_indices(12, contract, panel_hash, 9).expect("deterministic path");

        assert_eq!(first, second);
        assert_eq!(first.len(), 7);
        assert!(first.iter().all(|index| *index < 12));
    }

    #[test]
    fn horizon_change_rekeys_model() {
        let fixture = ScenarioFitFixture::build();
        let baseline = fixture.fit().expect("baseline joint refit");
        let mut changed = ScenarioFitFixture::build();
        changed.methodology.resampling_method =
            PortfolioScenarioResamplingMethod::StationaryBootstrap {
                expected_block_length: 2,
                scenario_horizon_buckets: 5,
            };
        changed.methodology.methodology_hash = changed
            .methodology
            .recomputed_hash()
            .expect("methodology hash");
        let changed = changed.fit().expect("changed-horizon joint refit");

        assert_ne!(
            baseline.artifact.pit_residual_panel_hash,
            changed.artifact.pit_residual_panel_hash
        );
        assert_eq!(
            baseline.artifact.capital_time_bucket_contract_digest,
            changed.artifact.capital_time_bucket_contract_digest
        );
        assert_ne!(
            baseline.artifact.content_hash,
            changed.artifact.content_hash
        );
    }

    #[test]
    fn capital_grid_rekeys_binding() {
        let fixture = ScenarioFitFixture::build();
        let baseline = fixture.fit().expect("baseline joint refit");
        let mut changed = ScenarioFitFixture::build();
        changed.methodology.discount_curve[0].end_secs = 172_800;
        changed.methodology.capital_time_bucket_contract_digest =
            CapitalTimeBucketContract::try_from(changed.methodology.discount_curve.as_slice())
                .expect("changed capital-time grid")
                .content_hash()
                .expect("changed capital-time contract hash");
        changed.methodology.methodology_hash = changed
            .methodology
            .recomputed_hash()
            .expect("methodology hash");
        let changed = changed.fit().expect("changed-grid joint refit");

        assert_ne!(
            baseline.artifact.capital_time_bucket_contract_digest,
            changed.artifact.capital_time_bucket_contract_digest
        );
        assert_ne!(baseline.binding, changed.binding);
    }

    #[test]
    fn refit_is_content_deterministic() {
        let fixture = ScenarioFitFixture::build();
        let first = fixture.fit().expect("first joint refit");
        let second = fixture.fit().expect("second joint refit");

        assert_eq!(first.artifact, second.artifact);
        assert_eq!(first.binding, second.binding);
        assert_eq!(
            first.binding.model_content_hash,
            first.artifact.content_hash
        );
        assert_eq!(
            first.binding.portfolio_scenario_model_artifact_id,
            first.artifact.portfolio_scenario_model_artifact_id
        );
    }

    #[test]
    fn economic_hash_excludes_lineage() {
        let fixture = ScenarioFitFixture::build();
        let baseline = fixture.fit().expect("baseline joint refit").artifact;
        let mut changed = baseline.clone();
        changed.route_fit_lineage[0]
            .model_lineage
            .evaluated_model_artifact_hash = hash(244);
        changed.content_hash = changed.recomputed_hash().expect("changed artifact hash");
        changed.portfolio_scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&changed.content_hash);

        assert_ne!(baseline.content_hash, changed.content_hash);
        assert_eq!(
            scenario_economic_function_hash(&baseline).expect("baseline economic hash"),
            scenario_economic_function_hash(&changed).expect("changed economic hash")
        );

        let mut economic_change = baseline.clone();
        economic_change.states[0].route_factors[0].executable_share_bps -= 1;
        economic_change.states[0].scenario_state_hash = economic_change.states[0]
            .recomputed_state_hash()
            .expect("changed state hash");
        assert_ne!(
            scenario_economic_function_hash(&baseline).expect("baseline economic hash"),
            scenario_economic_function_hash(&economic_change).expect("changed economic hash")
        );
    }

    #[test]
    fn refit_preserves_calibration_edge() {
        let fixture = ScenarioFitFixture::build();
        let fitted = fixture.fit().expect("distinct calibration edge");

        assert!(fitted.artifact.route_fit_lineage.iter().all(|lineage| {
            lineage.model_lineage.evaluated_model_version_id
                != lineage.model_lineage.calibration_source_model_version_id
                && lineage.model_lineage.evaluated_model_artifact_hash
                    != lineage.model_lineage.calibration_source_model_artifact_hash
                && lineage.model_lineage.evaluated_serving_contract_hash
                    != lineage
                        .model_lineage
                        .calibration_source_serving_contract_hash
        }));
    }

    #[test]
    fn refit_rejects_source_alias() {
        let mut fixture = ScenarioFitFixture::build();
        let crypto = fixture
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Crypto)
            .expect("crypto evidence");
        crypto.calibration_source_model_version_id = crypto.model_version_id;
        crypto.calibration_source_model_artifact_hash = crypto.model_artifact_hash;
        crypto.calibration_source_serving_contract_hash = crypto.serving_contract_hash;
        let CalibrationArtifactPayload::ModelScore(payload) = &mut crypto.calibration.payload
        else {
            panic!("model-score calibration");
        };
        payload.fit_contract.model.model_version_id = crypto.model_version_id;
        payload.fit_contract.model.artifact_hash = crypto.model_artifact_hash;
        payload.fit_contract.model.serving_contract_hash = crypto.serving_contract_hash;
        crypto.calibration.content_hash = payload
            .content_hash(
                crypto.calibration.fit_window_start,
                crypto.calibration.fit_window_end,
                &crypto.calibration.calibration_split_hash,
            )
            .expect("resealed calibration");
        fixture.refresh_compatibility();

        let error = fixture
            .fit()
            .expect_err("a promoted calibration source cannot alias the evaluated child");
        assert!(
            error
                .to_string()
                .contains("differs from its exact model or calibration contract")
        );
    }

    #[test]
    fn trade_policy_change_refits() {
        let fixture = ScenarioFitFixture::build();
        let baseline = fixture.fit().expect("baseline joint refit");
        let mut changed = ScenarioFitFixture::build();
        let weather = changed
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Weather)
            .expect("weather evidence");
        weather.trade_policy_contract_hash = hash(242);
        changed.refresh_compatibility();

        let refitted = changed
            .fit()
            .expect("prospective Trade Policy requires a fresh scenario fit");
        assert_eq!(
            refitted.artifact.trade_policy_contract_digest,
            changed.compatibility.trade_policy_contract_digest
        );
        assert_ne!(
            baseline.artifact.trade_policy_contract_digest,
            refitted.artifact.trade_policy_contract_digest
        );
        assert_ne!(
            baseline.artifact.content_hash,
            refitted.artifact.content_hash
        );
        assert_ne!(baseline.binding, refitted.binding);
    }

    #[test]
    fn methodology_erases_trade_policy() {
        let baseline = ScenarioFitFixture::build();
        let mut changed_routes = baseline
            .routes
            .iter()
            .map(|route| route_evidence(route.route, baseline.bound_at))
            .collect::<Vec<_>>();
        changed_routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Weather)
            .expect("weather evidence")
            .trade_policy_contract_hash = hash(243);
        let changed_compatibility = compatibility(&baseline.represented_routes, &changed_routes);
        let changed_promoted = scenario_template(
            baseline.bound_at,
            &baseline.represented_routes,
            changed_compatibility.trade_policy_contract_digest,
            &changed_routes,
        );
        let changed = PortfolioScenarioMethodology::from_promoted(&changed_promoted)
            .expect("changed Trade Policy methodology");

        assert_eq!(baseline.methodology, changed);
    }

    #[test]
    fn panel_binds_joint_order() {
        let fixture = ScenarioFitFixture::build();
        let baseline = fixture.fit().expect("baseline joint refit");
        let mut reordered = ScenarioFitFixture::build();
        let weather = reordered
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Weather)
            .expect("weather evidence");
        let mut path = weather.path_set.paths[0].clone();
        path.scenario_residuals.reverse();
        weather.path_set.paths = BacktestPaths::from(vec![path]);
        reseal_path_set(&mut weather.path_set);
        let reordered = reordered.fit().expect("reordered joint refit");

        assert_ne!(
            baseline.artifact.pit_residual_panel_hash,
            reordered.artifact.pit_residual_panel_hash
        );
        assert_ne!(
            baseline.artifact.content_hash,
            reordered.artifact.content_hash
        );
    }

    #[test]
    fn refit_rejects_sparse_coactivity() {
        let mut fixture = ScenarioFitFixture::build();
        let weather = fixture
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Weather)
            .expect("weather evidence");
        let mut path = weather.path_set.paths[0].clone();
        path.decision_times = decision_times(weather.path_set.window_start, 6)
            .into_iter()
            .take(8)
            .collect();
        path.group_returns.truncate(8);
        path.scenario_residuals.truncate(8);
        weather.path_set.paths = BacktestPaths::from(vec![path]);
        reseal_path_set(&mut weather.path_set);

        let error = fixture
            .fit()
            .expect_err("joint coactivity must fail closed");
        assert!(
            error
                .to_string()
                .contains("complete contemporaneous Route buckets")
        );
    }

    #[test]
    fn refit_rejects_clock_gap() {
        let mut fixture = ScenarioFitFixture::build();
        let weather = fixture
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Weather)
            .expect("weather evidence");
        let mut path = weather.path_set.paths[0].clone();
        path.decision_times.drain(4..8);
        path.group_returns.drain(4..8);
        path.scenario_residuals.drain(4..8);
        weather.path_set.paths = BacktestPaths::from(vec![path]);
        reseal_path_set(&mut weather.path_set);

        let error = fixture
            .fit()
            .expect_err("a missing PIT bucket cannot be treated as adjacent time");
        assert!(error.to_string().contains("missing canonical time bucket"));
    }

    #[test]
    fn refit_rejects_calibration_drift() {
        let mut fixture = ScenarioFitFixture::build();
        let crypto = fixture
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Crypto)
            .expect("crypto evidence");
        let CalibrationArtifactPayload::ModelScore(payload) = &mut crypto.calibration.payload
        else {
            panic!("model-score calibration");
        };
        payload.fit_contract.model.serving_contract_hash = hash(240);
        crypto.calibration.content_hash = payload
            .content_hash(
                crypto.calibration.fit_window_start,
                crypto.calibration.fit_window_end,
                &crypto.calibration.calibration_split_hash,
            )
            .expect("resealed calibration");
        fixture.refresh_compatibility();

        let error = fixture
            .fit()
            .expect_err("cross-artifact calibration drift must fail closed");
        assert!(
            error
                .to_string()
                .contains("differs from its exact model or calibration contract")
        );
    }

    #[test]
    fn refit_rejects_forged_digest() {
        let mut fixture = ScenarioFitFixture::build();
        fixture.compatibility.serving_contract_digest = hash(241);

        let error = fixture
            .fit()
            .expect_err("forged compatibility must fail closed");
        assert!(error.to_string().contains("incompatible"));
    }

    #[test]
    fn return_change_rekeys_binding() {
        let fixture = ScenarioFitFixture::build();
        let baseline = fixture.fit().expect("baseline joint refit");
        let mut changed = ScenarioFitFixture::build();
        let crypto = changed
            .routes
            .iter_mut()
            .find(|route| route.route == BuyModelRoute::Crypto)
            .expect("crypto evidence");
        let mut path = crypto.path_set.paths[0].clone();
        path.group_returns[2] = dec!(0.045);
        crypto.path_set.paths = BacktestPaths::from(vec![path]);
        reseal_path_set(&mut crypto.path_set);
        let changed = changed.fit().expect("changed joint refit");

        assert_ne!(
            baseline.artifact.content_hash,
            changed.artifact.content_hash
        );
        assert_ne!(
            baseline.binding.model_content_hash,
            changed.binding.model_content_hash
        );
        assert_ne!(
            baseline.binding.portfolio_scenario_model_artifact_id,
            changed.binding.portfolio_scenario_model_artifact_id
        );
    }

    #[test]
    fn refit_rejects_unordered_evidence() {
        let mut fixture = ScenarioFitFixture::build();
        fixture.routes.swap(0, 1);

        let error = fixture
            .fit()
            .expect_err("unordered Route evidence must fail closed");
        assert!(error.to_string().contains("compatibility"));
    }

    fn compatibility(
        represented_routes: &RepresentedRouteSet,
        routes: &[RouteEvidence],
    ) -> RouteCompatibilityDigests {
        RouteCompatibilityDigests::try_new(
            represented_routes,
            &routes
                .iter()
                .map(|route| RouteContractHash {
                    route: route.route,
                    content_hash: route.serving_contract_hash,
                })
                .collect::<Vec<_>>(),
            &routes
                .iter()
                .map(|route| RouteContractHash {
                    route: route.route,
                    content_hash: route.calibration.content_hash,
                })
                .collect::<Vec<_>>(),
            &routes
                .iter()
                .map(|route| RouteContractHash {
                    route: route.route,
                    content_hash: route.trade_policy_contract_hash,
                })
                .collect::<Vec<_>>(),
        )
        .expect("compatibility digests")
    }

    fn route_evidence(route: BuyModelRoute, bound_at: DateTime<Utc>) -> RouteEvidence {
        let seed = match route {
            BuyModelRoute::Pooled => 1_000,
            BuyModelRoute::Crypto => 2_000,
            BuyModelRoute::Weather => 3_000,
        };
        let model_version_id = ModelVersionId::new(Uuid::from_u128(seed + 1));
        let serving_contract_hash = hash(u8::try_from(seed / 100).expect("route hash seed"));
        let trade_policy_contract_hash =
            hash(u8::try_from(seed / 100 + 1).expect("trade-policy hash seed"));
        let model_artifact_hash =
            hash(u8::try_from(seed / 100 + 2).expect("model-artifact hash seed"));
        let calibration_source_model_version_id = ModelVersionId::new(Uuid::from_u128(seed + 101));
        let calibration_source_serving_contract_hash =
            hash(u8::try_from(seed / 100 + 15).expect("source serving hash seed"));
        let calibration_source_model_artifact_hash =
            hash(u8::try_from(seed / 100 + 16).expect("source artifact hash seed"));
        let training_dataset_id = TrainingDatasetId::new(Uuid::from_u128(seed + 2));
        let training_dataset_hash =
            hash(u8::try_from(seed / 100 + 3).expect("training-dataset hash seed"));
        let prediction_horizon_secs = 86_400;
        let contract = RouteModelContract {
            route,
            seed,
            bound_at,
            model_version_id,
            serving_contract_hash,
            model_artifact_hash,
            calibration_source_model_version_id,
            calibration_source_model_artifact_hash,
            calibration_source_serving_contract_hash,
            training_dataset_id,
            training_dataset_hash,
            prediction_horizon_secs,
        };
        let calibration = contract.calibration_artifact();
        let path_set = path_set(&contract, route_returns(route));
        RouteEvidence {
            route,
            model_version_id,
            model_artifact_hash,
            serving_contract_hash,
            calibration_source_model_version_id,
            calibration_source_model_artifact_hash,
            calibration_source_serving_contract_hash,
            trade_policy_contract_hash,
            prediction_horizon_secs,
            path_set,
            calibration,
        }
    }

    impl RouteModelContract {
        fn calibration_artifact(&self) -> CalibrationArtifactInfo {
            let contract = self;
            let seed = contract.seed;
            let fit_window_start = contract.bound_at - Duration::days(45);
            let fit_window_end = contract.bound_at - Duration::days(20);
            let calibration_split_hash =
                hash(u8::try_from(contract.seed / 100 + 4).expect("calibration-split hash seed"));
            let payload = ModelScoreCalibrationPayload {
                format_version: MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
                fit_contract: ModelScoreCalibrationFitContract {
                    model: ModelScoreCalibrationModelBinding {
                        model_version_id: contract.calibration_source_model_version_id,
                        artifact_hash: contract.calibration_source_model_artifact_hash,
                        serving_contract_hash: contract.calibration_source_serving_contract_hash,
                        model_spec_id: ModelSpecId::new(Uuid::from_u128(contract.seed + 3)),
                        model_spec_definition_hash: hash(
                            u8::try_from(contract.seed / 100 + 5).expect("model-spec hash seed"),
                        ),
                        model_family: ModelFamily::WeightedFactor,
                        profile_ref: ResearchProfileRef {
                            id: ResearchProfileId::new(format!(
                                "scenario_{}",
                                contract.route.as_str()
                            )),
                            version: 1,
                            content_hash: hash(
                                u8::try_from(contract.seed / 100 + 6).expect("profile hash seed"),
                            ),
                        },
                        category_scope: contract.route.category(),
                        prediction_horizon_secs: contract.prediction_horizon_secs,
                        training_dataset_id: contract.training_dataset_id,
                        training_dataset_hash: contract.training_dataset_hash,
                    },
                    calibration_dataset: ModelScoreCalibrationDatasetBinding {
                        calibration_dataset_id: TrainingDatasetId::new(Uuid::from_u128(
                            contract.seed + 4,
                        )),
                        dataset_hash: hash(
                            u8::try_from(seed / 100 + 7).expect("calibration dataset hash seed"),
                        ),
                        manifest_hash: hash(
                            u8::try_from(seed / 100 + 8).expect("manifest hash seed"),
                        ),
                        artifact_bytes_hash: hash(
                            u8::try_from(seed / 100 + 9).expect("artifact bytes hash seed"),
                        ),
                        source_slice_manifest_hash: hash(
                            u8::try_from(seed / 100 + 10).expect("source-slice hash seed"),
                        ),
                        feature_schema_hash: hash(
                            u8::try_from(seed / 100 + 11).expect("feature hash seed"),
                        ),
                        factor_schema_hash: hash(
                            u8::try_from(seed / 100 + 12).expect("factor hash seed"),
                        ),
                        label_schema_hash: hash(
                            u8::try_from(seed / 100 + 13).expect("label hash seed"),
                        ),
                    },
                    policy_snapshot: ModelScoreCalibrationPolicyBinding {
                        decision_policy_snapshot_id: DecisionPolicySnapshotId::new(
                            Uuid::from_u128(seed + 5),
                        ),
                        snapshot_hash: hash(
                            u8::try_from(seed / 100 + 14).expect("policy hash seed"),
                        ),
                    },
                },
                mapping: MonotoneMapping::Isotonic {
                    knots: vec![
                        IsotonicKnot {
                            score: dec!(-1),
                            probability: dec!(0.25),
                        },
                        IsotonicKnot {
                            score: dec!(1),
                            probability: dec!(0.75),
                        },
                    ],
                },
                reliability: ReliabilityReport {
                    bins: vec![
                        ReliabilityBin {
                            predicted_lo: dec!(0),
                            predicted_hi: dec!(0.5),
                            sample_count: 500,
                            mean_predicted: Probability::new(dec!(0.35)),
                            empirical_frequency: Probability::new(dec!(0.38)),
                            wilson_ci: (Probability::new(dec!(0.34)), Probability::new(dec!(0.42))),
                            mean_adverse_excursion_bps: Some(dec!(-700)),
                        },
                        ReliabilityBin {
                            predicted_lo: dec!(0.5),
                            predicted_hi: dec!(1),
                            sample_count: 500,
                            mean_predicted: Probability::new(dec!(0.68)),
                            empirical_frequency: Probability::new(dec!(0.7)),
                            wilson_ci: (Probability::new(dec!(0.66)), Probability::new(dec!(0.74))),
                            mean_adverse_excursion_bps: Some(dec!(-500)),
                        },
                    ],
                    brier_score: dec!(0.19),
                    log_loss: dec!(0.55),
                    ece: dec!(0.025),
                    n_samples: 1_000,
                },
                split_payout_rate: SplitPayoutRateEvidence {
                    total_sample_count: 1_000,
                    split_sample_count: 0,
                    empirical_probability: Probability::ZERO,
                    wilson_ci: (Probability::ZERO, Probability::new(dec!(0.003827))),
                    split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                        .expect("split payout ratio"),
                },
            };
            let content_hash = payload
                .content_hash(fit_window_start, fit_window_end, &calibration_split_hash)
                .expect("calibration content hash");
            CalibrationArtifactInfo {
                artifact_id: CalibrationArtifactId::new(Uuid::from_u128(seed + 6)),
                kind: CalibrationKind::ModelScore,
                content_hash,
                fit_window_start,
                fit_window_end,
                calibration_split_hash,
                sample_count: 1_000,
                payload: CalibrationArtifactPayload::ModelScore(Box::new(payload)),
                active: false,
                created_at: fit_window_end + Duration::hours(1),
            }
        }
    }

    fn path_set(contract: &RouteModelContract, group_returns: Vec<Decimal>) -> BacktestPathSetInfo {
        let seed = contract.seed;
        let bound_at = contract.bound_at;
        let model_version_id = contract.model_version_id;
        let serving_contract_hash = contract.serving_contract_hash;
        let model_artifact_hash = contract.model_artifact_hash;
        let training_dataset_id = contract.training_dataset_id;
        let training_dataset_hash = contract.training_dataset_hash;
        let window_start = bound_at - Duration::days(15);
        let window_end = bound_at - Duration::hours(1);
        let periods = decision_times(window_start, 1);
        let trial_grid = trial_grid(seed);
        let cscv_selection_evidence = selection_evidence(&periods, &group_returns, &trial_grid);
        let dsr_conservative_independent_trial_count = i64::from(
            cscv_selection_evidence
                .trial_dependence
                .conservative_independent_trial_count(),
        );
        let scenario_residuals = group_returns.iter().copied().map(Some).collect();
        let mut path_set = BacktestPathSetInfo {
            path_set_id: BacktestPathSetId::new(Uuid::from_u128(seed + 7)),
            model_version_id,
            model_run_id: ModelRunId::new(Uuid::from_u128(seed + 8)),
            training_dataset_id,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::new(Uuid::from_u128(seed + 9)),
            window_start,
            window_end,
            subject: CpcvPathSetSubject::new(
                model_artifact_hash,
                serving_contract_hash,
                training_dataset_hash,
                hash(u8::try_from(seed / 100 + 15).expect("dataset manifest hash seed")),
                hash(u8::try_from(seed / 100 + 16).expect("dataset bytes hash seed")),
                hash(u8::try_from(seed / 100 + 17).expect("policy snapshot hash seed")),
            ),
            methodology: CpcvMethodologyBinding::new(
                hash(u8::try_from(seed / 100 + 18).expect("config hash seed")),
                hash(u8::try_from(seed / 100 + 19).expect("portfolio caps hash seed")),
                hash(u8::try_from(seed / 100 + 20).expect("replay hash seed")),
                CpcvFoldCalibrationPolicy::SubjectHeuristic {
                    return_model_hash: hash(
                        u8::try_from(seed / 100 + 21).expect("return model hash seed"),
                    ),
                },
                CpcvTrialPathBinding::try_new(0, vec![0]).expect("trial path"),
                trial_grid,
            ),
            fold_artifacts: fold_artifacts(seed),
            path_count: 1,
            combination_count: 1,
            median_rank_ic: dec!(0.12),
            sharpe_distribution: SharpeDistribution {
                min: dec!(0.3),
                p25: dec!(0.4),
                median: dec!(0.5),
                p75: dec!(0.6),
                max: dec!(0.7),
                median_max_drawdown: Some(dec!(0.08)),
                median_tail_loss: Some(dec!(-0.03)),
                median_turnover: Some(dec!(0.2)),
                baseline_uplift: Some(dec!(0.04)),
            },
            paths: BacktestPaths::from(vec![BacktestPath {
                path_index: 0,
                decision_times: periods,
                group_returns,
                scenario_residuals,
                sharpe: dec!(0.5),
                rank_ic: dec!(0.12),
                max_drawdown: dec!(0.08),
                tail_loss: dec!(-0.03),
                turnover: Some(dec!(0.2)),
            }]),
            deflated_sharpe: dec!(0.95),
            dsr_benchmark_sharpe: dec!(0.1),
            pbo: cscv_selection_evidence.pbo,
            cscv_selection_evidence,
            min_track_record_length_secs: Some(2_592_000),
            dsr_conservative_independent_trial_count,
            trial_grid_count: 2,
            coord_search_effective_n: 1,
            path_set_hash: hash(1),
            created_at: bound_at - Duration::minutes(30),
        };
        reseal_path_set(&mut path_set);
        path_set
    }

    fn fold_artifacts(seed: u128) -> CpcvFoldArtifacts {
        let base = u8::try_from(seed / 100).expect("fold hash seed");
        CpcvFoldArtifacts::try_new(vec![
            CpcvFoldArtifact {
                identity: CpcvEstimatorIdentity::Validation {
                    combination_index: 0,
                    test_partitions_hash: hash(base + 30),
                    test_partition_count: 1,
                    test_groups_hash: hash(base + 31),
                    test_group_count: 1,
                },
                training_groups_hash: hash(base + 32),
                training_group_count: 5,
                calibration_fit_groups_hash: hash(base + 40),
                calibration_fit_group_count: 1,
                scenario_fit_groups_hash: hash(base + 46),
                scenario_fit_group_count: 1,
                model_artifact_hash: hash(base + 33),
                serving_contract_hash: hash(base + 34),
                model_payload_hash: hash(base + 35),
                calibration_function_hash: hash(base + 56),
                scenario_economic_function_hash: hash(base + 57),
                calibration_artifact_hash: hash(base + 41),
                scenario_model_hash: hash(base + 42),
            },
            CpcvFoldArtifact {
                identity: CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 0,
                    path_index: 0,
                    combination_index: 0,
                    test_partitions_hash: hash(base + 30),
                    test_partition_count: 1,
                    test_groups_hash: hash(base + 31),
                    test_group_count: 1,
                },
                training_groups_hash: hash(base + 36),
                training_group_count: 6,
                calibration_fit_groups_hash: hash(base + 43),
                calibration_fit_group_count: 1,
                scenario_fit_groups_hash: hash(base + 47),
                scenario_fit_group_count: 1,
                model_artifact_hash: hash(base + 37),
                serving_contract_hash: hash(base + 38),
                model_payload_hash: hash(base + 39),
                calibration_function_hash: hash(base + 58),
                scenario_economic_function_hash: hash(base + 59),
                calibration_artifact_hash: hash(base + 44),
                scenario_model_hash: hash(base + 45),
            },
            CpcvFoldArtifact {
                identity: CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 1,
                    path_index: 0,
                    combination_index: 0,
                    test_partitions_hash: hash(base + 30),
                    test_partition_count: 1,
                    test_groups_hash: hash(base + 31),
                    test_group_count: 1,
                },
                training_groups_hash: hash(base + 48),
                training_group_count: 6,
                calibration_fit_groups_hash: hash(base + 49),
                calibration_fit_group_count: 1,
                scenario_fit_groups_hash: hash(base + 50),
                scenario_fit_group_count: 1,
                model_artifact_hash: hash(base + 51),
                serving_contract_hash: hash(base + 52),
                model_payload_hash: hash(base + 53),
                calibration_function_hash: hash(base + 60),
                scenario_economic_function_hash: hash(base + 61),
                calibration_artifact_hash: hash(base + 54),
                scenario_model_hash: hash(base + 55),
            },
        ])
        .expect("fold artifacts")
    }

    fn trial_grid(seed: u128) -> CscvTrialGridBinding {
        let base = u8::try_from(seed / 100).expect("trial-grid hash seed");
        CscvTrialGridBinding::try_new(
            4,
            vec![
                CscvTrialDescriptor {
                    trial_id: 0,
                    label: "fixture-primary".to_owned(),
                    config_hash: hash(base + 56),
                },
                CscvTrialDescriptor {
                    trial_id: 1,
                    label: "fixture-challenger".to_owned(),
                    config_hash: hash(base + 57),
                },
            ],
        )
        .expect("trial grid")
    }

    fn selection_evidence(
        periods: &[DateTime<Utc>],
        primary_returns: &[Decimal],
        trial_grid: &CscvTrialGridBinding,
    ) -> CscvSelectionEvidence {
        let challenger_returns = primary_returns
            .iter()
            .map(|value| *value - dec!(0.002))
            .collect::<Vec<_>>();
        let matrix = TrialPerformanceMatrix::from_columns(
            periods.to_vec(),
            &[primary_returns.to_vec(), challenger_returns],
        )
        .expect("trial-performance matrix");
        analyze_selection_bias(&matrix, trial_grid).expect("CSCV selection evidence")
    }

    fn scenario_template(
        bound_at: DateTime<Utc>,
        routes: &RepresentedRouteSet,
        trade_policy_contract_digest: ContentHash,
        evidence: &[RouteEvidence],
    ) -> PortfolioScenarioModelArtifact {
        let as_of = bound_at - Duration::days(20);
        let fit_window_start = bound_at - Duration::days(60);
        let mut states = vec![
            scenario_state(0, PortfolioScenarioKind::PitBootstrap, "pit", routes, 2_000),
            scenario_state(
                1,
                PortfolioScenarioKind::CalibrationUncertainty,
                "calibration",
                routes,
                5_000,
            ),
            scenario_state(
                2,
                PortfolioScenarioKind::StructuralStress,
                "stress",
                routes,
                9_000,
            ),
        ];
        for state in &mut states {
            state.scenario_state_hash = state.recomputed_state_hash().expect("state hash");
        }
        let discount_curve = vec![DiscountCurvePoint {
            end_secs: 86_400,
            annualized_cost_bps: 500,
        }];
        let capital_time_bucket_contract_digest =
            CapitalTimeBucketContract::try_from(discount_curve.as_slice())
                .expect("template capital-time grid")
                .content_hash()
                .expect("template capital-time contract hash");
        let mut artifact = PortfolioScenarioModelArtifact {
            portfolio_scenario_model_artifact_id:
                PortfolioScenarioModelArtifactId::from_content_hash(&hash(2)),
            schema_version: SchemaVersion::FIRST,
            as_of,
            fit_window_start,
            time_bucket_secs: 86_400,
            ordered_routes: routes.routes.clone(),
            route_set_digest: routes.digest,
            serving_contract_digest: hash(3),
            calibration_contract_digest: hash(4),
            trade_policy_contract_digest,
            capital_time_bucket_contract_digest,
            scenario_random_stream_hash: hash(5),
            pit_residual_panel_hash: hash(6),
            calibration_uncertainty_model_hash: hash(7),
            stress_catalog_hash: hash(8),
            resampling_method: PortfolioScenarioResamplingMethod::StationaryBootstrap {
                expected_block_length: 2,
                scenario_horizon_buckets: 4,
            },
            route_fit_lineage: evidence
                .iter()
                .map(|route| PortfolioScenarioRouteFitLineage {
                    route: route.route,
                    model_lineage: route.model_lineage(),
                    fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
                        backtest_path_set_id: route.path_set.path_set_id,
                        backtest_path_set_hash: route.path_set.path_set_hash,
                        representative_path_index: 0,
                    },
                    calibration_artifact_id: route.calibration.artifact_id,
                    calibration_artifact_hash: route.calibration.content_hash,
                    trade_policy_contract_hash: route.trade_policy_contract_hash,
                    fit_window_start,
                    fit_window_end: as_of,
                })
                .collect(),
            states,
            distributions: vec![
                ScenarioDistribution {
                    distribution_id: "nominal".to_owned(),
                    nominal: true,
                    weights: scenario_weights([6_000, 3_000, 1_000]),
                },
                ScenarioDistribution {
                    distribution_id: "robust".to_owned(),
                    nominal: false,
                    weights: scenario_weights([3_000, 3_000, 4_000]),
                },
            ],
            discount_curve,
            content_hash: hash(9),
        };
        artifact.content_hash = artifact.recomputed_hash().expect("template hash");
        artifact.portfolio_scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&artifact.content_hash);
        artifact
    }

    fn scenario_state(
        scenario_index: u32,
        kind: PortfolioScenarioKind,
        label: &str,
        routes: &RepresentedRouteSet,
        quantile_bps: u32,
    ) -> PortfolioScenarioModelState {
        PortfolioScenarioModelState {
            scenario_index,
            kind,
            label: label.to_owned(),
            scenario_state_hash: hash(10),
            route_factors: routes
                .routes
                .iter()
                .copied()
                .map(|route| PortfolioScenarioRouteFactor {
                    route,
                    systematic_quantile_bps: quantile_bps,
                    systematic_weight_bps: 6_000,
                    calibrated_probability_shift_bps: 0,
                    split_probability_quantile_bps: match kind {
                        PortfolioScenarioKind::PitBootstrap => 5_000,
                        PortfolioScenarioKind::CalibrationUncertainty => 0,
                        PortfolioScenarioKind::StructuralStress => 10_000,
                    },
                    win_cash_recovery_bps: 10_000,
                    split_cash_recovery_bps: 5_000,
                    loss_cash_recovery_bps: 0,
                    executable_share_bps: 9_000,
                    capital_release_multiplier_bps: 10_000,
                    factor_lineage_hash: hash(
                        50 + u8::try_from(scenario_index).expect("state hash seed"),
                    ),
                })
                .collect(),
        }
    }

    fn scenario_weights(weights: [u32; 3]) -> Vec<ScenarioWeight> {
        weights
            .into_iter()
            .enumerate()
            .map(|(index, probability_bps)| ScenarioWeight {
                scenario_index: u32::try_from(index).expect("scenario index"),
                probability_bps,
            })
            .collect()
    }

    fn route_returns(route: BuyModelRoute) -> Vec<Decimal> {
        match route {
            BuyModelRoute::Pooled => vec![
                dec!(0.01),
                dec!(-0.01),
                dec!(0.015),
                dec!(-0.005),
                dec!(0.02),
                dec!(-0.01),
                dec!(0.012),
                dec!(-0.008),
                dec!(0.018),
                dec!(-0.012),
                dec!(0.011),
                dec!(-0.006),
            ],
            BuyModelRoute::Crypto => vec![
                dec!(0.02),
                dec!(-0.01),
                dec!(0.03),
                dec!(-0.02),
                dec!(0.01),
                dec!(-0.005),
                dec!(0.025),
                dec!(-0.015),
                dec!(0.018),
                dec!(-0.009),
                dec!(0.022),
                dec!(-0.011),
            ],
            BuyModelRoute::Weather => vec![
                dec!(0.01),
                dec!(-0.02),
                dec!(0.025),
                dec!(-0.015),
                dec!(0.02),
                dec!(-0.01),
                dec!(0.014),
                dec!(-0.012),
                dec!(0.019),
                dec!(-0.011),
                dec!(0.016),
                dec!(-0.008),
            ],
        }
    }

    fn decision_times(window_start: DateTime<Utc>, first_day: i64) -> Vec<DateTime<Utc>> {
        (0..12)
            .map(|offset| window_start + Duration::days(first_day + offset) + Duration::hours(1))
            .collect()
    }

    fn reseal_path_set(path_set: &mut BacktestPathSetInfo) {
        let path = path_set.paths.first().expect("path-set fixture path");
        path_set.cscv_selection_evidence = selection_evidence(
            &path.decision_times,
            &path.group_returns,
            &path_set.methodology.trial_grid,
        );
        path_set.pbo = path_set.cscv_selection_evidence.pbo;
        path_set.path_set_hash = hash(1);
        path_set.path_set_hash = path_set.expected_hash().expect("path-set hash");
    }

    fn hash(seed: u8) -> ContentHash {
        ContentHash::from_bytes([seed; 32])
    }
}
