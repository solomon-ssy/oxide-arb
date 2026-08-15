//! Deterministic materialization of report-specific joint scenario artifacts.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    ops::Deref,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::quant::{
        DiscountCurvePoint, PortfolioScenario, PortfolioScenarioArtifact,
        PortfolioScenarioFitEvidence, PortfolioScenarioKind, PortfolioScenarioModelArtifact,
        PortfolioScenarioModelState, PortfolioScenarioRouteFactor, PortfolioScenarioVisibility,
        RepresentedRouteSet, ScenarioMarketOutcome, ScenarioPayoutState,
        StructuralExclusivityGroup, StructuralOutcomeRef,
    },
    enums::quant::OutcomeSide,
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PortfolioScenarioModelArtifactBinding},
    types::{
        ContentHash, MarketId, PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId,
        Probability, Shares, TokenId, Usd, calibration::CalibratedPayoutDistribution,
    },
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde::Serialize;

use super::CapitalTimeBucketContract;

const BASIS_POINTS: u32 = 10_000;
const SECONDS_PER_YEAR: u64 = 31_536_000;
const USD_SCALE: u32 = 6;

/// One unique report-time market leg before joint states are materialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioScenarioLegInput {
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub calibrated_payout_distribution: CalibratedPayoutDistribution,
    /// Candidate-side bid depth observed at the frozen decision boundary. This is an exogenous
    /// venue-capacity input, not a function of any proposed portfolio tier.
    pub observed_exit_capacity_shares: Shares,
    pub base_capital_release_secs: u64,
    pub lineage_hash: ContentHash,
}

/// Structurally verified, immutably borrowed scenario-model contract.
///
/// The borrow prevents the model or its governance binding from being mutated
/// between full verification and concrete artifact generation. Per-decision
/// visibility is still checked for every generated artifact.
pub struct VerifiedPortfolioScenarioModel<'a> {
    binding: &'a PortfolioScenarioModelArtifactBinding,
    model: &'a PortfolioScenarioModelArtifact,
    represented_routes: &'a RepresentedRouteSet,
}

impl<'a> VerifiedPortfolioScenarioModel<'a> {
    /// Verify every model leaf, root, Route, distribution, and compatibility invariant once.
    pub fn verify(
        binding: &'a PortfolioScenarioModelArtifactBinding,
        model: &'a PortfolioScenarioModelArtifact,
        represented_routes: &'a RepresentedRouteSet,
    ) -> QuantResult<Self> {
        validate_model_contract(binding, model, represented_routes)?;
        Ok(Self {
            binding,
            model,
            represented_routes,
        })
    }

    /// Borrow an immutable scenario contract that was already fully verified
    /// at its owning construction boundary.
    ///
    /// This is crate-private so financial consumers cannot manufacture trust
    /// for deserialized input. [`crate::backtest::BacktestScenarioContext`]
    /// owns the only reusable boundary and keeps all three values immutable in
    /// one shared allocation after calling [`Self::verify`] exactly once.
    pub(crate) const fn from_verified(
        binding: &'a PortfolioScenarioModelArtifactBinding,
        model: &'a PortfolioScenarioModelArtifact,
        represented_routes: &'a RepresentedRouteSet,
    ) -> Self {
        Self {
            binding,
            model,
            represented_routes,
        }
    }

    #[must_use]
    pub const fn binding(&self) -> &PortfolioScenarioModelArtifactBinding {
        self.binding
    }

    #[must_use]
    pub const fn model(&self) -> &PortfolioScenarioModelArtifact {
        self.model
    }

    #[must_use]
    pub const fn represented_routes(&self) -> &RepresentedRouteSet {
        self.represented_routes
    }
}

/// Integrity- and shape-verified concrete scenario artifact.
///
/// Construction is either by the deterministic generator or by full
/// verification of a deserialized artifact. Financial consumers accept this
/// type so they cannot accidentally operate on unverified persistence input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedPortfolioScenarioArtifact {
    artifact: PortfolioScenarioArtifact,
}

impl SealedPortfolioScenarioArtifact {
    /// Verify a deserialized artifact from every outcome leaf to the root id.
    pub fn verify(artifact: PortfolioScenarioArtifact) -> QuantResult<Self> {
        verify_artifact(&artifact)?;
        Ok(Self { artifact })
    }

    fn from_generated(artifact: PortfolioScenarioArtifact) -> QuantResult<Self> {
        validate_artifact_shapes(&artifact)?;
        Ok(Self { artifact })
    }

    #[must_use]
    pub const fn artifact(&self) -> &PortfolioScenarioArtifact {
        &self.artifact
    }
}

impl From<SealedPortfolioScenarioArtifact> for PortfolioScenarioArtifact {
    fn from(value: SealedPortfolioScenarioArtifact) -> Self {
        value.artifact
    }
}

impl Deref for SealedPortfolioScenarioArtifact {
    type Target = PortfolioScenarioArtifact;

    fn deref(&self) -> &Self::Target {
        &self.artifact
    }
}

/// Complete frozen input for one concrete report scenario artifact.
#[derive(Clone, Copy)]
pub struct PortfolioScenarioGenerationInput<'contract, 'model> {
    pub model_contract: &'contract VerifiedPortfolioScenarioModel<'model>,
    pub decision_at: DateTime<Utc>,
    pub visibility: PortfolioScenarioVisibility,
    pub input_universe_hash: ContentHash,
    pub legs: &'contract [PortfolioScenarioLegInput],
}

/// Materializes one content-addressed scenario artifact without external I/O.
pub struct PortfolioScenarioGenerator;

impl PortfolioScenarioGenerator {
    /// Verify a promoted model/binding graph without materializing report markets.
    pub fn verify_model(
        binding: &PortfolioScenarioModelArtifactBinding,
        model: &PortfolioScenarioModelArtifact,
        represented_routes: &RepresentedRouteSet,
        decision_at: DateTime<Utc>,
        visibility: PortfolioScenarioVisibility,
    ) -> QuantResult<()> {
        let verified = VerifiedPortfolioScenarioModel::verify(binding, model, represented_routes)?;
        validate_model_visibility(&verified, decision_at, visibility)
    }

    /// Validate the promoted model and deterministically expand every concrete market leg.
    pub fn generate(
        input: PortfolioScenarioGenerationInput<'_, '_>,
    ) -> QuantResult<SealedPortfolioScenarioArtifact> {
        validate_model_visibility(input.model_contract, input.decision_at, input.visibility)?;
        validate_legs(&input)?;
        let model = input.model_contract.model();
        let represented_routes = input.model_contract.represented_routes();

        let mut scenarios = Vec::with_capacity(model.states.len());
        for state in &model.states {
            let factors = state
                .route_factors
                .iter()
                .map(|factor| (factor.route, factor))
                .collect::<BTreeMap<_, _>>();
            let mut outcomes = Vec::with_capacity(input.legs.len());
            for leg in input.legs {
                let factor = factors.get(&leg.route).copied().ok_or_else(|| {
                    scenario_error(format!(
                        "scenario-model state {} has no factor for Route {:?}",
                        state.scenario_index, leg.route
                    ))
                })?;
                outcomes.push(materialize_outcome(model, state, factor, leg)?);
            }
            outcomes.sort_by(|left, right| {
                (
                    left.route,
                    left.market_id.as_str(),
                    left.token_id.as_str(),
                    left.outcome_side.as_str(),
                )
                    .cmp(&(
                        right.route,
                        right.market_id.as_str(),
                        right.token_id.as_str(),
                        right.outcome_side.as_str(),
                    ))
            });
            let mut scenario = PortfolioScenario {
                scenario_index: state.scenario_index,
                kind: state.kind,
                label: state.label.clone(),
                scenario_model_state_hash: state.scenario_state_hash,
                scenario_state_hash: ContentHash::from_bytes([0_u8; 32]),
                market_outcomes: outcomes,
            };
            scenario.scenario_state_hash = scenario.recomputed_state_hash()?;
            scenarios.push(scenario);
        }

        let structural_exclusivity = structural_exclusivity(input.legs)?;
        let mut artifact = PortfolioScenarioArtifact {
            portfolio_scenario_artifact_id: PortfolioScenarioArtifactId::from_content_hash(
                &ContentHash::from_bytes([0_u8; 32]),
            ),
            portfolio_scenario_model_artifact_id: model.portfolio_scenario_model_artifact_id,
            scenario_model_content_hash: model.content_hash,
            schema_version: input.model_contract.binding().scenario_model_schema_version,
            decision_at: input.decision_at,
            visibility: input.visibility,
            input_universe_hash: input.input_universe_hash,
            ordered_routes: represented_routes.routes.clone(),
            route_set_digest: represented_routes.digest,
            serving_contract_digest: model.serving_contract_digest,
            calibration_contract_digest: model.calibration_contract_digest,
            recommendation_contract_digest: model.recommendation_contract_digest,
            evidence_regime: model.evidence_regime,
            capital_time_bucket_contract_digest: model.capital_time_bucket_contract_digest,
            scenarios,
            distributions: model.distributions.clone(),
            structural_exclusivity,
            discount_curve: model.discount_curve.clone(),
            content_hash: ContentHash::from_bytes([0_u8; 32]),
        };
        artifact.content_hash = artifact.recomputed_hash()?;
        artifact.portfolio_scenario_artifact_id =
            PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);
        SealedPortfolioScenarioArtifact::from_generated(artifact)
    }
}

fn validate_model_contract(
    binding: &PortfolioScenarioModelArtifactBinding,
    model: &PortfolioScenarioModelArtifact,
    represented_routes: &RepresentedRouteSet,
) -> QuantResult<()> {
    let model_hash = model.recomputed_hash()?;
    let capital_bucket_digest =
        CapitalTimeBucketContract::try_from(model.discount_curve.as_slice())
            .map_err(|error| {
                scenario_error(format!(
                    "scenario model has an invalid capital-time grid: {error}"
                ))
            })?
            .content_hash()?;
    if model_hash != model.content_hash
        || PortfolioScenarioModelArtifactId::from_content_hash(&model_hash)
            != model.portfolio_scenario_model_artifact_id
        || binding.portfolio_scenario_model_artifact_id
            != model.portfolio_scenario_model_artifact_id
        || binding.model_content_hash != model.content_hash
        || binding.scenario_model_schema_version != model.schema_version
        || binding.ordered_routes != represented_routes.routes
        || model.ordered_routes != represented_routes.routes
        || binding.route_set_digest != represented_routes.digest
        || model.route_set_digest != represented_routes.digest
        || binding.serving_contract_digest != model.serving_contract_digest
        || binding.calibration_contract_digest != model.calibration_contract_digest
        || binding.recommendation_contract_digest != model.recommendation_contract_digest
        || binding.capital_time_bucket_contract_digest != model.capital_time_bucket_contract_digest
        || model.capital_time_bucket_contract_digest != capital_bucket_digest
        || model.scenario_random_stream_hash == ContentHash::from_bytes([0_u8; 32])
        || model.fit_window_start >= model.as_of
        || model.time_bucket_secs == 0
        || model
            .route_fit_lineage
            .iter()
            .map(|lineage| lineage.route)
            .collect::<Vec<_>>()
            != represented_routes.routes
        || model.route_fit_lineage.iter().any(|lineage| {
            let model_lineage = lineage.model_lineage;
            let empty_hash = ContentHash::from_bytes([0_u8; 32]);
            lineage.fit_window_start < model.fit_window_start
                || lineage.fit_window_start >= lineage.fit_window_end
                || lineage.fit_window_end > model.as_of
                || model_lineage.evaluated_model_artifact_hash == empty_hash
                || model_lineage.evaluated_serving_contract_hash == empty_hash
                || model_lineage.calibration_source_model_artifact_hash == empty_hash
                || model_lineage.calibration_source_serving_contract_hash == empty_hash
                || match lineage.fit_evidence {
                    PortfolioScenarioFitEvidence::CpcvPath { .. } => {
                        model_lineage.evaluated_model_version_id
                            == model_lineage.calibration_source_model_version_id
                            || model_lineage.evaluated_model_artifact_hash
                                == model_lineage.calibration_source_model_artifact_hash
                            || model_lineage.evaluated_serving_contract_hash
                                == model_lineage.calibration_source_serving_contract_hash
                    }
                    PortfolioScenarioFitEvidence::NestedFold { .. } => {
                        model_lineage.evaluated_model_version_id
                            != model_lineage.calibration_source_model_version_id
                            || model_lineage.evaluated_model_artifact_hash
                                != model_lineage.calibration_source_model_artifact_hash
                            || model_lineage.evaluated_serving_contract_hash
                                != model_lineage.calibration_source_serving_contract_hash
                    }
                }
        })
    {
        return Err(scenario_error(
            "scenario-model binding, content identity, Route set, compatibility, or PIT boundary mismatch",
        )
        .into());
    }
    if model.states.is_empty() || model.distributions.is_empty() || model.discount_curve.is_empty()
    {
        return Err(scenario_error(
            "scenario model must contain states, distributions, and a discount curve",
        )
        .into());
    }
    let expected_indices = (0..model.states.len())
        .map(|index| {
            u32::try_from(index).map_err(|error| ReportError::NumericOverflow {
                field: "scenario_model.state_index",
                detail: error.to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let actual_indices = model
        .states
        .iter()
        .map(|state| state.scenario_index)
        .collect::<Vec<_>>();
    if actual_indices != expected_indices {
        return Err(scenario_error(
            "scenario-model state indices must be contiguous and canonically ordered",
        )
        .into());
    }
    for state in &model.states {
        validate_state(state, represented_routes)?;
    }
    validate_scenario_catalog(model, represented_routes)?;
    validate_distributions(model)?;
    validate_discount_curve(&model.discount_curve)
}

fn validate_model_visibility(
    verified: &VerifiedPortfolioScenarioModel<'_>,
    decision_at: DateTime<Utc>,
    visibility: PortfolioScenarioVisibility,
) -> QuantResult<()> {
    let binding = verified.binding();
    let model = verified.model();
    match visibility {
        PortfolioScenarioVisibility::PointInTime => {
            if binding.bound_at > decision_at || model.as_of > decision_at {
                return Err(scenario_error(
                    "scenario model or binding was not visible at the decision boundary",
                )
                .into());
            }
        }
        PortfolioScenarioVisibility::HistoricalReplay {
            governance_frozen_at,
        } => {
            if binding.bound_at > governance_frozen_at || model.as_of > decision_at {
                return Err(scenario_error(
                    "historical replay scenario violates its independent data or governance cutoff",
                )
                .into());
            }
        }
        PortfolioScenarioVisibility::PurgedCrossValidation {
            fit_evidence_hash,
            test_groups_hash,
        } => {
            if fit_evidence_hash != model.pit_residual_panel_hash
                || test_groups_hash == ContentHash::from_bytes([0_u8; 32])
            {
                return Err(scenario_error(
                    "purged cross-validation scenario use is not bound to its exact fit panel and held-out groups",
                )
                .into());
            }
        }
    }
    Ok(())
}

fn validate_state(
    state: &PortfolioScenarioModelState,
    represented_routes: &RepresentedRouteSet,
) -> QuantResult<()> {
    if state.label.trim().is_empty()
        || state.recomputed_state_hash()? != state.scenario_state_hash
        || state
            .route_factors
            .iter()
            .map(|factor| factor.route)
            .collect::<Vec<_>>()
            != represented_routes.routes
    {
        return Err(scenario_error(format!(
            "scenario-model state {} has invalid identity, label, or Route order",
            state.scenario_index
        ))
        .into());
    }
    for factor in &state.route_factors {
        if factor.systematic_quantile_bps >= BASIS_POINTS
            || factor.systematic_weight_bps > BASIS_POINTS
            || factor.split_probability_quantile_bps > BASIS_POINTS
            || factor.win_cash_recovery_bps > BASIS_POINTS
            || factor.split_cash_recovery_bps > BASIS_POINTS
            || factor.loss_cash_recovery_bps > BASIS_POINTS
            || factor.executable_share_bps == 0
            || factor.executable_share_bps > BASIS_POINTS
            || factor.capital_release_multiplier_bps == 0
            || factor.calibrated_probability_shift_bps < -10_000_i32
            || factor.calibrated_probability_shift_bps > 10_000_i32
            || factor.loss_cash_recovery_bps > factor.split_cash_recovery_bps
            || factor.split_cash_recovery_bps > factor.win_cash_recovery_bps
        {
            return Err(scenario_error(format!(
                "scenario-model state {} has an out-of-range Route factor",
                state.scenario_index
            ))
            .into());
        }
    }
    Ok(())
}

fn validate_scenario_catalog(
    model: &PortfolioScenarioModelArtifact,
    represented_routes: &RepresentedRouteSet,
) -> QuantResult<()> {
    let kinds = model
        .states
        .iter()
        .map(|state| state.kind)
        .collect::<HashSet<_>>();
    if kinds.len() != 3
        || !kinds.contains(&PortfolioScenarioKind::PitBootstrap)
        || !kinds.contains(&PortfolioScenarioKind::CalibrationUncertainty)
        || !kinds.contains(&PortfolioScenarioKind::StructuralStress)
    {
        return Err(scenario_error(
            "scenario model must contain PIT bootstrap, calibration uncertainty, and structural stress states",
        )
        .into());
    }
    for route in &represented_routes.routes {
        let mut uncertainty_quantiles = model
            .states
            .iter()
            .filter(|state| state.kind != PortfolioScenarioKind::PitBootstrap)
            .flat_map(|state| &state.route_factors)
            .filter(|factor| factor.route == *route)
            .map(|factor| factor.split_probability_quantile_bps);
        let Some(first) = uncertainty_quantiles.next() else {
            return Err(scenario_error(format!(
                "Route {route:?} has no split-probability uncertainty states"
            ))
            .into());
        };
        let (minimum, maximum) = uncertainty_quantiles
            .fold((first, first), |(minimum, maximum), quantile| {
                (minimum.min(quantile), maximum.max(quantile))
            });
        if minimum != 0 || maximum != BASIS_POINTS {
            return Err(scenario_error(format!(
                "Route {route:?} split uncertainty must span both Wilson interval endpoints"
            ))
            .into());
        }
    }
    Ok(())
}

fn validate_distributions(model: &PortfolioScenarioModelArtifact) -> QuantResult<()> {
    let scenario_count = model.states.len();
    let mut distribution_ids = HashSet::new();
    let mut nominal_count = 0_u32;
    for distribution in &model.distributions {
        if distribution.distribution_id.trim().is_empty()
            || !distribution_ids.insert(distribution.distribution_id.as_str())
        {
            return Err(
                scenario_error("scenario distribution ids must be non-empty and unique").into(),
            );
        }
        nominal_count += u32::from(distribution.nominal);
        if distribution.weights.len() != scenario_count {
            return Err(scenario_error(format!(
                "distribution {} does not cover every scenario exactly once",
                distribution.distribution_id
            ))
            .into());
        }
        let mut prior = None;
        let mut mass = 0_u32;
        let mut kind_mass = [0_u32; 3];
        for weight in &distribution.weights {
            if prior.is_some_and(|value| value >= weight.scenario_index)
                || usize::try_from(weight.scenario_index)
                    .ok()
                    .is_none_or(|index| index >= scenario_count)
            {
                return Err(scenario_error(format!(
                    "distribution {} has unordered or unknown scenario weights",
                    distribution.distribution_id
                ))
                .into());
            }
            mass = mass.checked_add(weight.probability_bps).ok_or_else(|| {
                ReportError::NumericOverflow {
                    field: "scenario_distribution.probability_bps",
                    detail: "distribution mass overflowed u32".to_owned(),
                }
            })?;
            let state_index = usize::try_from(weight.scenario_index).map_err(|error| {
                ReportError::NumericOverflow {
                    field: "scenario_distribution.scenario_index",
                    detail: error.to_string(),
                }
            })?;
            let kind_index = match model.states[state_index].kind {
                PortfolioScenarioKind::PitBootstrap => 0,
                PortfolioScenarioKind::CalibrationUncertainty => 1,
                PortfolioScenarioKind::StructuralStress => 2,
            };
            kind_mass[kind_index] = kind_mass[kind_index]
                .checked_add(weight.probability_bps)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "scenario_distribution.kind_probability_bps",
                    detail: "scenario-kind probability mass overflowed u32".to_owned(),
                })?;
            prior = Some(weight.scenario_index);
        }
        if mass != BASIS_POINTS {
            return Err(scenario_error(format!(
                "distribution {} mass is {mass}, expected {BASIS_POINTS}",
                distribution.distribution_id
            ))
            .into());
        }
        if kind_mass.contains(&0) {
            return Err(scenario_error(format!(
                "distribution {} must assign positive mass to every scenario provenance class",
                distribution.distribution_id
            ))
            .into());
        }
    }
    if nominal_count != 1 {
        return Err(
            scenario_error("scenario model must contain exactly one nominal distribution").into(),
        );
    }
    Ok(())
}

fn validate_discount_curve(curve: &[DiscountCurvePoint]) -> QuantResult<()> {
    if curve
        .windows(2)
        .any(|points| points[0].end_secs >= points[1].end_secs)
        || curve.first().is_none_or(|point| point.end_secs == 0)
    {
        return Err(scenario_error(
            "discount-curve tenors must be positive, unique, and strictly increasing",
        )
        .into());
    }
    Ok(())
}

fn validate_legs(input: &PortfolioScenarioGenerationInput<'_, '_>) -> QuantResult<()> {
    let mut identities = HashSet::new();
    for leg in input.legs {
        leg.calibrated_payout_distribution
            .validate()
            .map_err(|detail| {
                scenario_error(format!("invalid calibrated payout distribution: {detail}"))
            })?;
        if !input
            .model_contract
            .represented_routes()
            .routes
            .contains(&leg.route)
            || !leg.observed_exit_capacity_shares.is_positive()
            || leg.base_capital_release_secs == 0
            || !identities.insert((
                leg.route,
                leg.market_id.as_str(),
                leg.token_id.as_str(),
                leg.outcome_side,
            ))
        {
            return Err(scenario_error(
                "scenario legs must be unique, positive, and owned by represented Routes",
            )
            .into());
        }
    }
    Ok(())
}

fn materialize_outcome(
    model: &PortfolioScenarioModelArtifact,
    state: &PortfolioScenarioModelState,
    factor: &PortfolioScenarioRouteFactor,
    leg: &PortfolioScenarioLegInput,
) -> QuantResult<ScenarioMarketOutcome> {
    let idiosyncratic_quantile = market_quantile(model, state, leg)?;
    let systematic_weight = u64::from(factor.systematic_weight_bps);
    let idiosyncratic_weight = u64::from(BASIS_POINTS - factor.systematic_weight_bps);
    let blended = u64::from(factor.systematic_quantile_bps)
        .checked_mul(systematic_weight)
        .and_then(|value| {
            u64::from(idiosyncratic_quantile)
                .checked_mul(idiosyncratic_weight)
                .and_then(|idiosyncratic| value.checked_add(idiosyncratic))
        })
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_model.quantile",
            detail: "systematic/idiosyncratic quantile blend overflowed u64".to_owned(),
        })?
        / u64::from(BASIS_POINTS);
    let conditional_win_bps = probability_bps(
        "scenario_leg.winner_take_all_win_probability",
        leg.calibrated_payout_distribution
            .winner_take_all_win_probability,
    )?;
    let shifted = i64::from(conditional_win_bps)
        .checked_add(i64::from(factor.calibrated_probability_shift_bps))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_model.calibrated_probability_shift_bps",
            detail: "shifted probability overflowed i64".to_owned(),
        })?
        .clamp(0, i64::from(BASIS_POINTS));
    let shifted_conditional_win_bps =
        u32::try_from(shifted).map_err(|error| ReportError::NumericOverflow {
            field: "scenario_model.shifted_conditional_win_bps",
            detail: error.to_string(),
        })?;
    let split_probability = scenario_split_probability(state, factor, leg)?;
    let split_probability_bps =
        probability_bps("scenario_leg.split_probability", split_probability)?;
    let non_split_bps = BASIS_POINTS
        .checked_sub(split_probability_bps)
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_model.non_split_probability_bps",
            detail: "split probability exceeds the probability simplex".to_owned(),
        })?;
    let win_mass_bps = u64::from(non_split_bps)
        .checked_mul(u64::from(shifted_conditional_win_bps))
        .and_then(|value| value.checked_add(u64::from(BASIS_POINTS / 2)))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_model.win_probability_mass_bps",
            detail: "conditional win mass overflowed u64".to_owned(),
        })?
        / u64::from(BASIS_POINTS);
    let split_boundary = win_mass_bps
        .checked_add(u64::from(split_probability_bps))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_model.split_probability_boundary_bps",
            detail: "split probability boundary overflowed u64".to_owned(),
        })?;
    let blended = u32::try_from(blended).map_err(|error| ReportError::NumericOverflow {
        field: "scenario_model.blended_quantile_bps",
        detail: error.to_string(),
    })?;
    let payout_state = if u64::from(blended) < win_mass_bps {
        ScenarioPayoutState::Win
    } else if u64::from(blended) < split_boundary {
        ScenarioPayoutState::Split
    } else {
        ScenarioPayoutState::Loss
    };
    let recovery_bps = match payout_state {
        ScenarioPayoutState::Win => factor.win_cash_recovery_bps,
        ScenarioPayoutState::Split => factor.split_cash_recovery_bps,
        ScenarioPayoutState::Loss => factor.loss_cash_recovery_bps,
    };
    let release_secs = scaled_release(
        leg.base_capital_release_secs,
        factor.capital_release_multiplier_bps,
    )?;
    let max_shares = scaled_shares(
        leg.observed_exit_capacity_shares,
        factor.executable_share_bps,
    )?;
    let discounted_cash =
        discounted_cash_per_share(recovery_bps, release_secs, &model.discount_curve)?;
    let mut outcome = ScenarioMarketOutcome {
        route: leg.route,
        market_id: leg.market_id.clone(),
        token_id: leg.token_id.clone(),
        outcome_side: leg.outcome_side,
        payout_state,
        max_executable_exit_shares: max_shares,
        discounted_exit_cash_per_share_usd: discounted_cash,
        capital_release_secs: release_secs,
        source_lineage_hash: leg.lineage_hash,
        scenario_factor_lineage_hash: factor.factor_lineage_hash,
        outcome_lineage_hash: ContentHash::from_bytes([0_u8; 32]),
    };
    outcome.outcome_lineage_hash =
        outcome.recomputed_lineage_hash(model.content_hash, state.scenario_state_hash)?;
    Ok(outcome)
}

fn market_quantile(
    model: &PortfolioScenarioModelArtifact,
    state: &PortfolioScenarioModelState,
    leg: &PortfolioScenarioLegInput,
) -> QuantResult<u32> {
    let hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/scenario-market-quantile",
        2,
        &(
            model.scenario_random_stream_hash,
            state.scenario_index,
            leg.route,
            &leg.market_id,
            &leg.token_id,
            leg.outcome_side,
        ),
    )?;
    let bytes = hash.as_bytes();
    let value = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    Ok(value % BASIS_POINTS)
}

fn scenario_split_probability(
    state: &PortfolioScenarioModelState,
    factor: &PortfolioScenarioRouteFactor,
    leg: &PortfolioScenarioLegInput,
) -> QuantResult<Probability> {
    let distribution = leg.calibrated_payout_distribution;
    if state.kind == PortfolioScenarioKind::PitBootstrap {
        return Ok(distribution.split_probability);
    }
    let lower = distribution.split_probability_interval.0.inner();
    let upper = distribution.split_probability_interval.1.inner();
    let width = upper
        .checked_sub(lower)
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_leg.split_probability_interval",
            detail: "split probability interval is inverted".to_owned(),
        })?;
    let quantile = Decimal::from(factor.split_probability_quantile_bps)
        .checked_div(Decimal::from(BASIS_POINTS))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_model.split_probability_quantile_bps",
            detail: "split probability quantile division failed".to_owned(),
        })?;
    let probability =
        lower
            .checked_add(width.checked_mul(quantile).ok_or_else(|| {
                ReportError::NumericOverflow {
                    field: "scenario_leg.split_probability",
                    detail: "split interval interpolation overflowed Decimal".to_owned(),
                }
            })?)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "scenario_leg.split_probability",
                detail: "split probability interpolation overflowed Decimal".to_owned(),
            })?;
    Ok(Probability::new(probability))
}

fn probability_bps(field: &'static str, probability: Probability) -> QuantResult<u32> {
    let scaled = (probability.inner() * Decimal::from(BASIS_POINTS))
        .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven);
    scaled.to_u32().ok_or_else(|| {
        ReportError::NumericOverflow {
            field,
            detail: format!("probability {probability} does not fit integer basis points"),
        }
        .into()
    })
}

fn scaled_release(base: u64, multiplier_bps: u32) -> QuantResult<u64> {
    let numerator = base.checked_mul(u64::from(multiplier_bps)).ok_or_else(|| {
        ReportError::NumericOverflow {
            field: "scenario_market_outcome.capital_release_secs",
            detail: "capital-release multiplier overflowed u64".to_owned(),
        }
    })?;
    let adjusted = numerator
        .checked_add(u64::from(BASIS_POINTS - 1))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_market_outcome.capital_release_secs",
            detail: "capital-release ceiling adjustment overflowed u64".to_owned(),
        })?
        / u64::from(BASIS_POINTS);
    Ok(adjusted.max(1))
}

fn scaled_shares(shares: Shares, executable_bps: u32) -> QuantResult<Shares> {
    let scaled = shares
        .inner()
        .checked_mul(Decimal::from(executable_bps))
        .and_then(|value| value.checked_div(Decimal::from(BASIS_POINTS)))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario_market_outcome.max_executable_exit_shares",
            detail: "executable-share stress scaling overflowed Decimal".to_owned(),
        })?
        .round_dp_with_strategy(6, RoundingStrategy::ToZero);
    if scaled <= Decimal::ZERO {
        return Err(scenario_error(
            "scenario-model executable-share stress leaves no positive capacity",
        )
        .into());
    }
    Ok(Shares::new(scaled))
}

fn discounted_cash_per_share(
    recovery_bps: u32,
    release_secs: u64,
    curve: &[DiscountCurvePoint],
) -> QuantResult<Usd> {
    let point = curve
        .iter()
        .find(|point| release_secs <= point.end_secs)
        .ok_or_else(|| {
            scenario_error(format!(
                "discount curve does not cover release horizon {release_secs} seconds"
            ))
        })?;
    let recovery = Decimal::from(recovery_bps) / Decimal::from(BASIS_POINTS);
    let annualized = Decimal::from(point.annualized_cost_bps) / Decimal::from(BASIS_POINTS);
    let years = Decimal::from(release_secs) / Decimal::from(SECONDS_PER_YEAR);
    let denominator =
        Decimal::ONE
            .checked_add(annualized.checked_mul(years).ok_or_else(|| {
                ReportError::NumericOverflow {
                    field: "scenario_market_outcome.discount_factor",
                    detail: "annualized capital cost multiplication overflowed Decimal".to_owned(),
                }
            })?)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "scenario_market_outcome.discount_factor",
                detail: "discount denominator overflowed Decimal".to_owned(),
            })?;
    let discounted =
        recovery
            .checked_div(denominator)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "scenario_market_outcome.discounted_cash_per_share",
                detail: "discounted cash division failed".to_owned(),
            })?;
    Ok(Usd::new(discounted.round_dp_with_strategy(
        USD_SCALE,
        RoundingStrategy::MidpointNearestEven,
    )))
}

fn structural_exclusivity(
    legs: &[PortfolioScenarioLegInput],
) -> QuantResult<Vec<StructuralExclusivityGroup>> {
    let mut markets = BTreeMap::<&MarketId, HashSet<OutcomeSide>>::new();
    for leg in legs {
        markets
            .entry(&leg.market_id)
            .or_default()
            .insert(leg.outcome_side);
    }
    let mut groups = Vec::new();
    for (market_id, sides) in markets {
        if sides.len() < 2 {
            continue;
        }
        let mut sides = sides.into_iter().collect::<Vec<_>>();
        sides.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let group_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/market-structural-exclusivity",
            1,
            &(market_id, &sides),
        )?;
        groups.push(StructuralExclusivityGroup {
            group_id: group_hash.to_string(),
            members: sides
                .into_iter()
                .map(|outcome_side| StructuralOutcomeRef {
                    market_id: (*market_id).clone(),
                    outcome_side,
                })
                .collect(),
        });
    }
    Ok(groups)
}

fn verify_artifact(artifact: &PortfolioScenarioArtifact) -> QuantResult<()> {
    for scenario in &artifact.scenarios {
        for outcome in &scenario.market_outcomes {
            if outcome.recomputed_lineage_hash(
                artifact.scenario_model_content_hash,
                scenario.scenario_model_state_hash,
            )? != outcome.outcome_lineage_hash
            {
                return Err(scenario_error(format!(
                    "scenario {} contains an outcome whose leaf hash differs from its canonical preimage",
                    scenario.scenario_index
                ))
                .into());
            }
        }
        if scenario.recomputed_state_hash()? != scenario.scenario_state_hash {
            return Err(scenario_error(format!(
                "scenario {} root differs from its ordered outcome leaves",
                scenario.scenario_index
            ))
            .into());
        }
    }
    if artifact.recomputed_hash()? != artifact.content_hash
        || PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash)
            != artifact.portfolio_scenario_artifact_id
    {
        return Err(scenario_error(
            "scenario artifact id or root hash differs from its canonical preimage",
        )
        .into());
    }
    validate_artifact_shapes(artifact)
}

fn validate_artifact_shapes(artifact: &PortfolioScenarioArtifact) -> QuantResult<()> {
    if artifact.scenarios.is_empty() || artifact.distributions.is_empty() {
        return Err(scenario_error("scenarios and allowed distributions must be non-empty").into());
    }
    validate_artifact_states(artifact)?;
    validate_artifact_distributions(artifact)?;
    validate_artifact_structure(artifact)
}

fn validate_artifact_states(artifact: &PortfolioScenarioArtifact) -> QuantResult<()> {
    let mut has_pit_bootstrap = false;
    let mut has_calibration_uncertainty = false;
    let mut has_structural_stress = false;
    let mut labels = HashSet::new();
    let mut scenario_hashes = HashSet::new();
    let empty_hash = ContentHash::from_bytes([0_u8; 32]);
    for (expected, scenario) in artifact.scenarios.iter().enumerate() {
        let expected = u32::try_from(expected).map_err(|error| ReportError::NumericOverflow {
            field: "scenario_index",
            detail: error.to_string(),
        })?;
        if scenario.scenario_index != expected {
            return Err(scenario_error("scenario indexes must be contiguous and canonical").into());
        }
        if scenario.label.trim().is_empty()
            || scenario.scenario_model_state_hash == empty_hash
            || !labels.insert(scenario.label.as_str())
            || !scenario_hashes.insert(scenario.scenario_state_hash)
        {
            return Err(scenario_error(
                "scenario labels, model-state hashes, and state roots must be non-empty and unique",
            )
            .into());
        }
        match scenario.kind {
            PortfolioScenarioKind::PitBootstrap => has_pit_bootstrap = true,
            PortfolioScenarioKind::CalibrationUncertainty => {
                has_calibration_uncertainty = true;
            }
            PortfolioScenarioKind::StructuralStress => has_structural_stress = true,
        }
        let mut outcome_keys = HashSet::new();
        let mut outcome_hashes = HashSet::new();
        for outcome in &scenario.market_outcomes {
            let key = outcome_key(outcome);
            if !artifact.ordered_routes.contains(&outcome.route)
                || !outcome_keys.insert(key)
                || !outcome_hashes.insert(outcome.outcome_lineage_hash)
                || outcome.source_lineage_hash == empty_hash
                || outcome.scenario_factor_lineage_hash == empty_hash
                || !outcome.max_executable_exit_shares.is_positive()
                || outcome.discounted_exit_cash_per_share_usd.is_negative()
                || outcome.capital_release_secs == 0
            {
                return Err(scenario_error(format!(
                    "scenario {} has an invalid, duplicate, or out-of-route market outcome",
                    scenario.scenario_index
                ))
                .into());
            }
        }
        if scenario
            .market_outcomes
            .windows(2)
            .any(|window| outcome_key(&window[0]) >= outcome_key(&window[1]))
        {
            return Err(scenario_error(format!(
                "scenario {} market outcomes are not in strict canonical order",
                scenario.scenario_index
            ))
            .into());
        }
    }
    let reference = &artifact
        .scenarios
        .first()
        .ok_or_else(|| scenario_error("scenario artifact is empty"))?
        .market_outcomes;
    if artifact.scenarios.iter().skip(1).any(|scenario| {
        scenario.market_outcomes.len() != reference.len()
            || scenario
                .market_outcomes
                .iter()
                .zip(reference)
                .any(|(outcome, expected)| outcome_key(outcome) != outcome_key(expected))
    }) {
        return Err(scenario_error(
            "every scenario must contain the same strictly ordered market-outcome identities",
        )
        .into());
    }
    if !(has_pit_bootstrap && has_calibration_uncertainty && has_structural_stress) {
        return Err(scenario_error(
            "artifact must contain PIT-bootstrap, calibration-uncertainty, and structural-stress scenarios",
        )
        .into());
    }
    Ok(())
}

fn outcome_key(outcome: &ScenarioMarketOutcome) -> (BuyModelRoute, &str, &str, &str) {
    (
        outcome.route,
        outcome.market_id.as_str(),
        outcome.token_id.as_str(),
        outcome.outcome_side.as_str(),
    )
}

fn validate_artifact_distributions(artifact: &PortfolioScenarioArtifact) -> QuantResult<()> {
    let mut nominal_count = 0_u32;
    let scenario_count =
        u32::try_from(artifact.scenarios.len()).map_err(|error| ReportError::NumericOverflow {
            field: "scenario_count",
            detail: error.to_string(),
        })?;
    let expected_indexes = (0..scenario_count).collect::<BTreeSet<_>>();
    let mut distribution_ids = HashSet::new();
    for distribution in &artifact.distributions {
        nominal_count += u32::from(distribution.nominal);
        if !distribution_ids.insert(distribution.distribution_id.as_str()) {
            return Err(scenario_error("distribution identifiers must be unique").into());
        }
        if distribution.distribution_id.trim().is_empty()
            || distribution
                .weights
                .iter()
                .enumerate()
                .any(|(index, weight)| u32::try_from(index) != Ok(weight.scenario_index))
        {
            return Err(scenario_error(
                "distribution ids must be non-empty and weights must use canonical scenario order",
            )
            .into());
        }
        let indexes = distribution
            .weights
            .iter()
            .map(|weight| weight.scenario_index)
            .collect::<BTreeSet<_>>();
        let mass = distribution
            .weights
            .iter()
            .try_fold(0_u32, |sum, weight| sum.checked_add(weight.probability_bps))
            .ok_or_else(|| scenario_error("distribution probability mass overflow"))?;
        if indexes != expected_indexes
            || distribution.weights.len() != expected_indexes.len()
            || mass != BASIS_POINTS
        {
            return Err(scenario_error(
                "every distribution must cover each scenario exactly once and sum to 10000 bps",
            )
            .into());
        }
    }
    if nominal_count != 1 {
        return Err(scenario_error("exactly one nominal scenario distribution is required").into());
    }
    if artifact
        .distributions
        .windows(2)
        .any(|window| window[0].distribution_id >= window[1].distribution_id)
    {
        return Err(scenario_error(
            "scenario distributions must be strictly ordered by distribution id",
        )
        .into());
    }
    Ok(())
}

fn validate_artifact_structure(artifact: &PortfolioScenarioArtifact) -> QuantResult<()> {
    let mut structural_ids = HashSet::new();
    for group in &artifact.structural_exclusivity {
        let mut members = HashSet::new();
        if group.group_id.trim().is_empty()
            || !structural_ids.insert(group.group_id.as_str())
            || group.members.len() < 2
            || group.members.iter().any(|member| {
                !members.insert((member.market_id.as_str(), member.outcome_side.as_str()))
            })
        {
            return Err(scenario_error(
                "structural exclusivity groups require unique ids and at least two unique members",
            )
            .into());
        }
    }
    let bucket_ends = artifact
        .discount_curve
        .iter()
        .map(|point| point.end_secs)
        .collect::<Vec<_>>();
    if bucket_ends.is_empty() || bucket_ends.windows(2).any(|window| window[0] >= window[1]) {
        return Err(scenario_error(
            "discount curve time buckets must be non-empty and strictly increasing",
        )
        .into());
    }
    Ok(())
}

fn scenario_error(detail: impl Into<String>) -> ReportError {
    ReportError::ScenarioArtifact {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, QuantResult};
    use quant_pivot_models::{
        domain::quant::{
            DiscountCurvePoint, PortfolioScenarioEvidenceRegime, PortfolioScenarioFitEvidence,
            PortfolioScenarioKind, PortfolioScenarioModelArtifact, PortfolioScenarioModelState,
            PortfolioScenarioResamplingMethod, PortfolioScenarioRouteFactor,
            PortfolioScenarioRouteFitLineage, PortfolioScenarioRouteModelLineage,
            PortfolioScenarioVisibility, RepresentedRouteSet, ScenarioDistribution,
            ScenarioPayoutState, ScenarioWeight,
        },
        enums::quant::OutcomeSide,
        hashing::CanonicalDigest,
        runtime_config::{BuyModelRoute, PortfolioScenarioModelArtifactBinding},
        types::{
            BacktestPathSetId, CalibrationArtifactId, ContentHash, MarketId, ModelVersionId,
            PayoutRatio, PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId,
            Probability, SchemaVersion, Shares, TokenId, Usd,
            calibration::CalibratedPayoutDistribution,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CapitalTimeBucketContract, PortfolioScenarioArtifact, PortfolioScenarioGenerationInput,
        PortfolioScenarioGenerator, PortfolioScenarioLegInput, SealedPortfolioScenarioArtifact,
        VerifiedPortfolioScenarioModel, market_quantile, materialize_outcome,
    };

    struct Fixture {
        at: DateTime<Utc>,
        routes: RepresentedRouteSet,
        model: PortfolioScenarioModelArtifact,
        binding: PortfolioScenarioModelArtifactBinding,
        legs: Vec<PortfolioScenarioLegInput>,
        universe_hash: ContentHash,
    }

    impl Fixture {
        fn build() -> QuantResult<Self> {
            let at = Utc
                .timestamp_opt(1_750_000_000, 0)
                .single()
                .ok_or_else(|| QuantError::config("invalid scenario fixture timestamp"))?;
            let routes =
                RepresentedRouteSet::from_routes([BuyModelRoute::Weather, BuyModelRoute::Crypto])?;
            let serving = hash("serving")?;
            let calibration = hash("calibration")?;
            let trade_policy = hash("trade-policy")?;
            let discount_curve = vec![DiscountCurvePoint {
                end_secs: 86_400,
                annualized_cost_bps: 500,
            }];
            let time_buckets = CapitalTimeBucketContract::try_from(discount_curve.as_slice())
                .map_err(|error| QuantError::config(error.to_string()))?
                .content_hash()?;
            let mut states = vec![
                state(
                    0,
                    PortfolioScenarioKind::PitBootstrap,
                    "pit",
                    &routes,
                    2_000,
                )?,
                state(
                    1,
                    PortfolioScenarioKind::CalibrationUncertainty,
                    "calibration",
                    &routes,
                    5_000,
                )?,
                state(
                    2,
                    PortfolioScenarioKind::StructuralStress,
                    "stress",
                    &routes,
                    9_000,
                )?,
            ];
            for state in &mut states {
                state.scenario_state_hash = state.recomputed_state_hash()?;
            }
            let distributions = vec![
                distribution("nominal", true, [5_000, 3_000, 2_000]),
                distribution("robust", false, [3_000, 3_000, 4_000]),
            ];
            let mut model = PortfolioScenarioModelArtifact {
                portfolio_scenario_model_artifact_id:
                    PortfolioScenarioModelArtifactId::from_content_hash(&hash("pending-id")?),
                schema_version: SchemaVersion::FIRST,
                as_of: at,
                fit_window_start: at - Duration::days(30),
                time_bucket_secs: 3_600,
                ordered_routes: routes.routes.clone(),
                route_set_digest: routes.digest,
                serving_contract_digest: serving,
                calibration_contract_digest: calibration,
                recommendation_contract_digest: trade_policy,
                evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
                capital_time_bucket_contract_digest: time_buckets,
                scenario_random_stream_hash: hash("scenario-random-stream")?,
                pit_residual_panel_hash: hash("pit-panel")?,
                calibration_uncertainty_model_hash: hash("calibration-model")?,
                stress_catalog_hash: hash("stress-catalog")?,
                resampling_method: PortfolioScenarioResamplingMethod::StationaryBootstrap {
                    expected_block_length: 8,
                    scenario_horizon_buckets: 24,
                },
                route_fit_lineage: routes
                    .routes
                    .iter()
                    .copied()
                    .map(|route| PortfolioScenarioRouteFitLineage {
                        route,
                        model_lineage: PortfolioScenarioRouteModelLineage {
                            evaluated_model_version_id: ModelVersionId::from_v7(),
                            evaluated_model_artifact_hash: hash(&format!(
                                "evaluated-model-{route:?}"
                            ))
                            .expect("evaluated model hash"),
                            evaluated_serving_contract_hash: serving,
                            calibration_source_model_version_id: ModelVersionId::from_v7(),
                            calibration_source_model_artifact_hash: hash(&format!(
                                "calibration-source-model-{route:?}"
                            ))
                            .expect("calibration source model hash"),
                            calibration_source_serving_contract_hash: hash(&format!(
                                "calibration-source-serving-{route:?}"
                            ))
                            .expect("calibration source serving hash"),
                        },
                        fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
                            backtest_path_set_id: BacktestPathSetId::from_v7(),
                            backtest_path_set_hash: hash(&format!("path-{route:?}"))
                                .expect("path hash"),
                            representative_path_index: 0,
                        },
                        calibration_artifact_id: CalibrationArtifactId::from_v7(),
                        calibration_artifact_hash: hash(&format!("calibration-{route:?}"))
                            .expect("calibration hash"),
                        recommendation_contract_hash: trade_policy,
                        fit_window_start: at - Duration::days(30),
                        fit_window_end: at,
                    })
                    .collect(),
                states,
                distributions,
                discount_curve,
                content_hash: hash("pending-model")?,
            };
            model.content_hash = model.recomputed_hash()?;
            model.portfolio_scenario_model_artifact_id =
                PortfolioScenarioModelArtifactId::from_content_hash(&model.content_hash);
            let binding = PortfolioScenarioModelArtifactBinding {
                portfolio_scenario_model_artifact_id: model.portfolio_scenario_model_artifact_id,
                ordered_routes: routes.routes.clone(),
                route_set_digest: routes.digest,
                serving_contract_digest: serving,
                calibration_contract_digest: calibration,
                recommendation_contract_digest: trade_policy,
                scenario_model_schema_version: SchemaVersion::FIRST,
                capital_time_bucket_contract_digest: time_buckets,
                model_content_hash: model.content_hash,
                bound_at: at,
            };
            let legs = vec![
                leg(
                    BuyModelRoute::Crypto,
                    "crypto-market",
                    "crypto-yes",
                    dec!(0.8),
                )?,
                leg(
                    BuyModelRoute::Weather,
                    "weather-market",
                    "weather-yes",
                    dec!(0.65),
                )?,
            ];
            Ok(Self {
                at,
                routes,
                model,
                binding,
                legs,
                universe_hash: hash("universe")?,
            })
        }

        fn generate(&self) -> QuantResult<SealedPortfolioScenarioArtifact> {
            let model_contract =
                VerifiedPortfolioScenarioModel::verify(&self.binding, &self.model, &self.routes)?;
            PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
                model_contract: &model_contract,
                decision_at: self.at,
                visibility: PortfolioScenarioVisibility::PointInTime,
                input_universe_hash: self.universe_hash,
                legs: &self.legs,
            })
        }
    }

    #[test]
    fn mixed_route_generation_stable() -> QuantResult<()> {
        let fixture = Fixture::build()?;
        let forward = fixture.generate()?;
        let mut reversed_legs = fixture.legs.clone();
        reversed_legs.reverse();
        let model_contract = VerifiedPortfolioScenarioModel::verify(
            &fixture.binding,
            &fixture.model,
            &fixture.routes,
        )?;
        let reverse = PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
            model_contract: &model_contract,
            decision_at: fixture.at,
            visibility: PortfolioScenarioVisibility::PointInTime,
            input_universe_hash: fixture.universe_hash,
            legs: &reversed_legs,
        })?;

        assert_eq!(forward, reverse);
        assert_eq!(forward.scenarios.len(), 3);
        assert!(forward.scenarios.iter().all(|scenario| {
            scenario.market_outcomes.len() == 2
                && scenario.market_outcomes[0].route == BuyModelRoute::Crypto
                && scenario.market_outcomes[1].route == BuyModelRoute::Weather
        }));
        Ok(())
    }

    #[test]
    fn leaf_tamper_rejected() -> QuantResult<()> {
        let fixture = Fixture::build()?;
        let mut artifact = PortfolioScenarioArtifact::from(fixture.generate()?);
        artifact.scenarios[0].market_outcomes[0].discounted_exit_cash_per_share_usd = Usd::ZERO;

        assert!(
            SealedPortfolioScenarioArtifact::verify(artifact).is_err(),
            "a leaf mutation must fail even when the stored parent roots are unchanged"
        );
        Ok(())
    }

    #[test]
    fn raw_roundtrip_verifies() -> QuantResult<()> {
        let fixture = Fixture::build()?;
        let artifact = PortfolioScenarioArtifact::from(fixture.generate()?);

        SealedPortfolioScenarioArtifact::verify(artifact)?;
        Ok(())
    }

    #[test]
    fn layout_drift_rejected() -> QuantResult<()> {
        let fixture = Fixture::build()?;
        let mut artifact = PortfolioScenarioArtifact::from(fixture.generate()?);
        assert!(artifact.scenarios[1].market_outcomes.pop().is_some());
        artifact.scenarios[1].scenario_state_hash =
            artifact.scenarios[1].recomputed_state_hash()?;
        artifact.content_hash = artifact.recomputed_hash()?;
        artifact.portfolio_scenario_artifact_id =
            PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);

        let error = SealedPortfolioScenarioArtifact::verify(artifact)
            .expect_err("different cross-scenario outcome layouts must fail closed");
        assert!(error.to_string().contains(
            "every scenario must contain the same strictly ordered market-outcome identities"
        ));
        Ok(())
    }

    #[test]
    fn binding_drift_fails_closed() -> QuantResult<()> {
        let mut hash_drift = Fixture::build()?;
        hash_drift.binding.model_content_hash = hash("wrong-model")?;
        assert!(hash_drift.generate().is_err());

        let mut late_binding = Fixture::build()?;
        late_binding.binding.bound_at = late_binding.at + Duration::seconds(1);
        assert!(late_binding.generate().is_err());
        Ok(())
    }

    #[test]
    fn historical_binding_governance_clock() -> QuantResult<()> {
        let mut fixture = Fixture::build()?;
        let governance_frozen_at = fixture.at + Duration::days(30);
        fixture.binding.bound_at = governance_frozen_at;

        PortfolioScenarioGenerator::verify_model(
            &fixture.binding,
            &fixture.model,
            &fixture.routes,
            fixture.at,
            PortfolioScenarioVisibility::HistoricalReplay {
                governance_frozen_at,
            },
        )?;
        assert!(
            PortfolioScenarioGenerator::verify_model(
                &fixture.binding,
                &fixture.model,
                &fixture.routes,
                fixture.at,
                PortfolioScenarioVisibility::PointInTime,
            )
            .is_err()
        );
        assert!(
            PortfolioScenarioGenerator::verify_model(
                &fixture.binding,
                &fixture.model,
                &fixture.routes,
                fixture.at,
                PortfolioScenarioVisibility::HistoricalReplay {
                    governance_frozen_at: governance_frozen_at - Duration::seconds(1),
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn calibrated_probability_changes_cashflow() -> QuantResult<()> {
        let mut low = Fixture::build()?;
        low.legs[0]
            .calibrated_payout_distribution
            .winner_take_all_win_probability = Probability::new(dec!(0.1));
        let mut high = Fixture::build()?;
        high.legs[0]
            .calibrated_payout_distribution
            .winner_take_all_win_probability = Probability::new(dec!(0.99));
        let low_artifact = low.generate()?;
        let high_artifact = high.generate()?;
        let low_cash =
            low_artifact.scenarios[1].market_outcomes[0].discounted_exit_cash_per_share_usd;
        let high_cash =
            high_artifact.scenarios[1].market_outcomes[0].discounted_exit_cash_per_share_usd;

        assert!(high_cash > low_cash);
        let model_json = serde_json::to_string(&high.model)
            .map_err(|error| QuantError::config(error.to_string()))?;
        assert!(!model_json.contains("crypto-market"));
        assert!(!model_json.contains("weather-market"));
        Ok(())
    }

    #[test]
    fn market_draw_ignores_lineage() -> QuantResult<()> {
        let fixture = Fixture::build()?;
        let baseline = fixture
            .model
            .states
            .first()
            .cloned()
            .ok_or_else(|| QuantError::config("scenario fixture has no state"))?;
        let mut changed_lineage = baseline.clone();
        changed_lineage.route_factors[0].factor_lineage_hash = hash("changed-audit-lineage")?;
        changed_lineage.scenario_state_hash = changed_lineage.recomputed_state_hash()?;
        assert_ne!(
            baseline.scenario_state_hash,
            changed_lineage.scenario_state_hash
        );

        let leg = fixture
            .legs
            .first()
            .ok_or_else(|| QuantError::config("scenario fixture has no leg"))?;
        assert_eq!(
            market_quantile(&fixture.model, &baseline, leg)?,
            market_quantile(&fixture.model, &changed_lineage, leg)?
        );
        Ok(())
    }

    #[test]
    fn split_mass_materializes_half() -> QuantResult<()> {
        let mut fixture = Fixture::build()?;
        let leg = fixture
            .legs
            .first_mut()
            .ok_or_else(|| QuantError::config("scenario fixture has no leg"))?;
        leg.calibrated_payout_distribution = CalibratedPayoutDistribution {
            winner_take_all_win_probability: Probability::new(dec!(0.4)),
            split_probability: Probability::new(dec!(0.2)),
            split_probability_interval: (Probability::new(dec!(0.2)), Probability::new(dec!(0.2))),
            split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                .map_err(|error| QuantError::config(error.to_string()))?,
        };
        let leg = leg.clone();
        let mut state = fixture
            .model
            .states
            .first()
            .cloned()
            .ok_or_else(|| QuantError::config("scenario fixture has no state"))?;
        let factor = state
            .route_factors
            .iter_mut()
            .find(|factor| factor.route == leg.route)
            .ok_or_else(|| QuantError::config("scenario fixture has no Route factor"))?;
        factor.systematic_weight_bps = 10_000;
        factor.systematic_quantile_bps = 4_000;
        let factor = factor.clone();

        let outcome = materialize_outcome(&fixture.model, &state, &factor, &leg)?;

        assert_eq!(outcome.payout_state, ScenarioPayoutState::Split);
        assert!(outcome.discounted_exit_cash_per_share_usd.inner() < dec!(0.5));
        assert!(outcome.discounted_exit_cash_per_share_usd.inner() > dec!(0.49));
        Ok(())
    }

    fn state(
        scenario_index: u32,
        kind: PortfolioScenarioKind,
        label: &str,
        routes: &RepresentedRouteSet,
        quantile_bps: u32,
    ) -> QuantResult<PortfolioScenarioModelState> {
        let route_factors = routes
            .routes
            .iter()
            .copied()
            .map(|route| {
                Ok(PortfolioScenarioRouteFactor {
                    route,
                    systematic_quantile_bps: quantile_bps,
                    systematic_weight_bps: 10_000,
                    calibrated_probability_shift_bps: 0,
                    split_probability_quantile_bps: match kind {
                        PortfolioScenarioKind::PitBootstrap => 5_000,
                        PortfolioScenarioKind::CalibrationUncertainty => 0,
                        PortfolioScenarioKind::StructuralStress => 10_000,
                    },
                    win_cash_recovery_bps: 10_000,
                    split_cash_recovery_bps: 5_000,
                    loss_cash_recovery_bps: 2_500,
                    executable_share_bps: 10_000,
                    capital_release_multiplier_bps: 10_000,
                    factor_lineage_hash: hash(&format!("factor-{scenario_index}-{route:?}"))?,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(PortfolioScenarioModelState {
            scenario_index,
            kind,
            label: label.to_owned(),
            scenario_state_hash: hash("pending-state")?,
            route_factors,
        })
    }

    fn distribution(id: &str, nominal: bool, probabilities: [u32; 3]) -> ScenarioDistribution {
        ScenarioDistribution {
            distribution_id: id.to_owned(),
            nominal,
            weights: probabilities
                .into_iter()
                .enumerate()
                .map(|(index, probability_bps)| ScenarioWeight {
                    scenario_index: u32::try_from(index).expect("three scenario indexes fit u32"),
                    probability_bps,
                })
                .collect(),
        }
    }

    fn leg(
        route: BuyModelRoute,
        market: &str,
        token: &str,
        probability: Decimal,
    ) -> QuantResult<PortfolioScenarioLegInput> {
        Ok(PortfolioScenarioLegInput {
            route,
            market_id: MarketId::new(market),
            token_id: TokenId::new(token),
            outcome_side: OutcomeSide::Yes,
            calibrated_payout_distribution: CalibratedPayoutDistribution {
                winner_take_all_win_probability: Probability::new(probability),
                split_probability: Probability::new(dec!(0.02)),
                split_probability_interval: (
                    Probability::new(dec!(0.01)),
                    Probability::new(dec!(0.04)),
                ),
                split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                    .map_err(|error| QuantError::config(error.to_string()))?,
            },
            observed_exit_capacity_shares: Shares::new(dec!(100)),
            base_capital_release_secs: 3_600,
            lineage_hash: hash(&format!("leg-{market}-{token}"))?,
        })
    }

    fn hash(label: &str) -> QuantResult<ContentHash> {
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/scenario-generator-test",
            1,
            &label,
        )?)
    }
}
