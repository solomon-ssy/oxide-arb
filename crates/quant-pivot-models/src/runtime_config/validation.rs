//! Runtime-config semantic validation.
//!
//! This module validates only the current quant-pivot document. Legacy Endgame
//! and superseded pre-quant configuration paths are not accepted (clean-break
//! runtime configuration).

use crate::{
    enums::factor::FactorNormalization,
    runtime_config::{FeatureFamily, PerFactorNormalization},
    types::CalibrationArtifactId,
};

use super::{
    DecimalValue, DecisionPolicySnapshot, PolicyValidationConfig, ResearchValidationConfig,
    ResearchValidationTrialsConfig, SizingModelConfig,
    sections::{FeaturesConfig, KellySafetyConfig},
    validate_schedule_cadence,
};
use linkme::distributed_slice;
use quant_pivot_error::config_validation::{ConfigValidationError, ConfigValidationReport};
use rust_decimal::Decimal;
use std::collections::HashSet;

/// Extension hook for feature-config validation that requires research-plane schema knowledge.
///
/// Crates such as `quant-pivot-research` register validators here at link time so
/// [`validate_features`] remains the single features entry point inside
/// [`validate_runtime_config`].
pub type FeaturesConfigValidator = fn(&FeaturesConfig, &mut ConfigValidationReport);

#[allow(unsafe_code)]
#[distributed_slice]
pub static FEATURES_CONFIG_VALIDATORS: [FeaturesConfigValidator] = [..];

/// Mode-agnostic invariants for the boot policy-resource bundle.
#[must_use]
pub fn validate_runtime_config(config: &DecisionPolicySnapshot) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_selection(config, &mut report);
    validate_data_quality(config, &mut report);
    validate_features(config, &mut report);
    validate_factors(config, &mut report);
    validate_domain(config, &mut report);
    validate_model(config, &mut report);
    validate_quality_gate(config, &mut report);
    validate_training(config, &mut report);
    validate_reports(config, &mut report);
    validate_portfolio(config, &mut report);
    validate_execution(config, &mut report);
    validate_research(config, &mut report);
    report
}

fn validate_domain(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    let weather = &config.profile_artifacts.domain.definition.weather;
    if weather.max_forecast_age_secs == 0
        || weather.minimum_bias_samples_per_lead == 0
        || weather.calibration_lookback_days == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain.weather",
            detail: "forecast age and calibration sample/lookback limits must be positive"
                .to_owned(),
        });
    }
    if weather.minimum_complete_members != 31 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain.weather.minimum_complete_members",
            detail: "must equal the complete GEFS 31-member ensemble".to_owned(),
        });
    }
}

fn validate_selection(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    non_negative_decimal(
        "selection.min_liquidity_usd",
        &config.recommendation.selection.min_liquidity_usd,
        report,
    );
    non_negative_decimal(
        "selection.min_volume_24h_usd",
        &config.recommendation.selection.min_volume_24h_usd,
        report,
    );
    if config.recommendation.selection.min_time_to_resolution_secs
        > config.recommendation.selection.max_time_to_resolution_secs
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "selection.min_time_to_resolution_secs",
            detail: "must be <= selection.max_time_to_resolution_secs".to_owned(),
        });
    }
}

fn validate_data_quality(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    if config.recommendation.data_quality.max_book_age_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_book_age_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.recommendation.data_quality.max_ingest_lag_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_ingest_lag_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config
        .recommendation
        .data_quality
        .max_feature_bucket_age_secs
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_feature_bucket_age_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.recommendation.data_quality.max_stale_book_ratio_bps > 10_000 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_stale_book_ratio_bps",
            detail: "must be <= 10000 (100%)".to_owned(),
        });
    }
}

fn validate_features(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    if config
        .profile_artifacts
        .features
        .definition
        .feature_schema_version
        .get()
        <= 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.feature_schema_version",
            detail: "must be positive".to_owned(),
        });
    }
    non_empty_numbers(
        "features.bar_windows_secs",
        &config
            .profile_artifacts
            .features
            .definition
            .bar_windows_secs,
        report,
    );
    validate_momentum_features(config, report);
    non_empty_numbers(
        "features.volatility_windows_secs",
        &config
            .profile_artifacts
            .features
            .definition
            .volatility_windows_secs,
        report,
    );
    if config
        .profile_artifacts
        .features
        .definition
        .structural
        .shock_window_secs
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.shock_window_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    if config
        .profile_artifacts
        .features
        .definition
        .structural
        .book_churn_window_secs
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.book_churn_window_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    if config
        .profile_artifacts
        .features
        .definition
        .structural
        .trade_tape_window_secs
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.trade_tape_window_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    if config
        .profile_artifacts
        .features
        .definition
        .structural
        .trade_tape_min_unique_participants
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.trade_tape_min_unique_participants",
            detail: "must be > 0".to_owned(),
        });
    }
    non_negative_decimal(
        "features.structural.trade_tape_min_notional_usd",
        &config
            .profile_artifacts
            .features
            .definition
            .structural
            .trade_tape_min_notional_usd,
        report,
    );
    unit_ratio(
        "features.structural.trade_tape_min_coverage_ratio",
        &config
            .profile_artifacts
            .features
            .definition
            .structural
            .trade_tape_min_coverage_ratio,
        report,
    );
    for validator in FEATURES_CONFIG_VALIDATORS {
        validator(&config.profile_artifacts.features.definition, report);
    }
    validate_executable_price_feature_family(config, report);
    validate_feature_family_domain_coherence(config, report);
}

/// Every model family prices candidates from the actual primary/secondary
/// best-ask cells. Disabling the price-book family would make the runtime emit
/// an empty batch while appearing otherwise healthy, so reject that config at
/// its governed boundary.
fn validate_executable_price_feature_family(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    if !config
        .profile_artifacts
        .features
        .definition
        .enabled_feature_families
        .contains(&FeatureFamily::PriceBook)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.enabled_feature_families",
            detail: "must contain `PriceBook`; model entry prices require PIT-resolved primary and secondary best-ask FeatureCells"
                .to_owned(),
        });
    }
}

/// `features.enabled_feature_families` containing [`FeatureFamily::Domain`]
/// and `domain.enabled_by_family` must agree on whether the domain plane is
/// live — each direction alone is a real misconfiguration, not a benign
/// no-op:
///
/// - `Domain` enabled with **no** family turned on in `domain.enabled_by_family`
///   permanently registers domain feature columns that every vector resolves
///   to `domain: None` (a schema declaring signals that can never exist).
/// - A family enabled in `domain.enabled_by_family` while `Domain` is absent
///   from `enabled_feature_families` ingests and resolves linkages for a
///   vertical no feature/factor ever consumes (wasted ingest with zero
///   consumer, and a silent trap for anyone who later flips `Domain` on
///   expecting history to already exist consistently).
fn validate_feature_family_domain_coherence(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let domain_family_enabled = config
        .profile_artifacts
        .features
        .definition
        .enabled_feature_families
        .contains(&FeatureFamily::Domain);
    let any_vertical_enabled = config
        .profile_artifacts
        .domain
        .definition
        .enabled_by_family
        .values()
        .any(|&on| on);

    if domain_family_enabled && !any_vertical_enabled {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.enabled_feature_families",
            detail: "`Domain` is enabled but `domain.enabled_by_family` has no vertical \
                     turned on — every vector would carry a permanently empty domain slice"
                .to_owned(),
        });
    }
    if any_vertical_enabled && !domain_family_enabled {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain.enabled_by_family",
            detail: "a vertical is enabled but `features.enabled_feature_families` does not \
                     contain `Domain` — the domain plane would ingest and resolve linkages \
                     for a vertical no feature ever consumes"
                .to_owned(),
        });
    }
}

fn validate_momentum_features(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let momentum = &config.profile_artifacts.features.definition.momentum;
    non_empty_numbers(
        "features.momentum.roc_windows_secs",
        &momentum.roc_windows_secs,
        report,
    );
    non_empty_numbers(
        "features.momentum.slope_windows_secs",
        &momentum.slope_windows_secs,
        report,
    );
    if momentum.roc_lag_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.momentum.roc_lag_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if momentum.ema_fast_secs == 0 || momentum.ema_slow_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.momentum.ema_fast_secs",
            detail: "EMA half-lives (seconds) must be greater than zero".to_owned(),
        });
    }
    if momentum.ema_fast_secs >= momentum.ema_slow_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.momentum.ema_fast_secs",
            detail: "fast EMA half-life must be strictly less than the slow EMA half-life"
                .to_owned(),
        });
    }
    // The lag-skipped ROC needs a base older than the lag, so every ROC window
    // must exceed the lag or it degenerates to an empty span.
    for window in &momentum.roc_windows_secs {
        if *window <= momentum.roc_lag_secs {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "features.momentum.roc_windows_secs",
                detail: format!(
                    "each ROC window must exceed roc_lag_secs ({}); got {window}",
                    momentum.roc_lag_secs
                ),
            });
        }
    }
}

fn validate_factors(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    unit_ratio(
        "factors.min_factor_confidence",
        &config
            .profile_artifacts
            .scoring
            .definition
            .min_factor_confidence,
        report,
    );
    if config
        .profile_artifacts
        .scoring
        .definition
        .enabled_factor_families
        .is_empty()
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.enabled_factor_families",
            detail: "must contain at least one generic factor family".to_owned(),
        });
    }
    let mut seen = HashSet::new();
    for family in &config
        .profile_artifacts
        .scoring
        .definition
        .enabled_factor_families
    {
        if !family.is_config_selectable() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.enabled_factor_families",
                detail: format!(
                    "factor family `{family}` is not config-selectable; vertical/domain factors are routed by market category"
                ),
            });
        }
        if !seen.insert(*family) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.enabled_factor_families",
                detail: format!("duplicate family `{family}`"),
            });
        }
    }
    for name in config
        .profile_artifacts
        .scoring
        .definition
        .factor_weights
        .weights
        .keys()
    {
        if name.trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.factor_weights",
                detail: "factor names must not be empty".to_owned(),
            });
        }
    }
    validate_factor_normalization(config, report);
    validate_factor_cross_section(config, report);
    validate_factor_orthogonalize(config, report);
    validate_factor_structural(config, report);
}

/// Validate the structural factor plane (Phase 11.2.1). Every knob a structural
/// factor / bias-table fit reads is checked here so the compute path never falls
/// back on a silent default (the factor / fit parse the same values fail-closed).
fn validate_factor_structural(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let structural = &config.profile_artifacts.scoring.definition.structural;
    positive_decimal(
        "factors.structural.reversal_after_shock.shock_k",
        &structural.reversal_after_shock.shock_k,
        report,
    );
    positive_decimal(
        "factors.structural.reversal_after_shock.shock_cap",
        &structural.reversal_after_shock.shock_cap,
        report,
    );
    if structural.reversal_after_shock.shock_cap.value
        < structural.reversal_after_shock.shock_k.value
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.reversal_after_shock.shock_cap",
            detail: "shock_cap must be >= shock_k".to_owned(),
        });
    }
    if structural.negrisk.min_legs < 2 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.negrisk.min_legs",
            detail: "must be >= 2 (a neg-risk event has at least two YES legs)".to_owned(),
        });
    }

    let fl = &structural.favorite_longshot;
    if fl.bins == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.bins",
            detail: "must be >= 1".to_owned(),
        });
    }
    // Strictly-ascending, positive ttr bucket boundaries.
    let mut prev = 0_u64;
    for &bound in &fl.ttr_bucket_bounds_secs {
        if bound <= prev {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.structural.favorite_longshot.ttr_bucket_bounds_secs",
                detail: "boundaries must be strictly ascending and positive".to_owned(),
            });
            break;
        }
        prev = bound;
    }
    if fl.min_bin_samples == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.min_bin_samples",
            detail: "must be > 0".to_owned(),
        });
    }
    if fl.min_curve_samples == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.min_curve_samples",
            detail: "must be > 0".to_owned(),
        });
    }
    if fl.fit_sample_stride_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.fit_sample_stride_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    // Confidence level must sit in (0.5, 1) for a meaningful two-sided interval.
    let ci_confidence = fl.ci_confidence.value;
    if ci_confidence <= Decimal::new(5, 1) || ci_confidence >= Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.ci_confidence",
            detail: format!("`{ci_confidence}` must be within (0.5, 1)"),
        });
    }
    non_negative_decimal(
        "factors.structural.favorite_longshot.ic_significance_min",
        &fl.ic_significance_min,
        report,
    );
    // Pure format check (no IO): a malformed ref must be caught here, not
    // deferred until config activation (`BiasTableApplicator::reload`), which
    // additionally verifies existence + content hash — that IO-bound check
    // stays there; this one only guards the string shape.
    if let Some(raw) = fl.bias_table_ref.as_ref()
        && raw.trim().parse::<CalibrationArtifactId>().is_err()
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.bias_table_ref",
            detail: format!("`{raw}` is not a valid CalibrationArtifactId (UUID)"),
        });
    }
    validate_participant_concentration_weights(config, report);
}

fn validate_participant_concentration_weights(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let structural = &config.profile_artifacts.scoring.definition.structural;
    let pc = &structural.participant_concentration;
    non_negative_decimal(
        "factors.structural.participant_concentration.gini_weight",
        &pc.gini_weight,
        report,
    );
    non_negative_decimal(
        "factors.structural.participant_concentration.cr1_share_weight",
        &pc.cr1_share_weight,
        report,
    );
    non_negative_decimal(
        "factors.structural.participant_concentration.hhi_weight",
        &pc.hhi_weight,
        report,
    );
    if pc.gini_weight.value + pc.cr1_share_weight.value + pc.hhi_weight.value <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.participant_concentration",
            detail: "at least one composite weight must be positive".to_owned(),
        });
    }
}

fn validate_factor_normalization(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let normalization = &config.profile_artifacts.scoring.definition.normalization;
    winsor_percentile(
        "factors.normalization.default_winsor_p",
        &normalization.default_winsor_p,
        report,
    );
    positive_decimal(
        "factors.normalization.default_clamp_sigma",
        &normalization.default_clamp_sigma,
        report,
    );
    for (name, override_spec) in &normalization.per_factor {
        if name.trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.normalization.per_factor",
                detail: "factor names must not be empty".to_owned(),
            });
        }
        validate_per_factor_normalization(override_spec, report);
    }
}

fn validate_per_factor_normalization(
    spec: &PerFactorNormalization,
    report: &mut ConfigValidationReport,
) {
    if let Some(winsor_p) = &spec.winsor_p {
        winsor_percentile(
            "factors.normalization.per_factor.winsor_p",
            winsor_p,
            report,
        );
    }
    if let Some(clamp_sigma) = &spec.clamp_sigma {
        positive_decimal(
            "factors.normalization.per_factor.clamp_sigma",
            clamp_sigma,
            report,
        );
    }
    match spec.method {
        FactorNormalization::MinMax => match (&spec.min, &spec.max) {
            (Some(min), Some(max)) => {
                if max.value <= min.value {
                    report.errors.push(ConfigValidationError::InvalidValue {
                        field: "factors.normalization.per_factor.max",
                        detail: "MinMax max must be strictly greater than min".to_owned(),
                    });
                }
            }
            _ => report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.normalization.per_factor",
                detail: "MinMax normalization requires both min and max bounds".to_owned(),
            }),
        },
        FactorNormalization::WinsorizedZScore | FactorNormalization::Rank => {
            if spec.min.is_some() || spec.max.is_some() {
                report.errors.push(ConfigValidationError::InvalidValue {
                    field: "factors.normalization.per_factor",
                    detail: "min/max bounds are only valid for MinMax normalization".to_owned(),
                });
            }
        }
    }
}

fn validate_factor_cross_section(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let cross_section = &config.profile_artifacts.scoring.definition.cross_section;
    if cross_section.min_size < 2 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.cross_section.min_size",
            detail: "must be at least 2 (a cross-section of one has no dispersion)".to_owned(),
        });
    }
}

fn validate_factor_orthogonalize(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    unit_ratio(
        "factors.orthogonalize.max_correlation",
        &config
            .profile_artifacts
            .scoring
            .definition
            .orthogonalize
            .max_correlation,
        report,
    );
    let mut seen = HashSet::new();
    for dimension in &config
        .profile_artifacts
        .scoring
        .definition
        .orthogonalize
        .neutralize_by
    {
        if !seen.insert(*dimension) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.orthogonalize.neutralize_by",
                detail: format!("duplicate neutralize dimension `{dimension:?}`"),
            });
        }
    }
}

fn validate_model(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    unit_ratio(
        "model.min_model_confidence",
        &config.model_routing.model.min_model_confidence,
        report,
    );
    non_negative_decimal(
        "model.shadow_diff_threshold",
        &config.model_routing.model.shadow_diff_threshold,
        report,
    );
    if config.model_routing.model.calibration.min_samples_isotonic == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "model.calibration.min_samples_isotonic",
            detail: "must be > 0".to_owned(),
        });
    }
    // Must be strictly positive: `0` would make the calibration-dataset
    // purge/embargo primitive (Phase 11.3 §0) a no-op disjoint-only check,
    // silently defeating the anti-leakage guarantee the embargo gap exists
    // to enforce.
    if config.model_routing.model.calibration.embargo_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "model.calibration.embargo_secs",
            detail: "must be > 0 (an embargo of 0 is a no-op and defeats the \
                      purge/embargo anti-leakage guarantee)"
                .to_owned(),
        });
    }
    // Confidence level must sit in (0.5, 1) for a meaningful two-sided Wilson
    // interval — mirrors `factors.structural.favorite_longshot.ci_confidence`
    // (the sibling calibration-artifact family); `unit_ratio`'s full `[0, 1]`
    // range would let `0` reach the Wilson-interval math, which degenerates
    // at that boundary.
    let ci_confidence = config.model_routing.model.calibration.ci_confidence.value;
    if ci_confidence <= Decimal::new(5, 1) || ci_confidence >= Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "model.calibration.ci_confidence",
            detail: format!("`{ci_confidence}` must be within (0.5, 1)"),
        });
    }
}

fn validate_quality_gate(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    let gate = &config.profile_artifacts.research_method.model_promotion;
    unit_ratio(
        "quality_gate.min_label_coverage",
        &gate.min_label_coverage,
        report,
    );
    unit_ratio(
        "quality_gate.min_materialization_coverage",
        &gate.min_materialization_coverage,
        report,
    );
    unit_ratio("quality_gate.max_drawdown", &gate.max_drawdown, report);
    unit_ratio(
        "quality_gate.min_liquidity_exit_feasibility",
        &gate.min_liquidity_exit_feasibility,
        report,
    );
    unit_ratio(
        "quality_gate.min_shadow_overlap_stability",
        &gate.min_shadow_overlap_stability,
        report,
    );
    unit_ratio(
        "quality_gate.max_category_concentration",
        &gate.max_category_concentration,
        report,
    );
    if gate.required_shadow_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quality_gate.required_shadow_window_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
    unit_ratio(
        "quality_gate.sell.min_label_coverage",
        &gate.sell.min_label_coverage,
        report,
    );
    unit_ratio(
        "quality_gate.sell.min_l2_book_fidelity_ratio",
        &gate.sell.min_l2_book_fidelity_ratio,
        report,
    );
    unit_ratio(
        "quality_gate.sell.max_fallback_ratio",
        &gate.sell.max_fallback_ratio,
        report,
    );
    non_negative_decimal(
        "quality_gate.sell.rank_ic_min",
        &gate.sell.rank_ic_min,
        report,
    );
    unit_ratio("quality_gate.sell.max_pbo", &gate.sell.max_pbo, report);
}

fn validate_training(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    non_negative_decimal(
        "training.min_exit_depth_usd",
        &config
            .profile_artifacts
            .research_method
            .training
            .min_exit_depth_usd,
        report,
    );
    if config
        .profile_artifacts
        .research_method
        .training
        .max_book_staleness_ms
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "training.max_book_staleness_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_reports(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    if config.recommendation.reports.hard_candidate_ceiling < 100_000
        || !config
            .recommendation
            .reports
            .hard_candidate_ceiling
            .is_multiple_of(1_000)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.hard_candidate_ceiling",
            detail: "must be at least 100000 and rounded up to a multiple of 1000".to_owned(),
        });
    }
    if config.recommendation.reports.max_top_n == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.max_top_n",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.recommendation.reports.ad_hoc_default_top_n == 0
        || config.recommendation.reports.ad_hoc_default_top_n
            > config.recommendation.reports.max_top_n
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.ad_hoc_default_top_n",
            detail: "must be in 1..=reports.max_top_n".to_owned(),
        });
    }
    if config.recommendation.reports.fallback_horizon_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.fallback_horizon_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
    half_open_unit(
        "reports.entry_window_ratio",
        &config.recommendation.reports.entry_window_ratio,
        report,
    );
    for schedule in &config.report_schedule.schedules {
        if schedule.schedule_id.as_str().trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.schedule_id",
                detail: "must not be empty".to_owned(),
            });
        }
        if schedule.enabled {
            validate_schedule_cadence(&schedule.cadence, report);
        }
        if schedule.top_n == 0 || schedule.top_n > config.recommendation.reports.max_top_n {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.top_n",
                detail: "must be in 1..=reports.max_top_n".to_owned(),
            });
        }
    }
    let mut seen_schedule_ids = HashSet::new();
    for schedule in &config.report_schedule.schedules {
        let id = schedule.schedule_id.as_str().trim();
        if id.is_empty() {
            continue;
        }
        if !seen_schedule_ids.insert(id.to_owned()) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.schedule_id",
                detail: format!("duplicate schedule_id `{id}`"),
            });
        }
    }
    if config.pit_knowledge_lag_secs().is_none() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.schedules.knowledge_lag_secs",
            detail: "enabled schedules must share one knowledge_lag_secs".to_owned(),
        });
    }
}

fn validate_portfolio(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    let budget = &config.execution_risk.portfolio.budget;
    non_negative_decimal(
        "portfolio.budget.total_budget_usd",
        &budget.total_budget_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.budget.min_recommendation_usd",
        &budget.min_recommendation_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.budget.max_single_recommendation_usd",
        &budget.max_single_recommendation_usd,
        report,
    );

    let constraints = &config.execution_risk.portfolio.constraints;
    non_negative_decimal(
        "portfolio.constraints.max_market_exposure_usd",
        &constraints.max_market_exposure_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.constraints.max_event_exposure_usd",
        &constraints.max_event_exposure_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.constraints.max_category_exposure_usd",
        &constraints.max_category_exposure_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.constraints.max_correlated_exposure_usd",
        &constraints.max_correlated_exposure_usd,
        report,
    );
    unit_ratio(
        "portfolio.constraints.liquidity_usage_cap_pct",
        &constraints.liquidity_usage_cap_pct,
        report,
    );
    unit_ratio(
        "portfolio.constraints.correlation.cluster_threshold",
        &constraints.correlation.cluster_threshold,
        report,
    );

    validate_sizing(&config.execution_risk.portfolio.sizing, report);
    validate_kelly_safety(&config.execution_risk.portfolio.kelly_safety, report);
    validate_optimizer(config, report);
}

/// Highest sane `edge_uncertainty_k` (Phase 11.3 §6.1 `shrink = clamp(1 -
/// k·edge_std, floor, 1)`): `edge_std` is a Wilson-CI half-width in `[0, 0.5]`,
/// so `k = 10` already drives `shrink` to `floor` for any half-width above
/// `0.1` — an unbounded `k` has no further governance effect beyond making
/// every calibrated candidate collapse to the floor, i.e. a de facto (and
/// silent) disabling of edge-sensitivity rather than a deliberate one.
const MAX_EDGE_UNCERTAINTY_K: Decimal = Decimal::TEN;

/// Validate Kelly safety-layer parameters (Phase 11.3).
fn validate_kelly_safety(kelly: &KellySafetyConfig, report: &mut ConfigValidationReport) {
    bounded_decimal(
        "portfolio.kelly_safety.edge_uncertainty_k",
        &kelly.edge_uncertainty_k,
        Decimal::ZERO,
        MAX_EDGE_UNCERTAINTY_K,
        report,
    );
    // Must be strictly positive: a `0` floor lets `shrink` collapse all the
    // way to `0`, silently zeroing every calibrated candidate's Kelly size
    // instead of the intended "shrink, never eliminate" governance.
    half_open_unit(
        "portfolio.kelly_safety.edge_uncertainty_floor",
        &kelly.edge_uncertainty_floor,
        report,
    );
    half_open_unit(
        "portfolio.kelly_safety.max_aggregate_exposure_pct",
        &kelly.max_aggregate_exposure_pct,
        report,
    );
    half_open_unit(
        "portfolio.kelly_safety.binding_materiality_threshold",
        &kelly.binding_materiality_threshold,
        report,
    );
}

/// Validate the portfolio optimizer (`good_lp`) parameters.
fn validate_optimizer(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    let optimizer = &config.execution_risk.portfolio.optimizer;
    non_negative_decimal(
        "portfolio.optimizer.objective_return_weight",
        &optimizer.objective_return_weight,
        report,
    );
}

/// Validate the sizing model parameters.
fn validate_sizing(sizing: &SizingModelConfig, report: &mut ConfigValidationReport) {
    half_open_unit(
        "portfolio.sizing.kelly_fraction",
        &sizing.kelly_fraction,
        report,
    );
    half_open_unit(
        "portfolio.sizing.max_position_pct",
        &sizing.max_position_pct,
        report,
    );
}

fn validate_execution(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    let condition = &config.operational_control.entry_condition;
    if condition.backstop_interval_ms == 0
        || condition.next_evaluation_delay_ms == 0
        || condition.lease_duration_secs == 0
        || condition.lease_renew_interval_secs == 0
        || condition.pass_limit == 0
        || condition.expiry_batch_limit == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.entry_condition",
            detail: "all cadence, lease, and batch limits must be greater than zero".to_owned(),
        });
    }
    if condition.lease_renew_interval_secs >= condition.lease_duration_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.entry_condition.lease_renew_interval_secs",
            detail: "must be less than lease_duration_secs".to_owned(),
        });
    }
    if config.execution_authorization.auto_execution.enabled {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.auto_execution.enabled",
            detail:
                "Runtime v1 keeps AutoExecution blocked; Phase 11.11 owns its final governance gate"
                    .to_owned(),
        });
    }
    non_negative_decimal(
        "execution.auto_execution.max_total_usd_per_report",
        &config
            .execution_authorization
            .auto_execution
            .max_total_usd_per_report,
        report,
    );
    unit_ratio(
        "execution.auto_execution.min_confidence",
        &config.execution_authorization.auto_execution.min_confidence,
        report,
    );
    validate_semi_auto_canary(config, report);
    non_negative_decimal(
        "execution.capital.max_reserved_usd",
        &config.execution_risk.capital.max_reserved_usd,
        report,
    );
    non_negative_decimal(
        "execution.entry_order_policy.min_entry_book_depth_usd",
        &config
            .execution_risk
            .entry_order_policy
            .min_entry_book_depth_usd,
        report,
    );
    validate_execution_breaker(config, report);
    if config
        .operational_control
        .kill_switch
        .emergency_exit
        .max_slippage_bps
        == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.kill_switch.emergency_exit.max_slippage_bps",
            detail: "must be greater than zero".to_owned(),
        });
    }
    validate_settlement_redeem(config, report);
}

fn validate_semi_auto_canary(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    let canary = &config.execution_authorization.semi_auto.canary;
    non_negative_decimal(
        "execution.semi_auto.canary.max_total_cash_per_report",
        &canary.max_total_cash_per_report,
        report,
    );
    for tier in &canary.allowed_cash_budget_tiers_usd {
        positive_decimal(
            "execution.semi_auto.canary.allowed_cash_budget_tiers_usd",
            tier,
            report,
        );
    }
    if !canary.enabled {
        return;
    }
    if canary
        .policy_artifact_id
        .as_deref()
        .is_none_or(str::is_empty)
        || canary
            .policy_content_hash
            .as_deref()
            .is_none_or(str::is_empty)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.semi_auto.canary",
            detail: "an enabled canary must bind both policy_artifact_id and policy_content_hash"
                .to_owned(),
        });
    }
    let only_first_tier = canary.allowed_cash_budget_tiers_usd.len() == 1
        && canary.allowed_cash_budget_tiers_usd[0].value == Decimal::new(25, 0);
    if !only_first_tier {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.semi_auto.canary.allowed_cash_budget_tiers_usd",
            detail: "runtime v1 canary must contain exactly the $25 cash-budget tier".to_owned(),
        });
    }
    if canary.max_open_intents != 1 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.semi_auto.canary.max_open_intents",
            detail: "runtime v1 canary must allow exactly one open intent".to_owned(),
        });
    }
    if canary.max_total_cash_per_report.value != Decimal::new(25, 0) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.semi_auto.canary.max_total_cash_per_report",
            detail: "runtime v1 canary must cap each report at exactly $25 total cash".to_owned(),
        });
    }
    if canary
        .expires_at
        .as_deref()
        .is_none_or(|value| chrono::DateTime::parse_from_rfc3339(value).is_err())
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.semi_auto.canary.expires_at",
            detail: "an enabled canary requires an RFC3339 expiry".to_owned(),
        });
    }
}

fn validate_execution_breaker(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let breaker = &config.execution_risk.breaker;
    if breaker.venue_consecutive_failures_to_degrade == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.breaker.venue_consecutive_failures_to_degrade",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if breaker.venue_consecutive_failures_to_halt < breaker.venue_consecutive_failures_to_degrade {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.breaker.venue_consecutive_failures_to_halt",
            detail: "must be >= venue_consecutive_failures_to_degrade".to_owned(),
        });
    }
    if breaker.venue_error_rate_bps_to_halt == 0 || breaker.venue_error_rate_bps_to_halt > 10_000 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.breaker.venue_error_rate_bps_to_halt",
            detail: "must be in 1..=10000".to_owned(),
        });
    }
    for (field, value) in [
        (
            "execution.breaker.venue_min_window_samples",
            u64::from(breaker.venue_min_window_samples),
        ),
        (
            "execution.breaker.venue_window_secs",
            breaker.venue_window_secs,
        ),
        ("execution.breaker.cooldown_secs", breaker.cooldown_secs),
    ] {
        if value == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be greater than zero".to_owned(),
            });
        }
    }
    non_negative_decimal(
        "execution.breaker.daily_realized_loss_cap_usd",
        &breaker.daily_realized_loss_cap_usd,
        report,
    );
}

fn validate_research(config: &DecisionPolicySnapshot, report: &mut ConfigValidationReport) {
    non_negative_decimal(
        "research.training.lambda_tail",
        &config
            .profile_artifacts
            .research_method
            .research
            .training
            .lambda_tail,
        report,
    );
    half_open_unit(
        "research.training.tail_fraction",
        &config
            .profile_artifacts
            .research_method
            .research
            .training
            .tail_fraction,
        report,
    );
    non_negative_decimal(
        "research.training.lambda_turnover",
        &config
            .profile_artifacts
            .research_method
            .research
            .training
            .lambda_turnover,
        report,
    );
    non_negative_decimal(
        "research.training.lambda_l2",
        &config
            .profile_artifacts
            .research_method
            .research
            .training
            .lambda_l2,
        report,
    );
    let max_top_n = config.recommendation.reports.max_top_n;
    for (field, value) in [
        (
            "research.training.ndcg_k",
            config
                .profile_artifacts
                .research_method
                .research
                .training
                .ndcg_k,
        ),
        (
            "research.training.pseudo_top_n",
            config
                .profile_artifacts
                .research_method
                .research
                .training
                .pseudo_top_n,
        ),
    ] {
        if value == 0 || value > max_top_n {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: format!("must be in 1..=reports.max_top_n ({max_top_n})"),
            });
        }
    }
    validate_research_validation(config, report);
    validate_policy_validation(
        &config
            .profile_artifacts
            .research_method
            .research
            .policy_validation,
        report,
    );
}

fn validate_policy_validation(
    policy: &PolicyValidationConfig,
    report: &mut ConfigValidationReport,
) {
    if policy.max_candidates_per_experiment == 0 || policy.max_candidates_per_experiment > 32 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.policy_validation.max_candidates_per_experiment",
            detail: "must be in 1..=32; excess candidates fail the whole preflight".to_owned(),
        });
    }
    if policy.min_latency_profile_secs < 86_400 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.policy_validation.min_latency_profile_secs",
            detail: "must cover at least 24 hours".to_owned(),
        });
    }
}

/// Phase 11.5 `research.validation.*` methodology config validation.
fn validate_research_validation(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let validation = &config.profile_artifacts.research_method.research.validation;
    validate_research_validation_purge_cpcv(validation, report);
    validate_research_validation_trials(&validation.trials, report);
    validate_research_validation_pbo_gates(validation, report);
}

fn validate_research_validation_purge_cpcv(
    validation: &ResearchValidationConfig,
    report: &mut ConfigValidationReport,
) {
    half_open_unit(
        "research.validation.purge.embargo_pct",
        &validation.purge.embargo_pct,
        report,
    );

    let cpcv = &validation.cpcv;
    if cpcv.n_groups < 2 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.cpcv.n_groups",
            detail: "must be >= 2".to_owned(),
        });
    }
    if cpcv.k_test < 1 || cpcv.k_test >= cpcv.n_groups {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.cpcv.k_test",
            detail: "must be in 1..n_groups".to_owned(),
        });
    }
}

fn validate_research_validation_trials(
    trials: &ResearchValidationTrialsConfig,
    report: &mut ConfigValidationReport,
) {
    if trials.lambda_multipliers.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.trials.lambda_multipliers",
            detail: "must not be empty".to_owned(),
        });
    }
    for multiplier in &trials.lambda_multipliers {
        non_negative_decimal(
            "research.validation.trials.lambda_multipliers",
            multiplier,
            report,
        );
    }
    if trials.rank_loss_kinds.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.trials.rank_loss_kinds",
            detail: "must not be empty".to_owned(),
        });
    }
    if trials.forest_n_trees_multipliers.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.trials.forest_n_trees_multipliers",
            detail: "must not be empty".to_owned(),
        });
    }
    for multiplier in &trials.forest_n_trees_multipliers {
        non_negative_decimal(
            "research.validation.trials.forest_n_trees_multipliers",
            multiplier,
            report,
        );
    }
    if trials.linear_alpha_multipliers.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.trials.linear_alpha_multipliers",
            detail: "must not be empty".to_owned(),
        });
    }
    for multiplier in &trials.linear_alpha_multipliers {
        non_negative_decimal(
            "research.validation.trials.linear_alpha_multipliers",
            multiplier,
            report,
        );
    }
    let weighted_expanded = trials.lambda_multipliers.len() * trials.rank_loss_kinds.len();
    // Forest and linear multipliers apply to disjoint ClassicalKind families —
    // sum, not Cartesian product (matches `validation::trials::generate_classical`).
    let classical_expanded =
        trials.forest_n_trees_multipliers.len() + trials.linear_alpha_multipliers.len();
    let expanded_trials = weighted_expanded.max(classical_expanded);
    if trials.max_trials == 0 || (expanded_trials as u64) > u64::from(trials.max_trials) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.trials.max_trials",
            detail: format!(
                "must be >= the larger family grid size (weighted={weighted_expanded}, \
                 classical={classical_expanded})"
            ),
        });
    }
}

fn validate_research_validation_pbo_gates(
    validation: &ResearchValidationConfig,
    report: &mut ConfigValidationReport,
) {
    if validation.pbo.block_count < 4 || !validation.pbo.block_count.is_multiple_of(2) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.validation.pbo.block_count",
            detail: "must be even and >= 4".to_owned(),
        });
    }

    let gates = &validation.gates;
    non_negative_decimal(
        "research.validation.gates.rank_ic_min",
        &gates.rank_ic_min,
        report,
    );
    half_open_unit(
        "research.validation.gates.dsr_significance",
        &gates.dsr_significance,
        report,
    );
    unit_ratio("research.validation.gates.max_pbo", &gates.max_pbo, report);
    non_negative_decimal(
        "research.validation.gates.max_turnover",
        &gates.max_turnover,
        report,
    );
}

fn validate_settlement_redeem(
    config: &DecisionPolicySnapshot,
    report: &mut ConfigValidationReport,
) {
    let redeem = &config.execution_risk.settlement_redeem;
    if redeem.enabled {
        if redeem.interval_secs == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "execution.settlement_redeem.interval_secs",
                detail: "must be greater than zero when the redeem worker is enabled".to_owned(),
            });
        }
        if redeem.batch_size == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "execution.settlement_redeem.batch_size",
                detail: "must be greater than zero when the redeem worker is enabled".to_owned(),
            });
        }
        if redeem.max_attempts == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "execution.settlement_redeem.max_attempts",
                detail: "must be greater than zero when the redeem worker is enabled".to_owned(),
            });
        }
        if redeem.confirmation_blocks == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "execution.settlement_redeem.confirmation_blocks",
                detail: "must be at least one when the redeem worker is enabled".to_owned(),
            });
        }
    }
}

/// Validate a non-negative decimal.
fn non_negative_decimal(
    field: &'static str,
    value: &DecimalValue,
    report: &mut ConfigValidationReport,
) {
    if value.value < Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` must be >= 0", value.value),
        });
    }
}

/// Validate a decimal within an explicit inclusive `[min, max]` range.
fn bounded_decimal(
    field: &'static str,
    value: &DecimalValue,
    min: Decimal,
    max: Decimal,
    report: &mut ConfigValidationReport,
) {
    if !(min..=max).contains(&value.value) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` must be within [{min}, {max}]", value.value),
        });
    }
}

/// Validate a `[0, 1]` ratio.
fn unit_ratio(field: &'static str, value: &DecimalValue, report: &mut ConfigValidationReport) {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value.value) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` must be within [0, 1]", value.value),
        });
    }
}

/// Validate a half-open unit ratio in `(0, 1]` (the value must be strictly
/// positive — a zero fraction would size nothing — and at most full).
fn half_open_unit(field: &'static str, value: &DecimalValue, report: &mut ConfigValidationReport) {
    if value.value <= Decimal::ZERO || value.value > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` must be within (0, 1]", value.value),
        });
    }
}

/// Validate a strictly-positive decimal.
fn positive_decimal(
    field: &'static str,
    value: &DecimalValue,
    report: &mut ConfigValidationReport,
) {
    if value.value <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` must be > 0", value.value),
        });
    }
}

/// Validate a winsorize percentile: a decimal strictly within `(0, 0.5)`.
fn winsor_percentile(
    field: &'static str,
    value: &DecimalValue,
    report: &mut ConfigValidationReport,
) {
    let half = Decimal::new(5, 1);
    if value.value <= Decimal::ZERO || value.value >= half {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` must be within (0, 0.5)", value.value),
        });
    }
}

fn non_empty_numbers(field: &'static str, values: &[u64], report: &mut ConfigValidationReport) {
    if values.is_empty() || values.contains(&0) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: "must contain at least one positive value".to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigValidationError, DecisionPolicySnapshot, validate_runtime_config};
    use crate::{
        enums::factor::FactorFamily,
        runtime_config::{
            DecimalValue, FeatureFamily, POLICY_RESOURCE_SCHEMA_VERSION, ReportScheduleConfig,
            ScheduleCadence,
        },
        types::CalibrationArtifactId,
    };

    #[test]
    fn default_runtime_config_is_valid() {
        let report = validate_runtime_config(&DecisionPolicySnapshot::default());
        assert!(!report.has_errors());
    }

    #[test]
    fn invalid_decimal_is_rejected() {
        let mut value = serde_json::to_value(DecisionPolicySnapshot::default()).expect("policy");
        value["execution_risk"]["portfolio"]["budget"]["total_budget_usd"] =
            serde_json::json!({"value": "not-a-decimal"});
        assert!(serde_json::from_value::<DecisionPolicySnapshot>(value).is_err());
    }

    #[test]
    fn kill_switch_emergency_exit_slippage_must_be_positive() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .operational_control
            .kill_switch
            .emergency_exit
            .max_slippage_bps = 0;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn execution_breaker_rejects_invalid_thresholds_and_loss_cap() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .execution_risk
            .breaker
            .venue_consecutive_failures_to_degrade = 0;
        config
            .execution_risk
            .breaker
            .venue_consecutive_failures_to_halt = 0;
        config.execution_risk.breaker.venue_error_rate_bps_to_halt = 10_001;
        config.execution_risk.breaker.venue_min_window_samples = 0;
        config.execution_risk.breaker.venue_window_secs = 0;
        config.execution_risk.breaker.cooldown_secs = 0;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());

        let mut config = DecisionPolicySnapshot::default();
        config.execution_risk.breaker.daily_realized_loss_cap_usd =
            DecimalValue::new(rust_decimal_macros::dec!(-1));
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn runtime_config_rejects_zero_embargo_secs() {
        let mut config = DecisionPolicySnapshot::default();
        config.model_routing.model.calibration.embargo_secs = 0;
        let report = validate_runtime_config(&config);
        assert!(
            report.has_errors(),
            "a zero embargo defeats the purge/embargo anti-leakage guarantee"
        );
    }

    #[test]
    fn runtime_config_rejects_calibration_ci_confidence_outside_open_interval() {
        let mut config = DecisionPolicySnapshot::default();
        config.model_routing.model.calibration.ci_confidence =
            DecimalValue::new(rust_decimal_macros::dec!(0.5));
        let report = validate_runtime_config(&config);
        assert!(
            report.has_errors(),
            "ci_confidence must be strictly greater than 0.5"
        );

        let mut config = DecisionPolicySnapshot::default();
        config.model_routing.model.calibration.ci_confidence =
            DecimalValue::new(rust_decimal_macros::dec!(1.0));
        let report = validate_runtime_config(&config);
        assert!(
            report.has_errors(),
            "ci_confidence must be strictly less than 1.0"
        );

        let mut config = DecisionPolicySnapshot::default();
        config.model_routing.model.calibration.ci_confidence =
            DecimalValue::new(rust_decimal_macros::dec!(0.90));
        let report = validate_runtime_config(&config);
        assert!(
            !report.has_errors(),
            "0.90 is within the valid (0.5, 1) range"
        );
    }

    #[test]
    fn runtime_config_rejects_malformed_bias_table_ref_before_activation() {
        // A malformed ref must be caught at the pure semantic-validation pass
        // (this function, no IO), not deferred to `BiasTableApplicator::reload`
        // at config activation time.
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .scoring
            .definition
            .structural
            .favorite_longshot
            .bias_table_ref = Some("not-a-uuid".to_owned());
        let report = validate_runtime_config(&config);
        assert!(
            report.has_errors(),
            "a non-UUID bias_table_ref must fail validation before activation"
        );

        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .scoring
            .definition
            .structural
            .favorite_longshot
            .bias_table_ref = Some(CalibrationArtifactId::from_v7().to_string());
        let report = validate_runtime_config(&config);
        assert!(
            !report.has_errors(),
            "a well-formed CalibrationArtifactId UUID must pass the format check"
        );

        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .scoring
            .definition
            .structural
            .favorite_longshot
            .bias_table_ref = None;
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors(), "None must never be rejected");
    }

    #[test]
    fn runtime_config_rejects_unbounded_edge_uncertainty_k() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .execution_risk
            .portfolio
            .kelly_safety
            .edge_uncertainty_k = DecimalValue::new(rust_decimal_macros::dec!(10.01));
        let report = validate_runtime_config(&config);
        assert!(
            report.has_errors(),
            "an unbounded k silently collapses every calibrated candidate's shrink to the floor"
        );

        let mut config = DecisionPolicySnapshot::default();
        config
            .execution_risk
            .portfolio
            .kelly_safety
            .edge_uncertainty_k = DecimalValue::new(rust_decimal_macros::dec!(10));
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors(), "10 is the inclusive upper bound");
    }

    #[test]
    fn runtime_config_rejects_zero_edge_uncertainty_floor() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .execution_risk
            .portfolio
            .kelly_safety
            .edge_uncertainty_floor = DecimalValue::new(rust_decimal_macros::dec!(0));
        let report = validate_runtime_config(&config);
        assert!(
            report.has_errors(),
            "a zero floor lets edge-uncertainty shrink zero out Kelly sizing entirely"
        );
    }

    #[test]
    fn schema_version_matches_constant() {
        let config = DecisionPolicySnapshot::default();
        assert!(config.uses_current_resource_schemas());
        assert_eq!(
            config.recommendation.schema_version,
            POLICY_RESOURCE_SCHEMA_VERSION
        );
    }

    #[test]
    fn research_training_tail_fraction_must_be_positive_unit_fraction() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .tail_fraction = DecimalValue::new(rust_decimal_macros::dec!(0));
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());

        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .tail_fraction = DecimalValue::new(rust_decimal_macros::dec!(1.01));
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());

        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .tail_fraction = DecimalValue::new(rust_decimal_macros::dec!(0.10));
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }

    #[test]
    fn research_training_ndcg_and_pseudo_top_n_must_fit_max_top_n() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .ndcg_k = 0;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());

        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .pseudo_top_n = config.recommendation.reports.max_top_n + 1;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());

        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .ndcg_k = 20;
        config
            .profile_artifacts
            .research_method
            .research
            .training
            .pseudo_top_n = 20;
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }

    #[test]
    fn structural_family_in_config_is_accepted() {
        let config = DecisionPolicySnapshot::default();
        assert!(
            config
                .profile_artifacts
                .scoring
                .definition
                .enabled_factor_families
                .contains(&FactorFamily::Structural),
            "default config must enable the structural family"
        );
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }

    #[test]
    fn duplicate_factor_family_is_rejected() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .scoring
            .definition
            .enabled_factor_families
            .push(FactorFamily::Liquidity);
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn empty_factor_families_is_rejected() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .scoring
            .definition
            .enabled_factor_families
            .clear();
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn invalid_enabled_cron_schedule_is_rejected() {
        let mut config = DecisionPolicySnapshot::default();
        config.report_schedule.schedules = vec![ReportScheduleConfig {
            schedule_id: "bad".into(),
            enabled: true,
            top_n: 10,
            knowledge_lag_secs: 0,
            cadence: ScheduleCadence::Cron {
                expr: "not-a-cron".to_owned(),
                timezone: None,
            },
        }];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn duplicate_schedule_id_is_rejected() {
        let mut config = DecisionPolicySnapshot::default();
        config.report_schedule.schedules = vec![
            ReportScheduleConfig::default(),
            ReportScheduleConfig::default(),
        ];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn price_book_feature_family_is_required_for_executable_model_prices() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .features
            .definition
            .enabled_feature_families
            .retain(|family| *family != FeatureFamily::PriceBook);

        let report = validate_runtime_config(&config);
        assert!(report.errors.iter().any(|error| matches!(
            error,
            ConfigValidationError::InvalidValue { field, detail }
                if *field == "features.enabled_feature_families"
                    && detail.contains("PriceBook")
        )));
    }

    #[test]
    fn domain_feature_family_without_any_enabled_vertical_is_rejected() {
        let mut config = DecisionPolicySnapshot::default();
        assert!(
            config
                .profile_artifacts
                .features
                .definition
                .enabled_feature_families
                .contains(&FeatureFamily::Domain),
            "default config enables the Domain feature family"
        );
        // Disable every vertical while `Domain` stays enabled.
        for enabled in config
            .profile_artifacts
            .domain
            .definition
            .enabled_by_family
            .values_mut()
        {
            *enabled = false;
        }
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn enabled_vertical_without_domain_feature_family_is_rejected() {
        let mut config = DecisionPolicySnapshot::default();
        config
            .profile_artifacts
            .features
            .definition
            .enabled_feature_families
            .retain(|family| *family != FeatureFamily::Domain);
        assert!(
            config
                .profile_artifacts
                .domain
                .definition
                .enabled_by_family
                .values()
                .any(|&on| on),
            "default config enables the crypto vertical"
        );
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn domain_feature_family_coherent_with_enabled_vertical_is_accepted() {
        // The default config keeps both sides coherent — no regression beyond
        // `default_runtime_config_is_valid`, but explicit for this invariant.
        let config = DecisionPolicySnapshot::default();
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }
}
