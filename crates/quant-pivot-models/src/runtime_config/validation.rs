//! Runtime-config semantic validation.
//!
//! This module validates only the current quant-pivot document. Legacy Endgame
//! and superseded pre-quant configuration paths are not accepted (clean-break
//! runtime configuration).

use crate::runtime_config::FeatureFamily;

use super::{
    DecimalString, RuntimeConfig, SizingModelConfig,
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

/// Mode-agnostic runtime-config v5 invariants.
#[must_use]
pub fn validate_runtime_config(config: &RuntimeConfig) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_selection(config, &mut report);
    validate_data_quality(config, &mut report);
    validate_features(config, &mut report);
    validate_factors(config, &mut report);
    validate_model(config, &mut report);
    validate_quality_gate(config, &mut report);
    validate_training(config, &mut report);
    validate_reports(config, &mut report);
    validate_portfolio(config, &mut report);
    validate_execution(config, &mut report);
    report
}

fn validate_selection(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "selection.min_liquidity_usd",
        &config.selection.min_liquidity_usd,
        report,
    );
    decimal(
        "selection.min_volume_24h_usd",
        &config.selection.min_volume_24h_usd,
        report,
    );
    if config.selection.min_time_to_resolution_secs > config.selection.max_time_to_resolution_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "selection.min_time_to_resolution_secs",
            detail: "must be <= selection.max_time_to_resolution_secs".to_owned(),
        });
    }
    if config.selection.max_selection_size == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "selection.max_selection_size",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_data_quality(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.data_quality.max_book_age_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_book_age_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.data_quality.max_ingest_lag_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_ingest_lag_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.data_quality.max_feature_bucket_age_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_feature_bucket_age_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.data_quality.max_stale_book_ratio_bps > 10_000 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_stale_book_ratio_bps",
            detail: "must be <= 10000 (100%)".to_owned(),
        });
    }
}

fn validate_features(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.features.feature_schema_version.get() <= 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.feature_schema_version",
            detail: "must be positive".to_owned(),
        });
    }
    non_empty_numbers(
        "features.bar_windows_secs",
        &config.features.bar_windows_secs,
        report,
    );
    validate_momentum_features(config, report);
    non_empty_numbers(
        "features.volatility_windows_secs",
        &config.features.volatility_windows_secs,
        report,
    );
    if config.features.structural.shock_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.shock_window_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    if config.features.structural.book_churn_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.book_churn_window_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    if config.features.structural.trade_tape_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.structural.trade_tape_window_secs",
            detail: "must be > 0".to_owned(),
        });
    }
    if config
        .features
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
        &config.features.structural.trade_tape_min_notional_usd,
        report,
    );
    unit_ratio(
        "features.structural.trade_tape_min_coverage_ratio",
        &config.features.structural.trade_tape_min_coverage_ratio,
        report,
    );
    let mut seen = HashSet::new();
    for feature_ref in &config.features.required_features {
        let label = feature_ref.name.trim();
        if label.is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "features.required_features",
                detail: "feature names must not be empty".to_owned(),
            });
            continue;
        }
        if !seen.insert(label.to_owned()) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "features.required_features",
                detail: format!("duplicate feature `{label}`"),
            });
        }
    }
    for validator in FEATURES_CONFIG_VALIDATORS {
        validator(&config.features, report);
    }
    validate_feature_family_domain_coherence(config, report);
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
    config: &RuntimeConfig,
    report: &mut ConfigValidationReport,
) {
    let domain_family_enabled = config
        .features
        .enabled_feature_families
        .contains(&FeatureFamily::Domain);
    let any_vertical_enabled = config.domain.enabled_by_family.values().any(|&on| on);

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

fn validate_momentum_features(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let momentum = &config.features.momentum;
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

fn validate_factors(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "factors.min_factor_confidence",
        &config.factors.min_factor_confidence,
        report,
    );
    if config.factors.enabled_factor_families.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.enabled_factor_families",
            detail: "must contain at least one generic factor family".to_owned(),
        });
    }
    let mut seen = HashSet::new();
    for family in &config.factors.enabled_factor_families {
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
    for (name, weight) in &config.factors.factor_weights.weights {
        if name.trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.factor_weights",
                detail: "factor names must not be empty".to_owned(),
            });
        }
        decimal("factors.factor_weights", weight, report);
    }
    validate_factor_normalization(config, report);
    validate_factor_cross_section(config, report);
    validate_factor_orthogonalize(config, report);
    validate_factor_structural(config, report);
}

/// Validate the structural factor plane (Phase 11.2.1). Every knob a structural
/// factor / bias-table fit reads is checked here so the compute path never falls
/// back on a silent default (the factor / fit parse the same values fail-closed).
fn validate_factor_structural(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let structural = &config.factors.structural;
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
    if let (Ok(k), Ok(cap)) = (
        structural
            .reversal_after_shock
            .shock_k
            .value
            .parse::<Decimal>(),
        structural
            .reversal_after_shock
            .shock_cap
            .value
            .parse::<Decimal>(),
    ) && cap < k
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
    match fl.ci_confidence.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::new(5, 1) && parsed < Decimal::ONE => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.ci_confidence",
            detail: format!("`{parsed}` must be within (0.5, 1)"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.favorite_longshot.ci_confidence",
            detail: format!("`{}` is not a valid decimal string", fl.ci_confidence.value),
        }),
    }
    non_negative_decimal(
        "factors.structural.favorite_longshot.ic_significance_min",
        &fl.ic_significance_min,
        report,
    );
    validate_participant_concentration_weights(config, report);
}

fn validate_participant_concentration_weights(
    config: &RuntimeConfig,
    report: &mut ConfigValidationReport,
) {
    let structural = &config.factors.structural;
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
    if let (Ok(gini), Ok(cr1), Ok(hhi)) = (
        pc.gini_weight.value.parse::<Decimal>(),
        pc.cr1_share_weight.value.parse::<Decimal>(),
        pc.hhi_weight.value.parse::<Decimal>(),
    ) && gini + cr1 + hhi <= Decimal::ZERO
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.structural.participant_concentration",
            detail: "at least one composite weight must be positive".to_owned(),
        });
    }
}

fn validate_factor_normalization(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let normalization = &config.factors.normalization;
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
    spec: &super::PerFactorNormalization,
    report: &mut ConfigValidationReport,
) {
    use crate::enums::factor::FactorNormalization;

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
                decimal("factors.normalization.per_factor.min", min, report);
                decimal("factors.normalization.per_factor.max", max, report);
                if let (Ok(lo), Ok(hi)) =
                    (min.value.parse::<Decimal>(), max.value.parse::<Decimal>())
                    && hi <= lo
                {
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

fn validate_factor_cross_section(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let cross_section = &config.factors.cross_section;
    if cross_section.min_size < 2 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.cross_section.min_size",
            detail: "must be at least 2 (a cross-section of one has no dispersion)".to_owned(),
        });
    }
    if cross_section.historical_lookback_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.cross_section.historical_lookback_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_factor_orthogonalize(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    unit_ratio(
        "factors.orthogonalize.max_correlation",
        &config.factors.orthogonalize.max_correlation,
        report,
    );
    let mut seen = HashSet::new();
    for dimension in &config.factors.orthogonalize.neutralize_by {
        if !seen.insert(*dimension) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.orthogonalize.neutralize_by",
                detail: format!("duplicate neutralize dimension `{dimension:?}`"),
            });
        }
    }
}

fn validate_model(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "model.min_model_confidence",
        &config.model.min_model_confidence,
        report,
    );
    decimal(
        "model.candidate_score_floor",
        &config.model.candidate_score_floor,
        report,
    );
    decimal(
        "model.shadow_diff_threshold",
        &config.model.shadow_diff_threshold,
        report,
    );
    if config.model.calibration.min_samples_isotonic == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "model.calibration.min_samples_isotonic",
            detail: "must be > 0".to_owned(),
        });
    }
    unit_ratio(
        "model.calibration.ci_confidence",
        &config.model.calibration.ci_confidence,
        report,
    );
}

fn validate_quality_gate(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let gate = &config.quality_gate;
    unit_ratio(
        "quality_gate.min_label_coverage",
        &gate.min_label_coverage,
        report,
    );
    unit_ratio(
        "quality_gate.min_critical_feature_coverage",
        &gate.min_critical_feature_coverage,
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
    // rank IC is a correlation in [-1, 1]; only validate it parses.
    decimal("quality_gate.min_rank_ic", &gate.min_rank_ic, report);
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
    decimal(
        "quality_gate.sell.min_exit_alpha_rank_ic",
        &gate.sell.min_exit_alpha_rank_ic,
        report,
    );
}

fn validate_training(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "training.min_exit_depth_usd",
        &config.training.min_exit_depth_usd,
        report,
    );
    decimal(
        "training.min_selection_depth_usd",
        &config.training.min_selection_depth_usd,
        report,
    );
    if config.training.max_book_staleness_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "training.max_book_staleness_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_reports(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.reports.max_top_n == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.max_top_n",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.reports.fallback_horizon_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.fallback_horizon_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
    half_open_unit(
        "reports.entry_window_ratio",
        &config.reports.entry_window_ratio,
        report,
    );
    for schedule in &config.reports.schedules {
        if schedule.schedule_id.trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.schedule_id",
                detail: "must not be empty".to_owned(),
            });
        }
        if schedule.enabled {
            validate_schedule_cadence(&schedule.cadence, report);
        }
        if schedule.top_n == 0 || schedule.top_n > config.reports.max_top_n {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.top_n",
                detail: "must be in 1..=reports.max_top_n".to_owned(),
            });
        }
    }
    let mut seen_schedule_ids = HashSet::new();
    for schedule in &config.reports.schedules {
        let id = schedule.schedule_id.trim();
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
    if config.pit_source_delay_secs().is_none() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.schedules.source_delay_secs",
            detail: "enabled schedules must share one source_delay_secs".to_owned(),
        });
    }
}

fn validate_portfolio(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let budget = &config.portfolio.budget;
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

    let constraints = &config.portfolio.constraints;
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

    validate_sizing(&config.portfolio.sizing, report);
    validate_kelly_safety(&config.portfolio.kelly_safety, report);
    validate_optimizer(config, report);
}

/// Validate Kelly safety-layer parameters (Phase 11.3).
fn validate_kelly_safety(kelly: &KellySafetyConfig, report: &mut ConfigValidationReport) {
    non_negative_decimal(
        "portfolio.kelly_safety.edge_uncertainty_k",
        &kelly.edge_uncertainty_k,
        report,
    );
    unit_ratio(
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
fn validate_optimizer(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let optimizer = &config.portfolio.optimizer;
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
    match sizing.target_reward_multiple.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::ZERO => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field: "portfolio.sizing.target_reward_multiple",
            detail: format!("`{parsed}` must be > 0"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field: "portfolio.sizing.target_reward_multiple",
            detail: format!(
                "`{}` is not a valid decimal string",
                sizing.target_reward_multiple.value
            ),
        }),
    }
}

fn validate_execution(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "execution.auto_execution.max_total_usd_per_report",
        &config.execution.auto_execution.max_total_usd_per_report,
        report,
    );
    decimal(
        "execution.auto_execution.min_score",
        &config.execution.auto_execution.min_score,
        report,
    );
    decimal(
        "execution.auto_execution.min_confidence",
        &config.execution.auto_execution.min_confidence,
        report,
    );
    non_negative_decimal(
        "execution.capital.max_reserved_usd",
        &config.execution.capital.max_reserved_usd,
        report,
    );
    non_negative_decimal(
        "execution.entry_order_policy.min_entry_book_depth_usd",
        &config.execution.entry_order_policy.min_entry_book_depth_usd,
        report,
    );
    if config.execution.kill_switch.emergency_exit.max_slippage_bps == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.kill_switch.emergency_exit.max_slippage_bps",
            detail: "must be greater than zero".to_owned(),
        });
    }
    // Signal-degradation floor = entry_score × ratio. A ratio outside (0, 1]
    // would either never invalidate (0) or set the floor above the entry score
    // (> 1, forcing exits on the tiniest drift) — reject at load so the exit
    // monitor never scores the forced-exit tier against a nonsense threshold.
    half_open_unit(
        "execution.exit_monitor.signal_invalidation_ratio",
        &config.execution.exit_monitor.signal_invalidation_ratio,
        report,
    );
    validate_opportunistic_sell(config, report);
    validate_settlement_redeem(config, report);
}

fn validate_opportunistic_sell(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let policy = &config.execution.exit_monitor.opportunistic_sell;
    unit_ratio(
        "execution.exit_monitor.opportunistic_sell.min_confidence",
        &policy.min_confidence,
        report,
    );
    // The target cumulative exit fraction is capped at this value; a zero cap
    // would make every opportunistic verdict a no-op, and > 1 is nonsensical.
    half_open_unit(
        "execution.exit_monitor.opportunistic_sell.max_sell_pct",
        &policy.max_sell_pct,
        report,
    );
    half_open_unit(
        "execution.exit_monitor.opportunistic_sell.min_opportunistic_clip_pct",
        &policy.min_opportunistic_clip_pct,
        report,
    );
    unit_ratio(
        "execution.exit_monitor.opportunistic_sell.min_p_exit_better",
        &policy.min_p_exit_better,
        report,
    );
    if policy.min_expected_alpha_bps < 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.exit_monitor.opportunistic_sell.min_expected_alpha_bps",
            detail: "must be >= 0".to_owned(),
        });
    }
}

fn validate_settlement_redeem(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let redeem = &config.execution.settlement_redeem;
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
    if redeem.hold_to_resolution_enabled && redeem.hold_to_resolution_within_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.settlement_redeem.hold_to_resolution_within_secs",
            detail: "must be greater than zero when hold_to_resolution_enabled is true".to_owned(),
        });
    }
}

fn decimal(field: &'static str, value: &DecimalString, report: &mut ConfigValidationReport) {
    if value.value.parse::<Decimal>().is_err() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        });
    }
}

/// Validate a non-negative decimal (parses and is `>= 0`).
fn non_negative_decimal(
    field: &'static str,
    value: &DecimalString,
    report: &mut ConfigValidationReport,
) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if parsed >= Decimal::ZERO => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be >= 0"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

/// Validate a `[0, 1]` ratio: parses as a decimal and lies within the unit range.
fn unit_ratio(field: &'static str, value: &DecimalString, report: &mut ConfigValidationReport) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if (Decimal::ZERO..=Decimal::ONE).contains(&parsed) => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be within [0, 1]"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

/// Validate a half-open unit ratio in `(0, 1]` (the value must be strictly
/// positive — a zero fraction would size nothing — and at most full).
fn half_open_unit(field: &'static str, value: &DecimalString, report: &mut ConfigValidationReport) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::ZERO && parsed <= Decimal::ONE => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be within (0, 1]"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

/// Validate a strictly-positive decimal (parses and is `> 0`).
fn positive_decimal(
    field: &'static str,
    value: &DecimalString,
    report: &mut ConfigValidationReport,
) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::ZERO => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be > 0"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

/// Validate a winsorize percentile: a decimal strictly within `(0, 0.5)`.
fn winsor_percentile(
    field: &'static str,
    value: &DecimalString,
    report: &mut ConfigValidationReport,
) {
    let half = Decimal::new(5, 1);
    match value.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::ZERO && parsed < half => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be within (0, 0.5)"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
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
    use super::{RuntimeConfig, validate_runtime_config};
    use crate::{
        enums::factor::FactorFamily,
        runtime_config::{
            DecimalString, FeatureNameRef, RUNTIME_CONFIG_SCHEMA_VERSION, ReportScheduleConfig,
            ScheduleCadence,
        },
    };

    #[test]
    fn default_runtime_config_is_valid() {
        let report = validate_runtime_config(&RuntimeConfig::default());
        assert!(!report.has_errors());
    }

    #[test]
    fn invalid_decimal_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.portfolio.budget.total_budget_usd = DecimalString::new("not-a-decimal");
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn kill_switch_emergency_exit_slippage_must_be_positive() {
        let mut config = RuntimeConfig::default();
        config.execution.kill_switch.emergency_exit.max_slippage_bps = 0;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn schema_version_matches_constant() {
        assert_eq!(
            RuntimeConfig::default().schema_version,
            RUNTIME_CONFIG_SCHEMA_VERSION
        );
    }

    #[test]
    fn structural_family_in_config_is_accepted() {
        let config = RuntimeConfig::default();
        assert!(
            config
                .factors
                .enabled_factor_families
                .contains(&FactorFamily::Structural),
            "default config must enable the structural family"
        );
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }

    #[test]
    fn duplicate_factor_family_is_rejected() {
        let mut config = RuntimeConfig::default();
        config
            .factors
            .enabled_factor_families
            .push(FactorFamily::Liquidity);
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn empty_factor_families_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.factors.enabled_factor_families.clear();
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn empty_required_feature_name_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.features.required_features = vec![FeatureNameRef::new("   ")];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn invalid_enabled_cron_schedule_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.reports.schedules = vec![ReportScheduleConfig {
            schedule_id: "bad".to_owned(),
            enabled: true,
            top_n: 10,
            source_delay_secs: 0,
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
        let mut config = RuntimeConfig::default();
        config.reports.schedules = vec![
            ReportScheduleConfig::default(),
            ReportScheduleConfig::default(),
        ];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn duplicate_required_feature_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.features.required_features = vec![
            FeatureNameRef::new("book.spread_bps"),
            FeatureNameRef::new("book.spread_bps"),
        ];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn domain_feature_family_without_any_enabled_vertical_is_rejected() {
        use crate::runtime_config::FeatureFamily;

        let mut config = RuntimeConfig::default();
        assert!(
            config
                .features
                .enabled_feature_families
                .contains(&FeatureFamily::Domain),
            "default config enables the Domain feature family"
        );
        // Disable every vertical while `Domain` stays enabled.
        for enabled in config.domain.enabled_by_family.values_mut() {
            *enabled = false;
        }
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn enabled_vertical_without_domain_feature_family_is_rejected() {
        use crate::runtime_config::FeatureFamily;

        let mut config = RuntimeConfig::default();
        config
            .features
            .enabled_feature_families
            .retain(|family| *family != FeatureFamily::Domain);
        assert!(
            config.domain.enabled_by_family.values().any(|&on| on),
            "default config enables the crypto vertical"
        );
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn domain_feature_family_coherent_with_enabled_vertical_is_accepted() {
        // The default config keeps both sides coherent — no regression beyond
        // `default_runtime_config_is_valid`, but explicit for this invariant.
        let config = RuntimeConfig::default();
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }
}
