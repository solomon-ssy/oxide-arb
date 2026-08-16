use quant_pivot_models::types::{
    FeatureValue, builtin_research_profiles, stable_name::FeatureName,
};
use quant_pivot_research::feedback::{
    ConceptDriftDetail, CoverageGateInput, CoverageGateOutcome, CoverageNoActionReason,
    FeatureDriftDetail, LabelDriftDetail, PopulationBinKind, drift_observations, jensen_shannon,
    numeric_drift, target_rank_ic_drift,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn coverage_requires_new_labels() {
    let outcome = CoverageGateInput {
        policy_evaluation_count: 100,
        mature_label_count: 80,
        new_mature_label_count: 4,
        minimum_mature_labels: 50,
        minimum_new_mature_labels: 5,
        minimum_coverage: dec!(0.75),
    }
    .evaluate()
    .expect("valid frozen coverage counts");

    assert_eq!(
        outcome,
        CoverageGateOutcome::NoAction {
            reason: CoverageNoActionReason::InsufficientNewMatureLabels,
            coverage: dec!(0.8),
        }
    );
}

#[test]
fn drift_metrics_are_typed() {
    let baseline = [
        dec!(0),
        dec!(1),
        dec!(2),
        dec!(3),
        dec!(4),
        dec!(5),
        dec!(6),
        dec!(7),
        dec!(8),
        dec!(9),
    ];
    let evaluation = [
        dec!(5),
        dec!(6),
        dec!(7),
        dec!(8),
        dec!(9),
        dec!(10),
        dec!(11),
        dec!(12),
        dec!(13),
        dec!(14),
    ];
    let data = numeric_drift(&baseline, &evaluation)
        .expect("valid numeric drift")
        .expect("non-degenerate numeric samples");
    assert!(data.population_stability_index > Decimal::ZERO);
    assert!(data.kolmogorov_smirnov_statistic > Decimal::ZERO);
    assert!(data.kolmogorov_smirnov_p_value < dec!(0.2));

    let baseline_scores = [dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.4)];
    let baseline_labels = [dec!(0), dec!(0), dec!(1), dec!(1)];
    let evaluation_scores = [dec!(0.4), dec!(0.3), dec!(0.2), dec!(0.1)];
    let evaluation_labels = [dec!(0), dec!(0), dec!(1), dec!(1)];
    let concept = target_rank_ic_drift(
        &baseline_scores,
        &baseline_labels,
        &evaluation_scores,
        &evaluation_labels,
    )
    .expect("valid rank-IC samples")
    .expect("non-degenerate rank-IC samples");
    assert_eq!(concept.baseline_target_rank_ic, dec!(0.894427191));
    assert_eq!(concept.evaluation_target_rank_ic, dec!(-0.894427191));
    assert_eq!(concept.observed_drop, Decimal::ONE);

    assert_eq!(
        jensen_shannon(&[10, 0], &[0, 10])
            .expect("valid label histograms")
            .expect("non-empty label histograms"),
        Decimal::ONE
    );
    assert_eq!(
        jensen_shannon(&[5, 5], &[5, 5])
            .expect("valid label histograms")
            .expect("non-empty label histograms"),
        Decimal::ZERO
    );
}

#[test]
fn ks_extreme_is_finite() {
    let baseline = vec![Decimal::ZERO; 500];
    let evaluation = vec![Decimal::ONE; 500];
    let drift = numeric_drift(&baseline, &evaluation)
        .expect("extreme KS separation must remain evaluable")
        .expect("large samples provide sufficient KS evidence");

    assert_eq!(drift.kolmogorov_smirnov_statistic, Decimal::ONE);
    assert_eq!(drift.kolmogorov_smirnov_p_value, Decimal::ZERO);
}

#[test]
fn population_drift_keeps_missing() {
    let baseline = [
        Some(FeatureValue::Bool(false)),
        Some(FeatureValue::Bool(false)),
        None,
    ];
    let evaluation = [Some(FeatureValue::Bool(true)), None, None];
    let detail = FeatureDriftDetail::compute(
        FeatureName::from_static("test.boolean"),
        &baseline,
        &evaluation,
    )
    .expect("valid discrete population drift");

    assert_eq!(detail.baseline_total, 3);
    assert_eq!(detail.baseline_observed, 2);
    assert_eq!(detail.evaluation_total, 3);
    assert_eq!(detail.evaluation_observed, 1);
    assert!(
        detail
            .population_stability_index
            .is_some_and(|value| value > Decimal::ZERO)
    );
    assert!(
        detail
            .population_bins
            .iter()
            .any(|bin| bin.kind == PopulationBinKind::Missing)
    );
    assert!(detail.kolmogorov_smirnov_p_value.is_none());
}

#[test]
fn drift_headers_reproduce_detail() {
    let policy = builtin_research_profiles()
        .expect("built-in profiles")
        .into_iter()
        .next()
        .expect("at least one profile")
        .spec
        .feedback_policy;
    let data = FeatureDriftDetail::compute(
        FeatureName::from_static("test.numeric"),
        &[
            Some(FeatureValue::Decimal(dec!(0))),
            Some(FeatureValue::Decimal(dec!(1))),
        ],
        &[
            Some(FeatureValue::Decimal(dec!(10))),
            Some(FeatureValue::Decimal(dec!(11))),
        ],
    )
    .expect("valid numeric feature drift");
    let concept = ConceptDriftDetail {
        baseline_scored_count: 4,
        evaluation_scored_count: 4,
        summary: target_rank_ic_drift(
            &[dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.4)],
            &[dec!(0), dec!(0), dec!(1), dec!(1)],
            &[dec!(0.4), dec!(0.3), dec!(0.2), dec!(0.1)],
            &[dec!(0), dec!(0), dec!(1), dec!(1)],
        )
        .expect("valid rank-IC evidence"),
    };
    let label = LabelDriftDetail {
        baseline_counts: vec![4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        evaluation_counts: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4],
        divergence: Some(Decimal::ONE),
    };

    let observations = drift_observations(&policy, &[data], &concept, &label)
        .expect("aggregate exact drift headers");
    assert_eq!(observations.len(), 4);
    assert_eq!(observations[0].sample_count, 2);
    assert_eq!(observations[1].sample_count, 2);
    assert_eq!(observations[2].sample_count, 4);
    assert_eq!(observations[3].sample_count, 4);
    assert!(
        jensen_shannon(&[u64::MAX, 1], &[1, 1]).is_err(),
        "histogram aggregation must fail closed on count overflow"
    );
}
