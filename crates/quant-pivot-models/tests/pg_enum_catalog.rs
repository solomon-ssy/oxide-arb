//! Locks Rust `pg_enum!` labels ↔ [`SeaORM`] `string_value` ↔ Postgres `qp_*` type names.

use quant_pivot_models::{
    enums::{
        common::{MarketCategory, Side, StalenessLevel, TickSize},
        domain::DomainFamily,
        execution::{ExecutionOrderPhase, OrderIntentKind, OrderTypeKind, VenueOrderStatus},
        factor::{FactorDefinitionScope, FactorFamily},
        fee::FeeSource,
        market::{EventStatus, MarketStatus},
        model::{ClassicalKind, ModelFamily},
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, DataQualityStatus, ExecutionOrderState, FactorDirection,
            ModelGovernanceAction, ModelRunErrorCode, ModelRunKind, ModelRunStatus,
            OrderIntentStatus, OutcomeSide, PublicationStatus, QuantRuntimeMode,
            RecommendationOutcome, RecommendationReportStatus, RecommendationStatus, ReportKind,
            ReportTriggerKind, TrainingDatasetStatus,
        },
        rbac::{MenuKind, ResourceType, RoleKind, RoleStatus, UserStatus},
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    schema::pg_enum,
};
use sea_orm::{Iterable, entity::ActiveEnum};

macro_rules! assert_pg_enum {
    ($ty:ty, $expected:expr) => {{
        let expected: &[&str] = $expected;
        let labels: Vec<&str> = <$ty as Iterable>::iter()
            .map(|variant| variant.as_str())
            .collect();
        assert_eq!(
            labels,
            expected,
            "variant label drift for `{}`",
            stringify!($ty)
        );

        for variant in <$ty as Iterable>::iter() {
            let wire = variant.as_str();
            assert_eq!(variant.to_string(), wire);
            let round_trip = <$ty as ActiveEnum>::try_from_value(&variant.to_value())
                .expect("SeaORM round-trip");
            assert_eq!(round_trip.as_str(), wire);
            let parsed: $ty = wire.parse().expect("FromStr round-trip");
            assert_eq!(parsed, variant);
        }

        assert!(
            "__invalid_enum_label__".parse::<$ty>().is_err(),
            "FromStr must reject unknown labels for `{}`",
            stringify!($ty)
        );
    }};
}

macro_rules! assert_wire_enum_from_str {
    ($ty:ty, $variants:expr) => {{
        for variant in $variants {
            let wire = variant.as_str();
            assert_eq!(variant.to_string(), wire);
            let parsed: $ty = wire.parse().expect("FromStr round-trip");
            assert_eq!(parsed, variant);
        }
        assert!(
            "__invalid_enum_label__".parse::<$ty>().is_err(),
            "FromStr must reject unknown labels for `{}`",
            stringify!($ty)
        );
    }};
}

fn active_enum_type_name<E: ActiveEnum>() -> String {
    E::name().to_string()
}

#[test]
fn pg_enum_specs_are_unique_and_prefixed() {
    let specs = pg_enum::specs();
    assert_eq!(
        specs.len(),
        41,
        "expected exactly 41 Postgres enum types, got {}",
        specs.len()
    );

    for spec in &specs {
        assert!(
            spec.type_name.starts_with("qp_"),
            "enum type `{}` must use qp_ prefix",
            spec.type_name
        );
    }
}

#[test]
fn expected_pg_enum_types_are_registered() {
    let registered: std::collections::BTreeSet<_> =
        pg_enum::specs().into_iter().map(|s| s.type_name).collect();

    let expected = [
        "qp_account_source",
        "qp_approval_status",
        "qp_data_quality_status",
        "qp_event_status",
        "qp_execution_order_phase",
        "qp_execution_order_state",
        "qp_factor_definition_scope",
        "qp_factor_direction",
        "qp_factor_family",
        "qp_fee_source",
        "qp_market_category",
        "qp_market_status",
        "qp_menu_kind",
        "qp_model_family",
        "qp_model_governance_action",
        "qp_model_run_error_code",
        "qp_model_run_kind",
        "qp_model_run_status",
        "qp_operation_category",
        "qp_operation_outcome",
        "qp_order_intent_kind",
        "qp_order_intent_status",
        "qp_order_type_kind",
        "qp_outcome_side",
        "qp_publication_status",
        "qp_quant_runtime_mode",
        "qp_recommendation_outcome",
        "qp_recommendation_report_status",
        "qp_recommendation_status",
        "qp_report_kind",
        "qp_report_trigger_kind",
        "qp_resource_type",
        "qp_role_kind",
        "qp_role_status",
        "qp_runtime_config_activation_kind",
        "qp_runtime_config_source",
        "qp_side",
        "qp_tick_size",
        "qp_training_dataset_status",
        "qp_user_status",
        "qp_venue_order_status",
    ];

    for type_name in expected {
        assert!(
            registered.contains(type_name),
            "missing registered Postgres enum `{type_name}`"
        );
    }
}

#[test]
fn publication_status_merged_type_covers_model_and_factor_states() {
    assert_pg_enum!(
        PublicationStatus,
        &[
            "draft",
            "candidate",
            "shadow",
            "published",
            "retired",
            "rejected",
        ]
    );
    assert_eq!(
        active_enum_type_name::<PublicationStatus>(),
        "qp_publication_status"
    );
}

#[test]
fn core_quant_enums_match_wire_labels() {
    assert_pg_enum!(
        QuantRuntimeMode,
        &["report_only", "semi_auto", "auto_execution"]
    );
    assert_pg_enum!(ReportKind, &["top_n", "shadow_top_n", "post_run_audit"]);
    assert_pg_enum!(
        MarketStatus,
        &[
            "discovered",
            "active",
            "filtered",
            "paused",
            "settled",
            "delisted",
        ]
    );
    assert_pg_enum!(OrderIntentKind, &["buy"]);
    assert_pg_enum!(OrderTypeKind, &["fok", "gtc", "gtd"]);
    assert_pg_enum!(
        VenueOrderStatus,
        &[
            "filled",
            "partially_filled",
            "rejected",
            "cancelled",
            "open",
            "expired",
        ]
    );
}

#[test]
fn model_family_flat_labels_round_trip() {
    assert_pg_enum!(
        ModelFamily,
        &[
            "weighted_factor",
            "classical_random_forest",
            "classical_extra_trees",
            "classical_logistic_regression",
            "classical_ridge",
            "classical_lasso",
            "classical_elastic_net",
        ]
    );

    let variant = ModelFamily::ClassicalRandomForest;
    assert_eq!(variant.as_str(), "classical_random_forest");
    assert_eq!(variant.to_string(), "classical_random_forest");
}

#[test]
fn factor_family_flat_labels_include_domains() {
    assert_pg_enum!(
        FactorFamily,
        &[
            "liquidity",
            "microstructure",
            "momentum",
            "mean_reversion",
            "volatility",
            "activity",
            "resolution",
            "data_quality",
            "domain_sports",
            "domain_politics",
            "domain_crypto",
            "domain_weather",
            "domain_geopolitics",
        ]
    );

    assert_eq!(
        FactorFamily::DomainSports.definition_scope(),
        FactorDefinitionScope::DomainSports
    );
    assert_pg_enum!(
        FactorDefinitionScope,
        &[
            "generic",
            "domain_sports",
            "domain_politics",
            "domain_crypto",
            "domain_weather",
            "domain_geopolitics",
        ]
    );
}

#[test]
fn rbac_and_runtime_enums_are_cataloged() {
    assert_pg_enum!(UserStatus, &["active", "disabled"]);
    assert_pg_enum!(RoleKind, &["builtin", "custom"]);
    assert_pg_enum!(RoleStatus, &["enabled", "disabled"]);
    assert_pg_enum!(
        RuntimeConfigVersionSource,
        &["bootstrap", "operator", "import"]
    );
    assert_pg_enum!(
        RuntimeConfigActivationKind,
        &["initial", "promote", "rollback"]
    );
}

#[test]
fn sea_orm_type_names_match_pg_enum_specs() {
    let by_name: std::collections::BTreeMap<_, _> = pg_enum::specs()
        .into_iter()
        .map(|spec| (spec.type_name, spec))
        .collect();

    let pairs = [
        ("qp_market_status", active_enum_type_name::<MarketStatus>()),
        (
            "qp_publication_status",
            active_enum_type_name::<PublicationStatus>(),
        ),
        (
            "qp_quant_runtime_mode",
            active_enum_type_name::<QuantRuntimeMode>(),
        ),
        (
            "qp_market_category",
            active_enum_type_name::<MarketCategory>(),
        ),
        ("qp_fee_source", active_enum_type_name::<FeeSource>()),
        ("qp_side", active_enum_type_name::<Side>()),
    ];

    for (expected, actual) in pairs {
        assert_eq!(actual, expected);
        assert!(by_name.contains_key(expected));
    }

    assert_eq!(active_enum_type_name::<TickSize>(), "qp_tick_size");
    assert_eq!(active_enum_type_name::<EventStatus>(), "qp_event_status");
    assert_eq!(
        active_enum_type_name::<ExecutionOrderPhase>(),
        "qp_execution_order_phase"
    );
    assert_eq!(
        active_enum_type_name::<ExecutionOrderState>(),
        "qp_execution_order_state"
    );
    assert_eq!(
        active_enum_type_name::<AccountSource>(),
        "qp_account_source"
    );
    assert_eq!(active_enum_type_name::<OutcomeSide>(), "qp_outcome_side");
    assert_eq!(
        active_enum_type_name::<RecommendationStatus>(),
        "qp_recommendation_status"
    );
    assert_eq!(
        active_enum_type_name::<OrderIntentStatus>(),
        "qp_order_intent_status"
    );
    assert_eq!(
        active_enum_type_name::<ApprovalStatus>(),
        "qp_approval_status"
    );
    assert_eq!(
        active_enum_type_name::<ReportTriggerKind>(),
        "qp_report_trigger_kind"
    );
    assert_eq!(
        active_enum_type_name::<RecommendationReportStatus>(),
        "qp_recommendation_report_status"
    );
    assert_eq!(
        active_enum_type_name::<DataQualityStatus>(),
        "qp_data_quality_status"
    );
    assert_eq!(
        active_enum_type_name::<FactorDirection>(),
        "qp_factor_direction"
    );
    assert_eq!(
        active_enum_type_name::<TrainingDatasetStatus>(),
        "qp_training_dataset_status"
    );
    assert_eq!(active_enum_type_name::<ModelRunKind>(), "qp_model_run_kind");
    assert_eq!(
        active_enum_type_name::<ModelRunStatus>(),
        "qp_model_run_status"
    );
    assert_eq!(
        active_enum_type_name::<ModelRunErrorCode>(),
        "qp_model_run_error_code"
    );
    assert_eq!(
        active_enum_type_name::<ModelGovernanceAction>(),
        "qp_model_governance_action"
    );
    assert_eq!(
        active_enum_type_name::<RecommendationOutcome>(),
        "qp_recommendation_outcome"
    );
    assert_eq!(active_enum_type_name::<MenuKind>(), "qp_menu_kind");
    assert_eq!(active_enum_type_name::<ResourceType>(), "qp_resource_type");
    assert_eq!(
        active_enum_type_name::<OperationCategory>(),
        "qp_operation_category"
    );
    assert_eq!(
        active_enum_type_name::<OperationOutcome>(),
        "qp_operation_outcome"
    );
    assert_eq!(active_enum_type_name::<RoleKind>(), "qp_role_kind");
}

#[test]
fn wire_enums_from_str_round_trip() {
    use quant_pivot_models::enums::common::Side;

    assert_wire_enum_from_str!(Side, [Side::Buy, Side::Sell]);
    assert_wire_enum_from_str!(
        StalenessLevel,
        [
            StalenessLevel::Fresh,
            StalenessLevel::Acceptable,
            StalenessLevel::Stale,
            StalenessLevel::Expired,
        ]
    );
    assert_wire_enum_from_str!(DomainFamily, DomainFamily::ALL);
    assert_wire_enum_from_str!(
        ClassicalKind,
        [
            ClassicalKind::RandomForest,
            ClassicalKind::ExtraTrees,
            ClassicalKind::LogisticRegression,
            ClassicalKind::Ridge,
            ClassicalKind::Lasso,
            ClassicalKind::ElasticNet,
        ]
    );
}

#[test]
fn tick_size_from_str_trims_whitespace() {
    assert_eq!(
        " 0.01 ".parse::<TickSize>().expect("trimmed tick size"),
        TickSize::Hundredth
    );
}
