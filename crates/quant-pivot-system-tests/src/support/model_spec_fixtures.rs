//! Canonical immutable model-spec fixtures owned by system tests.

use quant_pivot_models::{
    domain::quant::NewModelSpec,
    enums::{common::MarketCategory, model::ModelFamily},
    types::{
        CRYPTO_PRICE_15M_HORIZON_SECS, ContentHash, ModelInputContract, ModelSpecId,
        ModelTrainingContract, POOLED_1H_HORIZON_SECS, POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID,
        ResearchProfileRef, SchemaVersion, WEATHER_FORECAST_24H_HORIZON_SECS,
        builtin_research_profiles,
        model_spec::{ModelSpecDefinition, ModelSpecThesis},
    },
};

/// Exact built-in Crypto `ResearchProfile` horizon in the persisted `i64` shape.
#[must_use]
pub fn crypto_horizon_secs() -> i64 {
    i64::try_from(CRYPTO_PRICE_15M_HORIZON_SECS).expect("Crypto profile horizon fits i64")
}

/// Exact built-in pooled `ResearchProfile` horizon in the persisted `i64` shape.
#[must_use]
pub fn pooled_horizon_secs() -> i64 {
    i64::try_from(POOLED_1H_HORIZON_SECS).expect("pooled profile horizon fits i64")
}

/// Exact built-in Weather `ResearchProfile` horizon in the persisted `i64` shape.
#[must_use]
pub fn weather_horizon_secs() -> i64 {
    i64::try_from(WEATHER_FORECAST_24H_HORIZON_SECS).expect("weather profile horizon fits i64")
}

/// Exact built-in Crypto `ResearchProfile` reference.
#[must_use]
pub fn crypto_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("built-in ResearchProfiles")
        .into_iter()
        .find(|profile| {
            profile.spec.category == Some(MarketCategory::Crypto)
                && profile.spec.target_horizon_secs == CRYPTO_PRICE_15M_HORIZON_SECS
                && profile.spec.feature_contract.requires_l2()
        })
        .expect("Crypto ResearchProfile")
        .profile_ref
}

/// Exact built-in pooled `ResearchProfile` reference.
#[must_use]
pub fn pooled_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("built-in ResearchProfiles")
        .into_iter()
        .find(|profile| {
            profile.spec.category.is_none()
                && profile.spec.target_horizon_secs == POOLED_1H_HORIZON_SECS
                && profile.spec.feature_contract.requires_l2()
        })
        .expect("pooled ResearchProfile")
        .profile_ref
}

/// Exact built-in pooled `ReportOnly` bootstrap `ResearchProfile` reference.
#[must_use]
pub fn pooled_bootstrap_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("built-in ResearchProfiles")
        .into_iter()
        .find(|profile| profile.profile_ref.id.as_str() == POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID)
        .expect("pooled bootstrap ResearchProfile")
        .profile_ref
}

/// Exact built-in Weather `ResearchProfile` reference.
#[must_use]
pub fn weather_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("built-in ResearchProfiles")
        .into_iter()
        .find(|profile| {
            profile.spec.category == Some(MarketCategory::Weather)
                && profile.spec.target_horizon_secs == WEATHER_FORECAST_24H_HORIZON_SECS
                && profile.spec.feature_contract.requires_l2()
        })
        .expect("Weather ResearchProfile")
        .profile_ref
}

/// Build valid typed spec lineage for tests that only need an enriched
/// `ModelVersionInfo` projection rather than a persisted spec row.
#[must_use]
pub fn model_spec_lineage_fixture(name: &str) -> (ModelSpecThesis, ContentHash) {
    let spec = new_model_spec_fixture(
        ModelSpecId::from_v7(),
        name,
        ModelFamily::WeightedFactor,
        pooled_horizon_secs(),
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::outcome_default(),
    );
    (spec.thesis, spec.definition_hash)
}

/// Build a valid content-addressed spec without duplicating hash construction
/// across fixtures.
#[must_use]
pub fn new_model_spec_fixture(
    model_spec_id: ModelSpecId,
    name: impl Into<String>,
    model_family: ModelFamily,
    prediction_horizon_secs: i64,
    input_contract: ModelInputContract,
    training_contract: ModelTrainingContract,
) -> NewModelSpec {
    let name = name.into();
    let thesis = ModelSpecThesis {
        summary: format!("Test fixture for {name}"),
        hypothesis: format!(
            "The governed inputs for {name} predict its declared target under frozen test data"
        ),
        limitations: vec![
            "Synthetic test fixture; never evidence for a production publication".to_owned(),
        ],
    };
    let definition = ModelSpecDefinition {
        name: &name,
        model_family,
        prediction_horizon_secs,
        feature_schema_version: SchemaVersion::FIRST,
        label_schema_version: SchemaVersion::FIRST,
        thesis: &thesis,
        input_contract: &input_contract,
        training_contract: &training_contract,
    };
    definition.validate().expect("valid fixture model spec");
    let definition_hash = definition.content_hash().expect("fixture spec hash");
    NewModelSpec {
        model_spec_id,
        name,
        model_family,
        prediction_horizon_secs,
        feature_schema_version: SchemaVersion::FIRST,
        label_schema_version: SchemaVersion::FIRST,
        thesis,
        input_contract,
        training_contract,
        definition_hash,
        created_by_user_id: None,
        created_by_label: "model-spec-fixture".to_owned(),
        created_by_role: None,
        reason: "construct deterministic model-spec test fixture".to_owned(),
    }
}
