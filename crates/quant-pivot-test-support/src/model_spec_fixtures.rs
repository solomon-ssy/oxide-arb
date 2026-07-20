//! Canonical immutable model-spec fixtures shared by integration harnesses.

use quant_pivot_models::{
    domain::NewModelSpec,
    enums::model::ModelFamily,
    types::{
        ContentHash, ModelInputContract, ModelSpecId, ModelTrainingContract, SchemaVersion,
        model_spec::{ModelSpecDefinition, ModelSpecThesis},
    },
};

/// Build valid typed spec lineage for tests that only need an enriched
/// `ModelVersionInfo` projection rather than a persisted spec row.
#[must_use]
pub fn model_spec_lineage_fixture(name: &str) -> (ModelSpecThesis, ContentHash) {
    let spec = new_model_spec_fixture(
        ModelSpecId::from_v7(),
        name,
        ModelFamily::WeightedFactor,
        86_400,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
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
