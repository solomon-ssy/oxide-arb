//! [`ModelSpecService`]: authoring of governed model specifications — the
//! production write path that seeds the offline research lifecycle root.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        api::{FeatureContractEntryView, FeatureContractView, FeatureNullPolicyView},
        ports::{CreateModelSpecCommand, GovernanceActor, ModelSpecPort},
        quant::{ModelSpecInfo, NewModelSpec},
    },
    runtime_config::FeaturesConfig,
    types::{
        ModelInputContract, ModelSpecId, SchemaVersion, model_spec::ModelSpecDefinition,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::traits::ModelRegistryRepository;
use quant_pivot_research::{
    features::{FeatureSchema, FeatureUnit, NullPolicy, PitRule, SourceRequirement, StalenessRule},
    hashing::ResearchHasher,
};

use crate::runtime_config::DecisionPolicyStore;

/// Dependencies for model-spec authoring.
pub struct ModelSpecDeps {
    /// Model registry persistence port.
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    /// Active governed feature catalog used to validate raw input membership.
    pub runtime_config: Arc<DecisionPolicyStore>,
}

/// Authoring orchestration for governed model specifications.
pub struct ModelSpecService {
    deps: ModelSpecDeps,
}

impl ModelSpecService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub const fn new(deps: ModelSpecDeps) -> Self {
        Self { deps }
    }
}

fn validate_input_contract(
    contract: &ModelInputContract,
    requested_version: SchemaVersion,
    features: &FeaturesConfig,
) -> QuantResult<()> {
    contract
        .validate()
        .map_err(|detail| QuantError::config(format!("invalid input_contract: {detail}")))?;
    if contract.inputs.is_empty() {
        return Err(QuantError::config(
            "invalid input_contract: at least one raw feature is required",
        ));
    }

    let schema = FeatureSchema::build(features)?;
    if requested_version != schema.version() {
        return Err(QuantError::config(format!(
            "feature_schema_version {} does not match active governed schema {}",
            requested_version.get(),
            schema.version().get(),
        )));
    }
    for input in &contract.inputs {
        let name = FeatureName::new(input.feature_name.clone());
        if !schema.contains(&name) {
            return Err(QuantError::config(format!(
                "input_contract references unknown feature `{}` in schema {}",
                input.feature_name,
                schema.version().get(),
            )));
        }
    }
    Ok(())
}

const fn feature_unit_wire(unit: FeatureUnit) -> &'static str {
    match unit {
        FeatureUnit::None => "none",
        FeatureUnit::Ratio => "ratio",
        FeatureUnit::Bps => "bps",
        FeatureUnit::Usd => "usd",
        FeatureUnit::Shares => "shares",
        FeatureUnit::Probability => "probability",
        FeatureUnit::Seconds => "seconds",
        FeatureUnit::Milliseconds => "milliseconds",
        FeatureUnit::Count => "count",
        FeatureUnit::PerSecond => "per_second",
    }
}

const fn source_requirement_wire(source: SourceRequirement) -> &'static str {
    match source {
        SourceRequirement::PublishedL2Book => "published_l2_book",
        SourceRequirement::GammaMetadata => "gamma_metadata",
        SourceRequirement::ResolvedLinkage => "resolved_linkage",
        SourceRequirement::MicrostructureWindow => "microstructure_window",
        SourceRequirement::NegRiskSiblingLegs => "neg_risk_sibling_legs",
        SourceRequirement::TradeTapeWindow => "trade_tape_window",
        SourceRequirement::DomainObservationWindow => "domain_observation_window",
    }
}

const fn point_in_time_rule_wire(rule: PitRule) -> &'static str {
    match rule {
        PitRule::BookVersionAtOrBeforeSourceCutoff => "book_version_at_or_before_source_cutoff",
        PitRule::FactAtOrBeforeSourceCutoff => "fact_at_or_before_source_cutoff",
        PitRule::MetadataVersionAtOrBeforeSourceCutoff => {
            "metadata_version_at_or_before_source_cutoff"
        }
        PitRule::LinkageVersionAtOrBeforeSourceCutoff => {
            "linkage_version_at_or_before_source_cutoff"
        }
    }
}

const fn staleness_policy_wire(policy: StalenessRule) -> &'static str {
    match policy {
        StalenessRule::None => "none",
        StalenessRule::MaxBookAge => "max_book_age",
        StalenessRule::MaxFeatureBucketAge => "max_feature_bucket_age",
        StalenessRule::MaxTradeTapeAge => "max_trade_tape_age",
        StalenessRule::MaxDomainObservationAge => "max_domain_observation_age",
    }
}

fn null_policy_view(policy: &NullPolicy) -> FeatureNullPolicyView {
    match policy {
        NullPolicy::RejectMarket => FeatureNullPolicyView {
            policy: "reject_market".to_owned(),
            value: None,
        },
        NullPolicy::NeutralValue(value) => FeatureNullPolicyView {
            policy: "neutral_value".to_owned(),
            value: Some(value.to_string()),
        },
        NullPolicy::Penalize => FeatureNullPolicyView {
            policy: "penalize".to_owned(),
            value: None,
        },
        NullPolicy::Optional => FeatureNullPolicyView {
            policy: "optional".to_owned(),
            value: None,
        },
    }
}

fn build_feature_contract(features: &FeaturesConfig) -> QuantResult<FeatureContractView> {
    let schema = FeatureSchema::build(features)?;
    let feature_schema_hash = ResearchHasher::feature_schema(&schema)?;
    let mut entries = schema
        .specs()
        .iter()
        .map(|spec| FeatureContractEntryView {
            name: spec.name.as_str().to_owned(),
            compute_revision: spec.compute_revision,
            family: spec.family,
            value_kind: spec.value_kind,
            unit: feature_unit_wire(spec.unit).to_owned(),
            null_policy: null_policy_view(&spec.null_policy),
            source: source_requirement_wire(spec.source_requirement).to_owned(),
            point_in_time_rule: point_in_time_rule_wire(spec.point_in_time_rule).to_owned(),
            staleness_policy: staleness_policy_wire(spec.staleness_policy).to_owned(),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(FeatureContractView {
        feature_schema_hash,
        feature_schema_version: schema.version(),
        features: entries,
    })
}

#[async_trait]
impl ModelSpecPort for ModelSpecService {
    async fn feature_contract(&self) -> QuantResult<FeatureContractView> {
        let runtime = self.deps.runtime_config.current();
        build_feature_contract(&runtime.profile_artifacts.features.definition)
    }

    async fn create(
        &self,
        command: CreateModelSpecCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelSpecInfo> {
        let runtime = self.deps.runtime_config.current();
        validate_input_contract(
            &command.input_contract,
            command.feature_schema_version,
            &runtime.profile_artifacts.features.definition,
        )?;
        command
            .training_contract
            .validate()
            .map_err(|detail| QuantError::config(format!("invalid training_contract: {detail}")))?;
        let definition = ModelSpecDefinition {
            name: &command.name,
            model_family: command.model_family,
            prediction_horizon_secs: command.prediction_horizon_secs,
            feature_schema_version: command.feature_schema_version,
            label_schema_version: command.label_schema_version,
            thesis: &command.thesis,
            input_contract: &command.input_contract,
            training_contract: &command.training_contract,
        };
        definition
            .validate()
            .map_err(|detail| QuantError::config(format!("invalid model spec: {detail}")))?;
        let definition_hash = definition
            .content_hash()
            .map_err(|error| QuantError::config(format!("model spec hash failed: {error}")))?;
        self.deps
            .model_registry
            .create_model_spec(NewModelSpec {
                model_spec_id: ModelSpecId::from_v7(),
                name: command.name,
                model_family: command.model_family,
                prediction_horizon_secs: command.prediction_horizon_secs,
                feature_schema_version: command.feature_schema_version,
                label_schema_version: command.label_schema_version,
                thesis: command.thesis,
                input_contract: command.input_contract,
                training_contract: command.training_contract,
                definition_hash,
                created_by_user_id: actor.user_id,
                created_by_label: actor.username,
                created_by_role: actor.role,
                reason: command.reason,
            })
            .await
            .map_err(Into::into)
    }

    async fn find(&self, model_spec_id: &ModelSpecId) -> QuantResult<Option<ModelSpecInfo>> {
        self.deps
            .model_registry
            .find_model_spec_by_id(model_spec_id)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::feature::FeatureValueKind,
        runtime_config::FeaturesConfig,
        types::{ModelInputContract, ModelInputSpec, SchemaVersion},
    };

    use super::{build_feature_contract, validate_input_contract};

    #[test]
    fn rejects_empty_encoded_unknown_and_wrong_schema_contracts() {
        let features = FeaturesConfig::default();
        let version = features.feature_schema_version;

        assert!(
            validate_input_contract(&ModelInputContract::default(), version, &features).is_err()
        );
        assert!(
            validate_input_contract(
                &ModelInputContract {
                    inputs: vec![ModelInputSpec::required("book.mid.__missing")],
                },
                version,
                &features,
            )
            .is_err()
        );
        assert!(
            validate_input_contract(
                &ModelInputContract::single_required("unknown.feature"),
                version,
                &features,
            )
            .is_err()
        );
        assert!(
            validate_input_contract(
                &ModelInputContract::single_required("book.mid"),
                SchemaVersion::new(version.get() + 1),
                &features,
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_non_empty_contract_from_active_schema() {
        let features = FeaturesConfig::default();
        validate_input_contract(
            &ModelInputContract::single_required("book.mid"),
            features.feature_schema_version,
            &features,
        )
        .expect("governed raw feature must be accepted");
    }

    #[test]
    fn feature_contract_is_hash_bound_sorted_and_describes_mid_price() {
        let features = FeaturesConfig::default();
        let contract = build_feature_contract(&features).expect("active feature contract");

        assert_eq!(
            contract.feature_schema_version,
            features.feature_schema_version
        );
        assert!(contract.feature_schema_hash.as_str().starts_with("blake3:"));
        assert!(
            contract
                .features
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
        );
        let mid = contract
            .features
            .iter()
            .find(|feature| feature.name == "book.mid")
            .expect("book.mid contract entry");
        assert_eq!(mid.compute_revision, 1);
        assert_eq!(mid.unit, "probability");
        assert_eq!(mid.source, "published_l2_book");
        let secondary_ask = contract
            .features
            .iter()
            .find(|feature| feature.name == "book.secondary_best_ask")
            .expect("secondary executable ask contract entry");
        assert_eq!(secondary_ask.compute_revision, 1);
        assert_eq!(secondary_ask.value_kind, FeatureValueKind::Probability);
        assert_eq!(secondary_ask.unit, "probability");
        assert_eq!(secondary_ask.null_policy.policy, "penalize");
        assert_eq!(secondary_ask.source, "published_l2_book");
        assert_eq!(secondary_ask.staleness_policy, "max_book_age");
    }
}
