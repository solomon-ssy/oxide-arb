//! Governed feature-catalog HTTP contracts.
//!
//! This is a read projection of the canonical research `FeatureSchema`, not a
//! second feature contract. The core service rebuilds it from the active runtime
//! configuration on every request, so model-spec authoring cannot drift onto a
//! hard-coded UI catalog.

use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    enums::feature::FeatureValueKind,
    runtime_config::FeatureFamily,
    types::{ContentHash, SchemaVersion},
};

/// Stable wire projection of one feature's missing-value policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FeatureNullPolicyView {
    /// Policy name (`reject_market`, `neutral_value`, `penalize`, `optional`).
    pub policy: String,
    /// Exact decimal neutral value when `policy = neutral_value`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// One raw feature available to model input contracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FeatureContractEntryView {
    pub name: String,
    pub compute_revision: u32,
    pub family: FeatureFamily,
    pub value_kind: FeatureValueKind,
    pub unit: String,
    pub null_policy: FeatureNullPolicyView,
    pub source: String,
    pub point_in_time_rule: String,
    pub staleness_policy: String,
}

/// Active, hash-bound feature catalog used by model-spec authoring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FeatureContractView {
    pub feature_schema_hash: ContentHash,
    pub feature_schema_version: SchemaVersion,
    pub features: Vec<FeatureContractEntryView>,
}

#[cfg(test)]
mod tests {
    use super::{FeatureContractEntryView, FeatureContractView, FeatureNullPolicyView};
    use crate::{
        enums::feature::FeatureValueKind,
        runtime_config::FeatureFamily,
        types::{ContentHash, SchemaVersion},
    };

    #[test]
    fn feature_contract_serializes_shape() {
        let view = FeatureContractView {
            feature_schema_hash: ContentHash::parse(concat!(
                "blake3:",
                "0000000000000000000000000000000000000000000000000000000000000000"
            ))
            .expect("canonical feature schema hash fixture"),
            feature_schema_version: SchemaVersion::FIRST,
            features: vec![FeatureContractEntryView {
                name: "book.mid".to_owned(),
                compute_revision: 1,
                family: FeatureFamily::PriceBook,
                value_kind: FeatureValueKind::Probability,
                unit: "probability".to_owned(),
                null_policy: FeatureNullPolicyView {
                    policy: "reject_market".to_owned(),
                    value: None,
                },
                source: "published_l2_book".to_owned(),
                point_in_time_rule: "book_version_at_or_before_source_cutoff".to_owned(),
                staleness_policy: "max_book_age".to_owned(),
            }],
        };

        let encoded = serde_json::to_value(view).expect("serialize feature contract view");
        assert_eq!(encoded["feature_schema_version"], 1);
        assert_eq!(encoded["features"][0]["family"], "price_book");
        assert_eq!(encoded["features"][0]["value_kind"], "probability");
        assert!(encoded["features"][0]["null_policy"].get("value").is_none());
    }
}
