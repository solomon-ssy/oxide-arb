//! Load-time integrity checks for route-bound model artifacts.
//!
//! Champion and shadow roles are properties of the exact activated
//! `ModelRouting` generation. The model-version row is an immutable artifact
//! registry entry and carries no independent lifecycle or serving authority.
//! Offline quality evidence is sealed into the candidate manifest and promotion
//! gate; online loading revalidates the immutable serving contract.

use quant_pivot_models::domain::quant::ModelVersionInfo;

/// Revalidate the persisted contract hash and every scalar projection.
pub fn model_contract_ok(version: &ModelVersionInfo) -> Result<(), String> {
    version
        .verified_serving_contract()
        .map(|_| ())
        .map_err(|error| {
            format!(
                "model {} has an invalid persisted serving contract: {error}",
                version.model_version_id
            )
        })
}

/// Validate an artifact selected by an activated champion route.
pub fn active_load_ok(version: &ModelVersionInfo) -> Result<(), String> {
    model_contract_ok(version)
}

/// Validate an artifact selected by an activated shadow route.
pub fn shadow_load_ok(version: &ModelVersionInfo) -> Result<(), String> {
    model_contract_ok(version)
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::types::ContentHash;

    use super::{active_load_ok, shadow_load_ok};
    use crate::service::model_serving_test_support::{model_artifact, model_version};

    #[test]
    fn route_rejects_contract_drift() {
        let mut active = model_version(&model_artifact(None));
        active.serving_contract_hash = ContentHash::from_bytes([7; 32]);
        let active_error = active_load_ok(&active).expect_err("active drift must fail");
        assert!(active_error.contains("invalid persisted serving contract"));

        let mut shadow = model_version(&model_artifact(None));
        shadow.serving_contract_hash = ContentHash::from_bytes([8; 32]);
        let shadow_error = shadow_load_ok(&shadow).expect_err("shadow drift must fail");
        assert!(shadow_error.contains("invalid persisted serving contract"));
    }
}
