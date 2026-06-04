use oxide_arb_models::{
    domain::control_factor::{
        ArtifactHash, MaterializationRunManifest, QualityGatePolicyRef, ReplayAccountScope,
        RuntimeConfigRef, SimulationConfig,
    },
    enums::{
        common::MarketCategory,
        control_factor::{ControlFactorType, MaterializationRunKind},
    },
    types::{EventId, MarketId, TokenId},
};
use serde::Serialize;

use crate::materialization::{MaterializationError, MaterializationResult};

pub struct ManifestHasher;
pub struct DedupeKeyHasher;
pub struct ArtifactHasher;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Blake3Digest(String);

#[derive(Serialize)]
struct DedupeCanonicalInput<'a> {
    run_kind: MaterializationRunKind,
    window_from: chrono::DateTime<chrono::Utc>,
    window_to: chrono::DateTime<chrono::Utc>,
    source_delay_secs: u64,
    market_ids: Vec<&'a MarketId>,
    event_ids: Vec<&'a EventId>,
    token_ids: Vec<&'a TokenId>,
    categories: Vec<MarketCategory>,
    replay_account_scope: &'a Option<ReplayAccountScope>,
    requested_factor_types: Vec<&'a ControlFactorType>,
    runtime_config_ref: &'a RuntimeConfigRef,
    simulation_config: &'a SimulationConfig,
    quality_gate_policy: &'a QualityGatePolicyRef,
    code_git_sha: &'a str,
}

impl Blake3Digest {
    fn from_canonical_json<T: Serialize>(value: &T) -> MaterializationResult<Self> {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        Ok(Self(format!(
            "blake3:{}",
            hex::encode(blake3::hash(&bytes).as_bytes())
        )))
    }

    fn into_string(self) -> String {
        self.0
    }
}

impl ManifestHasher {
    pub fn compute(manifest: &MaterializationRunManifest) -> MaterializationResult<String> {
        Blake3Digest::from_canonical_json(manifest).map(Blake3Digest::into_string)
    }
}

impl DedupeKeyHasher {
    pub fn compute(manifest: &MaterializationRunManifest) -> MaterializationResult<String> {
        let mut market_ids = manifest.markets.market_ids.iter().collect::<Vec<_>>();
        let mut event_ids = manifest.markets.event_ids.iter().collect::<Vec<_>>();
        let mut token_ids = manifest.markets.token_ids.iter().collect::<Vec<_>>();
        let mut categories = manifest.markets.categories.clone();
        let mut requested_factor_types = manifest.requested_factor_types.iter().collect::<Vec<_>>();
        market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        event_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        token_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        categories.sort_unstable();
        requested_factor_types.sort_by_key(|factor_type| factor_type.as_str());
        let input = DedupeCanonicalInput {
            run_kind: manifest.run_kind,
            window_from: manifest.window.from,
            window_to: manifest.window.to,
            source_delay_secs: manifest.source_delay_secs,
            market_ids,
            event_ids,
            token_ids,
            categories,
            replay_account_scope: &manifest.replay_account_scope,
            requested_factor_types,
            runtime_config_ref: &manifest.runtime_config_ref,
            simulation_config: &manifest.simulation_config,
            quality_gate_policy: &manifest.quality_gate_policy,
            code_git_sha: manifest.code_git_sha.as_str(),
        };
        Blake3Digest::from_canonical_json(&input).map(Blake3Digest::into_string)
    }
}

impl ArtifactHasher {
    pub fn compute<T: Serialize>(artifact: &T) -> MaterializationResult<ArtifactHash> {
        Blake3Digest::from_canonical_json(artifact).map(|digest| ArtifactHash(digest.into_string()))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use oxide_arb_models::{
        domain::control_factor::{
            DataRequirements, MarketFilterSpec, MaterializationRunManifest, QualityGatePolicyRef,
            RunTrigger, RuntimeConfigRef, SimulationConfig, TimeWindowSpec,
        },
        enums::control_factor::{
            ControlFactorType, MaterializationOutputPolicy, MaterializationRunKind,
        },
        types::{MarketId, MaterializationRunId, RuntimeConfigVersionId},
    };

    use crate::materialization::{DedupeKeyHasher, ManifestHasher};

    fn sample_manifest(factor_types: Vec<ControlFactorType>) -> MaterializationRunManifest {
        let now = Utc
            .with_ymd_and_hms(2026, 6, 3, 8, 0, 0)
            .single()
            .expect("fixed timestamp");
        MaterializationRunManifest {
            run_id: MaterializationRunId::new("cfmr_test"),
            run_kind: MaterializationRunKind::Scheduled,
            trigger: RunTrigger::Scheduled {
                schedule_id: "hourly".into(),
            },
            window: TimeWindowSpec::new(now - chrono::Duration::hours(1), now),
            source_delay_secs: 900,
            markets: MarketFilterSpec {
                market_ids: vec![MarketId::new("m2"), MarketId::new("m1")],
                event_ids: Vec::new(),
                token_ids: Vec::new(),
                categories: Vec::new(),
            },
            replay_account_scope: None,
            requested_factor_types: factor_types,
            data_requirements: DataRequirements {
                required_inputs: Vec::new(),
                production_required_inputs: Vec::new(),
                min_l2_coverage_ratio: None,
                require_settlement_truth: false,
                require_token_balances: false,
            },
            runtime_config_ref: RuntimeConfigRef::Version {
                version_id: RuntimeConfigVersionId::new("rcv_test"),
                config_hash: "sha256:cfg".into(),
            },
            simulation_config: SimulationConfig::production_default(),
            quality_gate_policy: QualityGatePolicyRef {
                policy_hash: "blake3:gate".into(),
            },
            output_policy: MaterializationOutputPolicy::NoFactorOutput,
            code_git_sha: "abc".into(),
            created_by: "test".into(),
            created_at: now,
        }
    }

    #[test]
    fn dedupe_key_is_order_independent_for_factor_types() {
        let left = sample_manifest(vec![
            ControlFactorType::BucketRisk,
            ControlFactorType::ExecutionQuality,
        ]);
        let right = sample_manifest(vec![
            ControlFactorType::ExecutionQuality,
            ControlFactorType::BucketRisk,
        ]);
        assert_eq!(
            DedupeKeyHasher::compute(&left).expect("left hash"),
            DedupeKeyHasher::compute(&right).expect("right hash")
        );
    }

    #[test]
    fn manifest_hash_includes_run_id_but_dedupe_does_not() {
        let left = sample_manifest(vec![ControlFactorType::BucketRisk]);
        let mut right = left.clone();
        right.run_id = MaterializationRunId::new("cfmr_other");
        assert_ne!(
            ManifestHasher::compute(&left).expect("left manifest hash"),
            ManifestHasher::compute(&right).expect("right manifest hash")
        );
        assert_eq!(
            DedupeKeyHasher::compute(&left).expect("left dedupe"),
            DedupeKeyHasher::compute(&right).expect("right dedupe")
        );
    }
}
