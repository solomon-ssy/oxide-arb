use chrono::{DateTime, Duration, Utc};
use oxide_arb_models::{
    domain::control_factor::{
        DataRequirements, MarketFilterSpec, MaterializationRunManifest,
        NewControlFactorMaterializationRun, QualityGatePolicyRef, ReplayAccountScope, RunTrigger,
        RuntimeConfigRef, SimulationConfig, TimeWindowSpec,
    },
    enums::control_factor::{
        ControlFactorType, MaterializationErrorCode, MaterializationOutputPolicy,
        MaterializationRunKind, MaterializationRunStatus,
    },
    types::MaterializationRunId,
};

use crate::materialization::{
    ArtifactHasher, DedupeKeyHasher, ManifestHasher, MaterializationError, MaterializationResult,
};

pub struct ManifestBuilder {
    run_kind: MaterializationRunKind,
    trigger: RunTrigger,
    trigger_time: DateTime<Utc>,
    interval: Duration,
    source_delay_secs: u64,
    markets: MarketFilterSpec,
    replay_account_scope: Option<ReplayAccountScope>,
    requested_factor_types: Vec<ControlFactorType>,
    data_requirements: DataRequirements,
    runtime_config_ref: RuntimeConfigRef,
    simulation_config: SimulationConfig,
    quality_gate_policy: QualityGatePolicyRef,
    output_policy: MaterializationOutputPolicy,
    code_git_sha: String,
    created_by: String,
    created_at: DateTime<Utc>,
}

pub struct ManifestBuilderInput {
    pub run_kind: MaterializationRunKind,
    pub trigger: RunTrigger,
    pub trigger_time: DateTime<Utc>,
    pub interval: Duration,
    pub source_delay_secs: u64,
    pub markets: MarketFilterSpec,
    pub replay_account_scope: Option<ReplayAccountScope>,
    pub requested_factor_types: Vec<ControlFactorType>,
    pub data_requirements: DataRequirements,
    pub runtime_config_ref: RuntimeConfigRef,
    pub simulation_config: SimulationConfig,
    pub quality_gate_policy: QualityGatePolicyRef,
    pub output_policy: MaterializationOutputPolicy,
    pub code_git_sha: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

impl ManifestBuilder {
    #[must_use]
    pub fn new(input: ManifestBuilderInput) -> Self {
        Self {
            run_kind: input.run_kind,
            trigger: input.trigger,
            trigger_time: input.trigger_time,
            interval: input.interval,
            source_delay_secs: input.source_delay_secs,
            markets: input.markets,
            replay_account_scope: input.replay_account_scope,
            requested_factor_types: input.requested_factor_types,
            data_requirements: input.data_requirements,
            runtime_config_ref: input.runtime_config_ref,
            simulation_config: input.simulation_config,
            quality_gate_policy: input.quality_gate_policy,
            output_policy: input.output_policy,
            code_git_sha: input.code_git_sha,
            created_by: input.created_by,
            created_at: input.created_at,
        }
    }

    pub fn build(self) -> MaterializationResult<SealedMaterializationManifest> {
        let window = self.window()?;
        let manifest = MaterializationRunManifest {
            run_id: MaterializationRunId::new_v7(),
            run_kind: self.run_kind,
            trigger: self.trigger,
            window,
            source_delay_secs: self.source_delay_secs,
            markets: self.markets,
            replay_account_scope: self.replay_account_scope,
            requested_factor_types: self.requested_factor_types,
            data_requirements: self.data_requirements,
            runtime_config_ref: self.runtime_config_ref,
            simulation_config: self.simulation_config,
            quality_gate_policy: self.quality_gate_policy,
            output_policy: self.output_policy,
            code_git_sha: self.code_git_sha,
            created_by: self.created_by,
            created_at: self.created_at,
        };
        if !manifest.window.is_valid() {
            return Err(MaterializationError::stable(
                MaterializationErrorCode::RunInvalidTransition.as_str(),
                "materialization window must have from < to",
            ));
        }
        let manifest_hash = ManifestHasher::compute(&manifest)?;
        let run_dedupe_key = if manifest.trigger.force_new_run() {
            None
        } else {
            Some(DedupeKeyHasher::compute(&manifest)?)
        };
        Ok(SealedMaterializationManifest {
            manifest,
            manifest_hash,
            run_dedupe_key,
        })
    }

    fn window(&self) -> MaterializationResult<TimeWindowSpec> {
        if self.interval <= Duration::zero() {
            return Err(MaterializationError::stable(
                MaterializationErrorCode::RunInvalidTransition.as_str(),
                "materialization interval must be positive",
            ));
        }
        let source_delay =
            Duration::seconds(i64::try_from(self.source_delay_secs).map_err(|error| {
                MaterializationError::stable(
                    MaterializationErrorCode::RunInvalidTransition.as_str(),
                    error.to_string(),
                )
            })?);
        let to = self.trigger_time - source_delay;
        Ok(TimeWindowSpec::new(to - self.interval, to))
    }
}

#[derive(Debug, Clone)]
pub struct SealedMaterializationManifest {
    pub manifest: MaterializationRunManifest,
    pub manifest_hash: String,
    pub run_dedupe_key: Option<String>,
}

impl TryFrom<&SealedMaterializationManifest> for NewControlFactorMaterializationRun {
    type Error = MaterializationError;

    fn try_from(sealed: &SealedMaterializationManifest) -> Result<Self, Self::Error> {
        let source_delay_secs =
            i64::try_from(sealed.manifest.source_delay_secs).map_err(|error| {
                MaterializationError::stable(
                    MaterializationErrorCode::RunInvalidTransition.as_str(),
                    error.to_string(),
                )
            })?;
        Ok(Self {
            materialization_run_id: sealed.manifest.run_id.clone(),
            run_dedupe_key: sealed.run_dedupe_key.clone(),
            run_kind: sealed.manifest.run_kind,
            trigger_type: sealed.manifest.trigger_type(),
            trigger_ref: sealed.manifest.trigger.trigger_ref().map(str::to_owned),
            status: MaterializationRunStatus::Queued,
            window_from: sealed.manifest.window.from,
            window_to: sealed.manifest.window.to,
            source_delay_secs,
            market_filter: serde_json::to_value(&sealed.manifest.markets)
                .map_err(|error| MaterializationError::Codec(error.to_string()))?,
            requested_factor_types: serde_json::to_value(&sealed.manifest.requested_factor_types)
                .map_err(|error| {
                MaterializationError::Codec(error.to_string())
            })?,
            data_requirements: serde_json::to_value(&sealed.manifest.data_requirements)
                .map_err(|error| MaterializationError::Codec(error.to_string()))?,
            runtime_config_ref: serde_json::to_value(&sealed.manifest.runtime_config_ref)
                .map_err(|error| MaterializationError::Codec(error.to_string()))?,
            simulation_config_hash: ArtifactHasher::compute(&sealed.manifest.simulation_config)?.0,
            quality_gate_policy_hash: sealed.manifest.quality_gate_policy.policy_hash.clone(),
            output_policy: sealed.manifest.output_policy,
            manifest: serde_json::to_value(&sealed.manifest)
                .map_err(|error| MaterializationError::Codec(error.to_string()))?,
            manifest_hash: sealed.manifest_hash.clone(),
            report: serde_json::json!({}),
            code_git_sha: sealed.manifest.code_git_sha.clone(),
            created_by: sealed.manifest.created_by.clone(),
            started_at: None,
            finished_at: None,
            failure_code: None,
            failure_detail: None,
            report_uri: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use oxide_arb_models::{
        domain::control_factor::{
            DataRequirements, MarketFilterSpec, QualityGatePolicyRef, RequiredInputDomain,
            RunTrigger, RuntimeConfigRef, SimulationConfig,
        },
        enums::control_factor::{MaterializationOutputPolicy, MaterializationRunKind},
        types::RuntimeConfigVersionId,
    };

    use crate::materialization::{ManifestBuilder, ManifestBuilderInput};

    #[test]
    fn source_delay_window_matches_phase_52_contract() {
        let trigger_time = Utc
            .with_ymd_and_hms(2026, 6, 3, 8, 0, 0)
            .single()
            .expect("fixed timestamp");
        let sealed = ManifestBuilder::new(ManifestBuilderInput {
            run_kind: MaterializationRunKind::Scheduled,
            trigger: RunTrigger::Scheduled {
                schedule_id: "hourly".into(),
            },
            trigger_time,
            interval: Duration::hours(1),
            source_delay_secs: 900,
            markets: MarketFilterSpec::default(),
            replay_account_scope: None,
            requested_factor_types: Vec::new(),
            data_requirements: DataRequirements {
                required_inputs: vec![RequiredInputDomain::RuntimeConfig],
                production_required_inputs: vec![RequiredInputDomain::RuntimeConfig],
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
            created_at: trigger_time,
        })
        .build()
        .expect("manifest builds");
        assert_eq!(
            sealed.manifest.window.to,
            trigger_time - Duration::minutes(15)
        );
        assert_eq!(
            sealed.manifest.window.from,
            trigger_time - Duration::minutes(75)
        );
    }
}
