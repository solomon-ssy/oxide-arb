//! Core implementation of the web-facing [`ReplayPort`].
//!
//! A web replay is a `Backfill`-triggered materialization run. Enqueuing only
//! seals a manifest and writes a `Queued` row (the materialization execute
//! worker later runs it), so this path performs no heavy resolver work and
//! never blocks the request. The simulation / quality-gate defaults mirror the
//! scheduler's production policy so web replays are consistent with scheduled
//! runs.

use async_trait::async_trait;
use chrono::Utc;
use oxide_arb_control::materialization::{ManifestBuilder, ManifestBuilderInput};
use oxide_arb_models::{
    domain::{
        ReplayEnqueueRequest, ReplayEnqueueResult, ReplayPort, RuntimeControlError,
        control_factor::{
            DataRequirements, EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
            NewControlFactorMaterializationRun, QualityGatePolicy, RequiredInputDomain, RunTrigger,
            RuntimeConfigRef, SimulationConfig,
        },
    },
    enums::control_factor::{MaterializationOutputPolicy, MaterializationRunKind},
};
use oxide_arb_repository::traits::ControlFactorRepository;
use std::sync::Arc;
use uuid::Uuid;

/// Live replay enqueue port backing `POST /replay`.
pub struct CoreReplay {
    control_factors: Arc<dyn ControlFactorRepository>,
    code_git_sha: String,
    created_by: String,
}

impl CoreReplay {
    #[must_use]
    pub fn new(control_factors: Arc<dyn ControlFactorRepository>) -> Self {
        Self {
            control_factors,
            code_git_sha: option_env!("GIT_SHA").unwrap_or("unknown").to_owned(),
            created_by: "web-replay".to_owned(),
        }
    }

    /// Conservative input set for an operator replay (settlement truth not
    /// required, so recent-window replays are not blocked).
    fn data_requirements() -> DataRequirements {
        let inputs = vec![
            RequiredInputDomain::TokenMapping,
            RequiredInputDomain::RuntimeConfig,
            RequiredInputDomain::CalibrationSnapshots,
            RequiredInputDomain::FeeSchedule,
            RequiredInputDomain::Trades,
            RequiredInputDomain::Positions,
        ];
        DataRequirements {
            required_inputs: inputs.clone(),
            production_required_inputs: inputs,
            min_l2_coverage_ratio: None,
            require_settlement_truth: false,
        }
    }
}

#[async_trait]
impl ReplayPort for CoreReplay {
    async fn enqueue(
        &self,
        request: ReplayEnqueueRequest,
    ) -> Result<ReplayEnqueueResult, RuntimeControlError> {
        let interval = request.to - request.from;
        if interval <= chrono::Duration::zero() {
            return Err(RuntimeControlError::Precondition(
                "replay window must have from < to".to_owned(),
            ));
        }
        let request_id = Uuid::now_v7().to_string();
        let sealed = ManifestBuilder::new(ManifestBuilderInput {
            run_kind: MaterializationRunKind::Backfill,
            trigger: RunTrigger::Backfill {
                request_id,
                reason: request.reason,
                force_new_run: request.force_new_run,
            },
            // window = [trigger_time - source_delay - interval, trigger_time];
            // with source_delay=0 and trigger_time=to this is exactly [from, to].
            trigger_time: request.to,
            interval,
            source_delay_secs: 0,
            markets: request.markets,
            replay_account_scope: request.replay_account_scope,
            requested_factor_types: request.requested_factor_types,
            data_requirements: Self::data_requirements(),
            runtime_config_ref: RuntimeConfigRef::ActiveAt { at: request.to },
            simulation_config: SimulationConfig::production_default(),
            quality_gate_policy: QualityGatePolicy::default(),
            output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
            code_git_sha: self.code_git_sha.clone(),
            created_by: self.created_by.clone(),
            created_at: Utc::now(),
        })
        .build()
        .map_err(|error| RuntimeControlError::Precondition(error.to_string()))?;

        let new_run = NewControlFactorMaterializationRun::try_from(&sealed)
            .map_err(|error| RuntimeControlError::Precondition(error.to_string()))?;
        let outcome = self
            .control_factors
            .enqueue_materialization_run(
                new_run,
                EnqueueMaterializationRunOptions {
                    force_new_run: request.force_new_run,
                    reason: None,
                },
            )
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))?;

        let (run, created) = match outcome {
            EnqueueMaterializationRunOutcome::Created(run) => (run, true),
            EnqueueMaterializationRunOutcome::DuplicateActive(run)
            | EnqueueMaterializationRunOutcome::DuplicateCompleted(run) => (run, false),
        };
        Ok(ReplayEnqueueResult { run, created })
    }
}
