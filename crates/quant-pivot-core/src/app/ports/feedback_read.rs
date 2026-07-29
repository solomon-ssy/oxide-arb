//! Authoritative feedback-cycle reads for the operator workbench.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
};
use quant_pivot_models::{
    domain::{
        api::{
            DriftReportListQuery, DriftReportView, FeedbackCohortCountsView,
            FeedbackCoverageDecision, FeedbackCoverageView, FeedbackCycleDetailView,
            FeedbackCycleListQuery, FeedbackCycleView, FeedbackEvaluationUseView,
            FeedbackOverviewView, FeedbackProfileOverviewView, FeedbackQueueView,
            FeedbackReadinessView, FeedbackStageEventView,
        },
        pagination::{PageRequest, Paginated},
        ports::{FeedbackReadPort, ResearchReadinessPort},
        quant::{FeedbackCycleInfo, FeedbackStageEventInfo},
    },
    enums::quant::{FeedbackStage, FeedbackStageEventKind},
    types::{
        ArtifactUri, ContentHash, FeedbackCycleId, ResearchProfileArtifact,
        builtin_research_profiles,
    },
};
use quant_pivot_repository::traits::FeedbackCycleRepository;
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback::{CoverageGateOutcome, FeedbackCoverageArtifact, FeedbackCoverageCodec},
};

/// Read-only feedback dependencies.
pub struct CoreFeedbackReadPort {
    cycles: Arc<dyn FeedbackCycleRepository>,
    readiness: Arc<dyn ResearchReadinessPort>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl CoreFeedbackReadPort {
    #[must_use]
    pub const fn new(
        cycles: Arc<dyn FeedbackCycleRepository>,
        readiness: Arc<dyn ResearchReadinessPort>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            cycles,
            readiness,
            artifacts,
        }
    }

    async fn load_coverage(
        &self,
        cycle: &FeedbackCycleInfo,
        timeline: &[FeedbackStageEventInfo],
    ) -> QuantResult<Option<FeedbackCoverageView>> {
        let Some(event) = timeline.iter().rev().find(|event| {
            event.stage == FeedbackStage::Coverage
                && event.event_kind == FeedbackStageEventKind::Succeeded
        }) else {
            return Ok(None);
        };
        let (Some(uri), Some(expected_hash)) = (event.evidence_uri.as_ref(), event.evidence_hash)
        else {
            return Err(FeedbackError::InvalidStageEvent {
                detail: format!(
                    "successful coverage event {} has no complete artifact evidence",
                    event.feedback_stage_event_id
                ),
            }
            .into());
        };
        let bytes = self.artifacts.get(uri).await?;
        let actual_hash = FeedbackCoverageCodec::bytes_hash(&bytes);
        if actual_hash != expected_hash {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash.to_string(),
            }
            .into());
        }
        let artifact = FeedbackCoverageCodec::decode(&bytes)?;
        Self::validate_coverage(cycle, &artifact)?;
        Ok(Some(Self::coverage_view(
            &artifact,
            uri.clone(),
            expected_hash,
        )))
    }

    fn validate_coverage(
        cycle: &FeedbackCycleInfo,
        artifact: &FeedbackCoverageArtifact,
    ) -> QuantResult<()> {
        let cycle_matches = (artifact.feedback_cycle_id, artifact.cycle_idempotency_hash)
            == (cycle.feedback_cycle_id, cycle.idempotency_hash);
        let profile_matches = (&artifact.profile_ref, artifact.feedback_policy_hash)
            == (&cycle.profile_ref, cycle.feedback_policy_hash);
        let champion_matches = (
            artifact.champion_model_version_id,
            artifact.champion_serving_contract_hash,
        ) == (
            cycle.champion_model_version_id,
            cycle.champion_serving_contract_hash,
        );
        if !cycle_matches || !profile_matches || !champion_matches {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: format!(
                    "coverage artifact {} differs from feedback cycle {}",
                    artifact.artifact_id, cycle.feedback_cycle_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn coverage_view(
        artifact: &FeedbackCoverageArtifact,
        artifact_uri: ArtifactUri,
        artifact_hash: ContentHash,
    ) -> FeedbackCoverageView {
        let (decision, reason_code, coverage) = match artifact.gate_outcome {
            CoverageGateOutcome::Advance { coverage } => {
                (FeedbackCoverageDecision::Advance, None, coverage)
            }
            CoverageGateOutcome::NoAction { reason, coverage } => (
                FeedbackCoverageDecision::NoAction,
                Some(reason.as_str().to_owned()),
                coverage,
            ),
        };
        FeedbackCoverageView {
            artifact_id: artifact.artifact_id,
            artifact_uri,
            artifact_hash,
            evaluation_window_start: artifact.evaluation_window.window_start(),
            label_cutoff: artifact.evaluation_window.cutoff(),
            policy_evaluation_count: artifact.gate_input.policy_evaluation_count,
            mature_label_count: artifact.gate_input.mature_label_count,
            new_mature_label_count: artifact.gate_input.new_mature_label_count,
            minimum_mature_labels: artifact.gate_input.minimum_mature_labels,
            minimum_new_mature_labels: artifact.gate_input.minimum_new_mature_labels,
            minimum_coverage: artifact.gate_input.minimum_coverage.normalize().to_string(),
            coverage: coverage.normalize().to_string(),
            decision,
            reason_code,
            model_learning: FeedbackCohortCountsView::from(&artifact.cohorts.model_learning),
            execution_learning: FeedbackCohortCountsView::from(
                &artifact.cohorts.execution_learning,
            ),
            policy_evaluation: FeedbackCohortCountsView::from(&artifact.cohorts.policy_evaluation),
        }
    }

    async fn profile_overview(
        &self,
        profile: ResearchProfileArtifact,
    ) -> QuantResult<FeedbackProfileOverviewView> {
        let policy_hash = profile
            .spec
            .feedback_policy
            .content_hash()
            .map_err(|error| QuantError::config(error.to_string()))?;
        let page = self
            .cycles
            .page_cycles(FeedbackCycleListQuery {
                profile_id: Some(profile.profile_ref.id.clone()),
                status: None,
                trigger_family: None,
                page: PageRequest::new(1, 1),
            })
            .await?;
        let latest = page.items.into_iter().next();
        let latest_coverage = if let Some(cycle) = latest.as_ref() {
            let timeline = self
                .cycles
                .list_stage_events(&cycle.feedback_cycle_id)
                .await?;
            self.load_coverage(cycle, &timeline).await?
        } else {
            None
        };
        let policy = profile.spec.feedback_policy;
        Ok(FeedbackProfileOverviewView {
            profile_ref: profile.profile_ref,
            category: profile.spec.category,
            activation_eligibility: profile.spec.activation_eligibility,
            feedback_policy_hash: policy_hash,
            evaluation_window_days: policy.evaluation_window_days,
            feedback_cadence_secs: policy.feedback_cadence_secs,
            minimum_mature_labels: policy.minimum_mature_labels,
            minimum_new_mature_labels: policy.minimum_new_mature_labels,
            retraining_cooldown_secs: policy.retraining_cooldown_secs,
            minimum_coverage: policy.minimum_coverage.normalize().to_string(),
            latest_cycle: latest.map(FeedbackCycleView::from),
            latest_coverage,
        })
    }
}

#[async_trait]
impl FeedbackReadPort for CoreFeedbackReadPort {
    async fn overview(&self) -> QuantResult<FeedbackOverviewView> {
        let generated_at = self.cycles.database_time().await?;
        let queue = self.cycles.queue_snapshot().await?;
        let revision = self.cycles.latest_outbox_revision().await?;
        let readiness = self
            .readiness
            .snapshot()
            .await?
            .map(|snapshot| FeedbackReadinessView {
                observed_at: snapshot.observed_at,
                required_history_days: snapshot.required_history_days,
                observed_history_days: snapshot.observed_history_days,
                retention_ready: snapshot.retention_ready,
                latency_ready: snapshot.latency_ready,
            });
        let profiles = builtin_research_profiles().map_err(QuantError::config)?;
        let mut profile_views = Vec::with_capacity(profiles.len());
        for profile in profiles {
            profile_views.push(self.profile_overview(profile).await?);
        }
        Ok(FeedbackOverviewView {
            revision,
            generated_at,
            queue: FeedbackQueueView::from(queue),
            readiness,
            profiles: profile_views,
        })
    }

    async fn list_cycles(
        &self,
        query: FeedbackCycleListQuery,
    ) -> QuantResult<Paginated<FeedbackCycleView>> {
        self.cycles
            .page_cycles(query)
            .await
            .map_err(QuantError::from)
            .map(|page| page.map(FeedbackCycleView::from))
    }

    async fn list_drift_reports(
        &self,
        query: DriftReportListQuery,
    ) -> QuantResult<Paginated<DriftReportView>> {
        self.cycles
            .page_drift_reports(query)
            .await
            .map_err(QuantError::from)
            .map(|page| page.map(DriftReportView::from))
    }

    async fn get_cycle(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> QuantResult<Option<FeedbackCycleDetailView>> {
        let Some(cycle) = self.cycles.find_cycle(cycle_id).await? else {
            return Ok(None);
        };
        let (timeline, drift_reports, evaluation_uses) = tokio::try_join!(
            self.cycles.list_stage_events(cycle_id),
            self.cycles.list_drift_reports(cycle_id),
            self.cycles.list_evaluation_uses(cycle_id),
        )?;
        let coverage = self.load_coverage(&cycle, &timeline).await?;
        Ok(Some(FeedbackCycleDetailView {
            cycle: FeedbackCycleView::from(cycle),
            timeline: timeline
                .into_iter()
                .map(FeedbackStageEventView::from)
                .collect(),
            coverage,
            drift_reports: drift_reports.into_iter().map(Into::into).collect(),
            evaluation_uses: evaluation_uses
                .into_iter()
                .map(FeedbackEvaluationUseView::from)
                .collect(),
        }))
    }
}
