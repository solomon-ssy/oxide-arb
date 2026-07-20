use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        FitTradePolicyRequest, JobProgressSink, Paginated, TradePolicyArtifactInfo,
        TradePolicyAuditListQuery, TradePolicyEvidenceDownloadView,
        TradePolicyEvidenceRowListQuery, TradePolicyEvidenceRowView,
        TradePolicyFitPreflightRequest, TradePolicyFitPreflightView,
        TradePolicyGovernanceAuditInfo, TradePolicyListQuery,
        TradePolicySourceSliceObjectListQuery, TradePolicySourceSliceObjectView,
        TradePolicySourceSliceView, TradePolicyTrialAttemptInfo, TradePolicyTrialListQuery,
        TradePolicyValidationListQuery, TradePolicyValidationRowInfo,
        TradePolicyValidationRowListQuery, TradePolicyValidationRunInfo,
    },
    enums::quant::TradePolicyStatus,
    types::{
        ResearchJobId, ResearchProfileArtifact, ResearchProfileId, TradePolicyArtifactId,
        TradePolicyEvidenceObjectKind, TradePolicyValidationRunId, TrainingDatasetId, UserId,
    },
};

#[async_trait]
pub trait TradePolicyPort: Send + Sync {
    fn list_profiles(&self) -> QuantResult<Vec<ResearchProfileArtifact>>;

    fn find_profile(
        &self,
        id: &ResearchProfileId,
        version: u32,
    ) -> QuantResult<Option<ResearchProfileArtifact>>;

    async fn preflight(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView>;

    async fn fit(
        &self,
        fit_job_id: &ResearchJobId,
        training_dataset_id: &TrainingDatasetId,
        request: FitTradePolicyRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo>;

    /// Independently verify frozen source/dataset/evidence rows, then perform
    /// the Draft → Validated CAS transition with its governance audit.
    async fn validate(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        artifact_id: &TradePolicyArtifactId,
        actor_id: UserId,
        reason: String,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo>;

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>>;

    async fn source_slice(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicySourceSliceView>>;

    async fn page_source_slice_objects(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicySourceSliceObjectListQuery,
    ) -> QuantResult<Option<Paginated<TradePolicySourceSliceObjectView>>>;

    async fn evidence_download(
        &self,
        artifact_id: &TradePolicyArtifactId,
        kind: TradePolicyEvidenceObjectKind,
    ) -> QuantResult<Option<TradePolicyEvidenceDownloadView>>;

    /// Page rows from one immutable evidence object after verifying its bundle
    /// identity, byte/row-chain hashes, trial-ledger binding, and typed schema.
    async fn page_evidence_rows(
        &self,
        artifact_id: &TradePolicyArtifactId,
        kind: TradePolicyEvidenceObjectKind,
        query: TradePolicyEvidenceRowListQuery,
    ) -> QuantResult<Option<Paginated<TradePolicyEvidenceRowView>>>;

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>>;

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> QuantResult<Paginated<TradePolicyGovernanceAuditInfo>>;

    /// Page the immutable candidate/fold/path attempts for one fit job.
    async fn page_trials(
        &self,
        fit_job_id: &ResearchJobId,
        query: TradePolicyTrialListQuery,
    ) -> QuantResult<Paginated<TradePolicyTrialAttemptInfo>>;

    async fn find_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
    ) -> QuantResult<Option<TradePolicyValidationRunInfo>>;

    async fn page_validations(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyValidationListQuery,
    ) -> QuantResult<Paginated<TradePolicyValidationRunInfo>>;

    async fn page_validation_rows(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        query: TradePolicyValidationRowListQuery,
    ) -> QuantResult<Paginated<TradePolicyValidationRowInfo>>;

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        target: TradePolicyStatus,
        actor_id: UserId,
        reason: String,
    ) -> QuantResult<TradePolicyArtifactInfo>;
}
