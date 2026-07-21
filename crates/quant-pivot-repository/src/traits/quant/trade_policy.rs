use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::{
            TradePolicyAuditListQuery, TradePolicyListQuery, TradePolicyValidationListQuery,
            TradePolicyValidationRowListQuery,
        },
        pagination::Paginated,
        quant::{
            CompleteTradePolicyValidation, FailTradePolicyValidation, NewTradePolicyArtifact,
            NewTradePolicyGovernanceAudit, NewTradePolicyTrialAttempt, NewTradePolicyValidationRow,
            NewTradePolicyValidationRun, TradePolicyArtifactInfo, TradePolicyGovernanceAuditInfo,
            TradePolicyTrialAttemptInfo, TradePolicyValidationRowInfo,
            TradePolicyValidationRunInfo,
        },
    },
    enums::quant::TradePolicyStatus,
    types::{ResearchJobId, TradePolicyArtifactId, TradePolicyValidationRunId},
};

#[async_trait::async_trait]
pub trait TradePolicyRepository: Send + Sync {
    async fn insert(
        &self,
        artifact: NewTradePolicyArtifact,
    ) -> Result<TradePolicyArtifactInfo, StorageError>;

    /// Append one terminal candidate/fold/path attempt. Replaying the same id
    /// is idempotent only when every immutable field and row hash match.
    async fn append_trial_attempt(
        &self,
        attempt: NewTradePolicyTrialAttempt,
    ) -> Result<TradePolicyTrialAttemptInfo, StorageError>;

    /// Stable ordered ledger prefix used to seal and independently re-hash one
    /// fit. `cutoff` is inclusive and prevents later rows from mutating a Draft.
    async fn list_trial_attempts(
        &self,
        fit_job_id: &ResearchJobId,
        cutoff: Option<DateTime<Utc>>,
    ) -> Result<Vec<TradePolicyTrialAttemptInfo>, StorageError>;

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyArtifactInfo>, StorageError>;

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> Result<Paginated<TradePolicyArtifactInfo>, StorageError>;

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> Result<Paginated<TradePolicyGovernanceAuditInfo>, StorageError>;

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        expected: TradePolicyStatus,
        target: TradePolicyStatus,
        audit: NewTradePolicyGovernanceAudit,
    ) -> Result<TradePolicyArtifactInfo, StorageError>;

    async fn begin_validation(
        &self,
        run: NewTradePolicyValidationRun,
    ) -> Result<TradePolicyValidationRunInfo, StorageError>;

    async fn append_validation_rows(
        &self,
        rows: Vec<NewTradePolicyValidationRow>,
    ) -> Result<(), StorageError>;

    async fn complete_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        completion: CompleteTradePolicyValidation,
    ) -> Result<(TradePolicyValidationRunInfo, TradePolicyArtifactInfo), StorageError>;

    async fn fail_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        failure: FailTradePolicyValidation,
    ) -> Result<TradePolicyValidationRunInfo, StorageError>;

    async fn find_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
    ) -> Result<Option<TradePolicyValidationRunInfo>, StorageError>;

    async fn latest_successful_validation(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyValidationRunInfo>, StorageError>;

    async fn page_validations(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyValidationListQuery,
    ) -> Result<Paginated<TradePolicyValidationRunInfo>, StorageError>;

    async fn page_validation_rows(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        query: TradePolicyValidationRowListQuery,
    ) -> Result<Paginated<TradePolicyValidationRowInfo>, StorageError>;
}
