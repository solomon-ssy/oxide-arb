//! Immutable inputs and results for point-in-time feedback-cohort classification.

use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::quant::{
        RecommendationExecutionOutcomeContractError, RecommendationExecutionOutcomeInfo,
        RecommendationInfo, RecommendationReportInfo, RecommendationResolutionOutcomeContractError,
        RecommendationResolutionOutcomeInfo,
    },
    enums::{
        common::MarketCategory,
        quant::{
            CohortCensorReason, CohortExclusionReason, FeedbackCohort, QuantRuntimeMode,
            RecommendationExecutionNoFillReason, RecommendationExecutionTerminalState,
            RecommendationReportStatus, RecommendationResolutionKind, RecommendationStatus,
            ReportKind,
        },
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, ExecutionOrderId, FactorDefinitionId,
        FeatureVectorId, MarketId, MarketSelectionId, ModelRunId, ModelVersionId, OrderIntentId,
        PayoutRatio, RecommendationId, RecommendationReportId, ReportDataQualitySnapshotId,
        ResearchProfileRef, Shares, TokenId,
    },
};

/// Maximum recommendations materialized by one feedback page.
pub const FEEDBACK_COHORT_PAGE_LIMIT: u32 = 1_000;

/// Exact profile and time bounds frozen before a feedback scan begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "FeedbackCohortWindowDocument",
    into = "FeedbackCohortWindowDocument"
)]
pub struct FeedbackCohortWindow {
    profile_ref: ResearchProfileRef,
    window_start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
}

impl FeedbackCohortWindow {
    pub fn try_new(
        profile_ref: ResearchProfileRef,
        window_start: DateTime<Utc>,
        cutoff: DateTime<Utc>,
    ) -> Result<Self, FeedbackCohortContractError> {
        if window_start > cutoff {
            return Err(FeedbackCohortContractError::InvalidWindow {
                window_start,
                cutoff,
            });
        }
        Ok(Self {
            profile_ref,
            window_start,
            cutoff,
        })
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn window_start(&self) -> DateTime<Utc> {
        self.window_start
    }

    #[must_use]
    pub const fn cutoff(&self) -> DateTime<Utc> {
        self.cutoff
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackCohortWindowDocument {
    profile_ref: ResearchProfileRef,
    window_start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
}

impl TryFrom<FeedbackCohortWindowDocument> for FeedbackCohortWindow {
    type Error = FeedbackCohortContractError;

    fn try_from(document: FeedbackCohortWindowDocument) -> Result<Self, Self::Error> {
        Self::try_new(document.profile_ref, document.window_start, document.cutoff)
    }
}

impl From<FeedbackCohortWindow> for FeedbackCohortWindowDocument {
    fn from(window: FeedbackCohortWindow) -> Self {
        Self {
            profile_ref: window.profile_ref,
            window_start: window.window_start,
            cutoff: window.cutoff,
        }
    }
}

/// Validated recommendation/report join used by every feedback cohort.
///
/// Construction verifies the exact decision-time lineage already frozen in
/// the report and recommendation. The recommendation's database-owned creation
/// instant is the canonical candidate availability used for resumable scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackRecommendationContext {
    recommendation_id: RecommendationId,
    recommendation_report_id: RecommendationReportId,
    profile_ref: ResearchProfileRef,
    report_kind: ReportKind,
    runtime_mode: QuantRuntimeMode,
    category: MarketCategory,
    market_id: MarketId,
    token_id: TokenId,
    decision_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    model_run_id: ModelRunId,
    model_version_id: ModelVersionId,
    market_selection_id: MarketSelectionId,
    feature_vector_id: FeatureVectorId,
    factor_definition_versions: Vec<FactorDefinitionId>,
    data_quality_snapshot_id: ReportDataQualitySnapshotId,
}

impl FeedbackRecommendationContext {
    /// Validate and freeze the exact report/recommendation lineage.
    pub fn try_from_report(
        recommendation: &RecommendationInfo,
        report: &RecommendationReportInfo,
    ) -> Result<Self, FeedbackCohortContractError> {
        if recommendation.recommendation_report_id != report.recommendation_report_id {
            return Err(FeedbackCohortContractError::RecommendationReportMismatch {
                recommendation_report_id: recommendation.recommendation_report_id,
                report_id: report.recommendation_report_id,
            });
        }
        if report.profile_id != report.profile_ref.id {
            return Err(FeedbackCohortContractError::ReportProfileIdentityMismatch);
        }
        if recommendation.profile_ref != report.profile_ref {
            return Err(FeedbackCohortContractError::RecommendationProfileMismatch);
        }

        let model_run_id = report
            .model_run_id
            .ok_or(FeedbackCohortContractError::MissingReportModelRun)?;
        if recommendation.evidence_refs.model_run_id != model_run_id {
            return Err(FeedbackCohortContractError::ModelRunMismatch);
        }
        if recommendation.evidence_refs.model_version_id != report.model_version_id {
            return Err(FeedbackCohortContractError::ModelVersionMismatch);
        }
        if recommendation.evidence_refs.market_selection_id != report.market_selection_id {
            return Err(FeedbackCohortContractError::MarketSelectionMismatch);
        }
        if recommendation.evidence_refs.decision_policy_snapshot_id
            != report.decision_policy_snapshot_id
        {
            return Err(FeedbackCohortContractError::DecisionPolicySnapshotMismatch);
        }
        if recommendation.evidence_refs.data_quality_snapshot_ref
            != report.data_quality_snapshot_ref
        {
            return Err(FeedbackCohortContractError::DataQualitySnapshotMismatch);
        }
        validate_publication_timeline(recommendation, report)?;

        Ok(Self {
            recommendation_id: recommendation.recommendation_id,
            recommendation_report_id: recommendation.recommendation_report_id,
            profile_ref: recommendation.profile_ref.clone(),
            report_kind: report.report_kind,
            runtime_mode: report.runtime_mode,
            category: recommendation.identity.category,
            market_id: recommendation.market_id.clone(),
            token_id: recommendation.token_id.clone(),
            decision_at: report.decision_at,
            available_at: recommendation.created_at,
            published_at: report.published_at,
            decision_policy_snapshot_id: report.decision_policy_snapshot_id,
            model_run_id,
            model_version_id: report.model_version_id,
            market_selection_id: report.market_selection_id,
            feature_vector_id: recommendation.evidence_refs.feature_vector_id,
            factor_definition_versions: recommendation
                .evidence_refs
                .factor_definition_versions
                .clone(),
            data_quality_snapshot_id: report.data_quality_snapshot_ref,
        })
    }

    #[must_use]
    pub const fn recommendation_id(&self) -> RecommendationId {
        self.recommendation_id
    }

    #[must_use]
    pub const fn recommendation_report_id(&self) -> RecommendationReportId {
        self.recommendation_report_id
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn report_kind(&self) -> ReportKind {
        self.report_kind
    }

    #[must_use]
    pub const fn runtime_mode(&self) -> QuantRuntimeMode {
        self.runtime_mode
    }

    #[must_use]
    pub const fn category(&self) -> MarketCategory {
        self.category
    }

    #[must_use]
    pub const fn market_id(&self) -> &MarketId {
        &self.market_id
    }

    #[must_use]
    pub const fn token_id(&self) -> &TokenId {
        &self.token_id
    }

    #[must_use]
    pub const fn decision_at(&self) -> DateTime<Utc> {
        self.decision_at
    }

    #[must_use]
    pub const fn available_at(&self) -> DateTime<Utc> {
        self.available_at
    }

    #[must_use]
    pub const fn published_at(&self) -> Option<DateTime<Utc>> {
        self.published_at
    }

    #[must_use]
    pub const fn decision_policy_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.decision_policy_snapshot_id
    }

    #[must_use]
    pub const fn model_run_id(&self) -> ModelRunId {
        self.model_run_id
    }

    #[must_use]
    pub const fn model_version_id(&self) -> ModelVersionId {
        self.model_version_id
    }

    #[must_use]
    pub const fn market_selection_id(&self) -> MarketSelectionId {
        self.market_selection_id
    }

    #[must_use]
    pub const fn feature_vector_id(&self) -> FeatureVectorId {
        self.feature_vector_id
    }

    #[must_use]
    pub fn factor_definition_versions(&self) -> &[FactorDefinitionId] {
        &self.factor_definition_versions
    }

    #[must_use]
    pub const fn data_quality_snapshot_id(&self) -> ReportDataQualitySnapshotId {
        self.data_quality_snapshot_id
    }
}

fn validate_publication_timeline(
    recommendation: &RecommendationInfo,
    report: &RecommendationReportInfo,
) -> Result<(), FeedbackCohortContractError> {
    if recommendation.created_at < report.decision_at {
        return Err(FeedbackCohortContractError::InvalidRecommendationTimeline);
    }
    if report.created_at < report.decision_at {
        return Err(FeedbackCohortContractError::InvalidRecommendationTimeline);
    }
    if let Some(published_at) = report.published_at {
        if published_at < report.created_at
            || published_at < recommendation.created_at
            || report.status == RecommendationReportStatus::Prepared
            || recommendation.status == RecommendationStatus::Prepared
        {
            return Err(FeedbackCohortContractError::InvalidPublicationState {
                report_status: report.status,
                recommendation_status: recommendation.status,
            });
        }
    } else if report_status_requires_publication(report.status)
        || recommendation_status_requires_publication(recommendation.status)
    {
        return Err(FeedbackCohortContractError::InvalidPublicationState {
            report_status: report.status,
            recommendation_status: recommendation.status,
        });
    }
    Ok(())
}

const fn report_status_requires_publication(status: RecommendationReportStatus) -> bool {
    matches!(
        status,
        RecommendationReportStatus::Published
            | RecommendationReportStatus::Superseded
            | RecommendationReportStatus::Expired
    )
}

const fn recommendation_status_requires_publication(status: RecommendationStatus) -> bool {
    matches!(
        status,
        RecommendationStatus::Published
            | RecommendationStatus::Superseded
            | RecommendationStatus::Expired
            | RecommendationStatus::IntentCreated
            | RecommendationStatus::Executed
    )
}

/// Submitted entry evidence visible to an execution-learning scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum FeedbackExecutionAttempt {
    NotAttempted,
    Submitted {
        order_intent_id: OrderIntentId,
        entry_execution_order_id: ExecutionOrderId,
        submitted_at: DateTime<Utc>,
    },
}

/// Total-order key for one exact-profile recommendation scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCohortCursor {
    available_at: DateTime<Utc>,
    recommendation_id: RecommendationId,
}

impl FeedbackCohortCursor {
    #[must_use]
    pub const fn new(available_at: DateTime<Utc>, recommendation_id: RecommendationId) -> Self {
        Self {
            available_at,
            recommendation_id,
        }
    }

    #[must_use]
    pub const fn available_at(self) -> DateTime<Utc> {
        self.available_at
    }

    #[must_use]
    pub const fn recommendation_id(self) -> RecommendationId {
        self.recommendation_id
    }
}

impl PartialOrd for FeedbackCohortCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FeedbackCohortCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.available_at.cmp(&other.available_at).then_with(|| {
            self.recommendation_id
                .as_uuid()
                .cmp(&other.recommendation_id.as_uuid())
        })
    }
}

/// One bounded cohort read over an immutable profile and PIT window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackCohortPageQuery {
    cohort: FeedbackCohort,
    window: FeedbackCohortWindow,
    after: Option<FeedbackCohortCursor>,
    limit: u32,
}

impl FeedbackCohortPageQuery {
    pub fn try_new(
        cohort: FeedbackCohort,
        window: FeedbackCohortWindow,
        after: Option<FeedbackCohortCursor>,
        limit: u32,
    ) -> Result<Self, FeedbackCohortContractError> {
        if !(1..=FEEDBACK_COHORT_PAGE_LIMIT).contains(&limit) {
            return Err(FeedbackCohortContractError::InvalidPageLimit {
                actual: limit,
                maximum: FEEDBACK_COHORT_PAGE_LIMIT,
            });
        }
        if let Some(cursor) = after
            && (cursor.available_at < window.window_start || cursor.available_at > window.cutoff)
        {
            return Err(FeedbackCohortContractError::CursorOutsideWindow {
                cursor_available_at: cursor.available_at,
                window_start: window.window_start,
                cutoff: window.cutoff,
            });
        }
        Ok(Self {
            cohort,
            window,
            after,
            limit,
        })
    }

    #[must_use]
    pub const fn cohort(&self) -> FeedbackCohort {
        self.cohort
    }

    #[must_use]
    pub const fn window(&self) -> &FeedbackCohortWindow {
        &self.window
    }

    #[must_use]
    pub const fn after(&self) -> Option<FeedbackCohortCursor> {
        self.after
    }

    #[must_use]
    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

/// One validated recommendation plus only the truth plane consumed by its cohort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackCohortCandidate {
    cohort: FeedbackCohort,
    context: FeedbackRecommendationContext,
    execution_attempt: Option<FeedbackExecutionAttempt>,
    resolution_outcome: Option<RecommendationResolutionOutcomeInfo>,
    execution_outcome: Option<RecommendationExecutionOutcomeInfo>,
}

impl FeedbackCohortCandidate {
    pub fn try_new(
        cohort: FeedbackCohort,
        context: FeedbackRecommendationContext,
        execution_attempt: Option<FeedbackExecutionAttempt>,
        resolution_outcome: Option<RecommendationResolutionOutcomeInfo>,
        execution_outcome: Option<RecommendationExecutionOutcomeInfo>,
    ) -> Result<Self, FeedbackCohortContractError> {
        let plane_is_valid = match cohort {
            FeedbackCohort::ModelLearning => {
                execution_attempt.is_none() && execution_outcome.is_none()
            }
            FeedbackCohort::ExecutionLearning => {
                execution_attempt.is_some() && resolution_outcome.is_none()
            }
            FeedbackCohort::PolicyEvaluation => execution_attempt.is_some(),
        };
        if !plane_is_valid {
            return Err(FeedbackCohortContractError::InvalidCandidateTruthPlane { cohort });
        }
        Ok(Self {
            cohort,
            context,
            execution_attempt,
            resolution_outcome,
            execution_outcome,
        })
    }

    #[must_use]
    pub const fn cohort(&self) -> FeedbackCohort {
        self.cohort
    }

    #[must_use]
    pub const fn context(&self) -> &FeedbackRecommendationContext {
        &self.context
    }

    #[must_use]
    pub const fn execution_attempt(&self) -> Option<FeedbackExecutionAttempt> {
        self.execution_attempt
    }

    #[must_use]
    pub const fn resolution_outcome(&self) -> Option<&RecommendationResolutionOutcomeInfo> {
        self.resolution_outcome.as_ref()
    }

    #[must_use]
    pub const fn execution_outcome(&self) -> Option<&RecommendationExecutionOutcomeInfo> {
        self.execution_outcome.as_ref()
    }

    #[must_use]
    pub const fn cursor(&self) -> FeedbackCohortCursor {
        FeedbackCohortCursor::new(self.context.available_at, self.context.recommendation_id)
    }
}

/// One bounded page with an exclusive continuation only when another row exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackCohortPage {
    candidates: Vec<FeedbackCohortCandidate>,
    next_cursor: Option<FeedbackCohortCursor>,
}

impl FeedbackCohortPage {
    pub fn try_new(
        cohort: FeedbackCohort,
        candidates: Vec<FeedbackCohortCandidate>,
        has_more: bool,
    ) -> Result<Self, FeedbackCohortContractError> {
        if candidates
            .iter()
            .any(|candidate| candidate.cohort != cohort)
        {
            return Err(FeedbackCohortContractError::MixedCohortPage);
        }
        if !candidates
            .windows(2)
            .all(|pair| pair[0].cursor() < pair[1].cursor())
        {
            return Err(FeedbackCohortContractError::UnorderedCohortPage);
        }
        let next_cursor = if has_more {
            Some(
                candidates
                    .last()
                    .ok_or(FeedbackCohortContractError::EmptyContinuationPage)?
                    .cursor(),
            )
        } else {
            None
        };
        Ok(Self {
            candidates,
            next_cursor,
        })
    }

    #[must_use]
    pub fn candidates(&self) -> &[FeedbackCohortCandidate] {
        &self.candidates
    }

    #[must_use]
    pub const fn next_cursor(&self) -> Option<FeedbackCohortCursor> {
        self.next_cursor
    }
}

/// Minimal immutable resolution truth admitted to a cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackResolutionEvidence {
    pub resolution_kind: RecommendationResolutionKind,
    pub token_payout_ratio: PayoutRatio,
    pub available_at: DateTime<Utc>,
    pub outcome_hash: ContentHash,
}

/// Minimal immutable execution truth admitted to a cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackExecutionEvidence {
    pub order_intent_id: OrderIntentId,
    pub entry_execution_order_id: ExecutionOrderId,
    pub terminal_state: RecommendationExecutionTerminalState,
    pub no_fill_reason: Option<RecommendationExecutionNoFillReason>,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub available_at: DateTime<Utc>,
    pub outcome_hash: ContentHash,
}

/// Cohort-specific evidence carried only after eligibility succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "cohort", content = "evidence")]
pub enum FeedbackCohortEvidence {
    ModelLearning(FeedbackResolutionEvidence),
    ExecutionLearning(FeedbackExecutionEvidence),
    PolicyEvaluation {
        execution_attempt: FeedbackExecutionAttempt,
        resolution_outcome_hash: Option<ContentHash>,
        execution_outcome_hash: Option<ContentHash>,
    },
}

/// Closed classification result for one recommendation and one cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", content = "detail")]
pub enum FeedbackCohortDecision {
    Eligible(FeedbackCohortEvidence),
    Excluded(CohortExclusionReason),
    Censored(CohortCensorReason),
}

/// Corrupt or contradictory feedback input. These are never normal reason codes.
#[derive(Debug, Error)]
pub enum FeedbackCohortContractError {
    #[error("feedback window start {window_start} is later than cutoff {cutoff}")]
    InvalidWindow {
        window_start: DateTime<Utc>,
        cutoff: DateTime<Utc>,
    },
    #[error("feedback page limit must be within 1..={maximum}, got {actual}")]
    InvalidPageLimit { actual: u32, maximum: u32 },
    #[error(
        "feedback cursor availability {cursor_available_at} is outside {window_start}..={cutoff}"
    )]
    CursorOutsideWindow {
        cursor_available_at: DateTime<Utc>,
        window_start: DateTime<Utc>,
        cutoff: DateTime<Utc>,
    },
    #[error("feedback candidate contains a truth plane forbidden for {cohort:?}")]
    InvalidCandidateTruthPlane { cohort: FeedbackCohort },
    #[error("feedback page mixes multiple cohorts")]
    MixedCohortPage,
    #[error("feedback page cursors are not strictly increasing")]
    UnorderedCohortPage,
    #[error("feedback page claims continuation without a returned candidate")]
    EmptyContinuationPage,
    #[error(
        "recommendation report id {recommendation_report_id} does not match report {report_id}"
    )]
    RecommendationReportMismatch {
        recommendation_report_id: RecommendationReportId,
        report_id: RecommendationReportId,
    },
    #[error("report profile_id does not match its immutable profile reference")]
    ReportProfileIdentityMismatch,
    #[error("recommendation profile reference does not match its report")]
    RecommendationProfileMismatch,
    #[error("report with recommendations is missing its model run")]
    MissingReportModelRun,
    #[error("recommendation model run does not match its report")]
    ModelRunMismatch,
    #[error("recommendation model version does not match its report")]
    ModelVersionMismatch,
    #[error("recommendation market selection does not match its report")]
    MarketSelectionMismatch,
    #[error("recommendation decision-policy snapshot does not match its report")]
    DecisionPolicySnapshotMismatch,
    #[error("recommendation data-quality snapshot does not match its report")]
    DataQualitySnapshotMismatch,
    #[error("recommendation/report creation precedes the frozen decision instant")]
    InvalidRecommendationTimeline,
    #[error(
        "publication evidence conflicts with report status {report_status:?} and recommendation status {recommendation_status:?}"
    )]
    InvalidPublicationState {
        report_status: RecommendationReportStatus,
        recommendation_status: RecommendationStatus,
    },
    #[error("feedback recommendation profile does not match the frozen cycle profile")]
    FrozenProfileMismatch,
    #[error("visible resolution outcome failed its immutable contract")]
    InvalidResolutionOutcome(#[source] RecommendationResolutionOutcomeContractError),
    #[error("resolution outcome recommendation identity mismatch")]
    ResolutionRecommendationMismatch,
    #[error("resolution outcome market identity mismatch")]
    ResolutionMarketMismatch,
    #[error("resolution outcome token identity mismatch")]
    ResolutionTokenMismatch,
    #[error("resolution truth was already observable at or before the recommendation decision")]
    ResolutionNotForwardLooking,
    #[error("visible execution outcome failed its immutable contract")]
    InvalidExecutionOutcome(#[source] RecommendationExecutionOutcomeContractError),
    #[error("execution outcome recommendation identity mismatch")]
    ExecutionRecommendationMismatch,
    #[error("execution outcome market identity mismatch")]
    ExecutionMarketMismatch,
    #[error("execution outcome token identity mismatch")]
    ExecutionTokenMismatch,
    #[error("execution outcome runtime mode does not match the recommendation report")]
    ExecutionRuntimeModeMismatch,
    #[error("ReportOnly recommendation contains a submitted execution attempt")]
    ReportOnlyExecutionAttempt,
    #[error("submitted execution attempt predates recommendation publication")]
    ExecutionAttemptBeforePublication,
    #[error("visible execution outcome has no submitted attempt")]
    ExecutionOutcomeWithoutAttempt,
    #[error("execution outcome does not match the submitted intent/order identity")]
    ExecutionAttemptIdentityMismatch,
    #[error("execution outcome became terminal before the submitted attempt")]
    ExecutionTerminalBeforeSubmission,
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use serde_json::json;

    use super::{
        FEEDBACK_COHORT_PAGE_LIMIT, FeedbackCohortCandidate, FeedbackCohortContractError,
        FeedbackCohortCursor, FeedbackCohortDecision, FeedbackCohortPage, FeedbackCohortPageQuery,
        FeedbackCohortWindow, FeedbackExecutionAttempt, FeedbackRecommendationContext,
    };
    use crate::{
        enums::{
            common::MarketCategory,
            quant::{
                CohortCensorReason, CohortExclusionReason, FeedbackCohort, QuantRuntimeMode,
                ReportKind,
            },
        },
        types::{
            DecisionPolicySnapshotId, ExecutionOrderId, FactorDefinitionId, FeatureVectorId,
            MarketId, MarketSelectionId, ModelRunId, ModelVersionId, OrderIntentId,
            RecommendationId, RecommendationReportId, ReportDataQualitySnapshotId, TokenId,
            builtin_research_profiles,
        },
    };

    fn page_context(available_at: DateTime<Utc>) -> FeedbackRecommendationContext {
        let profile_ref = builtin_research_profiles()
            .expect("research profiles")
            .into_iter()
            .next()
            .expect("profile")
            .profile_ref;
        FeedbackRecommendationContext {
            recommendation_id: RecommendationId::from_v7(),
            recommendation_report_id: RecommendationReportId::from_v7(),
            profile_ref,
            report_kind: ReportKind::TopN,
            runtime_mode: QuantRuntimeMode::SemiAuto,
            category: MarketCategory::Crypto,
            market_id: MarketId::new("feedback-page-market"),
            token_id: TokenId::new("1"),
            decision_at: available_at - Duration::minutes(1),
            available_at,
            published_at: Some(available_at + Duration::seconds(1)),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            market_selection_id: MarketSelectionId::from_v7(),
            feature_vector_id: FeatureVectorId::from_v7(),
            factor_definition_versions: vec![FactorDefinitionId::from_v7()],
            data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
        }
    }

    fn submitted_attempt(available_at: DateTime<Utc>) -> FeedbackExecutionAttempt {
        FeedbackExecutionAttempt::Submitted {
            order_intent_id: OrderIntentId::from_v7(),
            entry_execution_order_id: ExecutionOrderId::from_v7(),
            submitted_at: available_at + Duration::seconds(2),
        }
    }

    fn model_candidate(
        available_at: DateTime<Utc>,
    ) -> Result<FeedbackCohortCandidate, FeedbackCohortContractError> {
        FeedbackCohortCandidate::try_new(
            FeedbackCohort::ModelLearning,
            page_context(available_at),
            None,
            None,
            None,
        )
    }

    #[test]
    fn frozen_window_round_trips_and_rejects_invalid_or_unknown_content() {
        let profile_ref = builtin_research_profiles()
            .expect("research profiles")
            .into_iter()
            .next()
            .expect("profile")
            .profile_ref;
        let window_start = Utc
            .with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
            .single()
            .expect("window start");
        let window = FeedbackCohortWindow::try_new(
            profile_ref.clone(),
            window_start,
            window_start + Duration::days(1),
        )
        .expect("valid window");
        let encoded = serde_json::to_string(&window).expect("serialize window");
        let decoded: FeedbackCohortWindow =
            serde_json::from_str(&encoded).expect("deserialize window");
        assert_eq!(decoded, window);

        assert!(
            serde_json::from_value::<FeedbackCohortWindow>(json!({
                "profile_ref": profile_ref,
                "window_start": window_start + Duration::seconds(1),
                "cutoff": window_start,
            }))
            .is_err()
        );
        let mut unknown = serde_json::to_value(window).expect("window value");
        unknown
            .as_object_mut()
            .expect("window object")
            .insert("compatibility_alias".to_owned(), json!(true));
        assert!(serde_json::from_value::<FeedbackCohortWindow>(unknown).is_err());
    }

    #[test]
    fn decision_wire_shape_preserves_stable_exclusion_and_censor_codes() {
        assert_eq!(
            serde_json::to_value(FeedbackCohortDecision::Excluded(
                CohortExclusionReason::ReportOnlyNoExecutionAuthority,
            ))
            .expect("excluded decision"),
            json!({
                "status": "excluded",
                "detail": "report_only_no_execution_authority",
            })
        );
        assert_eq!(
            serde_json::to_value(FeedbackCohortDecision::Censored(
                CohortCensorReason::ResolutionUnavailableAtCutoff,
            ))
            .expect("censored decision"),
            json!({
                "status": "censored",
                "detail": "resolution_unavailable_at_cutoff",
            })
        );
    }

    #[test]
    fn page_query_bounds_limit_and_cursor_to_the_frozen_window() {
        let profile_ref = builtin_research_profiles()
            .expect("research profiles")
            .into_iter()
            .next()
            .expect("profile")
            .profile_ref;
        let window_start = Utc
            .with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
            .single()
            .expect("window start");
        let cutoff = window_start + Duration::days(1);
        let window =
            FeedbackCohortWindow::try_new(profile_ref, window_start, cutoff).expect("window");
        let recommendation_id = RecommendationId::from_v7();

        for actual in [0, FEEDBACK_COHORT_PAGE_LIMIT + 1] {
            assert!(matches!(
                FeedbackCohortPageQuery::try_new(
                    FeedbackCohort::ModelLearning,
                    window.clone(),
                    None,
                    actual,
                ),
                Err(FeedbackCohortContractError::InvalidPageLimit {
                    actual: rejected,
                    maximum: FEEDBACK_COHORT_PAGE_LIMIT,
                }) if rejected == actual
            ));
        }
        for cursor_available_at in [
            window_start - Duration::nanoseconds(1),
            cutoff + Duration::nanoseconds(1),
        ] {
            assert!(matches!(
                FeedbackCohortPageQuery::try_new(
                    FeedbackCohort::ModelLearning,
                    window.clone(),
                    Some(FeedbackCohortCursor::new(
                        cursor_available_at,
                        recommendation_id,
                    )),
                    1,
                ),
                Err(FeedbackCohortContractError::CursorOutsideWindow {
                    cursor_available_at: rejected,
                    ..
                }) if rejected == cursor_available_at
            ));
        }
        for cursor_available_at in [window_start, cutoff] {
            let query = FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ModelLearning,
                window.clone(),
                Some(FeedbackCohortCursor::new(
                    cursor_available_at,
                    recommendation_id,
                )),
                FEEDBACK_COHORT_PAGE_LIMIT,
            )
            .expect("inclusive boundary cursor");
            assert_eq!(
                query.after().expect("cursor").available_at(),
                cursor_available_at
            );
            assert_eq!(query.limit(), FEEDBACK_COHORT_PAGE_LIMIT);
        }
    }

    #[test]
    fn candidate_truth_plane_is_closed_by_cohort() {
        let available_at = Utc
            .with_ymd_and_hms(2026, 7, 1, 0, 1, 0)
            .single()
            .expect("available at");
        let attempt = submitted_attempt(available_at);

        assert!(model_candidate(available_at).is_ok());
        assert!(matches!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::ModelLearning,
                page_context(available_at),
                Some(attempt),
                None,
                None,
            ),
            Err(FeedbackCohortContractError::InvalidCandidateTruthPlane {
                cohort: FeedbackCohort::ModelLearning,
            })
        ));
        assert!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::ExecutionLearning,
                page_context(available_at),
                Some(attempt),
                None,
                None,
            )
            .is_ok()
        );
        assert!(matches!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::ExecutionLearning,
                page_context(available_at),
                None,
                None,
                None,
            ),
            Err(FeedbackCohortContractError::InvalidCandidateTruthPlane {
                cohort: FeedbackCohort::ExecutionLearning,
            })
        ));
        assert!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::PolicyEvaluation,
                page_context(available_at),
                Some(attempt),
                None,
                None,
            )
            .is_ok()
        );
        assert!(matches!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::PolicyEvaluation,
                page_context(available_at),
                None,
                None,
                None,
            ),
            Err(FeedbackCohortContractError::InvalidCandidateTruthPlane {
                cohort: FeedbackCohort::PolicyEvaluation,
            })
        ));
    }

    #[test]
    fn page_requires_one_cohort_strict_order_and_nonempty_continuation() {
        let first_at = Utc
            .with_ymd_and_hms(2026, 7, 1, 0, 1, 0)
            .single()
            .expect("first available at");
        let second_at = first_at + Duration::seconds(1);
        let first = model_candidate(first_at).expect("first candidate");
        let second = model_candidate(second_at).expect("second candidate");
        let page = FeedbackCohortPage::try_new(
            FeedbackCohort::ModelLearning,
            vec![first.clone(), second.clone()],
            true,
        )
        .expect("ordered continuation page");
        assert_eq!(page.candidates(), &[first.clone(), second.clone()]);
        assert_eq!(page.next_cursor(), Some(second.cursor()));
        assert_eq!(
            FeedbackCohortPage::try_new(
                FeedbackCohort::ModelLearning,
                vec![first.clone(), second.clone()],
                false,
            )
            .expect("terminal page")
            .next_cursor(),
            None
        );

        assert!(matches!(
            FeedbackCohortPage::try_new(FeedbackCohort::ModelLearning, vec![second, first], false,),
            Err(FeedbackCohortContractError::UnorderedCohortPage)
        ));
        let execution = FeedbackCohortCandidate::try_new(
            FeedbackCohort::ExecutionLearning,
            page_context(second_at + Duration::seconds(1)),
            Some(submitted_attempt(second_at)),
            None,
            None,
        )
        .expect("execution candidate");
        assert!(matches!(
            FeedbackCohortPage::try_new(FeedbackCohort::ModelLearning, vec![execution], false,),
            Err(FeedbackCohortContractError::MixedCohortPage)
        ));
        assert!(matches!(
            FeedbackCohortPage::try_new(FeedbackCohort::ModelLearning, Vec::new(), true),
            Err(FeedbackCohortContractError::EmptyContinuationPage)
        ));
    }
}
