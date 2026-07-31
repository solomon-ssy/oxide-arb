//! Immutable inputs and results for point-in-time feedback-cohort classification.

use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    domain::quant::{
        RecommendationExecutionRollupContractError, RecommendationExecutionRollupInfo,
        RecommendationInfo, RecommendationReportInfo, RecommendationResolutionOutcomeContractError,
        RecommendationResolutionOutcomeInfo,
    },
    enums::{
        common::MarketCategory,
        quant::{
            CohortCensorReason, CohortExclusionReason, FeedbackCohort, OutcomeSide,
            QuantRuntimeMode, RecommendationReportStatus, RecommendationResolutionKind,
            RecommendationStatus, ReportKind,
        },
    },
    types::{
        BookSnapshotRef, ContentHash, DecisionPolicySnapshotId, EventId, FactorDefinitionId,
        FeatureVectorId, MarketContext, MarketId, MarketSelectionId, ModelRunId, ModelVersionId,
        PayoutRatio, Probability, RecommendationFactorBreakdown, RecommendationId,
        RecommendationIdentity, RecommendationReportId, ReportDataQualitySnapshotId,
        ResearchProfileRef, Shares, TokenId, Usd,
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

/// Decision-time cohort bounds plus the later immutable-truth visibility cutoff.
///
/// A recommendation belongs to the cohort by `decision_window`; resolution and
/// execution facts are visible only through `truth_cutoff`. Keeping these
/// frontiers distinct prevents a prediction horizon from either censoring every
/// mature label or admitting recommendations created after the evaluation
/// window closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackCohortSnapshot {
    decision_window: FeedbackCohortWindow,
    truth_cutoff: DateTime<Utc>,
}

impl FeedbackCohortSnapshot {
    pub fn try_new(
        decision_window: FeedbackCohortWindow,
        truth_cutoff: DateTime<Utc>,
    ) -> Result<Self, FeedbackCohortContractError> {
        if truth_cutoff < decision_window.cutoff() {
            return Err(FeedbackCohortContractError::TruthCutoffBeforeDecision {
                decision_cutoff: decision_window.cutoff(),
                truth_cutoff,
            });
        }
        Ok(Self {
            decision_window,
            truth_cutoff,
        })
    }

    #[must_use]
    pub const fn decision_window(&self) -> &FeedbackCohortWindow {
        &self.decision_window
    }

    #[must_use]
    pub const fn truth_cutoff(&self) -> DateTime<Utc> {
        self.truth_cutoff
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
    event_id: EventId,
    token_id: TokenId,
    outcome_side: OutcomeSide,
    rank: i32,
    rank_before_portfolio: i32,
    composite_score: Probability,
    confidence: Probability,
    top_n: i32,
    horizon_secs: i64,
    decision_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    model_run_id: ModelRunId,
    model_version_id: ModelVersionId,
    market_selection_id: MarketSelectionId,
    feature_vector_id: FeatureVectorId,
    factor_definition_versions: Vec<FactorDefinitionId>,
    book_snapshot_ref: BookSnapshotRef,
    identity: RecommendationIdentity,
    market_context: MarketContext,
    factor_breakdown: RecommendationFactorBreakdown,
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
            event_id: recommendation.event_id.clone(),
            token_id: recommendation.token_id.clone(),
            outcome_side: recommendation.outcome_side,
            rank: recommendation.rank,
            rank_before_portfolio: recommendation.rank_before_portfolio,
            composite_score: recommendation.composite_score,
            confidence: recommendation.confidence,
            top_n: report.top_n,
            horizon_secs: report.horizon_secs,
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
            book_snapshot_ref: recommendation.evidence_refs.book_snapshot_ref.clone(),
            identity: recommendation.identity.clone(),
            market_context: recommendation.market_context.clone(),
            factor_breakdown: recommendation.factor_breakdown.clone(),
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
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    #[must_use]
    pub const fn token_id(&self) -> &TokenId {
        &self.token_id
    }

    #[must_use]
    pub const fn outcome_side(&self) -> OutcomeSide {
        self.outcome_side
    }

    #[must_use]
    pub const fn rank(&self) -> i32 {
        self.rank
    }

    #[must_use]
    pub const fn rank_before_portfolio(&self) -> i32 {
        self.rank_before_portfolio
    }

    #[must_use]
    pub const fn composite_score(&self) -> Probability {
        self.composite_score
    }

    #[must_use]
    pub const fn confidence(&self) -> Probability {
        self.confidence
    }

    #[must_use]
    pub const fn top_n(&self) -> i32 {
        self.top_n
    }

    #[must_use]
    pub const fn horizon_secs(&self) -> i64 {
        self.horizon_secs
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
    pub const fn book_snapshot_ref(&self) -> &BookSnapshotRef {
        &self.book_snapshot_ref
    }

    #[must_use]
    pub const fn identity(&self) -> &RecommendationIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn market_context(&self) -> &MarketContext {
        &self.market_context
    }

    #[must_use]
    pub const fn factor_breakdown(&self) -> &RecommendationFactorBreakdown {
        &self.factor_breakdown
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
    } else if report.status.requires_publication() || recommendation.status.requires_publication() {
        return Err(FeedbackCohortContractError::InvalidPublicationState {
            report_status: report.status,
            recommendation_status: recommendation.status,
        });
    }
    Ok(())
}

/// Final execution state derived only from an immutable recommendation rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum FeedbackExecutionState {
    NotAttempted,
    Attempted {
        attempt_count: u32,
        first_attempt_terminal_at: DateTime<Utc>,
        last_attempt_terminal_at: DateTime<Utc>,
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
    snapshot: FeedbackCohortSnapshot,
    after: Option<FeedbackCohortCursor>,
    limit: u32,
}

impl FeedbackCohortPageQuery {
    pub fn try_new(
        cohort: FeedbackCohort,
        snapshot: FeedbackCohortSnapshot,
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
            && cursor.available_at > snapshot.truth_cutoff
        {
            return Err(FeedbackCohortContractError::CursorAfterTruthCutoff {
                cursor_available_at: cursor.available_at,
                truth_cutoff: snapshot.truth_cutoff,
            });
        }
        Ok(Self {
            cohort,
            snapshot,
            after,
            limit,
        })
    }

    #[must_use]
    pub const fn cohort(&self) -> FeedbackCohort {
        self.cohort
    }

    #[must_use]
    pub const fn snapshot(&self) -> &FeedbackCohortSnapshot {
        &self.snapshot
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
    resolution_outcome: Option<RecommendationResolutionOutcomeInfo>,
    execution_rollup: Option<RecommendationExecutionRollupInfo>,
}

impl FeedbackCohortCandidate {
    pub fn try_new(
        cohort: FeedbackCohort,
        context: FeedbackRecommendationContext,
        resolution_outcome: Option<RecommendationResolutionOutcomeInfo>,
        execution_rollup: Option<RecommendationExecutionRollupInfo>,
    ) -> Result<Self, FeedbackCohortContractError> {
        let plane_is_valid = match cohort {
            FeedbackCohort::ModelLearning => execution_rollup.is_none(),
            FeedbackCohort::ExecutionLearning => resolution_outcome.is_none(),
            FeedbackCohort::PolicyEvaluation => true,
        };
        if !plane_is_valid {
            return Err(FeedbackCohortContractError::InvalidCandidateTruthPlane { cohort });
        }
        Ok(Self {
            cohort,
            context,
            resolution_outcome,
            execution_rollup,
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
    pub const fn resolution_outcome(&self) -> Option<&RecommendationResolutionOutcomeInfo> {
        self.resolution_outcome.as_ref()
    }

    #[must_use]
    pub const fn execution_rollup(&self) -> Option<&RecommendationExecutionRollupInfo> {
        self.execution_rollup.as_ref()
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
    pub resolved_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub outcome_hash: ContentHash,
}

/// Minimal immutable execution truth admitted to a cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackExecutionEvidence {
    pub intent_count: u32,
    pub attempt_count: u32,
    pub unfilled_attempt_count: u32,
    pub partially_filled_attempt_count: u32,
    pub fully_filled_attempt_count: u32,
    pub total_requested_shares: Shares,
    pub total_filled_shares: Shares,
    pub total_realized_pnl_usd: Usd,
    pub first_attempt_terminal_at: Option<DateTime<Utc>>,
    pub last_attempt_terminal_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub rollup_hash: ContentHash,
}

/// Cohort-specific evidence carried only after eligibility succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "cohort", content = "evidence")]
pub enum FeedbackCohortEvidence {
    ModelLearning(FeedbackResolutionEvidence),
    ExecutionLearning(FeedbackExecutionEvidence),
    PolicyEvaluation {
        execution_state: Option<FeedbackExecutionState>,
        resolution_outcome_hash: Option<ContentHash>,
        execution_rollup_hash: Option<ContentHash>,
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
    #[error(
        "feedback truth cutoff {truth_cutoff} is earlier than decision cutoff {decision_cutoff}"
    )]
    TruthCutoffBeforeDecision {
        decision_cutoff: DateTime<Utc>,
        truth_cutoff: DateTime<Utc>,
    },
    #[error("feedback page limit must be within 1..={maximum}, got {actual}")]
    InvalidPageLimit { actual: u32, maximum: u32 },
    #[error(
        "feedback cursor availability {cursor_available_at} is later than truth cutoff {truth_cutoff}"
    )]
    CursorAfterTruthCutoff {
        cursor_available_at: DateTime<Utc>,
        truth_cutoff: DateTime<Utc>,
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
    #[error("visible execution rollup failed its immutable contract")]
    InvalidExecutionRollup(#[source] RecommendationExecutionRollupContractError),
    #[error("execution rollup recommendation identity mismatch")]
    ExecutionRecommendationMismatch,
    #[error("execution rollup became terminal before recommendation publication")]
    ExecutionTerminalBeforePublication,
    #[error("execution rollup count cannot be represented by the feedback contract")]
    ExecutionCountOverflow,
    #[error("ReportOnly recommendation contains attempted execution")]
    ReportOnlyExecutionAttempt,
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        FEEDBACK_COHORT_PAGE_LIMIT, FeedbackCohortCandidate, FeedbackCohortContractError,
        FeedbackCohortCursor, FeedbackCohortDecision, FeedbackCohortPage, FeedbackCohortPageQuery,
        FeedbackCohortSnapshot, FeedbackCohortWindow, FeedbackRecommendationContext,
    };
    use crate::{
        domain::quant::{NewRecommendationExecutionRollup, RecommendationExecutionRollupInfo},
        enums::{
            common::{MarketCategory, TickSize},
            market::MarketStatus,
            quant::{
                CohortCensorReason, CohortExclusionReason, FeedbackCohort, OutcomeSide,
                QuantRuntimeMode, ReportKind,
            },
        },
        types::{
            BookSnapshotRef, BookSnapshotSource, ContentHash, DecisionPolicySnapshotId, EventId,
            FeatureVectorId, MarketContext, MarketId, MarketSelectionId, ModelRunId,
            ModelVersionId, Probability, RecommendationFactorBreakdown, RecommendationId,
            RecommendationIdentity, RecommendationReportId, ReportDataQualitySnapshotId, TokenId,
            Usd, builtin_research_profiles,
        },
    };

    fn page_context(available_at: DateTime<Utc>) -> FeedbackRecommendationContext {
        let profile_ref = builtin_research_profiles()
            .expect("research profiles")
            .into_iter()
            .next()
            .expect("profile")
            .profile_ref;
        let token_id = TokenId::new("1");
        let source_hash = ContentHash::from_bytes([7; 32]);
        FeedbackRecommendationContext {
            recommendation_id: RecommendationId::from_v7(),
            recommendation_report_id: RecommendationReportId::from_v7(),
            profile_ref,
            report_kind: ReportKind::TopN,
            runtime_mode: QuantRuntimeMode::SemiAuto,
            category: MarketCategory::Crypto,
            market_id: MarketId::new("feedback-page-market"),
            event_id: EventId::new("feedback-page-event"),
            token_id: token_id.clone(),
            outcome_side: OutcomeSide::Yes,
            rank: 1,
            rank_before_portfolio: 1,
            composite_score: Probability::ZERO,
            confidence: Probability::ZERO,
            top_n: 10,
            horizon_secs: 3_600,
            decision_at: available_at - Duration::minutes(1),
            available_at,
            published_at: Some(available_at + Duration::seconds(1)),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            market_selection_id: MarketSelectionId::from_v7(),
            feature_vector_id: FeatureVectorId::from_v7(),
            factor_definition_versions: Vec::new(),
            book_snapshot_ref: BookSnapshotRef {
                token_id,
                source: BookSnapshotSource::CanonicalL2 {
                    stream_session_id: Uuid::nil(),
                    token_sequence: 1,
                    source_event_hash: source_hash,
                    event_time_ms: (available_at - Duration::minutes(1)).timestamp_millis(),
                    ingestion_time_ms: available_at.timestamp_millis(),
                },
                content_hash: source_hash,
            },
            identity: RecommendationIdentity {
                category: MarketCategory::Crypto,
                question: "Feedback page fixture?".to_owned(),
                outcome_name: "Yes".to_owned(),
            },
            market_context: MarketContext {
                best_bid: None,
                best_ask: None,
                mid_price: None,
                spread_bps: None,
                depth_usd: Usd::ZERO,
                volume_24h_usd: None,
                book_age_ms: 60_000,
                time_to_resolution_secs: None,
                market_status: MarketStatus::Active,
                neg_risk: false,
                tick_size: TickSize::Hundredth,
                fee_rate: None,
            },
            factor_breakdown: RecommendationFactorBreakdown(Vec::new()),
            data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
        }
    }

    fn execution_rollup(available_at: DateTime<Utc>) -> RecommendationExecutionRollupInfo {
        let context = page_context(available_at);
        let terminal_at = context.published_at().expect("published") + Duration::seconds(1);
        let available_at = terminal_at + Duration::seconds(1);
        let seal = NewRecommendationExecutionRollup::aggregate(
            context.recommendation_id(),
            0,
            terminal_at,
            terminal_at,
            Vec::new(),
        )
        .expect("empty rollup");
        let rollup = seal.rollup;
        let rollup_hash = rollup
            .expected_rollup_hash(available_at)
            .expect("rollup hash");
        RecommendationExecutionRollupInfo {
            recommendation_id: rollup.recommendation_id,
            intent_count: rollup.intent_count,
            attempt_count: rollup.attempt_count,
            unfilled_attempt_count: rollup.unfilled_attempt_count,
            partially_filled_attempt_count: rollup.partially_filled_attempt_count,
            fully_filled_attempt_count: rollup.fully_filled_attempt_count,
            total_requested_shares: rollup.total_requested_shares,
            total_filled_shares: rollup.total_filled_shares,
            total_entry_fee_usd: rollup.total_entry_fee_usd,
            total_exit_fee_usd: rollup.total_exit_fee_usd,
            total_settlement_payout_usd: rollup.total_settlement_payout_usd,
            total_realized_pnl_usd: rollup.total_realized_pnl_usd,
            first_attempt_terminal_at: rollup.first_attempt_terminal_at,
            last_attempt_terminal_at: rollup.last_attempt_terminal_at,
            terminal_at: rollup.terminal_at,
            source_observed_at: rollup.source_observed_at,
            available_at,
            attempt_set_hash: rollup.attempt_set_hash,
            rollup_hash,
            created_at: available_at,
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
        )
    }

    #[test]
    fn frozen_rejects_invalid_unknown() {
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
    fn decision_wire_preserves_codes() {
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
    fn page_query_bounds_window() {
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
        let truth_cutoff = cutoff + Duration::hours(1);
        let snapshot =
            FeedbackCohortSnapshot::try_new(window.clone(), truth_cutoff).expect("cohort snapshot");
        let recommendation_id = RecommendationId::from_v7();

        for actual in [0, FEEDBACK_COHORT_PAGE_LIMIT + 1] {
            assert!(matches!(
                FeedbackCohortPageQuery::try_new(
                    FeedbackCohort::ModelLearning,
                    snapshot.clone(),
                    None,
                    actual,
                ),
                Err(FeedbackCohortContractError::InvalidPageLimit {
                    actual: rejected,
                    maximum: FEEDBACK_COHORT_PAGE_LIMIT,
                }) if rejected == actual
            ));
        }
        let cursor_available_at = truth_cutoff + Duration::nanoseconds(1);
        assert!(matches!(
            FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ModelLearning,
                snapshot.clone(),
                Some(FeedbackCohortCursor::new(
                    cursor_available_at,
                    recommendation_id,
                )),
                1,
            ),
            Err(FeedbackCohortContractError::CursorAfterTruthCutoff {
                cursor_available_at: rejected,
                ..
            }) if rejected == cursor_available_at
        ));
        for cursor_available_at in [window_start, cutoff, truth_cutoff] {
            let query = FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ModelLearning,
                snapshot.clone(),
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
        assert!(matches!(
            FeedbackCohortSnapshot::try_new(window, cutoff - Duration::nanoseconds(1),),
            Err(FeedbackCohortContractError::TruthCutoffBeforeDecision { .. })
        ));
    }

    #[test]
    fn candidate_truth_plane_cohort() {
        let available_at = Utc
            .with_ymd_and_hms(2026, 7, 1, 0, 1, 0)
            .single()
            .expect("available at");
        let rollup = execution_rollup(available_at);

        assert!(model_candidate(available_at).is_ok());
        assert!(matches!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::ModelLearning,
                page_context(available_at),
                None,
                Some(rollup.clone()),
            ),
            Err(FeedbackCohortContractError::InvalidCandidateTruthPlane {
                cohort: FeedbackCohort::ModelLearning,
            })
        ));
        assert!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::ExecutionLearning,
                page_context(available_at),
                None,
                Some(rollup),
            )
            .is_ok()
        );
        assert!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::ExecutionLearning,
                page_context(available_at),
                None,
                None,
            )
            .is_ok()
        );
        assert!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::PolicyEvaluation,
                page_context(available_at),
                None,
                None,
            )
            .is_ok()
        );
        assert!(
            FeedbackCohortCandidate::try_new(
                FeedbackCohort::PolicyEvaluation,
                page_context(available_at),
                None,
                None,
            )
            .is_ok()
        );
    }

    #[test]
    fn page_requires_one_continuation() {
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
