//! Quant-pivot runtime and report domain enums.

pg_enum! {
    type_name = "qp_quant_runtime_mode",
    /// Governed runtime mode for report generation and optional execution.
    @derive(Default, schemars::JsonSchema)
    pub enum QuantRuntimeMode {
        #[default]
        ReportOnly => "report_only",
        SemiAuto => "semi_auto",
        AutoExecution => "auto_execution",
    }
}

pg_enum! {
    type_name = "qp_trade_policy_validation_status",
    /// Independent validation-run lifecycle. Row diagnostics are append-only;
    /// only the run summary transitions from Running to one terminal state.
    @derive(Default)
    pub enum TradePolicyValidationStatus {
        #[default]
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl QuantRuntimeMode {
    /// Whether this mode may submit CLOB orders.
    #[must_use]
    pub const fn allows_order_submission(self) -> bool {
        matches!(self, Self::SemiAuto | Self::AutoExecution)
    }

    /// Whether this mode may auto-create order intents without human approval.
    #[must_use]
    pub const fn allows_auto_execution(self) -> bool {
        matches!(self, Self::AutoExecution)
    }

    /// Monotonic capability rank used to classify a transition as an upgrade
    /// (more capability) or a downgrade (`ReportOnly` < `SemiAuto` < `AutoExecution`).
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::ReportOnly => 0,
            Self::SemiAuto => 1,
            Self::AutoExecution => 2,
        }
    }

    /// Whether transitioning from this mode to `target` increases capability
    /// (an upgrade requiring preflight).
    ///
    /// `self == target` is not an upgrade (handled as a no-op upstream).
    /// Upgrades must pass mode preflight; downgrades (tightening) skip it.
    #[must_use]
    pub const fn is_upgrade_to(self, target: Self) -> bool {
        target.rank() > self.rank()
    }
}

pg_enum! {
    type_name = "qp_report_kind",
    /// Recommendation report category.
    @derive(Default)
    pub enum ReportKind {
        #[default]
        TopN => "top_n",
        ShadowTopN => "shadow_top_n",
        PostRunAudit => "post_run_audit",
    }
}

pg_enum! {
    type_name = "qp_report_trigger_kind",
    /// Stable report-generation trigger source.
    @derive(Default)
    pub enum ReportTriggerKind {
        #[default]
        Scheduled => "scheduled",
        AdHoc => "ad_hoc",
    }
}

pg_enum! {
    type_name = "qp_recommendation_report_status",
    /// Publication lifecycle of an immutable recommendation report artifact.
    @derive(Default)
    pub enum RecommendationReportStatus {
        #[default]
        Prepared => "prepared",
        Published => "published",
        Superseded => "superseded",
        Obsolete => "obsolete",
        Revoked => "revoked",
        Expired => "expired",
    }
}

impl RecommendationReportStatus {
    /// Whether this report may authorize creation of a new entry intent.
    #[must_use]
    pub const fn is_current_authority(self) -> bool {
        matches!(self, Self::Published)
    }

    /// Whether the immutable artifact has reached a terminal lifecycle state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Superseded | Self::Obsolete | Self::Revoked | Self::Expired
        )
    }

    /// Closed transition table for the report artifact FSM.
    #[must_use]
    pub const fn allows_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Prepared,
                Self::Published | Self::Obsolete | Self::Revoked
            ) | (
                Self::Published,
                Self::Superseded | Self::Revoked | Self::Expired
            )
        )
    }

    /// Whether this lifecycle state is valid only after publication.
    #[must_use]
    pub const fn requires_publication(self) -> bool {
        matches!(self, Self::Published | Self::Superseded | Self::Expired)
    }
}

pg_enum! {
    type_name = "qp_report_run_status",
    /// Durable lifecycle of one report build attempt.
    @derive(Default)
    pub enum ReportRunStatus {
        #[default]
        Queued => "queued",
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Skipped => "skipped",
        Abandoned => "abandoned",
    }
}

impl ReportRunStatus {
    /// Whether this run no longer accepts lifecycle transitions.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Abandoned
        )
    }

    /// Closed transition table for the durable report-run FSM.
    #[must_use]
    pub const fn allows_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running | Self::Skipped)
                | (
                    Self::Running,
                    Self::Succeeded | Self::Failed | Self::Abandoned
                )
        )
    }
}

pg_enum! {
    type_name = "qp_report_run_terminal_reason",
    /// Typed terminal reason for skipped, failed, or abandoned report runs.
    pub enum ReportRunTerminalReason {
        CoalescedByNewerOccurrence => "coalesced_by_newer_occurrence",
        ScheduleReconfigured => "schedule_reconfigured",
        QueueExpired => "queue_expired",
        BuildFailed => "build_failed",
        LeaseExpired => "lease_expired",
    }
}

pg_enum! {
    type_name = "qp_report_schedule_gap_reason",
    /// Reason a contiguous range of schedule occurrences was not materialized.
    pub enum ReportScheduleGapReason {
        CoordinatorLag => "coordinator_lag",
        CoalescedByNewerOccurrence => "coalesced_by_newer_occurrence",
        ScheduleReconfigured => "schedule_reconfigured",
    }
}

pg_enum! {
    type_name = "qp_report_fact_delivery_status",
    /// Delivery lifecycle for the two-table report fact bundle.
    @derive(Default)
    pub enum ReportFactDeliveryStatus {
        #[default]
        Pending => "pending",
        Delivering => "delivering",
        Retrying => "retrying",
        Failed => "failed",
        Verified => "verified",
        Cancelled => "cancelled",
    }
}

pg_enum! {
    type_name = "qp_recommendation_status",
    /// Lifecycle state for a single recommendation.
    @derive(Default)
    pub enum RecommendationStatus {
        #[default]
        Prepared => "prepared",
        Published => "published",
        Superseded => "superseded",
        Obsolete => "obsolete",
        Revoked => "revoked",
        Expired => "expired",
        IntentCreated => "intent_created",
        Executed => "executed",
    }
}

impl RecommendationStatus {
    /// Whether the recommendation still has authority to create a new intent.
    #[must_use]
    pub const fn allows_new_intent(self) -> bool {
        matches!(self, Self::Published | Self::IntentCreated)
    }

    /// Whether this recommendation no longer prevents report expiry roll-up.
    #[must_use]
    pub const fn completes_report_rollup(self) -> bool {
        matches!(
            self,
            Self::Superseded | Self::Obsolete | Self::Revoked | Self::Expired | Self::Executed
        )
    }

    /// Whether this lifecycle state is valid only after publication.
    #[must_use]
    pub const fn requires_publication(self) -> bool {
        matches!(
            self,
            Self::Published
                | Self::Superseded
                | Self::Expired
                | Self::IntentCreated
                | Self::Executed
        )
    }

    /// Statuses that retain new-intent authority for SQL `IN` filters.
    pub const NEW_INTENT_AUTHORITY: [Self; 2] = [Self::Published, Self::IntentCreated];

    /// Statuses that no longer block report expiry roll-up in SQL filters.
    pub const REPORT_ROLLUP_COMPLETE: [Self; 5] = [
        Self::Superseded,
        Self::Obsolete,
        Self::Revoked,
        Self::Expired,
        Self::Executed,
    ];
}

pg_enum! {
    type_name = "qp_outcome_side",
    /// Which binary-market outcome token a recommendation opens a position in.
    ///
    /// A recommendation is always an *opening* position (buy-to-open) in one
    /// outcome token, so the only directional choice it expresses is the outcome
    /// (`Yes`/`No`) — the token itself is identified by `token_id`. Buy/sell
    /// direction is an execution-layer concern (see [`crate::enums::common::Side`]),
    /// and the sell/exit plan is expressed entirely by `ExitPlan`; this enum never
    /// encodes a sell.
    pub enum OutcomeSide {
        Yes => "yes",
        No => "no",
    }
}

impl OutcomeSide {
    /// The stable `i8` code persisted to the `ClickHouse` `side` columns of the
    /// candidate / recommendation facts (`quant_signal_candidate_event` /
    /// `quant_report_recommendation_fact`). Append-only contract: never renumber an
    /// existing variant.
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::Yes => 1,
            Self::No => 2,
        }
    }
}

impl FeedbackCohort {
    /// Whether samples in this cohort require finalized resolution evidence.
    #[must_use]
    pub const fn requires_resolution(self) -> bool {
        matches!(self, Self::ModelLearning | Self::PolicyEvaluation)
    }

    /// Whether samples in this cohort require real execution evidence.
    #[must_use]
    pub const fn requires_execution(self) -> bool {
        matches!(self, Self::ExecutionLearning | Self::PolicyEvaluation)
    }
}

wire_enum! {
    /// Comparator for a typed price entry condition.
    pub enum PriceComparison {
        AtOrAbove => "at_or_above",
        AtOrBelow => "at_or_below",
    }
}

wire_enum! {
    /// Venue fill semantics required by an aggressive entry.
    pub enum FillRequirement {
        AllOrNothing => "all_or_nothing",
        AllowPartial => "allow_partial",
    }
}

pg_enum! {
    type_name = "qp_entry_condition_state",
    /// Durable state of a recommendation-level entry condition instance.
    @derive(Default)
    pub enum EntryConditionState {
        #[default]
        NotRequired => "not_required",
        Waiting => "waiting",
        Unavailable => "unavailable",
        Confirming => "confirming",
        Qualified => "qualified",
        Consumed => "consumed",
        Expired => "expired",
        Invalidated => "invalidated",
    }
}

pg_enum! {
    type_name = "qp_entry_condition_audit_action",
    /// Append-only semantic transition recorded for a condition instance.
    pub enum EntryConditionAuditAction {
        Created => "created",
        Evaluated => "evaluated",
        LeaseTakenOver => "lease_taken_over",
        Claimed => "claimed",
        Reverted => "reverted",
        Expired => "expired",
        Invalidated => "invalidated",
    }
}

pg_enum! {
    type_name = "qp_order_intent_status",
    /// Governed execution-intent lifecycle state.
    @derive(Default)
    pub enum OrderIntentStatus {
        #[default]
        Draft => "draft",
        PendingApproval => "pending_approval",
        Approved => "approved",
        ApprovedByPolicy => "approved_by_policy",
        AdmissionPending => "admission_pending",
        AdmissionRejected => "admission_rejected",
        Submitted => "submitted",
        PartiallyFilled => "partially_filled",
        Filled => "filled",
        Rejected => "rejected",
        Cancelled => "cancelled",
        Failed => "failed",
        Expired => "expired",
        Invalidated => "invalidated",
    }
}

impl OrderIntentStatus {
    /// Statuses that still participate in pre-submission invalidation cascades.
    #[must_use]
    pub const fn is_pre_submission_active(self) -> bool {
        matches!(
            self,
            Self::PendingApproval
                | Self::Approved
                | Self::ApprovedByPolicy
                | Self::AdmissionPending
        )
    }

    /// Whether another intent on the same recommendation must be rejected at create time.
    #[must_use]
    pub const fn blocks_sibling_intent_creation(self) -> bool {
        matches!(
            self,
            Self::PendingApproval
                | Self::Approved
                | Self::ApprovedByPolicy
                | Self::AdmissionPending
                | Self::Submitted
                | Self::PartiallyFilled
        )
    }

    /// Pre-submission statuses for invalidation cascades (SQL `IN` filters).
    pub const PRE_SUBMISSION_ACTIVE: [Self; 4] = [
        Self::PendingApproval,
        Self::Approved,
        Self::ApprovedByPolicy,
        Self::AdmissionPending,
    ];

    /// Statuses that block sibling intent creation (SQL `IN` filters).
    pub const SIBLING_INTENT_BLOCKING: [Self; 6] = [
        Self::PendingApproval,
        Self::Approved,
        Self::ApprovedByPolicy,
        Self::AdmissionPending,
        Self::Submitted,
        Self::PartiallyFilled,
    ];

    /// Open (capital-holding or in-flight) statuses used by the admission
    /// concurrency cap (`#21` `MaxOpenIntentsCheck`): reserved-but-unsubmitted
    /// intents plus those in flight at the venue.
    pub const OPEN: [Self; 6] = [
        Self::PendingApproval,
        Self::Approved,
        Self::ApprovedByPolicy,
        Self::AdmissionPending,
        Self::Submitted,
        Self::PartiallyFilled,
    ];

    /// Terminal intent statuses with no venue fill.
    pub const UNFILLED_TERMINAL: [Self; 6] = [
        Self::AdmissionRejected,
        Self::Rejected,
        Self::Cancelled,
        Self::Failed,
        Self::Expired,
        Self::Invalidated,
    ];

    /// Terminal statuses with at least partial fill.
    pub const FILLED_TERMINAL: [Self; 2] = [Self::Filled, Self::PartiallyFilled];
}

pg_enum! {
    type_name = "qp_approval_status",
    /// Human or policy approval state attached to an order intent.
    @derive(Default)
    pub enum ApprovalStatus {
        #[default]
        NotRequired => "not_required",
        Pending => "pending",
        Approved => "approved",
        Rejected => "rejected",
        Expired => "expired",
    }
}

pg_enum! {
    type_name = "qp_publication_status",
    /// Publication lifecycle for immutable model versions.
    @derive(Default)
    pub enum PublicationStatus {
        #[default]
        Candidate => "candidate",
        Shadow => "shadow",
        Published => "published",
        Retired => "retired",
    }
}

pg_enum! {
    type_name = "qp_model_version_derivation_kind",
    /// Immutable provenance for how one model-version artifact was produced.
    ///
    /// Publication state is deliberately separate: derivation never changes
    /// when a candidate advances through shadow/published/retired lifecycle.
    pub enum ModelVersionDerivationKind {
        Training => "training",
        ReturnCalibration => "return_calibration",
    }
}

pg_enum! {
    type_name = "qp_trade_policy_status",
    /// Governance lifecycle of a content-addressed trade policy artifact.
    @derive(Default)
    pub enum TradePolicyStatus {
        #[default]
        Draft => "draft",
        Validated => "validated",
        Published => "published",
        Retired => "retired",
    }
}

impl TradePolicyStatus {
    #[must_use]
    pub const fn allows_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Draft, Self::Validated)
                | (Self::Validated, Self::Published)
                | (Self::Published, Self::Retired)
        )
    }
}

pg_enum! {
    type_name = "qp_trade_policy_governance_action",
    pub enum TradePolicyGovernanceAction {
        Validate => "validate",
        Publish => "publish",
        Retire => "retire",
    }
}

impl PublicationStatus {
    /// Returns whether transitioning from `self` to `next` is allowed by the
    /// model publication state machine.
    #[must_use]
    pub const fn allows_transition_to(self, next: Self) -> bool {
        match self {
            Self::Candidate | Self::Shadow => {
                matches!(next, Self::Shadow | Self::Published)
            }
            Self::Published => matches!(next, Self::Published | Self::Retired),
            Self::Retired => matches!(next, Self::Published),
        }
    }
}

pg_enum! {
    type_name = "qp_model_governance_action",
    /// Model-governance action recorded in the `quant_model_governance_audit`
    /// trail. Append-only wire labels — never rename an existing value.
    pub enum ModelGovernanceAction {
        /// A candidate / shadow version was published (gated).
        Publish => "publish",
        /// A published version was retired.
        Retire => "retire",
        /// A candidate model version bound a calibrated return model.
        BindCalibration => "bind_calibration",
        /// A candidate / shadow version bound the exact CPCV path set used by
        /// publish quality gates, replacing implicit "latest" selection.
        BindPublishPathSet => "bind_publish_path_set",
    }
}

pg_enum! {
    type_name = "qp_data_quality_status",
    /// Point-in-time data quality classification.
    @derive(Default)
    pub enum DataQualityStatus {
        #[default]
        Fresh => "fresh",
        Acceptable => "acceptable",
        Degraded => "degraded",
        Stale => "stale",
        Insufficient => "insufficient",
    }
}

pg_enum! {
    type_name = "qp_factor_direction",
    /// Factor contribution direction.
    @derive(Default)
    pub enum FactorDirection {
        Positive => "positive",
        Negative => "negative",
        #[default]
        Neutral => "neutral",
    }
}

impl FactorDirection {
    /// The stable `i8` code persisted to the `quant_factor_event.direction`
    /// `ClickHouse` column (`+1` / `-1` / `0`). Append-only contract: never
    /// renumber an existing variant.
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::Positive => 1,
            Self::Negative => -1,
            Self::Neutral => 0,
        }
    }

    /// Reverse a directional factor's economic orientation.
    #[must_use]
    pub const fn reversed(self) -> Self {
        match self {
            Self::Positive => Self::Negative,
            Self::Negative => Self::Positive,
            Self::Neutral => Self::Neutral,
        }
    }
}

pg_enum! {
    type_name = "qp_training_dataset_status",
    /// Frozen training-dataset lifecycle state (ledger).
    ///
    /// Transitions: `Planned → Building → {Ready | InsufficientLabels | Failed}`;
    /// `Ready` may later become `Expired`. Integrity validation is part of the
    /// build transaction, so there is no operator-controlled intermediate state.
    @derive(Default)
    pub enum TrainingDatasetStatus {
        #[default]
        Planned => "planned",
        Building => "building",
        InsufficientLabels => "insufficient_labels",
        Ready => "ready",
        Expired => "expired",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_source_slice_status",
    /// Server-owned source-slice materialization lifecycle.
    ///
    /// Identity is immutable and content-addressed. A failed materialization
    /// retains its diagnostics and may be retried as a new ledger row only
    /// after the underlying evidence identity changes.
    @derive(Default)
    pub enum SourceSliceStatus {
        #[default]
        Materializing => "materializing",
        Ready => "ready",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_research_readiness_evidence_kind",
    /// Narrow operational evidence classes consumed by policy-fit preflight.
    pub enum ResearchReadinessEvidenceKind {
        RetentionRunway => "retention_runway",
        ShadowLatencyProfile => "shadow_latency_profile",
    }
}

pg_enum! {
    type_name = "qp_trade_policy_trial_scope",
    /// Immutable unit evaluated by one policy-fit trial attempt.
    pub enum TradePolicyTrialScope {
        Candidate => "candidate",
        Fold => "fold",
        Path => "path",
        LatencyStress => "latency_stress",
    }
}

pg_enum! {
    type_name = "qp_trade_policy_trial_status",
    /// Terminal outcome of an attempted policy-fit unit. There is no mutable
    /// in-progress row: an attempt is appended exactly once at completion.
    pub enum TradePolicyTrialStatus {
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

pg_enum! {
    type_name = "qp_research_job_kind",
    /// The kind of long-running research task a durable job carries.
    ///
    /// Each kind dispatches to the matching offline service (dataset build /
    /// classical-ML trainer / point-in-time backtest); the executor is chosen
    /// exhaustively so a new kind forces a compile error at the dispatch site.
    pub enum ResearchJobKind {
        DatasetBuild => "dataset_build",
        ModelTrain => "model_train",
        Backtest => "backtest",
        /// Fit a favorite-longshot bias-table artifact.
        BiasTableFit => "bias_table_fit",
        /// Fit a model-score `ProbabilityCalibrator` artifact.
        ModelCalibrationFit => "model_calibration_fit",
        /// Run Combinatorial Purged Cross-Validation + the governed trial
        /// grid over a model version.
        CpcvBacktest => "cpcv_backtest",
        /// Deterministic training/serving feature replay.
        FeatureParity => "feature_parity",
        /// Freeze cohort coverage and decide whether statistical drift may run.
        FeedbackCoverage => "feedback_coverage",
        /// Compute data/concept/label drift from immutable coverage evidence.
        FeedbackDrift => "feedback_drift",
        /// Seal the bounded Training/Calibration/Evaluation Dataset batch.
        FeedbackDatasetSeal => "feedback_dataset_seal",
        /// Train the predeclared candidate batch.
        FeedbackTraining => "feedback_training",
        /// Fit and bind the candidate calibration batch.
        FeedbackCalibration => "feedback_calibration",
        /// Run and bind the candidate CPCV evidence batch.
        FeedbackCpcv => "feedback_cpcv",
        /// Compare every CPCV-eligible challenger with the champion over the
        /// one-time reserved Evaluation holdout.
        FeedbackComparison => "feedback_comparison",
        /// Evaluate one F09-eligible challenger against exact production
        /// shadow observations from a published serving generation.
        FeedbackShadowReplay => "feedback_shadow_replay",
        /// Seal the evidence-only terminal decision from exact F06/F09/F10
        /// predecessors. This job has no route-promotion authority.
        FeedbackDecision => "feedback_decision",
        /// Fit a governed executable entry/exit policy artifact.
        TradePolicyFit => "trade_policy_fit",
        /// Independently re-read and validate a Draft trade policy before CAS
        /// governance transition.
        TradePolicyValidation => "trade_policy_validation",
    }
}

pg_enum! {
    type_name = "qp_research_job_status",
    /// Durable research-job lifecycle state.
    ///
    /// `Queued → Running → {Succeeded | Failed | Cancelled}`. A crashed/orphaned
    /// `Running` job is reclaimed to `Queued` by the boot recovery sweep (bounded
    /// by `recovery_attempt`); a graceful shutdown also returns an in-flight job to
    /// `Queued`. `Cancelled` is an operator terminal state (never auto-resumed).
    @derive(Default)
    pub enum ResearchJobStatus {
        #[default]
        Queued => "queued",
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

pg_enum! {
    type_name = "qp_research_job_result_kind",
    /// Concrete artifact namespace referenced by a terminal research job.
    pub enum ResearchJobResultKind {
        TrainingDataset => "training_dataset",
        ModelVersion => "model_version",
        BacktestReport => "backtest_report",
        BacktestPathSet => "backtest_path_set",
        CalibrationArtifact => "calibration_artifact",
        FeatureParityRun => "feature_parity_run",
        FeedbackCoverageArtifact => "feedback_coverage_artifact",
        FeedbackDriftArtifact => "feedback_drift_artifact",
        FeedbackLearningStageArtifact => "feedback_learning_stage_artifact",
        FeedbackComparisonArtifact => "feedback_comparison_artifact",
        FeedbackShadowReplayArtifact => "feedback_shadow_replay_artifact",
        FeedbackDecisionArtifact => "feedback_decision_artifact",
        TradePolicyArtifact => "trade_policy_artifact",
        TradePolicyValidationRun => "trade_policy_validation_run",
    }
}

impl ResearchJobStatus {
    /// Whether the job has reached a terminal state (no further transitions).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// Whether the job is still pending or executing (occupies a concurrency slot).
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

pg_enum! {
    type_name = "qp_feature_parity_run_kind",
    /// Scope that caused a deterministic feature-parity replay.
    pub enum FeatureParityRunKind {
        Sampled => "sampled",
        Full => "full",
    }
}

pg_enum! {
    type_name = "qp_feature_parity_run_status",
    /// Durable lifecycle of one parity replay.
    @derive(Default)
    pub enum FeatureParityRunStatus {
        #[default]
        Queued => "queued",
        Running => "running",
        PendingMaterialization => "pending_materialization",
        Passed => "passed",
        Mismatched => "mismatched",
        Failed => "failed",
    }
}

impl FeatureParityRunStatus {
    /// Whether no more work may be recorded for this run.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Passed | Self::Mismatched | Self::Failed)
    }
}

pg_enum! {
    type_name = "qp_feature_parity_latch_state",
    /// Governed admission latch state. Absence of a row is treated as open.
    pub enum FeatureParityLatchState {
        Open => "open",
        Clear => "clear",
    }
}

pg_enum! {
    type_name = "qp_feature_parity_state_transition",
    /// Why a new append-only latch-state row exists.
    pub enum FeatureParityStateTransition {
        BootstrapProof => "bootstrap_proof",
        DeterministicMismatch => "deterministic_mismatch",
        IntegrityFailure => "integrity_failure",
        GovernedAcknowledge => "governed_acknowledge",
    }
}

pg_enum! {
    type_name = "qp_exit_settlement_mode",
    /// Deterministic layer compared by one parity event.
    pub enum FeatureParityStage {
        Selection => "selection",
        Snapshot => "snapshot",
        Capture => "capture",
        FeatureCell => "feature_cell",
        DataQuality => "data_quality",
        Factor => "factor",
        ModelInput => "model_input",
        Prediction => "prediction",
    }
}

pg_enum! {
    type_name = "qp_redeem_policy",
    /// Row-level result recorded in the parity event fact stream.
    pub enum FeatureParityEventStatus {
        Matched => "matched",
        Mismatched => "mismatched",
        PendingMaterialization => "pending_materialization",
    }
}

wire_enum! {
    /// Explicit state carried by feature evidence; absence is never a value.
    @derive(PartialOrd, Ord)
    pub enum FeatureCellState {
        Observed => "observed",
        Substituted => "substituted",
        Missing => "missing",
        NotApplicable => "not_applicable",
    }
}

wire_enum! {
    /// Stable machine code recorded on a research job's `error_json.code`.
    ///
    /// A wire-only enum (lives inside the `error_json` JSONB payload, not a
    /// dedicated column), so it needs no Postgres `CREATE TYPE`.
    pub enum ResearchJobErrorCode {
        /// The offline service returned a business error.
        ExecutionFailed => "execution_failed",
        /// A boot recovery sweep re-queued this orphaned run.
        InterruptedByRestart => "interrupted_by_restart",
        /// Automatic recovery exceeded `max_recovery_attempts` (poison-pill quarantine).
        InterruptedExceededAttempts => "interrupted_exceeded_attempts",
        /// The operator cancelled the run.
        Cancelled => "cancelled",
    }
}

pg_enum! {
    type_name = "qp_model_weight_source",
    /// Governed source of weights used during a shadow-model comparison.
    pub enum ModelWeightSource {
        /// Frozen weights from the content-addressed model artifact.
        Artifact => "artifact",
        /// Runtime-config overlay for experimentation; never publish evidence.
        ConfigOverlay => "config_overlay",
    }
}

pg_enum! {
    type_name = "qp_model_run_kind",
    /// Model run purpose.
    @derive(Default)
    pub enum ModelRunKind {
        Training => "training",
        Backtest => "backtest",
        /// Calibration evidence replay over a purpose-bound held-out dataset.
        Calibration => "calibration",
        /// Combinatorial Purged Cross-Validation + governed trial-grid run.
        /// Distinct from single-path [`Self::Backtest`] so the
        /// ledger can audit which validation methodology produced a path set.
        Cpcv => "cpcv",
        Shadow => "shadow",
        #[default]
        LiveInference => "live_inference",
    }
}

pg_enum! {
    type_name = "qp_model_run_status",
    /// Model run terminal or in-flight status.
    @derive(Default)
    pub enum ModelRunStatus {
        #[default]
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

pg_enum! {
    type_name = "qp_model_run_error_code",
    /// Stable, queryable failure taxonomy for a terminal [`ModelRunStatus::Failed`]
    /// run. Append-only wire labels — never rename an existing value.
    pub enum ModelRunErrorCode {
        ActiveInferenceFailed => "active_inference_failed",
        ShadowInferenceFailed => "shadow_inference_failed",
        FeaturePlaneFailed => "feature_plane_failed",
        FactorPlaneFailed => "factor_plane_failed",
        SelectionFailed => "selection_failed",
        ArtifactLoadFailed => "artifact_load_failed",
        SchemaBindingFailed => "schema_binding_failed",
        TrainingFailed => "training_failed",
        CalibrationFailed => "calibration_failed",
        CancelledByOperator => "cancelled_by_operator",
    }
}

wire_enum! {
    /// Serialization format of a stored model artifact's bytes.
    ///
    /// The weighted-factor body serializes to canonical JSON; a classical
    /// (smartcore-backed) model serializes its trained estimator to `bincode`.
    /// Loading must verify this matches the artifact header before deserializing.
    @derive(Default)
    pub enum ModelSerializationFormat {
        /// Canonical JSON (weighted-factor body, deterministic).
        #[default]
        Json => "json",
        /// `bincode`-encoded estimator (classical models).
        Bincode => "bincode",
    }
}

pg_enum! {
    type_name = "qp_execution_order_state",
    /// Internal execution order state.
    @derive(Default)
    pub enum ExecutionOrderState {
        #[default]
        Planned => "planned",
        Accepted => "accepted",
        Submitted => "submitted",
        PartiallyFilled => "partially_filled",
        Filled => "filled",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled",
        Failed => "failed",
        Ambiguous => "ambiguous",
    }
}

impl ExecutionOrderState {
    /// Whether capital and position are already settled — reconciliation must
    /// leave terminal orders untouched (idempotency).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::PartiallyFilled | Self::Cancelled | Self::Failed
        )
    }
}

pg_enum! {
    type_name = "qp_recommendation_resolution_kind",
    /// Shape of the authoritative resolved payout vector.
    ///
    /// Both variants are terminal. An unresolved or invalid source produces no
    /// resolution-outcome row and therefore has no enum variant.
    pub enum RecommendationResolutionKind {
        /// Exactly one token pays `1` and every other token pays `0`.
        WinnerTakeAll => "winner_take_all",
        /// More than one token has a positive fractional payout.
        SplitPayout => "split_payout",
    }
}

pg_enum! {
    type_name = "qp_recommendation_execution_terminal_state",
    /// Fill state after a real execution attempt reaches terminal venue truth.
    ///
    /// `ReportOnly` and recommendations never submitted to the venue have no
    /// execution-outcome row; they are deliberately not represented as zero fill.
    pub enum RecommendationExecutionTerminalState {
        Unfilled => "unfilled",
        PartiallyFilled => "partially_filled",
        FullyFilled => "fully_filled",
    }
}

pg_enum! {
    type_name = "qp_recommendation_execution_no_fill_reason",
    /// Terminal venue evidence proving that a real entry attempt filled zero shares.
    ///
    /// Pre-submission rejection, missing execution authority, and ambiguous
    /// placement are absence/censor states and therefore have no value here.
    pub enum RecommendationExecutionNoFillReason {
        VenueRejected => "venue_rejected",
        VenueCancelled => "venue_cancelled",
        VenueExpired => "venue_expired",
        ReconciledNotFilled => "reconciled_not_filled",
    }
}

pg_enum! {
    type_name = "qp_feedback_cohort",
    /// Orthogonal point-in-time cohort produced for one feedback cycle.
    pub enum FeedbackCohort {
        /// Resolution labels for model training and calibration.
        ModelLearning => "model_learning",
        /// Real venue attempts for fill and execution-cost learning.
        ExecutionLearning => "execution_learning",
        /// Recommendation coverage and strategy-effect evaluation.
        PolicyEvaluation => "policy_evaluation",
    }
}

pg_enum! {
    type_name = "qp_feedback_cycle_status",
    /// Durable orchestration lifecycle. Business decisions are stored
    /// separately and exist only for a successful terminal cycle.
    @derive(Default)
    pub enum FeedbackCycleStatus {
        #[default]
        Queued => "queued",
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl FeedbackCycleStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    #[must_use]
    pub const fn allows_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Running | Self::Succeeded | Self::Failed | Self::Cancelled
                )
        )
    }
}

pg_enum! {
    type_name = "qp_feedback_decision",
    /// Business outcome produced only after a cycle succeeds.
    pub enum FeedbackDecision {
        NoAction => "no_action",
        ChallengerRejected => "challenger_rejected",
        CandidateReady => "candidate_ready",
        Promoted => "promoted",
    }
}

pg_enum! {
    type_name = "qp_feedback_trigger_family",
    /// Trigger family participates in cycle idempotency. Actor and reason are
    /// append-only timeline evidence and deliberately do not fork identity.
    pub enum FeedbackTriggerFamily {
        Scheduled => "scheduled",
        Manual => "manual",
    }
}

pg_enum! {
    type_name = "qp_feedback_stage",
    /// Closed feedback DAG stage vocabulary.
    pub enum FeedbackStage {
        Trigger => "trigger",
        Coverage => "coverage",
        Drift => "drift",
        DatasetSeal => "dataset_seal",
        Training => "training",
        Calibration => "calibration",
        Cpcv => "cpcv",
        Comparison => "comparison",
        ShadowReplay => "shadow_replay",
        Decision => "decision",
    }
}

impl FeedbackStage {
    /// Return the next executable stage in the closed feedback DAG.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self {
            Self::Trigger => Some(Self::Coverage),
            Self::Coverage => Some(Self::Drift),
            Self::Drift => Some(Self::DatasetSeal),
            Self::DatasetSeal => Some(Self::Training),
            Self::Training => Some(Self::Calibration),
            Self::Calibration => Some(Self::Cpcv),
            Self::Cpcv => Some(Self::Comparison),
            Self::Comparison => Some(Self::ShadowReplay),
            Self::ShadowReplay => Some(Self::Decision),
            Self::Decision => None,
        }
    }
}

pg_enum! {
    type_name = "qp_feedback_stage_event_kind",
    /// Append-only timeline event vocabulary.
    pub enum FeedbackStageEventKind {
        Triggered => "triggered",
        JobLinked => "job_linked",
        Started => "started",
        Succeeded => "succeeded",
        Failed => "failed",
        CancellationRequested => "cancellation_requested",
        Cancelled => "cancelled",
        LeaseRecovered => "lease_recovered",
    }
}

pg_enum! {
    type_name = "qp_feedback_drift_kind",
    /// Mutually exclusive drift families.
    pub enum FeedbackDriftKind {
        Data => "data",
        Concept => "concept",
        Label => "label",
    }
}

pg_enum! {
    type_name = "qp_feedback_drift_metric",
    /// Profile-governed metrics supported by the feedback methodology.
    pub enum FeedbackDriftMetric {
        PopulationStabilityIndex => "population_stability_index",
        KolmogorovSmirnovPValue => "kolmogorov_smirnov_p_value",
        RankIcDrop => "rank_ic_drop",
        JensenShannonDivergence => "jensen_shannon_divergence",
    }
}

impl FeedbackDriftMetric {
    #[must_use]
    pub const fn kind(self) -> FeedbackDriftKind {
        match self {
            Self::PopulationStabilityIndex | Self::KolmogorovSmirnovPValue => {
                FeedbackDriftKind::Data
            }
            Self::RankIcDrop => FeedbackDriftKind::Concept,
            Self::JensenShannonDivergence => FeedbackDriftKind::Label,
        }
    }

    #[must_use]
    pub const fn is_unit_interval(self) -> bool {
        !matches!(self, Self::PopulationStabilityIndex)
    }
}

pg_enum! {
    type_name = "qp_feedback_drift_assessment",
    /// Typed interpretation of one drift metric.
    pub enum FeedbackDriftAssessment {
        WithinThreshold => "within_threshold",
        ThresholdExceeded => "threshold_exceeded",
        InsufficientEvidence => "insufficient_evidence",
    }
}

pg_enum! {
    type_name = "qp_feedback_evaluation_purpose",
    /// Statistical purpose that irreversibly consumes an unseen holdout.
    pub enum FeedbackEvaluationPurpose {
        PromotionComparison => "promotion_comparison",
    }
}

wire_enum! {
    /// Stable reason a recommendation can never belong to a requested frozen cohort.
    pub enum CohortExclusionReason {
        RecommendationNotPublished => "recommendation_not_published",
        NonPrimaryReport => "non_primary_report",
        OutsideFrozenWindow => "outside_frozen_window",
        ReportOnlyNoExecutionAuthority => "report_only_no_execution_authority",
        ExecutionNotAttempted => "execution_not_attempted",
    }
}

wire_enum! {
    /// Stable reason an otherwise eligible sample is not mature at the frozen cutoff.
    pub enum CohortCensorReason {
        ResolutionUnavailableAtCutoff => "resolution_unavailable_at_cutoff",
        ExecutionOutcomeUnavailableAtCutoff => "execution_outcome_unavailable_at_cutoff",
    }
}

pg_enum! {
    type_name = "qp_account_source",
    /// Capital-base provenance for a report's sizing.
    ///
    /// Single real source (the Polymarket venue account). The enum is retained
    /// for evidence labelling and forward extension; there is **no** simulated or
    /// configured-budget source — credentials are required and the report fails
    /// closed without them.
    @derive(Default)
    pub enum AccountSource {
        #[default]
        Polymarket => "polymarket",
    }
}

wire_enum! {
    /// Position-sizing model that produced a recommendation's size.
    @derive(Default)
    pub enum SizingModelKind {
        /// Fractional Kelly — the single production sizing model.
        #[default]
        Kelly => "kelly",
    }
}

wire_enum! {
    /// The cap that bound a recommendation's final size.
    ///
    /// `None` means no hard cap bound the size (it was limited only by the
    /// Kelly / curve suggestion itself).
    @derive(Default)
    pub enum BindingConstraint {
        /// Total deployable portfolio budget.
        PortfolioBudget => "portfolio_budget",
        /// Available cash (collateral − reserved) exhausted.
        AvailableCash => "available_cash",
        /// Per-recommendation absolute size cap (`max_single_recommendation_usd`).
        SingleRecommendationCap => "single_recommendation_cap",
        /// Per-market exposure cap.
        SingleMarketCap => "single_market_cap",
        /// Per-event exposure cap.
        EventCap => "event_cap",
        /// Per-category exposure cap.
        CategoryCap => "category_cap",
        /// Visible-liquidity usage cap.
        LiquidityCap => "liquidity_cap",
        /// Correlated-cluster exposure cap (`max_correlated_exposure_usd`);
        /// also emitted when the Kelly-stage correlation shrink
        /// (`f_i /= 1 + (n-1)·ρ̄`) is the dominant sizing shrink. This shares
        /// the exposure cap's root cause but occurs at a different pipeline stage.
        CorrelationCap => "correlation_cap",
        /// Drawdown-scaling cap: the Kelly-stage drawdown multiplier was the
        /// dominant shrink on the final size.
        DrawdownCap => "drawdown_cap",
        /// Confidence-floor cap: the Kelly-stage confidence multiplier was the
        /// dominant shrink on the final size.
        ConfidenceCap => "confidence_cap",
        /// Fractional-Kelly upper bound.
        KellyCap => "kelly_cap",
        /// Total simultaneous portfolio exposure hard cap
        /// (`portfolio.kelly_safety.max_aggregate_exposure_pct`).
        AggregateExposureCap => "aggregate_exposure_cap",
        /// No hard cap bound the size.
        #[default]
        None => "none",
    }
}

wire_enum! {
    /// Why a report could not publish any recommendation (empty report).
    ///
    /// Every variant has an independent producer in the report builder — there
    /// are no wire-only placeholders (zero dead semantics).
    @derive(Default)
    pub enum EmptyReportReason {
        /// The market selection was empty.
        #[default]
        EmptySelection => "empty_selection",
        /// Data quality was insufficient for inference.
        InsufficientDataQuality => "insufficient_data_quality",
        /// The portfolio budget was already exhausted.
        PortfolioBudgetExhausted => "portfolio_budget_exhausted",
        /// Available cash (collateral − reserved) was exhausted before any
        /// candidate could be funded — distinct from "no signal".
        AvailableCashExhausted => "available_cash_exhausted",
        /// No candidate carried a positive signal.
        NoPositiveSignal => "no_positive_signal",
        /// The active model has no calibrated probability and therefore cannot
        /// produce a truthful capital plan.
        ReturnModelUncalibrated => "return_model_uncalibrated",
        /// The active model does not bind a Published, hash-consistent trade
        /// policy and therefore cannot produce an executable trade plan.
        TradePolicyUnavailable => "trade_policy_unavailable",
        /// The system was degraded below the generation threshold (readiness
        /// gate not `Operational`, or the portfolio solver was unavailable).
        SystemDegraded => "system_degraded",
    }
}

wire_enum! {
    /// Why a recommendation is ineligible for execution in a given mode.
    @derive(Default)
    pub enum IneligibilityReason {
        /// The runtime mode is report-only.
        #[default]
        ReportOnlyMode => "report_only_mode",
        /// The risk envelope failed validation.
        RiskEnvelopeInvalid => "risk_envelope_invalid",
        /// The model is not published.
        ModelNotPublished => "model_not_published",
        /// Inputs were stale at decision time.
        DataStale => "data_stale",
        /// Confidence was below the execution floor.
        LowConfidence => "low_confidence",
        /// An operator manually blocked execution.
        ManuallyBlocked => "manually_blocked",
        /// The candidate has not passed shadow comparison.
        ShadowNotPassed => "shadow_not_passed",
        /// The execution budget is exhausted.
        BudgetExhausted => "budget_exhausted",
        /// The model's return model is `Heuristic` (uncalibrated) — fail-closed
        /// `SemiAuto`/`AutoExecution` never build an intent
        /// off an uncalibrated return estimate.
        ReturnModelUncalibrated => "return_model_uncalibrated",
        /// `ModelVersion` has no hash-verified Published policy or no executable cohort.
        TradePolicyUnavailable => "trade_policy_unavailable",
    }
}

wire_enum! {
    /// Why a candidate was dropped during portfolio planning (not published).
    ///
    /// The planner records one reason per rejected candidate; the report rolls
    /// these up into [`crate::types::RejectionReasonCount`].
    @derive(Default)
    pub enum RejectionReason {
        /// No positive Kelly edge (`f* <= 0`); never funded.
        #[default]
        NoPositiveSignal => "no_positive_signal",
        /// The candidate's edge inputs were invalid (non-positive downside, or a
        /// degenerate win probability) — sizing refused to fabricate a bet.
        InvalidEdgeInputs => "invalid_edge_inputs",
        /// The return model did not provide a calibrated win probability.
        ReturnModelUncalibrated => "return_model_uncalibrated",
        /// No unique policy-validated decision-time entry tier could be walked.
        ExecutableEntryUnavailable => "executable_entry_unavailable",
        /// The allocated size fell below `min_recommendation_usd`.
        BelowMinSize => "below_min_size",
        /// The total deployable budget room was exhausted.
        BudgetExhausted => "budget_exhausted",
        /// The per-market exposure cap was exhausted.
        MarketCapExhausted => "market_cap_exhausted",
        /// The per-event exposure cap was exhausted.
        EventCapExhausted => "event_cap_exhausted",
        /// The per-category exposure cap was exhausted.
        CategoryCapExhausted => "category_cap_exhausted",
        /// The visible liquidity could not support the minimum useful size.
        LiquidityInfeasible => "liquidity_infeasible",
        /// The correlated-cluster exposure cap was exhausted.
        CorrelationCapExhausted => "correlation_cap_exhausted",
        /// Available cash (collateral − reserved) was exhausted.
        AvailableCashExhausted => "available_cash_exhausted",
        /// Fundable, but ranked beyond the report's `top_n` cut.
        BeyondTopN => "beyond_top_n",
        /// The total simultaneous portfolio exposure hard cap was exhausted.
        AggregateExposureCapExhausted => "aggregate_exposure_cap_exhausted",
    }
}

pg_enum! {
    type_name = "qp_calibration_kind",
    /// Which empirical calibration artifact family a
    /// Calibration artifact family to which an
    /// [`crate::types::CalibrationArtifactId`] belongs.
    ///
    /// Both kinds share one governance table, one content-hash/split-hash
    /// contract, and one activation lifecycle; only the payload shape differs.
    @derive(Default)
    pub enum CalibrationKind {
        /// A `ProbabilityCalibrator` mapping model score → `P(win)`, fit on an
        /// independent held-out calibration split.
        #[default]
        ModelScore => "model_score",
        /// A `FavoriteLongshotBiasTable` mapping market-implied price →
        /// empirical settlement frequency.
        MarketPriceBias => "market_price_bias",
        /// Frozen GEFS forecast-minus-observation bias by station and exact lead.
        WeatherStationLeadBias => "weather_station_lead_bias",
    }
}

wire_enum! {
    /// The fitting method a `ModelScore` `ProbabilityCalibrator` used.
    @derive(Default, schemars::JsonSchema)
    pub enum CalibrationMethod {
        /// Non-parametric monotone regression (pool-adjacent-violators).
        /// Preferred with `>= min_samples_isotonic` samples.
        #[default]
        Isotonic => "isotonic",
        /// Two-parameter sigmoid (Platt scaling); data-efficient for small
        /// samples or near-sigmoid miscalibration.
        Platt => "platt",
    }
}

wire_enum! {
    /// The source a `Calibrated` return model's downside (bps) is read from.
    ///
    /// A single variant today (`MfeMae`): the system's Kelly/TP-SL structure
    /// already treats "downside" as a stop distance, not a binary-settlement
    /// full loss, so `MfeMae` (the empirical max-adverse-excursion label) is
    /// the semantically correct source.
    @derive(Default, schemars::JsonSchema)
    pub enum DownsideSource {
        /// Mean `max_adverse_excursion_bps` observed in the calibration split's
        /// score bucket.
        #[default]
        MfeMae => "mfe_mae",
    }
}

pg_enum! {
    type_name = "qp_dataset_purpose",
    /// What a `TrainingDataset` row's materialized examples are used for.
    ///
    /// `Calibration` datasets are built via the same pipeline as `Training`
    /// datasets but must be time-disjoint **and** embargoed relative to the
    /// training dataset of the model version they calibrate — the minimal,
    /// literature-standard `WalkForwardSplit`-with-embargo purge primitive.
    @derive(Default)
    pub enum DatasetPurpose {
        #[default]
        Training => "training",
        Calibration => "calibration",
        /// Frozen, reusable holdout used only for out-of-sample evaluation.
        Evaluation => "evaluation",
        /// Raw PIT observations used exclusively to fit an executable policy.
        PolicyFit => "policy_fit",
    }
}

pg_enum! {
    type_name = "qp_exit_settlement_mode",
    /// Whether an open lot should leave the market before resolution or be held
    /// until the CTF payout vector is available.
    @derive(Default)
    pub enum ExitSettlementMode {
        /// Hold the position until the market resolves.
        #[default]
        HoldToResolution => "hold_to_resolution",
        /// Exit on the book before resolution per the exit plan.
        ExitBeforeResolution => "exit_before_resolution",
    }
}

pg_enum! {
    type_name = "qp_redeem_policy",
    /// Whether a resolved hold-to-resolution lot is redeemed by the system or
    /// left for an operator.
    @derive(Default)
    pub enum RedeemPolicy {
        /// Operator handles CTF redemption manually.
        #[default]
        Manual => "manual",
        /// System may redeem once resolution and wallet-balance checks pass.
        Auto => "auto",
    }
}

pg_enum! {
    type_name = "qp_execution_wallet_kind",
    /// Polymarket wallet shape used by money-moving on-chain actions.
    ///
    /// Drives both the venue signature type and the on-chain settlement route. An
    /// EOA signs and pays gas directly (funder == signer). Polymarket Proxy,
    /// Gnosis Safe, and Deposit Wallet contracts hold collateral/positions and
    /// are driven through the gasless relayer by their controlling signer.
    @derive(Default, schemars::JsonSchema)
    pub enum ExecutionWalletKind {
        /// Externally owned account; signer address must equal funder address.
        #[default]
        Eoa => "eoa",
        /// Polymarket Proxy wallet (EIP-1167 minimal proxy, Magic/email users);
        /// funder is the CREATE2-derived proxy address controlled by the EOA.
        Proxy => "proxy",
        /// Gnosis Safe (1-of-1, browser-wallet users); funder is the CREATE2-
        /// derived Safe address controlled by the EOA owner.
        GnosisSafe => "gnosis_safe",
        /// Official Deposit Wallet; funder is the deterministic current
        /// `BeaconProxy` wallet and CLOB orders use `Poly1271`.
        DepositWallet => "deposit_wallet",
    }
}

wire_enum! {
    /// LP solver backend that produced a portfolio plan's allocation.
    @derive(Default, schemars::JsonSchema)
    pub enum PortfolioSolverKind {
        /// Pure-Rust `microlp` (default; no native dependency, ships in any build).
        #[default]
        Microlp => "microlp",
        /// Native `HiGHS` (optional performance backend; `lp-solver-highs` feature).
        Highs => "highs",
    }
}

wire_enum! {
    /// Which LP solve mode produced the allocation.
    @derive(Default)
    pub enum PortfolioSolveMode {
        /// Exact MILP with binary `TopN` inclusion (the production primary path).
        #[default]
        MilpExact => "milp_exact",
        /// Continuous LP relaxation with deterministic integer recovery (the
        /// fail-closed fallback and the deterministic backtest mode).
        ContinuousRelaxation => "continuous_relaxation",
    }
}

wire_enum! {
    /// Terminal status of the portfolio optimizer for one plan (observability of
    /// which solve path actually produced the allocation).
    @derive(Default)
    pub enum OptimizerSolverStatus {
        /// Solved to proven optimality.
        #[default]
        Optimal => "optimal",
        /// A feasible (not proven-optimal) solution was returned.
        Feasible => "feasible",
        /// The model was infeasible (only contradictory constraints can cause this,
        /// as the empty allocation is always feasible).
        Infeasible => "infeasible",
        /// The MILP path failed and the continuous relaxation produced the plan.
        FellBackRelaxation => "fell_back_relaxation",
        /// No solver could produce a plan; an empty (all-zero) allocation was emitted.
        SolverUnavailable => "solver_unavailable",
    }
}

wire_enum! {
    /// Provenance of the correlation clusters used for the correlation-exposure cap.
    @derive(Default)
    pub enum CorrelationSource {
        /// Estimated from historical mid-price co-movement (Pearson on log returns).
        Historical => "historical",
        /// Event/category proxy clusters (insufficient history for estimation).
        Proxy => "proxy",
        /// Correlation constraint disabled; no clustering or correlated-exposure cap.
        #[default]
        Disabled => "disabled",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        CohortCensorReason, CohortExclusionReason, FeatureParityStage, FeedbackCohort,
        OrderIntentStatus, PublicationStatus, RecommendationExecutionTerminalState,
        RecommendationResolutionKind, RecommendationStatus,
    };

    #[test]
    fn model_publication_fsm_closed() {
        assert_eq!(PublicationStatus::default(), PublicationStatus::Candidate);
        for (status, wire) in [
            (PublicationStatus::Candidate, "candidate"),
            (PublicationStatus::Shadow, "shadow"),
            (PublicationStatus::Published, "published"),
            (PublicationStatus::Retired, "retired"),
        ] {
            assert_eq!(PublicationStatus::from_str(wire), Ok(status));
        }
        for removed in ["draft", "rejected"] {
            assert!(
                PublicationStatus::from_str(removed).is_err(),
                "removed factor-era publication state `{removed}` must fail closed"
            );
        }
        assert!(PublicationStatus::Candidate.allows_transition_to(PublicationStatus::Shadow));
        assert!(PublicationStatus::Shadow.allows_transition_to(PublicationStatus::Published));
        assert!(PublicationStatus::Published.allows_transition_to(PublicationStatus::Retired));
        assert!(!PublicationStatus::Candidate.allows_transition_to(PublicationStatus::Retired));
    }

    #[test]
    fn feedback_outcome_enums_stable() {
        for (state, wire) in [
            (
                RecommendationResolutionKind::WinnerTakeAll,
                "winner_take_all",
            ),
            (RecommendationResolutionKind::SplitPayout, "split_payout"),
        ] {
            let exhaustive_wire = match state {
                RecommendationResolutionKind::WinnerTakeAll => "winner_take_all",
                RecommendationResolutionKind::SplitPayout => "split_payout",
            };
            assert_eq!(wire, exhaustive_wire);
            assert_eq!(state.as_str(), wire);
            assert_eq!(
                serde_json::from_str::<RecommendationResolutionKind>(&format!("\"{wire}\""))
                    .expect("deserialize resolution kind"),
                state
            );
        }

        for (state, wire) in [
            (RecommendationExecutionTerminalState::Unfilled, "unfilled"),
            (
                RecommendationExecutionTerminalState::PartiallyFilled,
                "partially_filled",
            ),
            (
                RecommendationExecutionTerminalState::FullyFilled,
                "fully_filled",
            ),
        ] {
            let exhaustive_wire = match state {
                RecommendationExecutionTerminalState::Unfilled => "unfilled",
                RecommendationExecutionTerminalState::PartiallyFilled => "partially_filled",
                RecommendationExecutionTerminalState::FullyFilled => "fully_filled",
            };
            assert_eq!(wire, exhaustive_wire);
            assert_eq!(state.as_str(), wire);
        }

        for (cohort, wire) in [
            (FeedbackCohort::ModelLearning, "model_learning"),
            (FeedbackCohort::ExecutionLearning, "execution_learning"),
            (FeedbackCohort::PolicyEvaluation, "policy_evaluation"),
        ] {
            let exhaustive_wire = match cohort {
                FeedbackCohort::ModelLearning => "model_learning",
                FeedbackCohort::ExecutionLearning => "execution_learning",
                FeedbackCohort::PolicyEvaluation => "policy_evaluation",
            };
            assert_eq!(wire, exhaustive_wire);
            assert_eq!(cohort.as_str(), wire);
        }
    }

    #[test]
    fn cohort_reason_codes_disjoint() {
        for (reason, wire) in [
            (
                CohortExclusionReason::RecommendationNotPublished,
                "recommendation_not_published",
            ),
            (
                CohortExclusionReason::NonPrimaryReport,
                "non_primary_report",
            ),
            (
                CohortExclusionReason::OutsideFrozenWindow,
                "outside_frozen_window",
            ),
            (
                CohortExclusionReason::ReportOnlyNoExecutionAuthority,
                "report_only_no_execution_authority",
            ),
            (
                CohortExclusionReason::ExecutionNotAttempted,
                "execution_not_attempted",
            ),
        ] {
            let exhaustive_wire = match reason {
                CohortExclusionReason::RecommendationNotPublished => "recommendation_not_published",
                CohortExclusionReason::NonPrimaryReport => "non_primary_report",
                CohortExclusionReason::OutsideFrozenWindow => "outside_frozen_window",
                CohortExclusionReason::ReportOnlyNoExecutionAuthority => {
                    "report_only_no_execution_authority"
                }
                CohortExclusionReason::ExecutionNotAttempted => "execution_not_attempted",
            };
            assert_eq!(wire, exhaustive_wire);
            assert_eq!(reason.as_str(), wire);
        }

        for (reason, wire) in [
            (
                CohortCensorReason::ResolutionUnavailableAtCutoff,
                "resolution_unavailable_at_cutoff",
            ),
            (
                CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff,
                "execution_outcome_unavailable_at_cutoff",
            ),
        ] {
            let exhaustive_wire = match reason {
                CohortCensorReason::ResolutionUnavailableAtCutoff => {
                    "resolution_unavailable_at_cutoff"
                }
                CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff => {
                    "execution_outcome_unavailable_at_cutoff"
                }
            };
            assert_eq!(wire, exhaustive_wire);
            assert_eq!(reason.as_str(), wire);
        }
    }

    #[test]
    fn feature_parity_stage_quality() {
        for (wire, stage) in [
            ("capture", FeatureParityStage::Capture),
            ("data_quality", FeatureParityStage::DataQuality),
        ] {
            assert_eq!(FeatureParityStage::from_str(wire), Ok(stage));
            assert_eq!(
                serde_json::to_string(&stage).expect("serialize stage"),
                format!("\"{wire}\"")
            );
        }
    }

    #[test]
    fn recommendation_status_predicates_exhaustive() {
        for (status, allows_new_intent, completes_rollup) in [
            (RecommendationStatus::Prepared, false, false),
            (RecommendationStatus::Published, true, false),
            (RecommendationStatus::Superseded, false, true),
            (RecommendationStatus::Obsolete, false, true),
            (RecommendationStatus::Revoked, false, true),
            (RecommendationStatus::Expired, false, true),
            (RecommendationStatus::IntentCreated, true, false),
            (RecommendationStatus::Executed, false, true),
        ] {
            assert_eq!(status.allows_new_intent(), allows_new_intent, "{status:?}");
            assert_eq!(
                status.completes_report_rollup(),
                completes_rollup,
                "{status:?}"
            );
            assert_eq!(
                RecommendationStatus::NEW_INTENT_AUTHORITY.contains(&status),
                allows_new_intent,
                "{status:?} SQL authority must match domain predicate"
            );
            assert_eq!(
                RecommendationStatus::REPORT_ROLLUP_COMPLETE.contains(&status),
                completes_rollup,
                "{status:?} SQL roll-up set must match domain predicate"
            );
        }
    }

    #[test]
    fn order_intent_status_arrays() {
        for status in [
            OrderIntentStatus::Draft,
            OrderIntentStatus::PendingApproval,
            OrderIntentStatus::Approved,
            OrderIntentStatus::ApprovedByPolicy,
            OrderIntentStatus::AdmissionPending,
            OrderIntentStatus::AdmissionRejected,
            OrderIntentStatus::Submitted,
            OrderIntentStatus::PartiallyFilled,
            OrderIntentStatus::Filled,
            OrderIntentStatus::Rejected,
            OrderIntentStatus::Cancelled,
            OrderIntentStatus::Failed,
            OrderIntentStatus::Expired,
            OrderIntentStatus::Invalidated,
        ] {
            assert_eq!(
                status.is_pre_submission_active(),
                OrderIntentStatus::PRE_SUBMISSION_ACTIVE.contains(&status),
                "{status:?} is_pre_submission_active"
            );
            assert_eq!(
                status.blocks_sibling_intent_creation(),
                OrderIntentStatus::SIBLING_INTENT_BLOCKING.contains(&status),
                "{status:?} blocks_sibling_intent_creation"
            );
        }
    }
}
pg_enum! {
    type_name = "qp_parity_subject_kind",
    pub enum ParitySubjectKind {
        RecommendationReport => "recommendation_report",
        ModelRun => "model_run",
        ModelVersion => "model_version",
    }
}
