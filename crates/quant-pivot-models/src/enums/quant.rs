//! Quant-pivot runtime and report domain enums.

crate::pg_enum! {
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

crate::pg_enum! {
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

crate::pg_enum! {
    type_name = "qp_report_trigger_kind",
    /// Stable report-generation trigger source.
    @derive(Default)
    pub enum ReportTriggerKind {
        #[default]
        Scheduled => "scheduled",
        AdHoc => "ad_hoc",
    }
}

crate::pg_enum! {
    type_name = "qp_recommendation_report_status",
    /// Publication lifecycle state for a recommendation report.
    @derive(Default)
    pub enum RecommendationReportStatus {
        #[default]
        Building => "building",
        Published => "published",
        PublishedEmpty => "published_empty",
        Failed => "failed",
        Revoked => "revoked",
        Expired => "expired",
    }
}

crate::pg_enum! {
    type_name = "qp_recommendation_status",
    /// Lifecycle state for a single recommendation.
    @derive(Default)
    pub enum RecommendationStatus {
        #[default]
        Published => "published",
        Revoked => "revoked",
        Expired => "expired",
        IntentCreated => "intent_created",
        Executed => "executed",
        Attributed => "attributed",
    }
}

impl RecommendationStatus {
    /// Whether this is a terminal recommendation state — no further entry action
    /// is possible. A report rolls up to `Expired` only once every one of its
    /// recommendations is terminal. `Published` / `IntentCreated` are non-terminal
    /// (the recommendation is still actionable / in flight).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Revoked | Self::Expired | Self::Executed | Self::Attributed
        )
    }

    /// Whether a new [`OrderIntent`] may still be created from this recommendation.
    #[must_use]
    pub const fn is_actionable_for_intent(self) -> bool {
        matches!(self, Self::Published | Self::IntentCreated)
    }

    /// Statuses still eligible for intent creation (SQL `IN` filters).
    pub const ACTIONABLE_FOR_INTENT: [Self; 2] = [Self::Published, Self::IntentCreated];
}

crate::pg_enum! {
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
    /// `quant_recommendation_event`). Append-only contract: never renumber an
    /// existing variant.
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::Yes => 1,
            Self::No => 2,
        }
    }
}

crate::wire_enum! {
    /// How an entry plan becomes executable.
    @derive(Default)
    pub enum EntryTriggerKind {
        #[default]
        Immediate => "immediate",
        LimitPrice => "limit_price",
        Breakout => "breakout",
        Pullback => "pullback",
        TimeWindow => "time_window",
        DataEvent => "data_event",
    }
}

crate::wire_enum! {
    /// How an exit plan leaves a recommendation.
    @derive(Default)
    pub enum ExitTriggerKind {
        #[default]
        TakeProfit => "take_profit",
        StopLoss => "stop_loss",
        TimeExit => "time_exit",
        TrailingStop => "trailing_stop",
        SignalInvalidation => "signal_invalidation",
        Manual => "manual",
    }
}

crate::pg_enum! {
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
}

crate::pg_enum! {
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

crate::pg_enum! {
    type_name = "qp_publication_status",
    /// Publication lifecycle for model specs, model versions, and factor definitions.
    @derive(Default)
    pub enum PublicationStatus {
        #[default]
        Draft => "draft",
        Candidate => "candidate",
        Shadow => "shadow",
        Published => "published",
        Retired => "retired",
        Rejected => "rejected",
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
            Self::Draft | Self::Rejected => false,
        }
    }
}

crate::pg_enum! {
    type_name = "qp_model_governance_action",
    /// Model-governance action recorded in the `quant_model_governance_audit`
    /// trail. Append-only wire labels — never rename an existing value.
    pub enum ModelGovernanceAction {
        /// A candidate / shadow version was published (gated).
        Publish => "publish",
        /// A published version was retired.
        Retire => "retire",
        /// A published version was rolled back to its predecessor.
        Rollback => "rollback",
        /// A built training dataset was promoted to `Ready` (gated).
        DatasetReady => "dataset_ready",
    }
}

crate::pg_enum! {
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

crate::pg_enum! {
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
}

crate::pg_enum! {
    type_name = "qp_training_dataset_status",
    /// Frozen training-dataset lifecycle state (ledger).
    ///
    /// Transitions: `Planned → Building → {Built | InsufficientLabels | Failed}`;
    /// a `Built` dataset is promoted to `Ready` once it passes validation, and may
    /// later become `Expired`. `Failed` / `InsufficientLabels` are terminal.
    @derive(Default)
    pub enum TrainingDatasetStatus {
        #[default]
        Planned => "planned",
        Building => "building",
        Built => "built",
        InsufficientLabels => "insufficient_labels",
        Ready => "ready",
        Expired => "expired",
        Failed => "failed",
    }
}

crate::pg_enum! {
    type_name = "qp_model_run_kind",
    /// Model run purpose.
    @derive(Default)
    pub enum ModelRunKind {
        Training => "training",
        Backtest => "backtest",
        Shadow => "shadow",
        #[default]
        LiveInference => "live_inference",
    }
}

crate::pg_enum! {
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

crate::pg_enum! {
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
        CancelledByOperator => "cancelled_by_operator",
    }
}

crate::wire_enum! {
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

crate::pg_enum! {
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

crate::pg_enum! {
    type_name = "qp_recommendation_outcome",
    /// Recommendation attribution outcome.
    @derive(Default)
    pub enum RecommendationOutcome {
        #[default]
        Pending => "pending",
        Won => "won",
        Lost => "lost",
        ExpiredUnfilled => "expired_unfilled",
        Cancelled => "cancelled",
        Unknown => "unknown",
    }
}

crate::pg_enum! {
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

crate::wire_enum! {
    /// Position-sizing model that produced a recommendation's size.
    @derive(Default)
    pub enum SizingModelKind {
        /// Fractional Kelly — the single production sizing model.
        #[default]
        Kelly => "kelly",
    }
}

crate::wire_enum! {
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
        /// Drawdown-scaling cap.
        DrawdownCap => "drawdown_cap",
        /// Confidence-floor cap.
        ConfidenceCap => "confidence_cap",
        /// Operator manual cap.
        ManualCap => "manual_cap",
        /// Fractional-Kelly upper bound.
        KellyCap => "kelly_cap",
        /// No hard cap bound the size.
        #[default]
        None => "none",
    }
}

crate::wire_enum! {
    /// Why a report could not publish any recommendation (empty report).
    @derive(Default)
    pub enum EmptyReason {
        /// The market selection was empty.
        #[default]
        EmptySelection => "empty_selection",
        /// Data quality was insufficient for inference.
        InsufficientDataQuality => "insufficient_data_quality",
        /// The active model failed its quality gate.
        ModelQualityGateFailed => "model_quality_gate_failed",
        /// The portfolio budget was already exhausted.
        PortfolioBudgetExhausted => "portfolio_budget_exhausted",
        /// No candidate carried a positive signal.
        NoPositiveSignal => "no_positive_signal",
        /// The runtime mode disabled report generation.
        RuntimeModeDisabled => "runtime_mode_disabled",
        /// The system was degraded below the generation threshold.
        SystemDegraded => "system_degraded",
    }
}

crate::wire_enum! {
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
    }
}

crate::wire_enum! {
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
        /// Available cash (collateral − reserved) was exhausted.
        AvailableCashExhausted => "available_cash_exhausted",
        /// Fundable, but ranked beyond the report's `top_n` cut.
        BeyondTopN => "beyond_top_n",
    }
}

crate::wire_enum! {
    /// How an open position is intended to be settled at resolution.
    @derive(Default)
    pub enum SettlementPolicy {
        /// Hold the position until the market resolves, then redeem.
        #[default]
        HoldToResolution => "hold_to_resolution",
        /// Exit on the book before resolution per the exit plan.
        ExitBeforeResolution => "exit_before_resolution",
        /// Redeem winnings automatically once redeemable.
        AutoRedeem => "auto_redeem",
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderIntentStatus, RecommendationStatus};

    #[test]
    fn recommendation_terminal_states_drive_report_roll_up() {
        // Terminal: a report rolls up to Expired once every recommendation is one of these.
        for status in [
            RecommendationStatus::Revoked,
            RecommendationStatus::Expired,
            RecommendationStatus::Executed,
            RecommendationStatus::Attributed,
        ] {
            assert!(status.is_terminal(), "{status:?} must be terminal");
        }
        // Non-terminal: still actionable / in flight (report stays active).
        for status in [
            RecommendationStatus::Published,
            RecommendationStatus::IntentCreated,
        ] {
            assert!(!status.is_terminal(), "{status:?} must be non-terminal");
            assert!(
                status.is_actionable_for_intent(),
                "{status:?} must accept intent creation"
            );
            assert!(
                RecommendationStatus::ACTIONABLE_FOR_INTENT.contains(&status),
                "{status:?} must be in ACTIONABLE_FOR_INTENT"
            );
        }
        for status in [
            RecommendationStatus::Revoked,
            RecommendationStatus::Expired,
            RecommendationStatus::Executed,
            RecommendationStatus::Attributed,
        ] {
            assert!(
                !status.is_actionable_for_intent(),
                "{status:?} must not accept intent creation"
            );
            assert!(
                !RecommendationStatus::ACTIONABLE_FOR_INTENT.contains(&status),
                "{status:?} must not be in ACTIONABLE_FOR_INTENT"
            );
        }
    }

    #[test]
    fn order_intent_status_predicates_match_sql_arrays() {
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
