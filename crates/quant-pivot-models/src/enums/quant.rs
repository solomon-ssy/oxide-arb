//! Quant-pivot runtime and report domain enums.

active_string_enum! {
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
}

active_string_enum! {
    /// Recommendation report category.
    @derive(Default)
    pub enum ReportKind {
        #[default]
        TopN => "top_n",
        ShadowTopN => "shadow_top_n",
        PostRunAudit => "post_run_audit",
    }
}

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
    /// Directional action expressed by a model signal or recommendation.
    @derive(Default)
    pub enum SignalSide {
        #[default]
        BuyYes => "buy_yes",
        BuyNo => "buy_no",
        SellYes => "sell_yes",
        SellNo => "sell_no",
    }
}

impl SignalSide {
    /// The stable `i8` code persisted to `ClickHouse` `side` columns
    /// (`quant_signal_candidate_event` / `quant_recommendation_event` /
    /// `quant_execution_event`). Append-only contract: never renumber an existing
    /// variant.
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::BuyYes => 1,
            Self::BuyNo => 2,
            Self::SellYes => 3,
            Self::SellNo => 4,
        }
    }
}

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
    /// Governed execution-intent lifecycle state.
    @derive(Default)
    pub enum OrderIntentStatus {
        #[default]
        Draft => "draft",
        PendingApproval => "pending_approval",
        Approved => "approved",
        ApprovedByPolicy => "approved_by_policy",
        Rejected => "rejected",
        Expired => "expired",
        Submitted => "submitted",
        PartiallyFilled => "partially_filled",
        Filled => "filled",
        Cancelled => "cancelled",
        Failed => "failed",
    }
}

active_string_enum! {
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

active_string_enum! {
    /// Model publication lifecycle.
    @derive(Default)
    pub enum ModelPublicationStatus {
        #[default]
        Draft => "draft",
        Candidate => "candidate",
        Shadow => "shadow",
        Published => "published",
        Retired => "retired",
        Rejected => "rejected",
    }
}

impl ModelPublicationStatus {
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

active_string_enum! {
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

active_string_enum! {
    /// Factor definition lifecycle.
    @derive(Default)
    pub enum FactorDefinitionStatus {
        #[default]
        Draft => "draft",
        Candidate => "candidate",
        Shadow => "shadow",
        Published => "published",
        Retired => "retired",
        Rejected => "rejected",
    }
}

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
    /// Internal execution order state.
    @derive(Default)
    pub enum ExecutionOrderState {
        #[default]
        Draft => "draft",
        Submitted => "submitted",
        PartiallyFilled => "partially_filled",
        Filled => "filled",
        Cancelled => "cancelled",
        Failed => "failed",
    }
}

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
    /// Position-sizing model that produced a recommendation's size.
    @derive(Default)
    pub enum SizingModelKind {
        /// Fractional Kelly — the single production sizing model.
        #[default]
        Kelly => "kelly",
    }
}

active_string_enum! {
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

active_string_enum! {
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
        /// The venue account was unavailable (credentials / venue read failure).
        AccountUnavailable => "account_unavailable",
    }
}

active_string_enum! {
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

active_string_enum! {
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

active_string_enum! {
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
