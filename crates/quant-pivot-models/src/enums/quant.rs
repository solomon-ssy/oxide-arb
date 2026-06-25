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
