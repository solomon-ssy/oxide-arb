//! Fact data-plane enums persisted in Postgres.

active_string_enum! {
    pub enum BalanceSnapshotSource {
        InternalLedger => "internal_ledger",
        ClobApi => "clob_api",
        OnChain => "on_chain",
        Subgraph => "subgraph",
        ManualImport => "manual_import",
    }
}

active_string_enum! {
    pub enum ShadowDecisionType {
        WouldReject => "would_reject",
        WouldSize => "would_size",
        WouldScore => "would_score",
        NoEffect => "no_effect",
    }
}

active_string_enum! {
    pub enum ExitTriggerType {
        FixedStop => "fixed_stop",
        TrailingStop => "trailing_stop",
        TimeStop => "time_stop",
        EndgameZoneInvalidation => "endgame_zone_invalidation",
        OracleNewsInvalidation => "oracle_news_invalidation",
        MarketStatusChange => "market_status_change",
        ReconciliationCritical => "reconciliation_critical",
        ManualOperator => "manual_operator",
    }
}

active_string_enum! {
    pub enum ExitAction {
        Hold => "hold",
        Reduce => "reduce",
        FullExit => "full_exit",
        ManualReview => "manual_review",
        RedeemIfResolved => "redeem_if_resolved",
    }
}

active_string_enum! {
    pub enum ExitPlanStatus {
        Draft => "draft",
        ReviewRequired => "review_required",
        Approved => "approved",
        Rejected => "rejected",
        Executing => "executing",
        Completed => "completed",
        Cancelled => "cancelled",
    }
}

active_string_enum! {
    pub enum ExitOrderType {
        FokSell => "fok_sell",
        FakSell => "fak_sell",
        Manual => "manual",
        Redeem => "redeem",
    }
}

active_string_enum! {
    pub enum ExitExecutionOutcome {
        Submitted => "submitted",
        Filled => "filled",
        PartialFill => "partial_fill",
        Miss => "miss",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

active_string_enum! {
    pub enum UnwindAuditEventType {
        PlanCreated => "plan_created",
        PlanApproved => "plan_approved",
        ExecutionObserved => "execution_observed",
        PositionPatched => "position_patched",
        ManualAdjustment => "manual_adjustment",
    }
}
