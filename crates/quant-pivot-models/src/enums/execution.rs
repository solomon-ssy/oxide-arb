//! Execution-layer Postgres enums (`quant_order_intent`, `quant_execution_order`).

crate::pg_enum! {
    type_name = "qp_order_intent_kind",
    pub enum OrderIntentKind {
        Buy => "buy",
    }
}

crate::pg_enum! {
    type_name = "qp_execution_order_phase",
    pub enum ExecutionOrderPhase {
        Entry => "entry",
        Exit => "exit",
    }
}

crate::pg_enum! {
    type_name = "qp_order_type_kind",
    pub enum OrderTypeKind {
        Fok => "fok",
        Gtc => "gtc",
        Gtd => "gtd",
    }
}

crate::pg_enum! {
    type_name = "qp_venue_order_status",
    pub enum VenueOrderStatus {
        Filled => "filled",
        PartiallyFilled => "partially_filled",
        Rejected => "rejected",
        Cancelled => "cancelled",
        Open => "open",
        Expired => "expired",
    }
}

crate::pg_enum! {
    type_name = "qp_capital_allocation_state",
    pub enum CapitalAllocationState {
        Planned => "planned",
        Allocated => "allocated",
        Locked => "locked",
        Spent => "spent",
        Released => "released",
        Impaired => "impaired",
    }
}

crate::pg_enum! {
    type_name = "qp_position_ledger_state",
    pub enum PositionLedgerState {
        Open => "open",
        Closing => "closing",
        Closed => "closed",
        Settled => "settled",
    }
}

crate::pg_enum! {
    type_name = "qp_reconciliation_result",
    pub enum ReconciliationResult {
        Filled => "filled",
        NotFilled => "not_filled",
        PartiallyFilled => "partially_filled",
        Cancelled => "cancelled",
        Unresolvable => "unresolvable",
    }
}

crate::pg_enum! {
    type_name = "qp_kill_switch_state",
    pub enum KillSwitchState {
        Closed => "closed",
        ReportOnlyForced => "report_only_forced",
        ExecutionHalted => "execution_halted",
        ExitOnly => "exit_only",
        EmergencyHalted => "emergency_halted",
    }
}

crate::pg_enum! {
    type_name = "qp_exit_state",
    pub enum ExitState {
        NotStarted => "not_started",
        Monitoring => "monitoring",
        Triggered => "triggered",
        OrderSubmitted => "order_submitted",
        PartiallyExited => "partially_exited",
        Exited => "exited",
        Failed => "failed",
        ManualRequired => "manual_required",
    }
}

crate::wire_enum! {
    pub enum AdmissionOutcome {
        Allow => "allow",
        Deny => "deny",
        Defer => "defer",
    }
}

crate::wire_enum! {
    pub enum AdmissionCheckId {
        IntentState => "intent_state",
        RecommendationFreshness => "recommendation_freshness",
        ReportStatus => "report_status",
        RuntimeMode => "runtime_mode",
        ModelPublication => "model_publication",
        DataQuality => "data_quality",
        BookFreshness => "book_freshness",
        EntryTrigger => "entry_trigger",
        RiskEnvelopeHash => "risk_envelope_hash",
        CapitalBudget => "capital_budget",
        MarketExposure => "market_exposure",
        EventExposure => "event_exposure",
        CategoryExposure => "category_exposure",
        LiquidityDepth => "liquidity_depth",
        Slippage => "slippage",
        ManualBlock => "manual_block",
        KillSwitch => "kill_switch",
        VenueGuard => "venue_guard",
        CredentialReadiness => "credential_readiness",
        ExitMonitorReadiness => "exit_monitor_readiness",
    }
}

crate::wire_enum! {
    pub enum ReconciliationEvidenceKind {
        ClobOrderStatus => "clob_order_status",
        ClobTrades => "clob_trades",
        TokenBalanceDelta => "token_balance_delta",
        AccountBalanceDelta => "account_balance_delta",
        BookContext => "book_context",
        OperatorNote => "operator_note",
    }
}

crate::wire_enum! {
    pub enum ExitReason {
        TakeProfit => "take_profit",
        StopLoss => "stop_loss",
        TimeExit => "time_exit",
        PartialExit => "partial_exit",
        SignalInvalidated => "signal_invalidated",
        Manual => "manual",
        SettlementHold => "settlement_hold",
        KillSwitchEmergency => "kill_switch_emergency",
        RiskEnvelopeBreached => "risk_envelope_breached",
        MarketAbnormal => "market_abnormal",
        DataStale => "data_stale",
    }
}

crate::wire_enum! {
    pub enum ModeDenialReason {
        ReportOnly => "report_only",
        RecommendationIneligible => "recommendation_ineligible",
        RiskEnvelopeInvalid => "risk_envelope_invalid",
        KillSwitchBlocksEntry => "kill_switch_blocks_entry",
        AutoExecutionNotAllowed => "auto_execution_not_allowed",
    }
}

crate::wire_enum! {
    pub enum ApprovalInvalidation {
        RecommendationExpired => "recommendation_expired",
        ReportRevoked => "report_revoked",
        ModelVersionRetired => "model_version_retired",
        RuntimeConfigChanged => "runtime_config_changed",
        RiskEnvelopeMismatch => "risk_envelope_mismatch",
        DataQualityDegraded => "data_quality_degraded",
        KillSwitchOpened => "kill_switch_opened",
        IntentExpired => "intent_expired",
        OperatorCancelled => "operator_cancelled",
    }
}
