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

impl PositionLedgerState {
    /// Position states that allow filled-intent attribution to finalize.
    pub const ATTRIBUTION_READY: [Self; 2] = [Self::Closed, Self::Settled];
}

crate::pg_enum! {
    type_name = "qp_reconciliation_result",
    pub enum ReconciliationResult {
        /// Enqueued, not yet reconciled (truth not yet observed). The honest
        /// initial state for an in-flight order handed to reconciliation — boot
        /// recovery and `Ambiguous` venue outcomes land here. Distinct from
        /// `Unresolvable`, which is the recon worker's terminal "ran, could not
        /// resolve" verdict (05.5).
        Pending => "pending",
        Filled => "filled",
        NotFilled => "not_filled",
        PartiallyFilled => "partially_filled",
        Cancelled => "cancelled",
        Unresolvable => "unresolvable",
    }
}

impl ReconciliationResult {
    /// Whether this reconciliation verdict blocks final attribution (05.7).
    ///
    /// Terminal filled/settled facts require reconciled truth; `Pending` and
    /// `Unresolvable` mean the ledger is still ambiguous and attribution must
    /// wait for a later sweep.
    #[must_use]
    pub const fn blocks_final_attribution(self) -> bool {
        matches!(self, Self::Pending | Self::Unresolvable)
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

impl KillSwitchState {
    /// Whether new entry orders may be opened in this state.
    ///
    /// Only [`Closed`](Self::Closed) admits new entries; every tightened state
    /// blocks new exposure (fail-closed safety valve).
    #[must_use]
    pub const fn allows_new_entry(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Whether the exit monitor may submit *normal* (TP/SL/time/signal) exits.
    ///
    /// [`EmergencyHalted`](Self::EmergencyHalted) deliberately returns `false`
    /// here: emergency liquidation does **not** flow through the normal auto-exit
    /// path but through [`Self::requires_emergency_exit`] governed by
    /// `KillSwitchPolicy.emergency_exit` (05.6). [`ExecutionHalted`](Self::ExecutionHalted)
    /// freezes all automated action (manual handling only).
    #[must_use]
    pub const fn allows_auto_exit(self) -> bool {
        matches!(self, Self::Closed | Self::ReportOnlyForced | Self::ExitOnly)
    }

    /// Whether this state mandates the emergency-exit path over open positions.
    #[must_use]
    pub const fn requires_emergency_exit(self) -> bool {
        matches!(self, Self::EmergencyHalted)
    }

    /// Whether this is the emergency-halt state (clearing it requires operator ack).
    #[must_use]
    pub const fn is_emergency(self) -> bool {
        matches!(self, Self::EmergencyHalted)
    }

    /// Monotone restriction strength: higher blocks strictly more execution.
    ///
    /// Used by the governed control plane to detect *loosening* transitions
    /// (target rank below the current rank), which require operator
    /// acknowledgement when the current state is latched.
    #[must_use]
    pub const fn restriction_rank(self) -> u8 {
        match self {
            Self::Closed => 0,
            Self::ReportOnlyForced | Self::ExitOnly => 1,
            Self::ExecutionHalted => 2,
            Self::EmergencyHalted => 3,
        }
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

crate::pg_enum! {
    type_name = "qp_exit_reason",
    /// Why a position lot exited (persisted on `quant_order_intent.exit_reason`).
    pub enum ExitReason {
        TakeProfit => "take_profit",
        StopLoss => "stop_loss",
        TimeExit => "time_exit",
        PartialExit => "partial_exit",
        SignalInvalidated => "signal_invalidated",
        /// Opportunistic model-driven Sell (Phase 6 Sell scorer); contract +
        /// metric label land now so the exit loop is wired for it.
        Opportunistic => "opportunistic",
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

#[cfg(test)]
mod tests {
    use super::{KillSwitchState, ReconciliationResult};

    /// Behavior table (父文档 §8 / 05.1 §6) encoded as a single source of truth.
    /// Columns: state, new-entry, auto-exit, emergency-exit.
    const BEHAVIOR_TABLE: &[(KillSwitchState, bool, bool, bool)] = &[
        (KillSwitchState::Closed, true, true, false),
        (KillSwitchState::ReportOnlyForced, false, true, false),
        (KillSwitchState::ExecutionHalted, false, false, false),
        (KillSwitchState::ExitOnly, false, true, false),
        (KillSwitchState::EmergencyHalted, false, false, true),
    ];

    #[test]
    fn kill_switch_behavior_table_matches_spec() {
        for &(state, new_entry, auto_exit, emergency) in BEHAVIOR_TABLE {
            assert_eq!(
                state.allows_new_entry(),
                new_entry,
                "allows_new_entry for {state:?}"
            );
            assert_eq!(
                state.allows_auto_exit(),
                auto_exit,
                "allows_auto_exit for {state:?}"
            );
            assert_eq!(
                state.requires_emergency_exit(),
                emergency,
                "requires_emergency_exit for {state:?}"
            );
        }
    }

    #[test]
    fn only_closed_admits_new_entries() {
        assert!(KillSwitchState::Closed.allows_new_entry());
        for &(state, ..) in &BEHAVIOR_TABLE[1..] {
            assert!(!state.allows_new_entry(), "{state:?} must block new entry");
        }
    }

    #[test]
    fn only_emergency_is_emergency() {
        assert!(KillSwitchState::EmergencyHalted.is_emergency());
        assert!(!KillSwitchState::Closed.is_emergency());
        assert!(!KillSwitchState::ExecutionHalted.is_emergency());
    }

    #[test]
    fn reconciliation_blocks_final_attribution() {
        assert!(ReconciliationResult::Pending.blocks_final_attribution());
        assert!(ReconciliationResult::Unresolvable.blocks_final_attribution());
        assert!(!ReconciliationResult::Filled.blocks_final_attribution());
        assert!(!ReconciliationResult::NotFilled.blocks_final_attribution());
        assert!(!ReconciliationResult::PartiallyFilled.blocks_final_attribution());
        assert!(!ReconciliationResult::Cancelled.blocks_final_attribution());
    }
}
