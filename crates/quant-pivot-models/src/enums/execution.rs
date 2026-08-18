//! Execution-layer Postgres enums (`quant_order_intent`, `quant_execution_order`).

use crate::enums::common::OrderType;

pg_enum! {
    type_name = "qp_account_chain_execution_role",
    /// Whether the account-owned order was resting, active, or self-matched.
    pub enum AccountChainExecutionRole {
        Maker => "maker",
        Taker => "taker",
        SelfMatch => "self_match",
    }
}

pg_enum! {
    type_name = "qp_strategy_position_origin_kind",
    /// Exclusive authority that created one strategy-owned position lot.
    pub enum StrategyPositionOriginKind {
        SystemIntent => "system_intent",
        AccountRecoveryIncident => "account_recovery_incident",
        OpeningInventory => "opening_inventory",
    }
}

pg_enum! {
    type_name = "qp_account_execution_association_kind",
    /// Exclusive owner assigned to one immutable account chain execution.
    pub enum AccountExecutionAssociationKind {
        SystemOrder => "system_order",
        RecoveryIncident => "recovery_incident",
        OpeningInventory => "opening_inventory",
    }
}

pg_enum! {
    type_name = "qp_account_recovery_incident_kind",
    pub enum AccountRecoveryIncidentKind {
        UnknownExternalExecution => "unknown_external_execution",
        BreakGlassRestart => "break_glass_restart",
        OpeningInventory => "opening_inventory",
        AccountMismatch => "account_mismatch",
    }
}

pg_enum! {
    type_name = "qp_account_recovery_incident_status",
    pub enum AccountRecoveryIncidentStatus {
        Open => "open",
        Reconciling => "reconciling",
        Sealed => "sealed",
    }
}

pg_enum! {
    type_name = "qp_account_pause_submission_state",
    pub enum AccountPauseSubmissionState {
        Prepared => "prepared",
        Dispatched => "dispatched",
        Ambiguous => "ambiguous",
        Confirmed => "confirmed",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_execution_order_phase",
    pub enum ExecutionOrderPhase {
        Entry => "entry",
        Exit => "exit",
    }
}

pg_enum! {
    type_name = "qp_order_type_kind",
    pub enum OrderTypeKind {
        Fok => "fok",
        Fak => "fak",
        Gtc => "gtc",
        Gtd => "gtd",
    }
}

impl From<OrderType> for OrderTypeKind {
    fn from(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Fok => Self::Fok,
            OrderType::Fak => Self::Fak,
            OrderType::Gtc => Self::Gtc,
            OrderType::Gtd { .. } => Self::Gtd,
        }
    }
}

pg_enum! {
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

pg_enum! {
    type_name = "qp_venue_trade_status",
    /// Execution status reported by Polymarket's authenticated trades API.
    pub enum VenueTradeStatus {
        Matched => "matched",
        Mined => "mined",
        Confirmed => "confirmed",
        Retrying => "retrying",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_capital_allocation_state",
    pub enum CapitalAllocationState {
        Allocated => "allocated",
        Locked => "locked",
        Spent => "spent",
        Released => "released",
        Impaired => "impaired",
    }
}

pg_enum! {
    type_name = "qp_position_ledger_state",
    pub enum PositionLedgerState {
        Open => "open",
        Closing => "closing",
        Closed => "closed",
        Settled => "settled",
    }
}

pg_enum! {
    type_name = "qp_reconciliation_result",
    pub enum ReconciliationResult {
        /// Enqueued, not yet reconciled (truth not yet observed). The honest
        /// initial state for an in-flight order handed to reconciliation — boot
        /// recovery and `Ambiguous` venue outcomes land here. Distinct from
        /// `Unresolvable`, which is the recon worker's terminal "ran, could not
        /// resolve" verdict.
        Pending => "pending",
        Filled => "filled",
        NotFilled => "not_filled",
        PartiallyFilled => "partially_filled",
        Cancelled => "cancelled",
        Unresolvable => "unresolvable",
    }
}

pg_enum! {
    type_name = "qp_kill_switch_state",
    pub enum KillSwitchState {
        Closed => "closed",
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
    /// `KillSwitchPolicy.emergency_exit`. [`ExecutionHalted`](Self::ExecutionHalted)
    /// freezes all automated action (manual handling only).
    #[must_use]
    pub const fn allows_auto_exit(self) -> bool {
        matches!(self, Self::Closed | Self::ExitOnly)
    }

    /// Whether a new governed settlement recovery submission may be created.
    /// Existing durable identities are tracked in every state and do not use
    /// this gate.
    #[must_use]
    pub const fn allows_settlement_recovery_submission(self) -> bool {
        matches!(self, Self::Closed | Self::ExitOnly)
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
            Self::ExitOnly => 1,
            Self::ExecutionHalted => 2,
            Self::EmergencyHalted => 3,
        }
    }
}

pg_enum! {
    type_name = "qp_exit_state",
    @derive(schemars::JsonSchema)
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

impl ExitReason {
    /// Closed runtime reason set every policy candidate must govern explicitly.
    pub const ALL: [Self; 13] = [
        Self::TakeProfit,
        Self::StopLoss,
        Self::TimeExit,
        Self::PartialExit,
        Self::SignalInvalidated,
        Self::Opportunistic,
        Self::Manual,
        Self::SettlementHold,
        Self::ResolutionRedeem,
        Self::KillSwitchEmergency,
        Self::RiskEnvelopeBreached,
        Self::MarketAbnormal,
        Self::DataStale,
    ];
}

wire_enum! {
    pub enum AdmissionOutcome {
        Allow => "allow",
        Deny => "deny",
        Defer => "defer",
    }
}

wire_enum! {
    pub enum AdmissionCheckId {
        IntentState => "intent_state",
        RecommendationFreshness => "recommendation_freshness",
        ReportStatus => "report_status",
        AuthorizationPolicy => "authorization_policy",
        SettlementRecovery => "settlement_recovery",
        ModelRouteBinding => "model_route_binding",
        DataQuality => "data_quality",
        BookFreshness => "book_freshness",
        VenueMetadata => "venue_metadata",
        EntryConditionPlan => "entry_condition",
        RiskEnvelopeHash => "risk_envelope_hash",
        CapitalBudget => "capital_budget",
        MaxOpenIntents => "max_open_intents",
        MaxReservedCapital => "max_reserved_capital",
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
        /// Defense-in-depth re-check: the frozen model artifact's
        /// return model must still be `Calibrated` at submission time, even
        /// though `report/composer.rs` already denied executable authority
        /// eligibility for uncalibrated candidates at report-build time.
        CalibratedReturnModel => "calibrated_return_model",
    }
}

wire_enum! {
    pub enum ReconciliationEvidenceKind {
        ClobOrderStatus => "clob_order_status",
        ClobTrades => "clob_trades",
        OnChainSettlement => "on_chain_settlement",
        TokenBalanceDelta => "token_balance_delta",
        AccountBalanceDelta => "account_balance_delta",
        BookContext => "book_context",
        OperatorNote => "operator_note",
    }
}

pg_enum! {
    type_name = "qp_exit_reason",
    /// Why a position lot exited (persisted on `quant_order_intent.exit_reason`).
    @derive(schemars::JsonSchema)
    pub enum ExitReason {
        TakeProfit => "take_profit",
        StopLoss => "stop_loss",
        TimeExit => "time_exit",
        PartialExit => "partial_exit",
        SignalInvalidated => "signal_invalidated",
        /// Opportunistic model-driven Sell emitted by the Sell scorer.
        Opportunistic => "opportunistic",
        Manual => "manual",
        SettlementHold => "settlement_hold",
        ResolutionRedeem => "resolution_redeem",
        KillSwitchEmergency => "kill_switch_emergency",
        RiskEnvelopeBreached => "risk_envelope_breached",
        MarketAbnormal => "market_abnormal",
        DataStale => "data_stale",
    }
}

wire_enum! {
    pub enum ApprovalInvalidation {
        RecommendationExpired => "recommendation_expired",
        ReportSuperseded => "report_superseded",
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
    use super::KillSwitchState;

    /// Kill-switch behavior encoded as a single source of truth.
    /// Columns: state, new-entry, auto-exit, emergency-exit.
    const BEHAVIOR_TABLE: &[(KillSwitchState, bool, bool, bool)] = &[
        (KillSwitchState::Closed, true, true, false),
        (KillSwitchState::ExecutionHalted, false, false, false),
        (KillSwitchState::ExitOnly, false, true, false),
        (KillSwitchState::EmergencyHalted, false, false, true),
    ];

    #[test]
    fn kill_switch_matches_spec() {
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
    fn only_closed_admits_entries() {
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
}
