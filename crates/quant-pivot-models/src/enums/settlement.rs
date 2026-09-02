//! Settlement case, authorization, submission, and reconciliation state.

pg_enum! {
    type_name = "qp_settlement_route",
    /// Canonical Polymarket V2 collateral-adapter route.
    pub enum SettlementRoute {
        StandardV2 => "standard_v2",
        NegRiskV2 => "neg_risk_v2",
    }
}

pg_enum! {
    type_name = "qp_settlement_write_policy",
    /// Runtime authority for creating new settlement chain submissions.
    @derive(Default)
    pub enum SettlementWritePolicy {
        #[default]
        Disabled => "disabled",
        GovernedCanary => "governed_canary",
        OperatorApproval => "operator_approval",
        PolicyAutomatic => "policy_automatic",
    }
}

pg_enum! {
    type_name = "qp_settlement_readiness_status",
    /// Persisted readiness truth at a specific Polygon block.
    pub enum SettlementReadinessStatus {
        Unchecked => "unchecked",
        Ready => "ready",
        Blocked => "blocked",
    }
}

pg_enum! {
    type_name = "qp_settlement_case_state",
    /// Business lifecycle of one `(market, funder)` settlement case.
    pub enum SettlementCaseState {
        Discovered => "discovered",
        Prepared => "prepared",
        Submitted => "submitted",
        Confirmed => "confirmed",
        RetryScheduled => "retry_scheduled",
        ReconciliationRequired => "reconciliation_required",
        ManualRequired => "manual_required",
        NotRequired => "not_required",
    }
}

pg_enum! {
    type_name = "qp_settlement_authorization_state",
    /// Operator-approval batch authorization lifecycle, independent from ERC-1155 approval.
    pub enum SettlementAuthorizationState {
        NotRequired => "not_required",
        Pending => "pending",
        Approved => "approved",
        Revoked => "revoked",
        Consumed => "consumed",
        Expired => "expired",
    }
}

pg_enum! {
    type_name = "qp_settlement_effective_policy",
    /// Account-wide policy for a full-balance adapter redemption.
    pub enum SettlementEffectivePolicy {
        /// Every contributing lot explicitly authorizes hold-to-resolution auto redemption.
        AutomaticEligible => "automatic_eligible",
        /// At least one lot or inventory fact requires operator-owned recovery.
        ManualOnly => "manual_only",
    }
}

pg_enum! {
    type_name = "qp_settlement_governed_action_kind",
    /// Exact governed operation recorded before any money-moving transport call.
    pub enum SettlementGovernedActionKind {
        OutcomeTokenApproval => "outcome_token_approval",
        OutcomeTokenRevocation => "outcome_token_revocation",
        CanaryGrant => "canary_grant",
    }
}

pg_enum! {
    type_name = "qp_settlement_governed_action_state",
    /// Lifecycle for an immutable, RBAC-authorized settlement action.
    pub enum SettlementGovernedActionState {
        Authorized => "authorized",
        RetryScheduled => "retry_scheduled",
        Consumed => "consumed",
        Revoked => "revoked",
        Expired => "expired",
        ReconciliationRequired => "reconciliation_required",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_settlement_submission_kind",
    /// Transport and identity domain of a chain submission.
    @derive(schemars::JsonSchema)
    pub enum SettlementSubmissionKind {
        DirectEoa => "direct_eoa",
        Relayer => "relayer",
        ExternallyObserved => "externally_observed",
    }
}

pg_enum! {
    type_name = "qp_settlement_submission_purpose",
    /// Money-moving purpose carried by one immutable prepared call.
    pub enum SettlementSubmissionPurpose {
        OutcomeTokenApproval => "outcome_token_approval",
        OutcomeTokenRevocation => "outcome_token_revocation",
        Redeem => "redeem",
    }
}

pg_enum! {
    type_name = "qp_settlement_submission_state",
    /// Recoverable lifecycle of one durable prepared submission identity.
    pub enum SettlementSubmissionState {
        Prepared => "prepared",
        Dispatching => "dispatching",
        AwaitingChainHash => "awaiting_chain_hash",
        AwaitingFinality => "awaiting_finality",
        Confirmed => "confirmed",
        Failed => "failed",
    }
}

pg_enum! {
    type_name = "qp_settlement_failure_code",
    /// Closed failure classification used by control flow and operator tooling.
    pub enum SettlementFailureCode {
        RouteNotReady => "route_not_ready",
        BalanceMismatch => "balance_mismatch",
        SimulationReverted => "simulation_reverted",
        TransportUncertain => "transport_uncertain",
        SubmissionRejected => "submission_rejected",
        RelayerTerminalFailure => "relayer_terminal_failure",
        OnChainReverted => "on_chain_reverted",
        ReceiptEvidenceMismatch => "receipt_evidence_mismatch",
        PayoutMismatch => "payout_mismatch",
        DeploymentChanged => "deployment_changed",
        AuthorizationInvalid => "authorization_invalid",
        ExecutionNotQuiescent => "execution_not_quiescent",
        LeaseLost => "lease_lost",
        LedgerUnavailable => "ledger_unavailable",
        LocalInvariant => "local_invariant",
        ExternalEvidenceIncomplete => "external_evidence_incomplete",
    }
}

pg_enum! {
    type_name = "qp_settlement_reconciliation_state",
    /// Evidence-recovery state, independent from the case and submission FSMs.
    pub enum SettlementReconciliationState {
        NotRequired => "not_required",
        AwaitingRelayerHash => "awaiting_relayer_hash",
        AwaitingReceipt => "awaiting_receipt",
        EvidenceMismatch => "evidence_mismatch",
        OperatorReviewRequired => "operator_review_required",
        Reconciled => "reconciled",
    }
}
