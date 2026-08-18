//! Governed policy-resource enums.

pg_enum! {
    type_name = "qp_decision_policy_snapshot_source",
    @derive(schemars::JsonSchema)
    pub enum DecisionPolicySnapshotSource {
        Bootstrap => "bootstrap",
        Activation => "activation",
        Rollback => "rollback",
    }
}

pg_enum! {
    type_name = "qp_policy_actor_kind",
    @derive(schemars::JsonSchema)
    pub enum PolicyActorKind {
        Operator => "operator",
        System => "system",
    }
}

pg_enum! {
    type_name = "qp_profile_artifact_kind",
    @derive(schemars::JsonSchema, PartialOrd, Ord)
    pub enum ProfileArtifactKind {
        Feature => "feature",
        Scoring => "scoring",
        Domain => "domain",
        ResearchMethod => "research_method",
    }
}

wire_enum! {
    @derive(schemars::JsonSchema, PartialOrd, Ord)
    pub enum PolicyApplyBoundary {
        ReportRunClaim => "report_run_claim",
        OrderIntentCreation => "order_intent_creation",
        ModelEvaluationClaim => "model_evaluation_claim",
        FutureReportRunReconcile => "future_report_run_reconcile",
        OperationalAdmission => "operational_admission",
        ExecutionAuthorizationAdmission => "execution_authorization_admission",
    }
}

wire_enum! {
    /// Closed operation-log action vocabulary for Config governance.
    pub enum ConfigAuditAction {
        DraftCreated => "config.draft_created",
        DraftValidated => "config.draft_validated",
        ApprovalRecorded => "config.approval_recorded",
        RevisionActivated => "config.revision_activated",
        RevisionRolledBack => "config.revision_rolled_back",
    }
}

wire_enum! {
    @derive(schemars::JsonSchema, PartialOrd, Ord)
    pub enum PolicyConsumer {
        MarketSelection => "market_selection",
        ReportCoordinator => "report_coordinator",
        RecommendationComposer => "recommendation_composer",
        DataQualityGate => "data_quality_gate",
        PortfolioOptimizer => "portfolio_optimizer",
        OrderIntentService => "order_intent_service",
        ExecutionAdmission => "execution_admission",
        ExitMonitor => "exit_monitor",
        ModelRunner => "model_runner",
        ReportScheduler => "report_scheduler",
        WorkerAdmission => "worker_admission",
        AlertDispatcher => "alert_dispatcher",
    }
}

wire_enum! {
    @derive(schemars::JsonSchema)
    pub enum PolicyValidationSeverity {
        Error => "error",
        Warning => "warning",
    }
}

wire_enum! {
    @derive(schemars::JsonSchema)
    pub enum PolicyValidationCode {
        SchemaVersionMismatch => "schema_version_mismatch",
        ResourceKindMismatch => "resource_kind_mismatch",
        SemanticConstraint => "semantic_constraint",
        DependencyUnavailable => "dependency_unavailable",
        ArtifactIncompatible => "artifact_incompatible",
        CredentialUnavailable => "credential_unavailable",
        ScheduleInvalid => "schedule_invalid",
        AuthorizationDenied => "authorization_denied",
    }
}

wire_enum! {
    @derive(schemars::JsonSchema)
    pub enum PolicyPreflightCheckKind {
        TypedSchema => "typed_schema",
        SemanticValidation => "semantic_validation",
        ConsumerPreparation => "consumer_preparation",
        ArtifactCompatibility => "artifact_compatibility",
        CredentialAvailability => "credential_availability",
        SchedulePreview => "schedule_preview",
        ExecutionCapability => "execution_capability",
    }
}

wire_enum! {
    /// Closed, localizable explanation vocabulary for Config preflight rows.
    @derive(schemars::JsonSchema)
    pub enum PolicyPreflightDetailCode {
        TypedDocumentDecoded => "typed_document_decoded",
        SemanticValidationPassed => "semantic_validation_passed",
        SemanticValidationFailed => "semantic_validation_failed",
        ConsumerPreparationPassed => "consumer_preparation_passed",
        ConsumerPreparationSkipped => "consumer_preparation_skipped",
        ConsumerPreparationFailed => "consumer_preparation_failed",
    }
}

wire_enum! {
    @derive(schemars::JsonSchema)
    pub enum CheckOutcome {
        Passed => "passed",
        Failed => "failed",
        NotApplicable => "not_applicable",
    }
}

pg_enum! {
    type_name = "qp_config_resource_kind",
    @derive(Default, schemars::JsonSchema, PartialOrd, Ord)
    pub enum ConfigResourceKind {
        #[default]
        RecommendationPolicy => "recommendation_policy",
        ExecutionRiskPolicy => "execution_risk_policy",
        ModelRouting => "model_routing",
        ReportSchedule => "report_schedule",
        OperationsPolicy => "operations_policy",
        ExecutionAuthorizationPolicy => "execution_authorization_policy",
    }
}

impl ConfigResourceKind {
    /// All governed resource kinds in stable console order.
    pub const ALL: [Self; 6] = [
        Self::RecommendationPolicy,
        Self::ExecutionRiskPolicy,
        Self::ModelRouting,
        Self::ReportSchedule,
        Self::OperationsPolicy,
        Self::ExecutionAuthorizationPolicy,
    ];
}

pg_enum! {
    type_name = "qp_policy_revision_status",
    @derive(schemars::JsonSchema)
    pub enum PolicyRevisionStatus {
        Draft => "draft",
        Validated => "validated",
    }
}

pg_enum! {
    type_name = "qp_policy_approval_decision",
    @derive(schemars::JsonSchema)
    pub enum PolicyApprovalDecision {
        Approved => "approved",
        Rejected => "rejected",
    }
}

pg_enum! {
    type_name = "qp_policy_activation_kind",
    @derive(schemars::JsonSchema)
    pub enum PolicyActivationKind {
        Initial => "initial",
        Promote => "promote",
        ModelBootstrap => "model_bootstrap",
        ModelShadowBinding => "model_shadow_binding",
        ModelShadowCancellation => "model_shadow_cancellation",
        ModelShadowRejection => "model_shadow_rejection",
        ModelPromotion => "model_promotion",
        Rollback => "rollback",
    }
}
