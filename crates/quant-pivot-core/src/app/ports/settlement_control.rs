//! Live settlement readiness and governed authorization control plane.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use quant_pivot_api::{
    settlement::contracts::{
        AlloySettlementChainReader, ContractDeploymentVerifier, SettlementCredentialAvailability,
        SettlementDeploymentCatalog, SettlementDeploymentProvenance, VerifiedSettlementDeployment,
    },
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::SettlementDeployConfig,
    domain::{
        api::settlement_redeem::{
            SettlementCanaryPreflightRequest, SettlementChainSubmissionView,
            SettlementDeploymentProvenanceView, SettlementGovernedAction,
            SettlementGovernedActionApplyRequest, SettlementGovernedActionBlockReason,
            SettlementGovernedActionDetailView, SettlementGovernedActionListQuery,
            SettlementGovernedActionPreflightView, SettlementGovernedActionRevokeRequest,
            SettlementGovernedActionScope, SettlementGovernedActionView,
            SettlementOperatorApprovalPreflightRequest, SettlementReadinessView,
            SettlementRouteReadinessView,
        },
        pagination::Paginated,
        ports::settlement_control::SettlementControlPort,
        quant::{
            settlement::{
                ApproveSettlementAuthorization, RevokeSettlementAuthorization, SettlementRedeemInfo,
            },
            settlement_governance::{NewSettlementGovernedAction, RevokeSettlementGovernedAction},
        },
    },
    enums::{
        quant::{EntryAuthorizationPolicy, ExecutionWalletKind},
        settlement::{
            SettlementAuthorizationState, SettlementEffectivePolicy, SettlementGovernedActionKind,
            SettlementGovernedActionState, SettlementReadinessStatus, SettlementRoute,
            SettlementWritePolicy,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, EvmAddress, ExecutionAccountId, SettlementEvidenceVersion,
        SettlementGovernedActionId, SettlementRedeemId, Usd, UserId,
    },
};
use quant_pivot_repository::traits::quant::{
    settlement_governance::SettlementGovernanceRepository,
    settlement_redeem::SettlementRedeemRepository,
};
use tokio::sync::Mutex;

use crate::{
    execution::{
        SettlementLifecyclePublisher,
        settlement_recovery_admission::{
            SettlementRecoveryAdmission, SettlementRecoveryAdmissionBlockReason,
            SettlementRecoveryAdmissionEvidence, SettlementRecoveryAdmissionPort,
            SettlementRecoveryAdmissionRequest,
        },
    },
    governance::RuntimeControlsHandle,
};

const SETTLEMENT_ACTION_PREFLIGHT_DOMAIN: &str = "quant-pivot.settlement-action-preflight";
const SETTLEMENT_ACTION_PREFLIGHT_SCHEMA_VERSION: u32 = 1;

struct SettlementReadinessCacheEntry {
    cached_at: Instant,
    routes: Vec<SettlementRouteReadinessView>,
}

/// Runtime implementation. The verifier is signer-free; mutations are limited
/// to exact authorization compare-and-swap operations in `PostgreSQL`.
pub struct CoreSettlementControlPort {
    repository: Arc<dyn SettlementRedeemRepository>,
    governance: Arc<dyn SettlementGovernanceRepository>,
    verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    catalog: SettlementDeploymentCatalog,
    topology: WalletTopology,
    credentials: SettlementCredentialAvailability,
    config: SettlementDeployConfig,
    runtime_controls: RuntimeControlsHandle,
    execution_account_id: ExecutionAccountId,
    readiness_cache: Mutex<Option<SettlementReadinessCacheEntry>>,
    lifecycle: Arc<SettlementLifecyclePublisher>,
}

/// Complete dependency set for the settlement control plane.
pub struct CoreSettlementControlPortDeps {
    pub repository: Arc<dyn SettlementRedeemRepository>,
    pub governance: Arc<dyn SettlementGovernanceRepository>,
    pub verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    pub catalog: SettlementDeploymentCatalog,
    pub topology: WalletTopology,
    pub credentials: SettlementCredentialAvailability,
    pub config: SettlementDeployConfig,
    pub runtime_controls: RuntimeControlsHandle,
    pub execution_account_id: ExecutionAccountId,
    pub lifecycle: Arc<SettlementLifecyclePublisher>,
}

impl CoreSettlementControlPort {
    #[must_use]
    pub fn new(deps: CoreSettlementControlPortDeps) -> Self {
        Self {
            repository: deps.repository,
            governance: deps.governance,
            verifier: deps.verifier,
            catalog: deps.catalog,
            topology: deps.topology,
            credentials: deps.credentials,
            config: deps.config,
            runtime_controls: deps.runtime_controls,
            execution_account_id: deps.execution_account_id,
            readiness_cache: Mutex::new(None),
            lifecycle: deps.lifecycle,
        }
    }

    async fn route_readiness(
        &self,
        route: SettlementRoute,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRouteReadinessView> {
        let target_adapter = self.catalog.current_target(route);
        let runtime_code_hash = self.catalog.current_code_hash(route).ok_or_else(|| {
            QuantError::config(format!(
                "settlement catalog has no current runtime code hash for {route}"
            ))
        })?;
        let authority = provenance_view(self.catalog.authority_provenance());
        let corroboration = (route == SettlementRoute::NegRiskV2)
            .then(|| provenance_view(self.catalog.corroboration_provenance()));
        let result = self
            .verifier
            .verify(route, &self.topology, self.credentials, checked_at)
            .await;
        Ok(match result {
            Ok(capability) => ready_route_view(&capability, authority, corroboration, checked_at),
            Err(readiness) => SettlementRouteReadinessView {
                route,
                wallet_kind: self.topology.kind,
                status: readiness.status,
                blocking_reasons: readiness.reasons,
                advisories: self.catalog.advisories(route),
                authority,
                corroboration,
                target_adapter,
                runtime_code_hash,
                observed_block_number: readiness.checked_block,
                observed_block_hash: readiness.checked_block_hash,
                deployment_digest: readiness.deployment_digest,
                deployment_evidence_version: self.catalog.evidence_version().clone(),
                operator_approved: None,
                checked_at,
            },
        })
    }

    async fn fresh_readiness(
        &self,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementReadinessView> {
        let (standard, neg_risk) = tokio::try_join!(
            self.route_readiness(SettlementRoute::StandardV2, checked_at),
            self.route_readiness(SettlementRoute::NegRiskV2, checked_at),
        )?;
        Ok(SettlementReadinessView {
            settlement_write_policy: self.runtime_controls.settlement_write_policy(),
            routes: vec![standard, neg_risk],
        })
    }

    async fn cached_ui_readiness(
        &self,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementReadinessView> {
        let mut cache = self.readiness_cache.lock().await;
        let ttl = Duration::from_secs(self.config.readiness_ui_cache_secs);
        if let Some(entry) = cache.as_ref()
            && entry.cached_at.elapsed() < ttl
        {
            return Ok(SettlementReadinessView {
                settlement_write_policy: self.runtime_controls.settlement_write_policy(),
                routes: entry.routes.clone(),
            });
        }
        let fresh = self.fresh_readiness(checked_at).await?;
        *cache = Some(SettlementReadinessCacheEntry {
            cached_at: Instant::now(),
            routes: fresh.routes.clone(),
        });
        drop(cache);
        Ok(fresh)
    }

    fn preflight(
        action: SettlementGovernedAction,
        route: SettlementRoute,
        desired_approval: Option<bool>,
        readiness: SettlementReadinessView,
    ) -> SettlementGovernedActionPreflightView {
        let route_readiness = readiness.routes.iter().find(|item| item.route == route);
        let mut blocking_reasons = Vec::new();
        if readiness.settlement_write_policy == SettlementWritePolicy::Disabled {
            blocking_reasons
                .push(SettlementGovernedActionBlockReason::SettlementWritePolicyDisabled);
        }
        if let Some(route_readiness) = route_readiness {
            if route_readiness.status != SettlementReadinessStatus::Ready
                || !route_readiness.blocking_reasons.is_empty()
                || route_readiness.operator_approved.is_none()
            {
                blocking_reasons.push(SettlementGovernedActionBlockReason::DeploymentNotReady);
            }
            if let Some(reason) = route_readiness
                .operator_approved
                .and_then(|approved| approval_state_reason(desired_approval, approved))
            {
                blocking_reasons.push(reason);
            }
        } else {
            blocking_reasons.push(SettlementGovernedActionBlockReason::DeploymentNotReady);
        }
        SettlementGovernedActionPreflightView {
            action,
            allowed: blocking_reasons.is_empty(),
            blocking_reasons,
            scope: None,
            preflight_token: None,
            expires_at: None,
            readiness,
        }
    }

    fn preflight_expiry(&self, checked_at: DateTime<Utc>) -> QuantResult<DateTime<Utc>> {
        let ttl = i64::try_from(self.config.operator_authorization_ttl_secs).map_err(|error| {
            QuantError::config(format!(
                "settlement action preflight TTL is not representable: {error}"
            ))
        })?;
        checked_at
            .checked_add_signed(TimeDelta::seconds(ttl))
            .ok_or_else(|| QuantError::config("settlement action preflight expiry overflow"))
    }

    fn issue_preflight(
        mut preflight: SettlementGovernedActionPreflightView,
        scope: SettlementGovernedActionScope,
    ) -> QuantResult<SettlementGovernedActionPreflightView> {
        if preflight.allowed {
            preflight.preflight_token = Some(action_preflight_token(&scope)?);
            preflight.expires_at = Some(scope.expires_at());
            preflight.scope = Some(scope);
        }
        Ok(preflight)
    }

    async fn create_governed_action(
        &self,
        request: SettlementGovernedActionApplyRequest,
        actor: UserId,
        authorized_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionView> {
        let reason = request.reason.trim();
        if reason.is_empty() {
            return Err(QuantError::config(
                "settlement governed-action reason must not be blank",
            ));
        }
        if request.scope.expires_at() <= authorized_at
            || action_preflight_token(&request.scope)? != request.preflight_token
        {
            return Err(QuantError::config(
                "settlement governed-action preflight is expired or digest-mismatched",
            ));
        }

        let fresh = match &request.scope {
            SettlementGovernedActionScope::OperatorApproval {
                route,
                desired_approval,
                ..
            } => {
                self.operator_approval_preflight(
                    SettlementOperatorApprovalPreflightRequest {
                        route: *route,
                        desired_approval: *desired_approval,
                    },
                    authorized_at,
                )
                .await?
            }
            SettlementGovernedActionScope::Canary {
                route,
                settlement_redeem_id,
                maximum_payout_usd,
                ..
            } => {
                self.canary_preflight(
                    SettlementCanaryPreflightRequest {
                        route: *route,
                        settlement_redeem_id: *settlement_redeem_id,
                        maximum_payout_usd: *maximum_payout_usd,
                    },
                    authorized_at,
                )
                .await?
            }
        };
        let fresh_scope = fresh.scope.as_ref().ok_or_else(|| {
            QuantError::config(format!(
                "settlement governed-action apply is currently blocked: {:?}",
                fresh.blocking_reasons
            ))
        })?;
        if !same_action_scope_identity(&request.scope, fresh_scope) {
            return Err(QuantError::config(
                "settlement governed-action scope changed after preflight",
            ));
        }

        let columns = action_scope_columns(&request.scope);
        let route_readiness = exact_route_scope(&fresh.readiness, columns.route)?;
        let verified_block_number = route_readiness.observed_block_number.ok_or_else(|| {
            QuantError::config("fresh governed-action capability has no verified block number")
        })?;
        let verified_block_hash = route_readiness.observed_block_hash.clone().ok_or_else(|| {
            QuantError::config("fresh governed-action capability has no verified block hash")
        })?;
        let verified_block_number = i64::try_from(verified_block_number).map_err(|error| {
            QuantError::config(format!(
                "governed-action verified block exceeds bigint: {error}"
            ))
        })?;
        let action = self
            .governance
            .create_action(NewSettlementGovernedAction {
                settlement_governed_action_id: SettlementGovernedActionId::from_v7(),
                execution_account_id: self.execution_account_id,
                settlement_redeem_id: columns.settlement_redeem_id,
                kind: columns.kind,
                state: SettlementGovernedActionState::Authorized,
                route: Some(columns.route),
                target_adapter: Some(columns.target_adapter),
                deployment_digest: Some(columns.deployment_digest),
                deployment_evidence_version: Some(columns.deployment_evidence_version),
                verified_block_number: Some(verified_block_number),
                verified_block_hash: Some(verified_block_hash),
                desired_approval: columns.desired_approval,
                authorization_digest: columns.authorization_digest,
                payout_ceiling_usd: columns.payout_ceiling_usd,
                scope_digest: request.preflight_token,
                idempotency_key: request.idempotency_key,
                authorization_reason: reason.to_owned(),
                authorized_by: actor,
                revoked_by: None,
                revocation_reason: None,
                expires_at: request.scope.expires_at(),
                authorized_at,
                consumed_at: None,
                revoked_at: None,
                failure_code: None,
                retry_count: 0,
                claim_owner: None,
                lease_expires_at: None,
                next_attempt_at: Some(authorized_at),
                last_error: None,
            })
            .await?;
        Ok(action.into())
    }
}

#[async_trait]
impl SettlementRecoveryAdmissionPort for CoreSettlementControlPort {
    async fn evaluate_recovery_admission(
        &self,
        request: SettlementRecoveryAdmissionRequest,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRecoveryAdmission> {
        if request.execution_account_id != self.execution_account_id {
            return Ok(SettlementRecoveryAdmission::Blocked(
                SettlementRecoveryAdmissionBlockReason::ExecutionAccountMismatch,
            ));
        }
        if !automatic_recovery_policy_matches(request.authorization_policy, request.write_policy) {
            return Ok(SettlementRecoveryAdmission::Blocked(
                SettlementRecoveryAdmissionBlockReason::RuntimePolicyMismatch {
                    authorization_policy: request.authorization_policy,
                    write_policy: request.write_policy,
                },
            ));
        }

        let readiness = self.route_readiness(request.route, checked_at).await?;
        let deployment_ready = readiness.status == SettlementReadinessStatus::Ready
            && readiness.blocking_reasons.is_empty();
        let (Some(deployment_digest), Some(verified_block_number), Some(verified_block_hash)) = (
            readiness.deployment_digest,
            readiness.observed_block_number,
            readiness.observed_block_hash,
        ) else {
            return Ok(SettlementRecoveryAdmission::Blocked(
                SettlementRecoveryAdmissionBlockReason::DeploymentNotReady,
            ));
        };
        if !deployment_ready {
            return Ok(SettlementRecoveryAdmission::Blocked(
                SettlementRecoveryAdmissionBlockReason::DeploymentNotReady,
            ));
        }
        // Deployment fingerprint Ready is not money-entry Ready: Auto recovery
        // inventory requires live ERC-1155 approval before risk can open.
        if readiness.operator_approved != Some(true) {
            return Ok(SettlementRecoveryAdmission::Blocked(
                SettlementRecoveryAdmissionBlockReason::OperatorApprovalMissing,
            ));
        }

        let confirmed_canary =
            if request.authorization_policy == EntryAuthorizationPolicy::PolicyAutomatic {
                self.governance
                    .has_confirmed_canary(
                        &request.execution_account_id,
                        request.route,
                        deployment_digest,
                    )
                    .await?
            } else {
                false
            };
        if request.authorization_policy == EntryAuthorizationPolicy::PolicyAutomatic
            && !confirmed_canary
        {
            return Ok(SettlementRecoveryAdmission::Blocked(
                SettlementRecoveryAdmissionBlockReason::ConfirmedCanaryMissing,
            ));
        }

        Ok(SettlementRecoveryAdmission::Ready(
            SettlementRecoveryAdmissionEvidence {
                route: request.route,
                wallet_kind: readiness.wallet_kind,
                target_adapter: readiness.target_adapter,
                deployment_digest,
                verified_block_number,
                verified_block_hash,
                confirmed_canary,
            },
        ))
    }
}

const fn approval_state_reason(
    desired_approval: Option<bool>,
    approved: bool,
) -> Option<SettlementGovernedActionBlockReason> {
    match (desired_approval, approved) {
        (Some(true), true) | (Some(false), false) => {
            Some(SettlementGovernedActionBlockReason::OperatorApprovalAlreadySatisfied)
        }
        (None, false) => Some(SettlementGovernedActionBlockReason::OperatorApprovalRequired),
        _ => None,
    }
}

const fn authorization_policy_matches(
    policy: SettlementWritePolicy,
    authorization_policy: EntryAuthorizationPolicy,
) -> bool {
    match policy {
        SettlementWritePolicy::Disabled => false,
        SettlementWritePolicy::GovernedCanary | SettlementWritePolicy::OperatorApproval => {
            matches!(
                authorization_policy,
                EntryAuthorizationPolicy::OperatorApprovalRequired
            )
        }
        SettlementWritePolicy::PolicyAutomatic => {
            matches!(
                authorization_policy,
                EntryAuthorizationPolicy::PolicyAutomatic
            )
        }
    }
}

const fn automatic_recovery_policy_matches(
    authorization_policy: EntryAuthorizationPolicy,
    policy: SettlementWritePolicy,
) -> bool {
    matches!(
        (authorization_policy, policy),
        (
            EntryAuthorizationPolicy::OperatorApprovalRequired,
            SettlementWritePolicy::OperatorApproval
        ) | (
            EntryAuthorizationPolicy::PolicyAutomatic,
            SettlementWritePolicy::PolicyAutomatic
        )
    )
}

fn action_preflight_token(scope: &SettlementGovernedActionScope) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_typed(
        SETTLEMENT_ACTION_PREFLIGHT_DOMAIN,
        SETTLEMENT_ACTION_PREFLIGHT_SCHEMA_VERSION,
        scope,
    )
    .map_err(Into::into)
}

fn same_action_scope_identity(
    requested: &SettlementGovernedActionScope,
    fresh: &SettlementGovernedActionScope,
) -> bool {
    match (requested, fresh) {
        (
            SettlementGovernedActionScope::OperatorApproval {
                execution_account_id,
                route,
                wallet_kind,
                target_adapter,
                deployment_digest,
                deployment_evidence_version,
                desired_approval,
                ..
            },
            SettlementGovernedActionScope::OperatorApproval {
                execution_account_id: fresh_account,
                route: fresh_route,
                wallet_kind: fresh_wallet,
                target_adapter: fresh_target,
                deployment_digest: fresh_deployment,
                deployment_evidence_version: fresh_evidence,
                desired_approval: fresh_desired,
                ..
            },
        ) => {
            execution_account_id == fresh_account
                && route == fresh_route
                && wallet_kind == fresh_wallet
                && target_adapter == fresh_target
                && deployment_digest == fresh_deployment
                && deployment_evidence_version == fresh_evidence
                && desired_approval == fresh_desired
        }
        (
            SettlementGovernedActionScope::Canary {
                execution_account_id,
                route,
                wallet_kind,
                target_adapter,
                deployment_digest,
                deployment_evidence_version,
                settlement_redeem_id,
                authorization_digest,
                maximum_payout_usd,
                ..
            },
            SettlementGovernedActionScope::Canary {
                execution_account_id: fresh_account,
                route: fresh_route,
                wallet_kind: fresh_wallet,
                target_adapter: fresh_target,
                deployment_digest: fresh_deployment,
                deployment_evidence_version: fresh_evidence,
                settlement_redeem_id: fresh_redeem,
                authorization_digest: fresh_authorization,
                maximum_payout_usd: fresh_payout,
                ..
            },
        ) => {
            execution_account_id == fresh_account
                && route == fresh_route
                && wallet_kind == fresh_wallet
                && target_adapter == fresh_target
                && deployment_digest == fresh_deployment
                && deployment_evidence_version == fresh_evidence
                && settlement_redeem_id == fresh_redeem
                && authorization_digest == fresh_authorization
                && maximum_payout_usd == fresh_payout
        }
        _ => false,
    }
}

struct GovernedActionColumns {
    kind: SettlementGovernedActionKind,
    route: SettlementRoute,
    target_adapter: EvmAddress,
    deployment_digest: ContentHash,
    deployment_evidence_version: SettlementEvidenceVersion,
    settlement_redeem_id: Option<SettlementRedeemId>,
    desired_approval: Option<bool>,
    authorization_digest: Option<ContentHash>,
    payout_ceiling_usd: Option<Usd>,
}

fn action_scope_columns(scope: &SettlementGovernedActionScope) -> GovernedActionColumns {
    match scope {
        SettlementGovernedActionScope::OperatorApproval {
            route,
            target_adapter,
            deployment_digest,
            deployment_evidence_version,
            desired_approval,
            ..
        } => GovernedActionColumns {
            kind: if *desired_approval {
                SettlementGovernedActionKind::OutcomeTokenApproval
            } else {
                SettlementGovernedActionKind::OutcomeTokenRevocation
            },
            route: *route,
            target_adapter: target_adapter.clone(),
            deployment_digest: *deployment_digest,
            deployment_evidence_version: deployment_evidence_version.clone(),
            settlement_redeem_id: None,
            desired_approval: Some(*desired_approval),
            authorization_digest: None,
            payout_ceiling_usd: None,
        },
        SettlementGovernedActionScope::Canary {
            route,
            target_adapter,
            deployment_digest,
            deployment_evidence_version,
            settlement_redeem_id,
            authorization_digest,
            maximum_payout_usd,
            ..
        } => GovernedActionColumns {
            kind: SettlementGovernedActionKind::CanaryGrant,
            route: *route,
            target_adapter: target_adapter.clone(),
            deployment_digest: *deployment_digest,
            deployment_evidence_version: deployment_evidence_version.clone(),
            settlement_redeem_id: Some(*settlement_redeem_id),
            desired_approval: None,
            authorization_digest: Some(*authorization_digest),
            payout_ceiling_usd: Some(*maximum_payout_usd),
        },
    }
}

#[async_trait]
impl SettlementControlPort for CoreSettlementControlPort {
    async fn readiness(&self, checked_at: DateTime<Utc>) -> QuantResult<SettlementReadinessView> {
        self.cached_ui_readiness(checked_at).await
    }

    async fn approve_authorization(
        &self,
        settlement_redeem_id: SettlementRedeemId,
        digest: ContentHash,
        actor: UserId,
        approved_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRedeemInfo> {
        let committed = self
            .repository
            .approve_authorization(ApproveSettlementAuthorization {
                settlement_redeem_id,
                digest,
                actor,
                approved_at,
            })
            .await?;
        self.lifecycle.committed(&committed);
        Ok(committed)
    }

    async fn revoke_authorization(
        &self,
        settlement_redeem_id: SettlementRedeemId,
        digest: ContentHash,
        actor: UserId,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRedeemInfo> {
        let committed = self
            .repository
            .revoke_authorization(RevokeSettlementAuthorization {
                settlement_redeem_id,
                digest,
                actor,
                revoked_at,
            })
            .await?;
        self.lifecycle.committed(&committed);
        Ok(committed)
    }

    async fn operator_approval_preflight(
        &self,
        request: SettlementOperatorApprovalPreflightRequest,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPreflightView> {
        let readiness = self.fresh_readiness(checked_at).await?;
        let action = if request.desired_approval {
            SettlementGovernedAction::OperatorApproval
        } else {
            SettlementGovernedAction::OperatorRevocation
        };
        let preflight = Self::preflight(
            action,
            request.route,
            Some(request.desired_approval),
            readiness,
        );
        let mut preflight = preflight;
        let controls = self.runtime_controls.snapshot();
        if preflight.readiness.settlement_write_policy != controls.settlement_write_policy
            || !authorization_policy_matches(
                controls.settlement_write_policy,
                controls.entry_authorization_policy,
            )
        {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::AuthorizationPolicyMismatch,
            );
        }
        if !preflight.allowed {
            return Ok(preflight);
        }
        let expires_at = self.preflight_expiry(checked_at)?;
        let route = exact_route_scope(&preflight.readiness, request.route)?.clone();
        let scope = SettlementGovernedActionScope::OperatorApproval {
            execution_account_id: self.execution_account_id,
            route: request.route,
            wallet_kind: route.wallet_kind,
            target_adapter: route.target_adapter.clone(),
            deployment_digest: route.deployment_digest.ok_or_else(|| {
                QuantError::config("ready settlement route has no deployment digest")
            })?,
            deployment_evidence_version: route.deployment_evidence_version,
            desired_approval: request.desired_approval,
            expires_at,
        };
        Self::issue_preflight(preflight, scope)
    }

    async fn canary_preflight(
        &self,
        request: SettlementCanaryPreflightRequest,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPreflightView> {
        let readiness = self.fresh_readiness(checked_at).await?;
        let mut preflight = Self::preflight(
            SettlementGovernedAction::Canary,
            request.route,
            None,
            readiness,
        );
        let controls = self.runtime_controls.snapshot();
        if controls.entry_authorization_policy != EntryAuthorizationPolicy::OperatorApprovalRequired
        {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::OperatorAuthorizationRequired,
            );
        }
        if controls.settlement_write_policy != SettlementWritePolicy::GovernedCanary
            || preflight.readiness.settlement_write_policy != controls.settlement_write_policy
        {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::AuthorizationPolicyMismatch,
            );
        }
        let case = self
            .repository
            .find_by_id(&request.settlement_redeem_id)
            .await?;
        let Some(case) = case else {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::SettlementCaseNotFound,
            );
            return Ok(preflight);
        };
        let route = exact_route_scope(&preflight.readiness, request.route)?.clone();
        let case_scope_matches = case.execution_account_id == self.execution_account_id
            && case.route == request.route
            && case.wallet_kind == route.wallet_kind
            && case.target_adapter.as_ref() == Some(&route.target_adapter)
            && case.deployment_digest == route.deployment_digest
            && case.deployment_evidence_version.as_ref()
                == Some(&route.deployment_evidence_version);
        if !case_scope_matches {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::SettlementCaseScopeMismatch,
            );
        }
        if case.effective_policy != SettlementEffectivePolicy::AutomaticEligible {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::ManualOnlyInventory,
            );
        }
        if self
            .repository
            .count_unsettled_execution_orders(&case.market_id, &case.execution_account_id)
            .await?
            > 0
        {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::ExecutionNotQuiescent,
            );
        }
        if case.authorization_state != SettlementAuthorizationState::Approved
            || case.authorization_digest.is_none()
            || case
                .authorization_expires_at
                .is_none_or(|expires_at| expires_at <= checked_at)
        {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::SettlementAuthorizationNotApproved,
            );
        }
        if !request.maximum_payout_usd.is_positive()
            || case
                .expected_payout_usd
                .is_none_or(|expected| expected > request.maximum_payout_usd)
        {
            push_action_reason(
                &mut preflight,
                SettlementGovernedActionBlockReason::CanaryPayoutLimitExceeded,
            );
        }
        if !preflight.allowed {
            return Ok(preflight);
        }
        let expires_at = self.preflight_expiry(checked_at)?;
        let authorization_digest = case.authorization_digest.ok_or_else(|| {
            QuantError::config("eligible settlement canary has no authorization digest")
        })?;
        let deployment_digest = route.deployment_digest.ok_or_else(|| {
            QuantError::config("ready settlement canary route has no deployment digest")
        })?;
        let scope = SettlementGovernedActionScope::Canary {
            execution_account_id: self.execution_account_id,
            route: request.route,
            wallet_kind: route.wallet_kind,
            target_adapter: route.target_adapter.clone(),
            deployment_digest,
            deployment_evidence_version: route.deployment_evidence_version,
            settlement_redeem_id: request.settlement_redeem_id,
            authorization_digest,
            maximum_payout_usd: request.maximum_payout_usd,
            expires_at,
        };
        Self::issue_preflight(preflight, scope)
    }

    async fn apply_governed_action(
        &self,
        request: SettlementGovernedActionApplyRequest,
        actor: UserId,
        authorized_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionView> {
        self.create_governed_action(request, actor, authorized_at)
            .await
    }

    async fn list_governed_actions(
        &self,
        query: SettlementGovernedActionListQuery,
    ) -> QuantResult<Paginated<SettlementGovernedActionView>> {
        self.governance
            .page_actions(query)
            .await
            .map(|page| page.map(SettlementGovernedActionView::from))
            .map_err(Into::into)
    }

    async fn get_governed_action(
        &self,
        action_id: &SettlementGovernedActionId,
    ) -> QuantResult<Option<SettlementGovernedActionDetailView>> {
        let Some(action) = self.governance.find_action(action_id).await? else {
            return Ok(None);
        };
        let submission = self
            .governance
            .find_submission_by_action(action_id)
            .await?
            .map(SettlementChainSubmissionView::from);
        Ok(Some(SettlementGovernedActionDetailView {
            action: action.into(),
            submission,
        }))
    }

    async fn revoke_governed_action(
        &self,
        action_id: SettlementGovernedActionId,
        request: SettlementGovernedActionRevokeRequest,
        actor: UserId,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionView> {
        let reason = request.reason.trim();
        if reason.is_empty() {
            return Err(QuantError::config(
                "settlement governed-action revocation reason must not be blank",
            ));
        }
        self.governance
            .revoke_action(RevokeSettlementGovernedAction {
                settlement_governed_action_id: action_id,
                expected_scope_digest: request.scope_digest,
                actor,
                reason: reason.to_owned(),
                revoked_at,
            })
            .await
            .map(Into::into)
            .map_err(Into::into)
    }
}

fn provenance_view(
    provenance: &SettlementDeploymentProvenance,
) -> SettlementDeploymentProvenanceView {
    SettlementDeploymentProvenanceView {
        source: provenance.source,
        source_url: provenance.source_url.to_owned(),
        revision: provenance.revision.map(str::to_owned),
        retrieved_at: provenance.retrieved_at.to_owned(),
    }
}

fn ready_route_view(
    capability: &VerifiedSettlementDeployment,
    authority: SettlementDeploymentProvenanceView,
    corroboration: Option<SettlementDeploymentProvenanceView>,
    checked_at: DateTime<Utc>,
) -> SettlementRouteReadinessView {
    SettlementRouteReadinessView {
        route: capability.route(),
        wallet_kind: capability.wallet_kind(),
        status: SettlementReadinessStatus::Ready,
        blocking_reasons: Vec::new(),
        advisories: capability.advisories().to_vec(),
        authority,
        corroboration,
        target_adapter: capability.target().clone(),
        runtime_code_hash: capability.target_code_hash().clone(),
        observed_block_number: Some(capability.verified_block()),
        observed_block_hash: Some(capability.verified_block_hash().clone()),
        deployment_digest: Some(capability.deployment_digest()),
        deployment_evidence_version: capability.evidence_version().clone(),
        operator_approved: Some(capability.operator_approved()),
        checked_at,
    }
}

fn push_action_reason(
    preflight: &mut SettlementGovernedActionPreflightView,
    reason: SettlementGovernedActionBlockReason,
) {
    if !preflight.blocking_reasons.contains(&reason) {
        preflight.blocking_reasons.push(reason);
    }
    preflight.allowed = false;
    preflight.scope = None;
    preflight.preflight_token = None;
    preflight.expires_at = None;
}

fn exact_route_scope(
    readiness: &SettlementReadinessView,
    route: SettlementRoute,
) -> QuantResult<&SettlementRouteReadinessView> {
    readiness
        .routes
        .iter()
        .find(|item| item.route == route)
        .ok_or_else(|| QuantError::config(format!("settlement readiness omitted route {route}")))
}

#[must_use]
pub const fn settlement_credentials(
    wallet_kind: ExecutionWalletKind,
    relayer_ready: bool,
) -> SettlementCredentialAvailability {
    match wallet_kind {
        ExecutionWalletKind::Eoa => SettlementCredentialAvailability::DirectEoaSigner,
        ExecutionWalletKind::Proxy
        | ExecutionWalletKind::GnosisSafe
        | ExecutionWalletKind::DepositWallet
            if relayer_ready =>
        {
            SettlementCredentialAvailability::Relayer
        }
        ExecutionWalletKind::Proxy
        | ExecutionWalletKind::GnosisSafe
        | ExecutionWalletKind::DepositWallet => SettlementCredentialAvailability::Missing,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, TimeZone, Utc};
    use quant_pivot_models::{
        domain::api::settlement_redeem::{
            SettlementGovernedActionBlockReason, SettlementGovernedActionScope,
        },
        enums::{
            quant::{EntryAuthorizationPolicy, ExecutionWalletKind},
            settlement::{SettlementRoute, SettlementWritePolicy},
        },
        types::{
            ContentHash, EvmAddress, ExecutionAccountId, SettlementEvidenceVersion,
            SettlementRedeemId, Usd,
        },
    };
    use rust_decimal::Decimal;

    use super::{action_preflight_token, approval_state_reason, automatic_recovery_policy_matches};

    fn scope() -> SettlementGovernedActionScope {
        SettlementGovernedActionScope::Canary {
            execution_account_id: ExecutionAccountId::from_v7(),
            route: SettlementRoute::StandardV2,
            wallet_kind: ExecutionWalletKind::Eoa,
            target_adapter: EvmAddress::parse("0xada100db00ca00073811820692005400218fce1f")
                .expect("target adapter"),
            deployment_digest: ContentHash::from_bytes([0x31; 32]),
            deployment_evidence_version: SettlementEvidenceVersion::parse("test-v1")
                .expect("evidence version"),
            settlement_redeem_id: SettlementRedeemId::from_v7(),
            authorization_digest: ContentHash::from_bytes([0x32; 32]),
            maximum_payout_usd: Usd::ONE,
            expires_at: Utc
                .with_ymd_and_hms(2026, 7, 23, 1, 0, 0)
                .single()
                .expect("fixed expiry"),
        }
    }

    #[test]
    fn canary_preflight_binds_expiry() {
        let base = scope();
        let base_token = action_preflight_token(&base).expect("base preflight token");

        let mut changed_digest = base.clone();
        if let SettlementGovernedActionScope::Canary {
            deployment_digest, ..
        } = &mut changed_digest
        {
            *deployment_digest = ContentHash::from_bytes([0x33; 32]);
        }
        assert_ne!(
            action_preflight_token(&changed_digest).expect("changed digest token"),
            base_token
        );

        let mut changed_wallet = base.clone();
        if let SettlementGovernedActionScope::Canary { wallet_kind, .. } = &mut changed_wallet {
            *wallet_kind = ExecutionWalletKind::Proxy;
        }
        assert_ne!(
            action_preflight_token(&changed_wallet).expect("changed wallet token"),
            base_token
        );

        let mut changed_limit = base.clone();
        if let SettlementGovernedActionScope::Canary {
            maximum_payout_usd, ..
        } = &mut changed_limit
        {
            *maximum_payout_usd = Usd::new(Decimal::new(2, 0));
        }
        assert_ne!(
            action_preflight_token(&changed_limit).expect("changed limit token"),
            base_token
        );

        let mut changed_expiry = base;
        if let SettlementGovernedActionScope::Canary { expires_at, .. } = &mut changed_expiry {
            *expires_at += TimeDelta::seconds(1);
        }
        assert_ne!(
            action_preflight_token(&changed_expiry).expect("changed expiry token"),
            base_token
        );
    }

    #[test]
    fn deployment_readiness_operator_gates() {
        assert_eq!(approval_state_reason(Some(true), false), None);
        assert_eq!(
            approval_state_reason(Some(true), true),
            Some(SettlementGovernedActionBlockReason::OperatorApprovalAlreadySatisfied)
        );
        assert_eq!(
            approval_state_reason(None, false),
            Some(SettlementGovernedActionBlockReason::OperatorApprovalRequired)
        );
        assert_eq!(approval_state_reason(None, true), None);
    }

    #[test]
    fn automatic_recovery_requires_pair() {
        assert!(automatic_recovery_policy_matches(
            EntryAuthorizationPolicy::OperatorApprovalRequired,
            SettlementWritePolicy::OperatorApproval,
        ));
        assert!(automatic_recovery_policy_matches(
            EntryAuthorizationPolicy::PolicyAutomatic,
            SettlementWritePolicy::PolicyAutomatic,
        ));
        assert!(!automatic_recovery_policy_matches(
            EntryAuthorizationPolicy::OperatorApprovalRequired,
            SettlementWritePolicy::GovernedCanary,
        ));
        assert!(!automatic_recovery_policy_matches(
            EntryAuthorizationPolicy::PolicyAutomatic,
            SettlementWritePolicy::OperatorApproval,
        ));
        assert!(!automatic_recovery_policy_matches(
            EntryAuthorizationPolicy::OperatorApprovalRequired,
            SettlementWritePolicy::Disabled,
        ));
    }
}
