//! Governed settlement control-plane boundary.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::settlement_redeem::{
            SettlementCanaryPreflightRequest, SettlementGovernedActionApplyRequest,
            SettlementGovernedActionDetailView, SettlementGovernedActionListQuery,
            SettlementGovernedActionPreflightView, SettlementGovernedActionRevokeRequest,
            SettlementGovernedActionView, SettlementOperatorApprovalPreflightRequest,
            SettlementReadinessView,
        },
        pagination::Paginated,
        quant::settlement::SettlementRedeemInfo,
    },
    types::{ContentHash, SettlementGovernedActionId, SettlementRedeemId, UserId},
};

/// Read-only deployment truth plus exact operator-authorization mutations.
///
/// Operator-approval and canary preflights are signer-free. A canary token
/// binds the exact case, payout limit, route, wallet kind, deployment digest,
/// authorization digest, and expiry; funds apply remains a separate release.
#[async_trait]
pub trait SettlementControlPort: Send + Sync {
    async fn readiness(&self, checked_at: DateTime<Utc>) -> QuantResult<SettlementReadinessView>;

    async fn approve_authorization(
        &self,
        settlement_redeem_id: SettlementRedeemId,
        digest: ContentHash,
        actor: UserId,
        approved_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRedeemInfo>;

    async fn revoke_authorization(
        &self,
        settlement_redeem_id: SettlementRedeemId,
        digest: ContentHash,
        actor: UserId,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRedeemInfo>;

    async fn operator_approval_preflight(
        &self,
        request: SettlementOperatorApprovalPreflightRequest,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPreflightView>;

    async fn canary_preflight(
        &self,
        request: SettlementCanaryPreflightRequest,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPreflightView>;

    async fn apply_governed_action(
        &self,
        request: SettlementGovernedActionApplyRequest,
        actor: UserId,
        authorized_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionView>;

    async fn list_governed_actions(
        &self,
        query: SettlementGovernedActionListQuery,
    ) -> QuantResult<Paginated<SettlementGovernedActionView>>;

    async fn get_governed_action(
        &self,
        action_id: &SettlementGovernedActionId,
    ) -> QuantResult<Option<SettlementGovernedActionDetailView>>;

    async fn revoke_governed_action(
        &self,
        action_id: SettlementGovernedActionId,
        request: SettlementGovernedActionRevokeRequest,
        actor: UserId,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionView>;
}
