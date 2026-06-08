//! `ControlFactorRegistry` — governance orchestration over the repository.
//!
//! Each mutating method takes an `AuditActor` envelope (actor / role /
//! `request_id` / reason — role *authorization* is enforced at the transport
//! boundary, see
//! `docs/plans/phase6-web-layer.md` §13), validates governance invariants, builds
//! the chained-audit content, and delegates to a single atomic repository
//! operation. The service never mutates state outside the repository.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use oxide_arb_error::control::{GovernanceError, RegistryError};
use oxide_arb_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo,
        control_factor::{
            AuditActor, ControlFactorPublicationInfo, ControlFactorValue, ControlFactorValueInfo,
            ExpireFactorsOutcome, NewControlFactorAuditEvent, PublishPublicationOutcome,
        },
    },
    enums::control_factor::{AuditResourceType, ControlAuditEventType, PublicationMode},
    types::{ControlFactorId, FactorPublicationId},
};
use oxide_arb_repository::traits::{ControlFactorRepository, RuntimeConfigVersionRepository};
use tokio::sync::Notify;

use crate::governance::publication::{PublicationDraft, PublicationManager};

/// Operator request to stage a Shadow / Published publication.
pub struct PublicationRequest {
    pub factor_ids: Vec<ControlFactorId>,
    pub idempotency_key: String,
    pub effective_from: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub manual_risk_expansion_approval: bool,
}

/// Governance service over the control-factor registry.
pub struct ControlFactorRegistry {
    repo: Arc<dyn ControlFactorRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    /// Wakes the live snapshot refresher after publication mutations. The notify
    /// is non-blocking and must never be awaited on the governance hot path.
    snapshot_refresh_notify: Option<Arc<Notify>>,
}

impl ControlFactorRegistry {
    #[must_use]
    pub fn new(
        repo: Arc<dyn ControlFactorRepository>,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    ) -> Self {
        Self {
            repo,
            runtime_config,
            snapshot_refresh_notify: None,
        }
    }

    /// Attach the live refresher wake handle (typically
    /// [`FactorRefresher::notify_handle`] in `oxide-arb-core`).
    #[must_use]
    pub fn with_snapshot_refresh_notify(mut self, notify: Arc<Notify>) -> Self {
        self.snapshot_refresh_notify = Some(notify);
        self
    }

    /// Rejects a candidate factor with a reason and chained audit.
    pub async fn reject_factor(
        &self,
        envelope: AuditActor,
        factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, RegistryError> {
        envelope.validate()?;
        let audit = factor_audit(ControlAuditEventType::FactorRejected, &envelope, factor_id);
        Ok(self
            .repo
            .reject_factor(factor_id, &envelope.reason, audit)
            .await?)
    }

    /// Promotes candidate factors into an active Shadow publication.
    pub async fn promote_to_shadow(
        &self,
        envelope: AuditActor,
        request: PublicationRequest,
    ) -> Result<PublishPublicationOutcome, RegistryError> {
        self.publish_internal(envelope, PublicationMode::Shadow, request)
            .await
    }

    /// Publishes shadow factors into an active Published publication.
    pub async fn publish(
        &self,
        envelope: AuditActor,
        request: PublicationRequest,
    ) -> Result<PublishPublicationOutcome, RegistryError> {
        self.publish_internal(envelope, PublicationMode::Published, request)
            .await
    }

    async fn publish_internal(
        &self,
        envelope: AuditActor,
        mode: PublicationMode,
        request: PublicationRequest,
    ) -> Result<PublishPublicationOutcome, RegistryError> {
        envelope.validate()?;

        let active = self.repo.load_active_publication(mode).await?;
        let previous = active.as_ref().map(|active| active.publication_id.clone());
        PublicationManager::check_rollback_target(mode, previous.as_ref(), active.is_some())?;

        if matches!(mode, PublicationMode::Published)
            && let Some(active) = &active
        {
            let active_factors = self.load_typed_factors(&active.factor_ids).await?;
            let new_factors = self.load_typed_factors(&request.factor_ids).await?;
            PublicationManager::check_risk_expansion(
                &new_factors,
                &active_factors,
                request.manual_risk_expansion_approval,
            )?;
        }

        let publication = PublicationManager::seal(PublicationDraft {
            publication_id: FactorPublicationId::from_v7(),
            mode,
            factor_ids: request.factor_ids,
            previous_publication_id: previous,
            effective_from: request.effective_from.unwrap_or_else(Utc::now),
            expires_at: request.expires_at,
            approved_by: envelope.actor.clone(),
            approval_reason: envelope.reason.clone(),
            idempotency_key: request.idempotency_key,
        })?;

        let audit = publication_audit(
            ControlAuditEventType::PublicationCreated,
            &envelope,
            &publication.publication_id,
            serde_json::json!({
                "mode": publication.mode,
                "factor_ids": publication.factor_ids,
                "manual_risk_expansion_approval": request.manual_risk_expansion_approval,
            }),
        );

        let outcome = self.repo.publish_publication(publication, audit).await?;
        self.signal_snapshot_refresh();
        Ok(outcome)
    }

    /// Rolls the active publication back to a known-good target.
    pub async fn rollback_publication(
        &self,
        envelope: AuditActor,
        active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
    ) -> Result<ControlFactorPublicationInfo, RegistryError> {
        envelope.validate()?;
        let audit = publication_audit(
            ControlAuditEventType::PublicationRolledBack,
            &envelope,
            active_publication_id,
            serde_json::json!({ "target_publication_id": target_publication_id }),
        );
        let info = self
            .repo
            .rollback_publication(active_publication_id, target_publication_id, audit)
            .await?;
        self.signal_snapshot_refresh();
        Ok(info)
    }

    /// Sweeps TTL-due factors, writing one chained `FactorExpired` audit each.
    pub async fn expire_due_factors(
        &self,
        envelope: AuditActor,
    ) -> Result<ExpireFactorsOutcome, RegistryError> {
        envelope.validate()?;
        Ok(self.repo.expire_factors(Utc::now(), envelope).await?)
    }

    /// Creates an immutable runtime-config version with chained audit.
    pub async fn create_runtime_config_version(
        &self,
        envelope: AuditActor,
        version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, RegistryError> {
        envelope.validate()?;
        let audit = runtime_config_audit(
            ControlAuditEventType::RuntimeConfigVersionCreated,
            &envelope,
            &version.runtime_config_version_id.to_string(),
            serde_json::json!({ "config_hash": version.config_hash }),
        );
        Ok(self
            .runtime_config
            .create_version_governed(version, audit)
            .await?)
    }

    /// Activates a runtime-config version, recording the chained audit event id
    /// on the activation row.
    pub async fn activate_runtime_config_version(
        &self,
        envelope: AuditActor,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, RegistryError> {
        envelope.validate()?;
        let audit = runtime_config_audit(
            ControlAuditEventType::RuntimeConfigActivated,
            &envelope,
            &activation.runtime_config_version_id.to_string(),
            serde_json::json!({
                "activation_kind": activation.activation_kind,
                "rollback_target_version_id": activation.rollback_target_version_id,
            }),
        );
        Ok(self
            .runtime_config
            .activate_version_governed(activation, audit)
            .await?)
    }

    async fn load_typed_factors(
        &self,
        factor_ids: &[ControlFactorId],
    ) -> Result<Vec<ControlFactorValue>, RegistryError> {
        let mut typed = Vec::with_capacity(factor_ids.len());
        for factor_id in factor_ids {
            if let Some(info) = self.repo.load_factor(factor_id).await? {
                typed.push(info.to_typed().map_err(GovernanceError::from)?);
            }
        }
        Ok(typed)
    }

    /// Wake the live refresher so publication changes propagate without waiting
    /// for the periodic poll fallback.
    fn signal_snapshot_refresh(&self) {
        if let Some(notify) = &self.snapshot_refresh_notify {
            notify.notify_one();
        }
    }
}

fn factor_audit(
    event_type: ControlAuditEventType,
    envelope: &AuditActor,
    factor_id: &ControlFactorId,
) -> NewControlFactorAuditEvent {
    NewControlFactorAuditEvent {
        event_type,
        actor: envelope.actor.clone(),
        actor_role: envelope.actor_role.clone(),
        resource_type: AuditResourceType::Factor,
        resource_id: factor_id.to_string(),
        request_id: envelope.request_id.clone(),
        reason: envelope.reason.clone(),
        before_hash: None,
        after_hash: None,
        diff: serde_json::json!({ "reason": envelope.reason }),
    }
}

fn publication_audit(
    event_type: ControlAuditEventType,
    envelope: &AuditActor,
    publication_id: &FactorPublicationId,
    diff: serde_json::Value,
) -> NewControlFactorAuditEvent {
    NewControlFactorAuditEvent {
        event_type,
        actor: envelope.actor.clone(),
        actor_role: envelope.actor_role.clone(),
        resource_type: AuditResourceType::Publication,
        resource_id: publication_id.to_string(),
        request_id: envelope.request_id.clone(),
        reason: envelope.reason.clone(),
        before_hash: None,
        after_hash: None,
        diff,
    }
}

fn runtime_config_audit(
    event_type: ControlAuditEventType,
    envelope: &AuditActor,
    version_id: &str,
    diff: serde_json::Value,
) -> NewControlFactorAuditEvent {
    NewControlFactorAuditEvent {
        event_type,
        actor: envelope.actor.clone(),
        actor_role: envelope.actor_role.clone(),
        resource_type: AuditResourceType::RuntimeConfigVersion,
        resource_id: version_id.to_owned(),
        request_id: envelope.request_id.clone(),
        reason: envelope.reason.clone(),
        before_hash: None,
        after_hash: None,
        diff,
    }
}
