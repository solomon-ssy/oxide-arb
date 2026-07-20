//! Canonical state hashes for WORM operation-log rows written inside PG transactions.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::NewOperationLog, hashing::canonical_state_hash, types::ContentHash,
};
use serde::Serialize;

pub fn hash_state<T: Serialize>(value: &T) -> Result<ContentHash, StorageError> {
    canonical_state_hash(value)
        .map_err(|error| StorageError::Codec(format!("canonical state hash failed: {error}")))
}

pub fn apply_transition_hashes<TBefore, TAfter>(
    mut log: NewOperationLog,
    before: &TBefore,
    after: &TAfter,
) -> Result<NewOperationLog, StorageError>
where
    TBefore: Serialize,
    TAfter: Serialize,
{
    log.before_hash = Some(hash_state(before)?);
    log.after_hash = Some(hash_state(after)?);
    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::apply_transition_hashes;
    use quant_pivot_models::{
        domain::NewOperationLog,
        enums::{
            operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
            rbac::ResourceType,
        },
        types::{OperationDetailDocument, OperationLogId},
    };

    #[test]
    fn apply_transition_hashes_populates_before_and_after() {
        let log = NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: "test".into(),
            actor_user_id: None,
            actor_username: Some("system".to_owned()),
            acting_role: None,
            category: OperationCategory::System,
            action: "test.transition".into(),
            resource_type: Some(ResourceType::System),
            resource_id: Some("system".to_owned()),
            http_method: OperationHttpMethod::System,
            http_path: "/system/test".to_owned(),
            http_status: 200,
            outcome: OperationOutcome::Success,
            client_ip: None,
            user_agent: None,
            latency_ms: 0,
            detail: OperationDetailDocument::empty(),
            before_hash: None,
            after_hash: None,
            governance_audit_event_id: None,
            governance_audit_sequence: None,
        };
        let enriched = apply_transition_hashes(
            log,
            &serde_json::json!({"before": 1}),
            &serde_json::json!({"after": 2}),
        )
        .expect("transition hashes");
        assert!(enriched.before_hash.is_some());
        assert!(enriched.after_hash.is_some());
        assert_ne!(enriched.before_hash, enriched.after_hash);
    }
}
