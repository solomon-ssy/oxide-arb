//! Append-only audit hash-chain primitives.
//!
//! The governance audit log is a single global chain: each event carries a
//! monotonic `sequence`, a `prev_event_hash` linking to its predecessor, and an
//! `event_hash` computed over its canonical content. The repository assigns
//! `sequence` / `prev_event_hash` under an advisory lock; this module owns the
//! deterministic hash function and the chain verifier so the same logic is used
//! at write time and by independent auditors.

use super::persistence::ControlFactorAuditEventInfo;
use crate::{
    enums::control_factor::{AuditResourceType, ControlAuditEventType},
    hashing::CanonicalDigest,
};
use chrono::{DateTime, Utc};
use oxide_arb_error::control::{AuditChainError, CanonicalDigestError};
use serde::Serialize;

/// Canonical, hashable content of one audit event.
///
/// Field order is part of the hash contract; never reorder without a chain
/// migration. `serde` serializes struct fields in declaration order.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEventContent<'a> {
    pub sequence: i64,
    pub event_type: ControlAuditEventType,
    pub actor: &'a str,
    pub actor_role: &'a str,
    pub resource_type: AuditResourceType,
    pub resource_id: &'a str,
    pub request_id: &'a str,
    pub reason: &'a str,
    pub before_hash: Option<&'a str>,
    pub after_hash: Option<&'a str>,
    pub diff: &'a serde_json::Value,
    pub prev_event_hash: Option<&'a str>,
    pub created_at: DateTime<Utc>,
}

impl AuditEventContent<'_> {
    /// Computes the tamper-evident `blake3:<hex>` digest for this event.
    pub fn event_hash(&self) -> Result<String, CanonicalDigestError> {
        CanonicalDigest::blake3_json(self)
    }
}

/// Verifier for a stored audit chain slice.
pub struct AuditChain;

impl AuditChain {
    /// Verifies a slice of audit events ordered ascending by `sequence`.
    ///
    /// Checks, for each event: contiguous sequence, predecessor linkage within
    /// the slice, and that the recomputed `event_hash` matches the stored hash.
    /// If the slice begins at sequence 1 (chain genesis), its `prev_event_hash`
    /// must be absent. Sub-ranges (not starting at 1) are verified for internal
    /// consistency only.
    pub fn verify(events: &[ControlFactorAuditEventInfo]) -> Result<(), AuditChainError> {
        let mut prev: Option<&ControlFactorAuditEventInfo> = None;
        for event in events {
            if let Some(previous) = prev {
                let expected_sequence = previous.sequence + 1;
                if event.sequence != expected_sequence {
                    return Err(AuditChainError::SequenceGap {
                        expected: expected_sequence,
                        actual: event.sequence,
                    });
                }
                if event.prev_event_hash.as_deref() != Some(previous.event_hash.as_str()) {
                    return Err(AuditChainError::BrokenLink {
                        sequence: event.sequence,
                    });
                }
            } else if event.sequence == 1 && event.prev_event_hash.is_some() {
                return Err(AuditChainError::GenesisPrevNotNull {
                    sequence: event.sequence,
                });
            }

            let recomputed = AuditEventContent {
                sequence: event.sequence,
                event_type: event.event_type,
                actor: event.actor.as_str(),
                actor_role: event.actor_role.as_str(),
                resource_type: event.resource_type,
                resource_id: event.resource_id.as_str(),
                request_id: event.request_id.as_str(),
                reason: event.reason.as_str(),
                before_hash: event.before_hash.as_deref(),
                after_hash: event.after_hash.as_deref(),
                diff: &event.diff,
                prev_event_hash: event.prev_event_hash.as_deref(),
                created_at: event.created_at,
            }
            .event_hash()?;

            if recomputed != event.event_hash {
                return Err(AuditChainError::HashMismatch {
                    sequence: event.sequence,
                    expected: recomputed,
                    actual: event.event_hash.clone(),
                });
            }

            prev = Some(event);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{AuditChain, AuditEventContent};
    use crate::{
        domain::control_factor::ControlFactorAuditEventInfo,
        enums::control_factor::{AuditResourceType, ControlAuditEventType},
        types::AuditEventId,
    };
    use chrono::{TimeZone, Utc};
    use oxide_arb_error::control::AuditChainError;

    fn sealed_event(sequence: i64, prev_event_hash: Option<String>) -> ControlFactorAuditEventInfo {
        let created_at = Utc
            .with_ymd_and_hms(2026, 6, 5, 0, 0, u32::try_from(sequence).unwrap_or(0))
            .single()
            .expect("timestamp");
        let diff = serde_json::json!({ "seq": sequence });
        let event_hash = AuditEventContent {
            sequence,
            event_type: ControlAuditEventType::FactorCreated,
            actor: "op",
            actor_role: "operator",
            resource_type: AuditResourceType::Factor,
            resource_id: "cf_1",
            request_id: "req-1",
            reason: "test",
            before_hash: None,
            after_hash: None,
            diff: &diff,
            prev_event_hash: prev_event_hash.as_deref(),
            created_at,
        }
        .event_hash()
        .expect("hash");
        ControlFactorAuditEventInfo {
            event_id: AuditEventId::new_v7(),
            sequence,
            event_type: ControlAuditEventType::FactorCreated,
            actor: "op".into(),
            actor_role: "operator".into(),
            resource_type: AuditResourceType::Factor,
            resource_id: "cf_1".into(),
            request_id: "req-1".into(),
            reason: "test".into(),
            before_hash: None,
            after_hash: None,
            diff,
            prev_event_hash,
            event_hash,
            created_at,
        }
    }

    fn valid_chain() -> Vec<ControlFactorAuditEventInfo> {
        let first = sealed_event(1, None);
        let second = sealed_event(2, Some(first.event_hash.clone()));
        let third = sealed_event(3, Some(second.event_hash.clone()));
        vec![first, second, third]
    }

    #[test]
    fn verifies_a_well_formed_chain() {
        assert!(AuditChain::verify(&valid_chain()).is_ok());
        assert!(AuditChain::verify(&[]).is_ok());
    }

    #[test]
    fn detects_tampered_event_hash() {
        let mut chain = valid_chain();
        chain[1].reason = "tampered".into();
        assert!(matches!(
            AuditChain::verify(&chain),
            Err(AuditChainError::HashMismatch { sequence: 2, .. })
        ));
    }

    #[test]
    fn detects_sequence_gap() {
        let mut chain = valid_chain();
        chain.remove(1);
        assert!(matches!(
            AuditChain::verify(&chain),
            Err(AuditChainError::SequenceGap {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn detects_broken_prev_link() {
        let mut chain = valid_chain();
        chain[2].prev_event_hash = Some("blake3:forged".into());
        assert!(matches!(
            AuditChain::verify(&chain),
            Err(AuditChainError::HashMismatch { sequence: 3, .. }
                | AuditChainError::BrokenLink { sequence: 3 })
        ));
    }

    #[test]
    fn detects_genesis_with_predecessor() {
        let mut chain = valid_chain();
        chain[0].prev_event_hash = Some("blake3:ghost".into());
        // Genesis sequence==1 with a predecessor hash is rejected (either the
        // explicit genesis check or the recomputed-hash mismatch).
        assert!(AuditChain::verify(&chain).is_err());
    }
}
