//! Capability-declared domain-source expectations and lifecycle rules.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_domain_source_expectation,
    enums::domain::{DomainFamily, DomainSourceExpectationStatus},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceExpectationId, DomainSourceId, MarketId,
    },
};

/// Canonically ordered markets affected by one expected source binding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct AffectedMarketIds(pub Vec<MarketId>);

impl AffectedMarketIds {
    #[must_use]
    pub fn new(mut market_ids: Vec<MarketId>) -> Self {
        market_ids.sort();
        market_ids.dedup();
        Self(market_ids)
    }
}

/// Canonically ordered immutable research profiles affected by a binding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct AffectedProfileIds(pub Vec<String>);

impl AffectedProfileIds {
    #[must_use]
    pub fn new(mut profile_ids: Vec<String>) -> Self {
        profile_ids.sort();
        profile_ids.dedup();
        Self(profile_ids)
    }
}

/// Persisted expected source binding, including honest pre-cursor health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_domain_source_expectation::Entity")]
pub struct DomainSourceExpectationInfo {
    pub expectation_id: DomainSourceExpectationId,
    pub family: DomainFamily,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub capability_registry_hash: ContentHash,
    pub binding_hash: ContentHash,
    pub required: bool,
    pub credential_required: bool,
    pub freshness_secs: i64,
    pub affected_market_ids: AffectedMarketIds,
    pub affected_profile_ids: AffectedProfileIds,
    pub status: DomainSourceExpectationStatus,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    DomainSourceExpectationInfo,
    quant_domain_source_expectation::Model,
    {
        expectation_id,
        family,
        source_id,
        instrument_key,
        capability_registry_hash,
        binding_hash,
        required,
        credential_required,
        freshness_secs,
        affected_market_ids,
        affected_profile_ids,
        status,
        status_reason,
        created_at,
        updated_at,
    }
);

/// Complete upsert payload emitted by capability reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_domain_source_expectation::ActiveModel")]
pub struct UpsertDomainSourceExpectation {
    pub expectation_id: DomainSourceExpectationId,
    pub family: DomainFamily,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub capability_registry_hash: ContentHash,
    pub binding_hash: ContentHash,
    pub required: bool,
    pub credential_required: bool,
    pub freshness_secs: i64,
    pub affected_market_ids: AffectedMarketIds,
    pub affected_profile_ids: AffectedProfileIds,
    pub status: DomainSourceExpectationStatus,
    pub status_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// Static definition resolved from the immutable capability registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainSourceExpectationDefinition {
    pub family: DomainFamily,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub capability_registry_hash: ContentHash,
    pub required: bool,
    pub credential_required: bool,
    pub freshness_secs: i64,
    pub affected_market_ids: Vec<MarketId>,
    pub affected_profile_ids: Vec<String>,
}

impl UpsertDomainSourceExpectation {
    pub fn identity_id(
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> Result<DomainSourceExpectationId, String> {
        let identity_hash = CanonicalDigest::content_hash_json(&(
            "domain_source_expectation_identity_v1",
            source_id,
            instrument_key,
        ))
        .map_err(|error| format!("domain source expectation identity hash failed: {error}"))?;
        Ok(DomainSourceExpectationId::from_identity_hash(
            &identity_hash,
        ))
    }

    /// Build the content-addressed source definition and validate its initial state.
    pub fn new(
        definition: DomainSourceExpectationDefinition,
        status: DomainSourceExpectationStatus,
        status_reason: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<Self, String> {
        let DomainSourceExpectationDefinition {
            family,
            source_id,
            instrument_key,
            capability_registry_hash,
            required,
            credential_required,
            freshness_secs,
            affected_market_ids,
            affected_profile_ids,
        } = definition;
        let affected_market_ids = AffectedMarketIds::new(affected_market_ids);
        let affected_profile_ids = AffectedProfileIds::new(affected_profile_ids);
        if affected_profile_ids.0.is_empty() {
            return Err("domain source expectation must affect at least one profile".to_owned());
        }
        if freshness_secs <= 0 {
            return Err("domain source expectation freshness must be positive".to_owned());
        }
        validate_status_reason(status, status_reason.as_deref())?;
        let expectation_id = Self::identity_id(&source_id, &instrument_key)?;
        let binding_hash = CanonicalDigest::content_hash_json(&(
            "domain_source_expectation_v1",
            family,
            &source_id,
            &instrument_key,
            &capability_registry_hash,
            required,
            credential_required,
            freshness_secs,
            &affected_market_ids,
            &affected_profile_ids,
        ))
        .map_err(|error| format!("domain source expectation hash failed: {error}"))?;
        Ok(Self {
            expectation_id,
            family,
            source_id,
            instrument_key,
            capability_registry_hash,
            binding_hash,
            required,
            credential_required,
            freshness_secs,
            affected_market_ids,
            affected_profile_ids,
            status,
            status_reason,
            updated_at,
        })
    }
}

/// Optimistic lifecycle transition for one expectation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainSourceExpectationTransition {
    pub expectation_id: DomainSourceExpectationId,
    pub from: DomainSourceExpectationStatus,
    pub to: DomainSourceExpectationStatus,
    pub reason: Option<String>,
}

impl DomainSourceExpectationTransition {
    pub fn validate(&self) -> Result<(), String> {
        if self.from == self.to {
            return Err("domain source expectation transition must change status".to_owned());
        }
        let allowed = match self.from {
            DomainSourceExpectationStatus::NotStarted => true,
            DomainSourceExpectationStatus::Live => matches!(
                self.to,
                DomainSourceExpectationStatus::Stale
                    | DomainSourceExpectationStatus::Failed
                    | DomainSourceExpectationStatus::Unsupported
            ),
            DomainSourceExpectationStatus::Stale => matches!(
                self.to,
                DomainSourceExpectationStatus::Live
                    | DomainSourceExpectationStatus::Failed
                    | DomainSourceExpectationStatus::Unsupported
            ),
            DomainSourceExpectationStatus::CredentialBlocked => matches!(
                self.to,
                DomainSourceExpectationStatus::NotStarted
                    | DomainSourceExpectationStatus::Unsupported
            ),
            DomainSourceExpectationStatus::Failed => matches!(
                self.to,
                DomainSourceExpectationStatus::NotStarted
                    | DomainSourceExpectationStatus::Live
                    | DomainSourceExpectationStatus::Stale
                    | DomainSourceExpectationStatus::Unsupported
            ),
            DomainSourceExpectationStatus::Unsupported => matches!(
                self.to,
                DomainSourceExpectationStatus::NotStarted
                    | DomainSourceExpectationStatus::CredentialBlocked
            ),
        };
        if !allowed {
            return Err(format!(
                "illegal domain source expectation transition {} -> {}",
                self.from, self.to
            ));
        }
        validate_status_reason(self.to, self.reason.as_deref())
    }
}

fn validate_status_reason(
    status: DomainSourceExpectationStatus,
    reason: Option<&str>,
) -> Result<(), String> {
    if matches!(
        status,
        DomainSourceExpectationStatus::CredentialBlocked
            | DomainSourceExpectationStatus::Failed
            | DomainSourceExpectationStatus::Unsupported
    ) && reason.is_none_or(|value| value.trim().is_empty())
    {
        return Err(format!(
            "domain source expectation status {status} requires a reason"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{
        DomainSourceExpectationDefinition, DomainSourceExpectationTransition,
        UpsertDomainSourceExpectation,
    };
    use crate::{
        enums::domain::{DomainFamily, DomainSourceExpectationStatus},
        types::{ContentHash, DomainInstrumentKey, DomainSourceExpectationId, DomainSourceId},
    };

    fn hash(fill: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
    }

    #[test]
    fn definition_hash_is_canonical_and_identity_is_stable() {
        let build = |profiles: Vec<String>| {
            UpsertDomainSourceExpectation::new(
                DomainSourceExpectationDefinition {
                    family: DomainFamily::Crypto,
                    source_id: DomainSourceId::binance(),
                    instrument_key: DomainInstrumentKey::new("BINANCE:BTCUSDT:1m"),
                    capability_registry_hash: hash('a'),
                    required: true,
                    credential_required: false,
                    freshness_secs: 120,
                    affected_market_ids: Vec::new(),
                    affected_profile_ids: profiles,
                },
                DomainSourceExpectationStatus::NotStarted,
                None,
                Utc::now(),
            )
            .expect("expectation")
        };
        let left = build(vec!["weather".to_owned(), "crypto".to_owned()]);
        let right = build(vec!["crypto".to_owned(), "weather".to_owned()]);
        assert_eq!(left.binding_hash, right.binding_hash);
        assert_eq!(left.expectation_id, right.expectation_id);
    }

    #[test]
    fn coverage_changes_binding_without_changing_source_identity() {
        let build = |profiles: Vec<String>| {
            UpsertDomainSourceExpectation::new(
                DomainSourceExpectationDefinition {
                    family: DomainFamily::Crypto,
                    source_id: DomainSourceId::binance(),
                    instrument_key: DomainInstrumentKey::new("BINANCE:BTCUSDT:1m"),
                    capability_registry_hash: hash('a'),
                    required: true,
                    credential_required: false,
                    freshness_secs: 120,
                    affected_market_ids: Vec::new(),
                    affected_profile_ids: profiles,
                },
                DomainSourceExpectationStatus::NotStarted,
                None,
                Utc::now(),
            )
            .expect("expectation")
        };
        let before = build(vec!["crypto_price_15m".to_owned()]);
        let after = build(vec![
            "crypto_price_15m".to_owned(),
            "crypto_price_hourly".to_owned(),
        ]);
        assert_eq!(before.expectation_id, after.expectation_id);
        assert_ne!(before.binding_hash, after.binding_hash);
    }

    #[test]
    fn lifecycle_rejects_false_health_and_missing_blocker_reason() {
        let invalid = DomainSourceExpectationTransition {
            expectation_id: DomainSourceExpectationId::from_v7(),
            from: DomainSourceExpectationStatus::CredentialBlocked,
            to: DomainSourceExpectationStatus::Live,
            reason: None,
        };
        assert!(invalid.validate().is_err());

        let missing_reason = DomainSourceExpectationTransition {
            expectation_id: DomainSourceExpectationId::from_v7(),
            from: DomainSourceExpectationStatus::Live,
            to: DomainSourceExpectationStatus::Failed,
            reason: None,
        };
        assert!(missing_reason.validate().is_err());
    }
}
