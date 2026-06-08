//! Canonical BLAKE3 digest for control-factor publications.

use chrono::{DateTime, Utc};
use oxide_arb_error::control::GovernanceError;
use oxide_arb_models::{
    domain::control_factor::ControlFactorPublication, hashing::CanonicalDigest,
};
use serde::Serialize;

/// Canonical publication fields hashed for tamper detection (excludes `publication_hash` and `status`).
#[derive(Serialize)]
struct PublicationCanonicalInput {
    publication_id: String,
    mode: String,
    factor_ids: Vec<String>,
    previous_publication_id: Option<String>,
    effective_from: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    approved_by: Option<String>,
    approval_reason: String,
}

pub struct PublicationHasher;

impl PublicationHasher {
    /// Computes `blake3:<hex>` over the canonical publication payload.
    pub fn compute(publication: &ControlFactorPublication) -> Result<String, GovernanceError> {
        Ok(CanonicalDigest::blake3_json(&canonical_input(publication))?)
    }

    /// Recomputes the digest and compares it to `publication.publication_hash`.
    pub fn verify(publication: &ControlFactorPublication) -> Result<(), GovernanceError> {
        if publication.publication_hash.is_empty() {
            return Err(GovernanceError::PublicationHashMismatch {
                expected: "<non-empty>".into(),
                actual: String::new(),
            });
        }
        let expected = Self::compute(publication)?;
        if publication.publication_hash == expected {
            Ok(())
        } else {
            Err(GovernanceError::PublicationHashMismatch {
                expected,
                actual: publication.publication_hash.clone(),
            })
        }
    }

    /// Writes the canonical digest onto `publication.publication_hash`.
    pub fn seal(publication: &mut ControlFactorPublication) -> Result<(), GovernanceError> {
        publication.publication_hash = Self::compute(publication)?;
        Ok(())
    }
}

fn canonical_input(publication: &ControlFactorPublication) -> PublicationCanonicalInput {
    let mut factor_ids = publication
        .factor_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    factor_ids.sort();

    PublicationCanonicalInput {
        publication_id: publication.publication_id.to_string(),
        mode: publication.mode.as_str().to_owned(),
        factor_ids,
        previous_publication_id: publication
            .previous_publication_id
            .as_ref()
            .map(ToString::to_string),
        effective_from: publication.effective_from,
        expires_at: publication.expires_at,
        approved_by: publication.approved_by.clone(),
        approval_reason: publication.approval_reason.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::PublicationHasher;
    use chrono::{TimeZone, Utc};
    use oxide_arb_models::{
        domain::control_factor::ControlFactorPublication,
        enums::control_factor::{PublicationMode, PublicationStatus},
        types::{ControlFactorId, FactorPublicationId},
    };
    use oxide_arb_test_support::seeded_uuid;

    fn sample_publication(factor_ids: Vec<&str>) -> ControlFactorPublication {
        let effective_from = Utc
            .with_ymd_and_hms(2026, 6, 2, 12, 0, 0)
            .single()
            .expect("fixed test timestamp");
        ControlFactorPublication {
            publication_id: FactorPublicationId::new(seeded_uuid("cfp_test")),
            mode: PublicationMode::Shadow,
            factor_ids: factor_ids
                .into_iter()
                .map(|name| ControlFactorId::new(seeded_uuid(name)))
                .collect(),
            previous_publication_id: None,
            status: PublicationStatus::Pending,
            effective_from,
            expires_at: effective_from + chrono::Duration::days(1),
            approved_by: Some("operator".into()),
            approval_reason: "review".into(),
            publication_hash: String::new(),
        }
    }

    #[test]
    fn hash_is_order_independent_for_factor_ids() {
        let mut left = sample_publication(vec!["cf_a", "cf_b"]);
        let mut right = sample_publication(vec!["cf_b", "cf_a"]);
        PublicationHasher::seal(&mut left).unwrap();
        PublicationHasher::seal(&mut right).unwrap();
        assert_eq!(left.publication_hash, right.publication_hash);
    }

    #[test]
    fn verify_detects_tampering() {
        let mut publication = sample_publication(vec!["cf_a"]);
        PublicationHasher::seal(&mut publication).unwrap();
        publication.approval_reason.push('!');
        assert!(PublicationHasher::verify(&publication).is_err());
    }
}
