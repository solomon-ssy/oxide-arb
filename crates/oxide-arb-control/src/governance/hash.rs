//! Canonical BLAKE3 digest for control-factor publications.

use chrono::{DateTime, Utc};
use oxide_arb_error::control::GovernanceError;
use oxide_arb_models::{
    domain::control_factor::ControlFactorPublication,
    types::{ControlFactorId, FactorPublicationId},
};
use serde::Serialize;

/// Canonical publication fields hashed for tamper detection (excludes `publication_hash` and `status`).
#[derive(Serialize)]
struct PublicationCanonicalInput<'a> {
    publication_id: &'a str,
    mode: &'a str,
    factor_ids: Vec<String>,
    previous_publication_id: Option<&'a str>,
    effective_from: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    approved_by: Option<&'a str>,
    approval_reason: &'a str,
}

pub struct PublicationHasher;

impl PublicationHasher {
    /// Computes `blake3:<hex>` over the canonical publication payload.
    pub fn compute(publication: &ControlFactorPublication) -> Result<String, GovernanceError> {
        let digest = canonical_digest(publication)?;
        Ok(format!("blake3:{}", hex::encode(digest)))
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

fn canonical_digest(publication: &ControlFactorPublication) -> Result<[u8; 32], GovernanceError> {
    let mut factor_ids = publication
        .factor_ids
        .iter()
        .map(ControlFactorId::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    factor_ids.sort();

    let input = PublicationCanonicalInput {
        publication_id: publication.publication_id.as_str(),
        mode: publication.mode.as_str(),
        factor_ids,
        previous_publication_id: publication
            .previous_publication_id
            .as_ref()
            .map(FactorPublicationId::as_str),
        effective_from: publication.effective_from,
        expires_at: publication.expires_at,
        approved_by: publication.approved_by.as_deref(),
        approval_reason: publication.approval_reason.as_str(),
    };

    let bytes = serde_json::to_vec(&input)
        .map_err(|error| GovernanceError::HashInput(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
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

    fn sample_publication(factor_ids: Vec<&str>) -> ControlFactorPublication {
        let effective_from = Utc
            .with_ymd_and_hms(2026, 6, 2, 12, 0, 0)
            .single()
            .expect("fixed test timestamp");
        ControlFactorPublication {
            publication_id: FactorPublicationId::new("cfp_test"),
            mode: PublicationMode::Shadow,
            factor_ids: factor_ids.into_iter().map(ControlFactorId::new).collect(),
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
