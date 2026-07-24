//! Immutable per-market capability classification for Crypto and Weather.

use quant_pivot_error::hashing::CanonicalDigestError;
use serde::{Deserialize, Serialize};

use crate::{
    enums::domain::DomainFamily,
    hashing::CanonicalDigest,
    types::{
        ContentHash, MarketId, ResolverVersion,
        domain_capability::{DomainCapabilityReasonCode, DomainContractFamily},
    },
};

/// Current immutable catalog-classification envelope version.
pub const DOMAIN_CATALOG_CLASSIFICATION_FORMAT_VERSION: u32 = 1;

/// One market's explicit capability disposition.
///
/// `UnsupportedTemplate` is deliberately not a completion terminal. It is a
/// typed blocker that replaces the old generic unresolved bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DomainMarketClassificationOutcome {
    Supported,
    CredentialBlocked {
        reason_code: DomainCapabilityReasonCode,
    },
    InsufficientEvidence {
        reason_code: DomainCapabilityReasonCode,
    },
    Excluded {
        reason_code: DomainCapabilityReasonCode,
    },
    UnsupportedTemplate {
        reason_code: DomainCapabilityReasonCode,
    },
}

impl DomainMarketClassificationOutcome {
    /// Whether the outcome is one of the four allowed local-completion states.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::UnsupportedTemplate { .. })
    }
}

/// Deterministic classification of one active catalog market.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DomainMarketClassification {
    pub market_id: MarketId,
    pub family: DomainFamily,
    pub contract_family: Option<DomainContractFamily>,
    pub outcome: DomainMarketClassificationOutcome,
    /// Hash of the complete market + event catalog objects used by the classifier.
    pub metadata_hash: ContentHash,
}

/// Content-addressed full-catalog classification evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainCatalogClassificationArtifact {
    pub format_version: u32,
    pub resolver_version: ResolverVersion,
    pub capability_registry_hash: ContentHash,
    pub catalog_hash: ContentHash,
    pub classifications: Vec<DomainMarketClassification>,
    pub artifact_hash: ContentHash,
}

impl DomainCatalogClassificationArtifact {
    /// Canonicalize and content-address a complete classification scan.
    pub fn new(
        resolver_version: ResolverVersion,
        capability_registry_hash: ContentHash,
        mut classifications: Vec<DomainMarketClassification>,
    ) -> Result<Self, CanonicalDigestError> {
        classifications.sort();
        classifications.dedup();
        let catalog_hash = catalog_hash(&classifications)?;
        let artifact_hash = Self::compute_hash(
            resolver_version,
            &capability_registry_hash,
            &catalog_hash,
            &classifications,
        )?;
        Ok(Self {
            format_version: DOMAIN_CATALOG_CLASSIFICATION_FORMAT_VERSION,
            resolver_version,
            capability_registry_hash,
            catalog_hash,
            classifications,
            artifact_hash,
        })
    }

    /// Verify version, uniqueness, scope integrity and both content hashes.
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != DOMAIN_CATALOG_CLASSIFICATION_FORMAT_VERSION {
            return Err(format!(
                "unsupported domain catalog classification format {}, expected {}",
                self.format_version, DOMAIN_CATALOG_CLASSIFICATION_FORMAT_VERSION
            ));
        }
        if self.classifications.is_empty() {
            return Err("domain catalog classification artifact is empty".to_owned());
        }
        if self
            .classifications
            .windows(2)
            .any(|rows| rows[0].market_id >= rows[1].market_id)
        {
            return Err(
                "domain catalog classifications must have unique, sorted market ids".to_owned(),
            );
        }
        if self.classifications.iter().any(|row| {
            row.contract_family
                .is_some_and(|contract_family| (contract_family).family_for() != row.family)
        }) {
            return Err("domain catalog classification family mismatch".to_owned());
        }
        let expected_catalog = catalog_hash(&self.classifications).map_err(|e| e.to_string())?;
        if self.catalog_hash != expected_catalog {
            return Err("domain catalog classification catalog hash mismatch".to_owned());
        }
        let expected_artifact = Self::compute_hash(
            self.resolver_version,
            &self.capability_registry_hash,
            &self.catalog_hash,
            &self.classifications,
        )
        .map_err(|e| e.to_string())?;
        if self.artifact_hash != expected_artifact {
            return Err("domain catalog classification artifact hash mismatch".to_owned());
        }
        Ok(())
    }

    /// Count blockers which must be zero before local completion.
    #[must_use]
    pub fn unsupported_template_count(&self) -> usize {
        self.classifications
            .iter()
            .filter(|row| {
                matches!(
                    row.outcome,
                    DomainMarketClassificationOutcome::UnsupportedTemplate { .. }
                )
            })
            .count()
    }

    fn compute_hash(
        resolver_version: ResolverVersion,
        capability_registry_hash: &ContentHash,
        catalog_hash: &ContentHash,
        classifications: &[DomainMarketClassification],
    ) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(&(
            "domain_catalog_classification_v1",
            DOMAIN_CATALOG_CLASSIFICATION_FORMAT_VERSION,
            resolver_version,
            capability_registry_hash,
            catalog_hash,
            classifications,
        ))
    }
}

impl DomainContractFamily {
    const fn family_for(self) -> DomainFamily {
        match self {
            Self::CryptoDirection | Self::CryptoThreshold | Self::CryptoBand => {
                DomainFamily::Crypto
            }
            Self::WeatherDailyTemperature
            | Self::WeatherPrecipitation
            | Self::WeatherAqi
            | Self::WeatherTornado
            | Self::WeatherTropicalCyclone
            | Self::WeatherGlobalTemperature
            | Self::WeatherSeaIce
            | Self::WeatherWindExtreme => DomainFamily::Weather,
        }
    }
}

fn catalog_hash(
    classifications: &[DomainMarketClassification],
) -> Result<ContentHash, CanonicalDigestError> {
    let inputs: Vec<_> = classifications
        .iter()
        .map(|row| (&row.market_id, &row.metadata_hash))
        .collect();
    CanonicalDigest::content_hash_json(&("domain_catalog_input_v1", inputs))
}

#[cfg(test)]
mod tests {
    use super::{
        DomainCatalogClassificationArtifact, DomainMarketClassification,
        DomainMarketClassificationOutcome,
    };
    use crate::{
        enums::domain::DomainFamily,
        types::{
            ContentHash, MarketId, ResolverVersion,
            domain_capability::{DomainCapabilityReasonCode, DomainContractFamily},
        },
    };

    fn hash(fill: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
    }

    fn row(id: &str, metadata: char) -> DomainMarketClassification {
        DomainMarketClassification {
            market_id: MarketId::new(id),
            family: DomainFamily::Weather,
            contract_family: Some(DomainContractFamily::WeatherPrecipitation),
            outcome: DomainMarketClassificationOutcome::UnsupportedTemplate {
                reason_code: DomainCapabilityReasonCode::RecognizedWeatherFamilyParserUnavailable,
            },
            metadata_hash: hash(metadata),
        }
    }

    #[test]
    fn artifact_order_rejects_tampering() {
        let left = DomainCatalogClassificationArtifact::new(
            ResolverVersion::new(4),
            hash('c'),
            vec![row("b", 'b'), row("a", 'a')],
        )
        .expect("artifact");
        let right = DomainCatalogClassificationArtifact::new(
            ResolverVersion::new(4),
            hash('c'),
            vec![row("a", 'a'), row("b", 'b')],
        )
        .expect("artifact");
        assert_eq!(left, right);
        assert_eq!(left.unsupported_template_count(), 2);
        assert!(left.validate().is_ok());

        let mut tampered = left;
        tampered.classifications[0].metadata_hash = hash('d');
        assert!(tampered.validate().is_err());
    }
}
