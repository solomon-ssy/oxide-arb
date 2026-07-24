//! Immutable capability registry for the Crypto and Weather verticals.

use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    enums::domain::{DomainFamily, LinkageSourceRole},
    hashing::CanonicalDigest,
    types::{ContentHash, DomainSourceId, ResearchProfileRef, ResolverVersion},
};

/// Current immutable capability-registry envelope version.
pub const DOMAIN_CAPABILITY_REGISTRY_FORMAT_VERSION: u32 = 1;

/// Closed contract families supported or explicitly classified by the catalog reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainContractFamily {
    CryptoDirection,
    CryptoThreshold,
    CryptoBand,
    WeatherDailyTemperature,
    WeatherPrecipitation,
    WeatherAqi,
    WeatherTornado,
    WeatherTropicalCyclone,
    WeatherGlobalTemperature,
    WeatherSeaIce,
    WeatherWindExtreme,
}

/// Stable machine-readable reason codes used by capability and catalog audits.
///
/// These values are a closed contract: operator-facing prose is derived at the
/// API/UI boundary, while immutable evidence stores only the semantic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainCapabilityReasonCode {
    ChainlinkDataStreamsCredentialsRequired,
    MatureLabelsUnavailable,
    StatisticalPowerBelowGate,
    EffectUncertaintyAboveGate,
    CryptoNonPriceContract,
    CryptoPriceTemplateNotGrounded,
    CryptoFdvContract,
    CryptoPathDependentPriceContract,
    CryptoNonSpotValuationContract,
    CryptoRelativePerformanceContract,
    CryptoProtocolMetricContract,
    CryptoUnsupportedAsset,
    WeatherDailyTemperatureTemplateNotGrounded,
    WeatherHistoricalCalibrationUnavailable,
    WeatherAmbiguousFractionalBucketOwnership,
    RecognizedWeatherFamilyParserUnavailable,
    WeatherEarthquakeContract,
    WeatherVolcanoContract,
    WeatherHealthContract,
    WeatherSpaceContract,
    WeatherTechnologyContract,
    WeatherMixedHazardContract,
    WeatherNonAtmosphericTagNoise,
    CategorySubjectMismatch,
}

/// Canonical unit attached to every capability before observations are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainMeasurementUnit {
    Usd,
    Celsius,
    Millimeter,
    Aqi,
    Count,
    Knot,
    CelsiusAnomaly,
    MillionSquareKilometer,
}

impl DomainMeasurementUnit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Usd => "usd",
            Self::Celsius => "celsius",
            Self::Millimeter => "millimeter",
            Self::Aqi => "aqi",
            Self::Count => "count",
            Self::Knot => "knot",
            Self::CelsiusAnomaly => "celsius_anomaly",
            Self::MillionSquareKilometer => "million_square_kilometer",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "usd" => Some(Self::Usd),
            "celsius" => Some(Self::Celsius),
            "millimeter" => Some(Self::Millimeter),
            "aqi" => Some(Self::Aqi),
            "count" => Some(Self::Count),
            "knot" => Some(Self::Knot),
            "celsius_anomaly" => Some(Self::CelsiusAnomaly),
            "million_square_kilometer" => Some(Self::MillionSquareKilometer),
            _ => None,
        }
    }
}

/// Long-form Weather measurement variable. New contract families extend this
/// taxonomy without introducing a family-specific fact table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherVariable {
    Temperature,
    TemperatureMaximum,
    TemperatureMinimum,
    Precipitation,
    Aqi,
    WindSpeed,
    WindGust,
    TornadoCount,
    CycloneIntensity,
    GlobalTemperatureAnomaly,
    SeaIceExtent,
}

impl WeatherVariable {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Temperature => "temperature",
            Self::TemperatureMaximum => "temperature_maximum",
            Self::TemperatureMinimum => "temperature_minimum",
            Self::Precipitation => "precipitation",
            Self::Aqi => "aqi",
            Self::WindSpeed => "wind_speed",
            Self::WindGust => "wind_gust",
            Self::TornadoCount => "tornado_count",
            Self::CycloneIntensity => "cyclone_intensity",
            Self::GlobalTemperatureAnomaly => "global_temperature_anomaly",
            Self::SeaIceExtent => "sea_ice_extent",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "temperature" => Some(Self::Temperature),
            "temperature_maximum" => Some(Self::TemperatureMaximum),
            "temperature_minimum" => Some(Self::TemperatureMinimum),
            "precipitation" => Some(Self::Precipitation),
            "aqi" => Some(Self::Aqi),
            "wind_speed" => Some(Self::WindSpeed),
            "wind_gust" => Some(Self::WindGust),
            "tornado_count" => Some(Self::TornadoCount),
            "cyclone_intensity" => Some(Self::CycloneIntensity),
            "global_temperature_anomaly" => Some(Self::GlobalTemperatureAnomaly),
            "sea_ice_extent" => Some(Self::SeaIceExtent),
            _ => None,
        }
    }
}

/// Timezone policy used to derive the contract's decision/aggregation window.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DomainTimezonePolicy {
    Utc,
    StationLocal,
    Named { timezone: String },
}

/// Whether a source binding can run without operator-provided credentials.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCredentialPolicy {
    Public,
    Required { credential_key: String },
}

/// One expected source role frozen into a contract capability.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CapabilitySourceBinding {
    pub role: LinkageSourceRole,
    pub source_id: DomainSourceId,
    /// Stable template expanded with the resolved asset/station/site identity.
    pub instrument_template: String,
    /// Whether absence or staleness blocks this capability's readiness gate.
    pub required: bool,
    pub credential_policy: SourceCredentialPolicy,
    pub freshness_secs: u64,
}

/// Serving eligibility of a capability in this build/deployment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CapabilityEligibility {
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
}

/// One immutable vertical capability.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DomainContractCapability {
    pub family: DomainFamily,
    pub contract_family: DomainContractFamily,
    pub subject_scope: Vec<String>,
    pub parser_template: String,
    pub source_bindings: Vec<CapabilitySourceBinding>,
    pub unit: DomainMeasurementUnit,
    pub precision: Decimal,
    pub timezone_policy: DomainTimezonePolicy,
    pub pit_available: bool,
    pub profile: Option<ResearchProfileRef>,
    pub dependency_hashes: Vec<ContentHash>,
    pub eligibility: CapabilityEligibility,
}

/// Content-addressed registry shared by resolver, ingest, feature and serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainCapabilityRegistryArtifact {
    pub format_version: u32,
    pub resolver_version: ResolverVersion,
    pub contracts: Vec<DomainContractCapability>,
    pub registry_hash: ContentHash,
}

impl DomainCapabilityRegistryArtifact {
    /// Build a canonical registry. Contract/source/dependency order is part of
    /// the contract and therefore normalized before hashing.
    pub fn new(
        resolver_version: ResolverVersion,
        mut contracts: Vec<DomainContractCapability>,
    ) -> Result<Self, CanonicalDigestError> {
        for contract in &mut contracts {
            contract.subject_scope.sort();
            contract.subject_scope.dedup();
            contract.source_bindings.sort();
            contract.source_bindings.dedup();
            contract.dependency_hashes.sort();
            contract.dependency_hashes.dedup();
        }
        contracts.sort();
        contracts.dedup();
        let registry_hash = compute_registry_hash(resolver_version, &contracts)?;
        Ok(Self {
            format_version: DOMAIN_CAPABILITY_REGISTRY_FORMAT_VERSION,
            resolver_version,
            contracts,
            registry_hash,
        })
    }

    /// Verify the envelope version, canonical ordering and content address.
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != DOMAIN_CAPABILITY_REGISTRY_FORMAT_VERSION {
            return Err(format!(
                "unsupported domain capability registry format {}, expected {}",
                self.format_version, DOMAIN_CAPABILITY_REGISTRY_FORMAT_VERSION
            ));
        }
        if self.contracts.windows(2).any(|rows| rows[0] >= rows[1]) {
            return Err("domain capability contracts must be unique and canonically sorted".into());
        }
        for contract in &self.contracts {
            if contract.subject_scope.is_empty()
                || contract.source_bindings.is_empty()
                || contract.precision <= Decimal::ZERO
                || contract
                    .source_bindings
                    .iter()
                    .any(|binding| binding.freshness_secs == 0)
            {
                return Err("domain capability contains an incomplete source contract".into());
            }
            if contract
                .subject_scope
                .windows(2)
                .any(|rows| rows[0] >= rows[1])
                || contract
                    .source_bindings
                    .windows(2)
                    .any(|rows| rows[0] >= rows[1])
                || contract
                    .dependency_hashes
                    .windows(2)
                    .any(|rows| rows[0] >= rows[1])
            {
                return Err("domain capability members must be unique and sorted".into());
            }
        }
        let expected = compute_registry_hash(self.resolver_version, &self.contracts)
            .map_err(|error| error.to_string())?;
        if expected != self.registry_hash {
            return Err("domain capability registry content hash mismatch".into());
        }
        Ok(())
    }
}

fn compute_registry_hash(
    resolver_version: ResolverVersion,
    contracts: &[DomainContractCapability],
) -> Result<ContentHash, CanonicalDigestError> {
    CanonicalDigest::content_hash_json(&(
        "domain_capability_registry_v1",
        DOMAIN_CAPABILITY_REGISTRY_FORMAT_VERSION,
        resolver_version,
        contracts,
    ))
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{
        CapabilityEligibility, CapabilitySourceBinding, DomainCapabilityRegistryArtifact,
        DomainContractCapability, DomainContractFamily, DomainMeasurementUnit,
        DomainTimezonePolicy, SourceCredentialPolicy,
    };
    use crate::{
        enums::domain::{DomainFamily, LinkageSourceRole},
        types::{DomainSourceId, ResolverVersion},
    };

    fn capability(scope: &[&str]) -> DomainContractCapability {
        DomainContractCapability {
            family: DomainFamily::Crypto,
            contract_family: DomainContractFamily::CryptoDirection,
            subject_scope: scope.iter().map(|value| (*value).to_owned()).collect(),
            parser_template: "crypto_direction_v1".to_owned(),
            source_bindings: vec![CapabilitySourceBinding {
                role: LinkageSourceRole::Feature,
                source_id: DomainSourceId::binance(),
                instrument_template: "BINANCE:{symbol}:1m".to_owned(),
                required: true,
                credential_policy: SourceCredentialPolicy::Public,
                freshness_secs: 60,
            }],
            unit: DomainMeasurementUnit::Usd,
            precision: dec!(0.00000001),
            timezone_policy: DomainTimezonePolicy::Utc,
            pit_available: true,
            profile: None,
            dependency_hashes: Vec::new(),
            eligibility: CapabilityEligibility::Supported,
        }
    }

    #[test]
    fn registry_hash_after_canonicalization() {
        let left = DomainCapabilityRegistryArtifact::new(
            ResolverVersion::new(3),
            vec![capability(&["ETH", "BTC"])],
        )
        .expect("left registry");
        let right = DomainCapabilityRegistryArtifact::new(
            ResolverVersion::new(3),
            vec![capability(&["BTC", "ETH"])],
        )
        .expect("right registry");
        assert_eq!(left, right);
        assert!(left.validate().is_ok());
    }

    #[test]
    fn registry_hash_binds_version() {
        let v3 = DomainCapabilityRegistryArtifact::new(
            ResolverVersion::new(3),
            vec![capability(&["BTC"])],
        )
        .expect("v3 registry");
        let v4 = DomainCapabilityRegistryArtifact::new(
            ResolverVersion::new(4),
            vec![capability(&["BTC"])],
        )
        .expect("v4 registry");
        assert_ne!(v3.registry_hash, v4.registry_hash);
    }
}
