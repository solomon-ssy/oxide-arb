//! Factor family taxonomy: generic planes plus vertical domain families.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use super::domain::DomainFamily;
use schemars::JsonSchema;
use sea_orm::Iterable;

/// Parse failure for a persisted factor-family label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFactorFamilyError {
    pub label: String,
}

impl Display for ParseFactorFamilyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "unknown factor family `{label}`", label = self.label)
    }
}

impl std::error::Error for ParseFactorFamilyError {}

crate::pg_enum! {
    type_name = "qp_factor_family",
    @derive(JsonSchema)
    pub enum FactorFamily {
        Liquidity => "liquidity",
        Microstructure => "microstructure",
        Momentum => "momentum",
        MeanReversion => "mean_reversion",
        Volatility => "volatility",
        Activity => "activity",
        Resolution => "resolution",
        DataQuality => "data_quality",
        DomainSports => "domain_sports",
        DomainPolitics => "domain_politics",
        DomainCrypto => "domain_crypto",
        DomainWeather => "domain_weather",
        DomainGeopolitics => "domain_geopolitics",
    }
}

crate::pg_enum! {
    type_name = "qp_factor_definition_scope",
    pub enum FactorDefinitionScope {
        Generic => "generic",
        DomainSports => "domain_sports",
        DomainPolitics => "domain_politics",
        DomainCrypto => "domain_crypto",
        DomainWeather => "domain_weather",
        DomainGeopolitics => "domain_geopolitics",
    }
}

impl FactorFamily {
    pub const ALL_GENERIC: [Self; 8] = [
        Self::Liquidity,
        Self::Microstructure,
        Self::Momentum,
        Self::MeanReversion,
        Self::Volatility,
        Self::Activity,
        Self::Resolution,
        Self::DataQuality,
    ];

    #[must_use]
    pub const fn is_generic(self) -> bool {
        matches!(
            self,
            Self::Liquidity
                | Self::Microstructure
                | Self::Momentum
                | Self::MeanReversion
                | Self::Volatility
                | Self::Activity
                | Self::Resolution
                | Self::DataQuality
        )
    }

    #[must_use]
    pub const fn definition_scope(self) -> FactorDefinitionScope {
        match self {
            Self::DomainSports => FactorDefinitionScope::DomainSports,
            Self::DomainPolitics => FactorDefinitionScope::DomainPolitics,
            Self::DomainCrypto => FactorDefinitionScope::DomainCrypto,
            Self::DomainWeather => FactorDefinitionScope::DomainWeather,
            Self::DomainGeopolitics => FactorDefinitionScope::DomainGeopolitics,
            _ => FactorDefinitionScope::Generic,
        }
    }

    #[must_use]
    pub const fn domain_family(self) -> Option<DomainFamily> {
        match self {
            Self::DomainSports => Some(DomainFamily::Sports),
            Self::DomainPolitics => Some(DomainFamily::Politics),
            Self::DomainCrypto => Some(DomainFamily::Crypto),
            Self::DomainWeather => Some(DomainFamily::Weather),
            Self::DomainGeopolitics => Some(DomainFamily::Geopolitics),
            _ => None,
        }
    }

    #[must_use]
    pub const fn from_domain(domain: DomainFamily) -> Self {
        match domain {
            DomainFamily::Sports => Self::DomainSports,
            DomainFamily::Politics => Self::DomainPolitics,
            DomainFamily::Crypto => Self::DomainCrypto,
            DomainFamily::Weather => Self::DomainWeather,
            DomainFamily::Geopolitics => Self::DomainGeopolitics,
        }
    }
}

impl FromStr for FactorFamily {
    type Err = ParseFactorFamilyError;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        Self::iter()
            .find(|variant| variant.as_str() == label)
            .ok_or_else(|| ParseFactorFamilyError {
                label: label.to_owned(),
            })
    }
}

impl FromStr for DomainFamily {
    type Err = ParseFactorFamilyError;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        match label {
            "sports" => Ok(Self::Sports),
            "politics" => Ok(Self::Politics),
            "crypto" => Ok(Self::Crypto),
            "weather" => Ok(Self::Weather),
            "geopolitics" => Ok(Self::Geopolitics),
            _ => Err(ParseFactorFamilyError {
                label: label.to_owned(),
            }),
        }
    }
}
