//! Factor family taxonomy: generic planes plus vertical domain families.
//!
//! [`FactorFamily`] is the single authority for runtime-config gating, research
//! factor specs, and Postgres / `ClickHouse` persistence labels.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::domain::DomainFamily;

/// Parse failure for a persisted or wire factor-family label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFactorFamilyError {
    /// The label that could not be parsed.
    pub label: String,
}

impl Display for ParseFactorFamilyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "unknown factor family `{label}`", label = self.label)
    }
}

impl std::error::Error for ParseFactorFamilyError {}

/// Factor family grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FactorFamily {
    /// Order-book depth / available liquidity.
    Liquidity,
    /// Order-flow microstructure.
    Microstructure,
    /// Trend / momentum.
    Momentum,
    /// Mean reversion.
    MeanReversion,
    /// Realized / implied volatility regime.
    Volatility,
    /// Market activity (quote/trade rate).
    Activity,
    /// Resolution timing / ambiguity.
    Resolution,
    /// Data-quality-derived factors.
    DataQuality,
    /// Vertical/domain-specific factors.
    Domain(DomainFamily),
}

impl FactorFamily {
    /// Every generic family eligible for `FactorsConfig.enabled_factor_families`.
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

    /// Whether this family is a generic-plane family (config-gated).
    #[must_use]
    pub const fn is_generic(self) -> bool {
        !matches!(self, Self::Domain(_))
    }

    /// The stable label persisted to `quant_factor_definition.factor_family`
    /// and `quant_factor_event.factor_family`.
    ///
    /// This is an **append-only contract**: generic families use their flat
    /// `snake_case` name; vertical families use `domain.<vertical>`. Never change
    /// an existing label — it would silently rewrite persisted analytics.
    #[must_use]
    pub fn as_wire(self) -> String {
        match self {
            Self::Liquidity => "liquidity".to_owned(),
            Self::Microstructure => "microstructure".to_owned(),
            Self::Momentum => "momentum".to_owned(),
            Self::MeanReversion => "mean_reversion".to_owned(),
            Self::Volatility => "volatility".to_owned(),
            Self::Activity => "activity".to_owned(),
            Self::Resolution => "resolution".to_owned(),
            Self::DataQuality => "data_quality".to_owned(),
            Self::Domain(family) => format!("domain.{}", family.as_str()),
        }
    }

    /// The stable label persisted to `quant_factor_definition.scope`.
    ///
    /// Generic-plane families map to `"generic"`; vertical families use
    /// `domain:<vertical>`. This is an **append-only contract** — do not change
    /// existing labels (distinct from [`Self::as_wire`], which targets the
    /// `factor_family` column).
    #[must_use]
    pub fn definition_scope(self) -> String {
        match self {
            Self::Domain(family) => format!("domain:{}", family.as_str()),
            _ => "generic".to_owned(),
        }
    }
}

impl Display for FactorFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_wire())
    }
}

impl FromStr for FactorFamily {
    type Err = ParseFactorFamilyError;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        let err = || ParseFactorFamilyError {
            label: label.to_owned(),
        };
        if let Some(stripped) = label.strip_prefix("domain.") {
            let domain = DomainFamily::from_str(stripped).map_err(|_| err())?;
            return Ok(Self::Domain(domain));
        }
        match label {
            "liquidity" => Ok(Self::Liquidity),
            "microstructure" => Ok(Self::Microstructure),
            "momentum" => Ok(Self::Momentum),
            "mean_reversion" => Ok(Self::MeanReversion),
            "volatility" => Ok(Self::Volatility),
            "activity" => Ok(Self::Activity),
            "resolution" => Ok(Self::Resolution),
            "data_quality" => Ok(Self::DataQuality),
            _ => Err(err()),
        }
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
