//! Factor family taxonomy: generic planes plus vertical domain families.

use super::domain::DomainFamily;
use schemars::JsonSchema;

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

crate::wire_enum! {
    /// The normalization **method** a factor's raw value is mapped through.
    ///
    /// This is the factor's semantic contract (persisted in the definition JSON
    /// and bound into `factor_schema_hash`); the distributional *parameters*
    /// (`winsor_p`, `clamp_sigma`, min/max bounds) are resolved from runtime
    /// config — never hardcoded in code.
    @derive(JsonSchema)
    pub enum FactorNormalization {
        /// Cross-sectional winsorize (to `[p, 1-p]`) then z-score, `±σ` clamped.
        WinsorizedZScore => "winsorized_zscore",
        /// Cross-sectional average rank mapped to `[0, 1]` (distribution-free).
        Rank => "rank",
        /// Per-market min/max into `[0, 1]` against a *semantic* domain (bounds
        /// from config, e.g. `data_quality ∈ [0, 1]`). Not a distribution fit.
        MinMax => "min_max",
    }
}

impl FactorNormalization {
    /// Whether the method requires the full same-`as_of` cross-section (and so
    /// is subject to the small-cross-section policy). `MinMax` is per-market.
    #[must_use]
    pub const fn is_cross_sectional(self) -> bool {
        matches!(self, Self::WinsorizedZScore | Self::Rank)
    }
}

crate::pg_enum! {
    type_name = "qp_normalization_source",
    /// How a factor's normalized score was derived (audit + analytics).
    @derive(JsonSchema)
    pub enum NormalizationSource {
        /// Normalized against the same-`as_of` market cross-section.
        CrossSection => "cross_section",
        /// Per-market scaling against a fixed semantic domain (`MinMax`).
        PerMarket => "per_market",
        /// Small cross-section → normalized against the factor's historical rolling
        /// distribution (empirical quantile).
        HistoricalQuantile => "historical_quantile",
    }
}

crate::pg_enum! {
    type_name = "qp_factor_indeterminate_reason",
    /// Why a factor produced **no** normalized score (never a silent neutral).
    @derive(JsonSchema)
    pub enum FactorIndeterminateReason {
        /// The present cross-section was smaller than `cross_section.min_size`.
        CrossSectionTooSmall => "cross_section_too_small",
        /// Every present value was equal — the cross-section carries no signal.
        ZeroVariance => "zero_variance",
        /// Small cross-section + `historical_quantile` policy but no usable history.
        NoHistory => "no_history",
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
