//! Factor family taxonomy.
//!
//! Generic planes, the platform-internal structural plane (Phase 11.2.1), and
//! the category-routed external domain planes (Phase 11.2.2).

use schemars::JsonSchema;

pg_enum! {
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
        /// Prediction-market structural signals (neg-risk leg drift, shock-gated
        /// reversal, resolution-proximity regime, favorite-longshot). Platform-
        /// computable from existing facts — no external data source.
        Structural => "structural",
        /// Crypto external vertical (Binance underlying price + Chainlink
        /// oracle). Routed by market category via `DomainFactorRegistry` —
        /// never selectable through `enabled_factor_families`.
        DomainCrypto => "domain_crypto",
        /// Airport daily maximum/minimum Weather external vertical.
        DomainWeather => "domain_weather",
    }
}

pg_enum! {
    type_name = "qp_factor_definition_scope",
    pub enum FactorDefinitionScope {
        Generic => "generic",
        Structural => "structural",
        /// Category-routed crypto domain factors (Phase 11.2.2).
        DomainCrypto => "domain_crypto",
        /// Category-routed Weather domain factors.
        DomainWeather => "domain_weather",
    }
}

wire_enum! {
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

pg_enum! {
    type_name = "qp_normalization_source",
    /// How a factor's normalized score was derived (audit + analytics).
    @derive(JsonSchema)
    pub enum NormalizationSource {
        /// Normalized against the same-`as_of` market cross-section.
        CrossSection => "cross_section",
        /// Per-market scaling against a fixed semantic domain (`MinMax`).
        PerMarket => "per_market",
        /// Small cross-section → normalized against the empirical training CDF
        /// frozen into the model artifact.
        FrozenReferenceQuantile => "frozen_reference_quantile",
    }
}

pg_enum! {
    type_name = "qp_factor_value_state",
    /// The authoritative outcome state of a persisted factor value — orthogonal
    /// to `indeterminate_reason` (which is populated only for `Indeterminate`).
    ///
    /// This makes a structurally **not-applicable** factor (a neg-risk factor on
    /// a binary market) durably distinguishable from a **missing-input** factor:
    /// both carry no score, but they are different truths and must render / audit
    /// differently (never conflated into one null bucket).
    @derive(JsonSchema)
    pub enum FactorValueState {
        /// The factor produced a usable normalized score.
        Scored => "scored",
        /// A single declared input was absent (existing missing-input semantics).
        MissingInput => "missing_input",
        /// The factor does not apply to this market's structure (not a data gap).
        NotApplicable => "not_applicable",
        /// The factor should have computed but an input was structurally absent,
        /// or the cross-section could not normalize (see `indeterminate_reason`).
        Indeterminate => "indeterminate",
    }
}

pg_enum! {
    type_name = "qp_factor_indeterminate_reason",
    /// Why a factor produced **no** normalized score (never a silent neutral).
    @derive(JsonSchema)
    pub enum FactorIndeterminateReason {
        /// The present cross-section was smaller than `cross_section.min_size`.
        CrossSectionTooSmall => "cross_section_too_small",
        /// Every present value was equal — the cross-section carries no signal.
        ZeroVariance => "zero_variance",
        /// Small cross-section + frozen-reference policy but the model artifact
        /// has no usable reference CDF for this factor.
        NoFrozenReference => "no_frozen_reference",
        /// A neg-risk sibling leg's order book was absent at `as_of`, so the
        /// full-leg structural factor could not be computed (never a silent zero).
        LegBookMissing => "leg_book_missing",
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

    /// Whether this is the platform-internal structural plane (Phase 11.2.1).
    #[must_use]
    pub const fn is_structural(self) -> bool {
        matches!(self, Self::Structural)
    }

    /// Whether this is a category-routed external domain plane (Phase 11.2.2).
    #[must_use]
    pub const fn is_domain(self) -> bool {
        matches!(self, Self::DomainCrypto | Self::DomainWeather)
    }

    /// Whether this family is config-selectable via `enabled_factor_families`
    /// (generic + structural are; domain families route by market category).
    #[must_use]
    pub const fn is_config_selectable(self) -> bool {
        self.is_generic() || self.is_structural()
    }

    #[must_use]
    pub const fn definition_scope(self) -> FactorDefinitionScope {
        match self {
            Self::Structural => FactorDefinitionScope::Structural,
            Self::DomainCrypto => FactorDefinitionScope::DomainCrypto,
            Self::DomainWeather => FactorDefinitionScope::DomainWeather,
            _ => FactorDefinitionScope::Generic,
        }
    }
}
