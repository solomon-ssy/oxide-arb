//! Category-routed domain factor plane.
//!
//! Domain factors are **never** config-selectable through
//! `factors.enabled_factor_families`: [`DomainFactorRegistry`] routes them by
//! the market's category → [`DomainFamily`] mapping, gated by
//! `domain.enabled_by_family`. Inside a batch the factor **columns** are shared
//! (cross-sectional normalization needs aligned columns), so routing is
//! enforced per market inside each computer: a market whose category maps to a
//! different (or no) vertical yields [`RawFactorEligibility::NotApplicable`] —
//! structurally absent, never entering the cross-section; a category-mapped
//! market with missing domain inputs yields `raw_value: None` with zero
//! confidence (`DomainMissing` semantics — never a silent zero). The same
//! `NotApplicable` eligibility (not `DomainMissing`) is also used when the
//! market maps to the vertical but an *input feature itself* carries
//! [`quant_pivot_models::types::NullReason::NotApplicable`] (e.g. a basis feature that
//! structurally cannot exist for a Binance-settled market) — see
//! [`read_input`]; this is never conflated with a transient data-source gap.

use std::sync::Arc;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::{FactorFamily, FactorNormalization},
        quant::FactorDirection,
    },
    runtime_config::DomainConfig,
    types::{FactorDefinitionId, Probability},
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    factors::{
        computer::FactorComputer,
        identity::provisional_factor_definition_id,
        names::{
            DOMAIN_CRYPTO_BETA_REGIME, DOMAIN_CRYPTO_STRIKE_PRESSURE,
            DOMAIN_WEATHER_ENSEMBLE_BIN_PROBABILITY, DOMAIN_WEATHER_ENSEMBLE_SPREAD,
            DOMAIN_WEATHER_NOAA_RESOLUTION_BASIS_RISK, DOMAIN_WEATHER_OBSERVED_EXTREME_HEADROOM,
        },
        value::{
            FactorDefinitionDocument, FactorDriver, FactorName, FactorOutputKind, RawFactor,
            RawFactorEligibility,
        },
    },
    features::{
        FeatureCellState, FeatureName, FeatureValue, FeatureVector, feature_scalar,
        names::{
            domain_crypto,
            domain_crypto::{
                DISTANCE_TO_STRIKE, TIME_TO_OBSERVATION, UNDERLYING_MOMENTUM,
                UNDERLYING_REALIZED_VOL,
            },
            domain_weather::{
                ENSEMBLE_BIN_PROBABILITY, ENSEMBLE_SPREAD, NOAA_RESOLUTION_BASIS_RISK,
                OBSERVED_EXTREME_HEADROOM,
            },
            market::CATEGORY,
        },
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Seconds per day (strike-pressure time normalization).
const SECS_PER_DAY: f64 = 86_400.0;

/// Category-keyed registry of domain `(spec, computer)` pairs.
///
/// Built once per round from the frozen [`DomainConfig`]; only enabled
/// families register. [`Self::for_category`] is the routing surface consumed
/// by governance/introspection; the engine merges [`Self::all`] into its
/// column set (per-market applicability is enforced inside the computers).
pub struct DomainFactorRegistry {
    factors: Vec<(
        DomainFamily,
        FactorDefinitionDocument,
        Arc<dyn FactorComputer>,
    )>,
}

impl DomainFactorRegistry {
    /// Register every enabled vertical's factors.
    #[must_use]
    pub fn build(domain: &DomainConfig) -> Self {
        let mut factors: Vec<(
            DomainFamily,
            FactorDefinitionDocument,
            Arc<dyn FactorComputer>,
        )> = Vec::new();
        if domain.family_enabled(DomainFamily::Crypto) {
            for (spec, computer) in crypto_domain_factors() {
                factors.push((DomainFamily::Crypto, spec, computer));
            }
        }
        if domain.family_enabled(DomainFamily::Weather) {
            for (spec, computer) in weather_domain_factors() {
                factors.push((DomainFamily::Weather, spec, computer));
            }
        }
        Self { factors }
    }

    /// The factors that route to a market category (empty for unmapped
    /// categories or disabled families).
    #[must_use]
    pub fn for_category(
        &self,
        category: MarketCategory,
    ) -> Vec<&(
        DomainFamily,
        FactorDefinitionDocument,
        Arc<dyn FactorComputer>,
    )> {
        let Some(family) = DomainFamily::for_category(category) else {
            return Vec::new();
        };
        self.factors
            .iter()
            .filter(|(entry_family, _, _)| *entry_family == family)
            .collect()
    }

    /// Every registered `(spec, computer)` pair (engine column merge).
    #[must_use]
    pub fn all(&self) -> Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)> {
        self.factors
            .iter()
            .map(|(_, spec, computer)| (spec.clone(), Arc::clone(computer)))
            .collect()
    }

    /// Whether no domain factor is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.factors.is_empty()
    }
}

/// The crypto vertical's `(spec, computer)` pairs.
#[must_use]
pub fn crypto_domain_factors() -> Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)> {
    vec![strike_pressure_factor(), beta_regime_factor()]
}

/// The Weather vertical's four governed factors. Each consumes the typed
/// feature with the same stable semantic name; category routing prevents
/// cross-vertical normalization inputs.
#[must_use]
pub fn weather_domain_factors() -> Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)> {
    [
        (
            DOMAIN_WEATHER_ENSEMBLE_BIN_PROBABILITY,
            ENSEMBLE_BIN_PROBABILITY,
            FactorDirection::Positive,
        ),
        (
            DOMAIN_WEATHER_ENSEMBLE_SPREAD,
            ENSEMBLE_SPREAD,
            FactorDirection::Negative,
        ),
        (
            DOMAIN_WEATHER_OBSERVED_EXTREME_HEADROOM,
            OBSERVED_EXTREME_HEADROOM,
            FactorDirection::Neutral,
        ),
        (
            DOMAIN_WEATHER_NOAA_RESOLUTION_BASIS_RISK,
            NOAA_RESOLUTION_BASIS_RISK,
            FactorDirection::Negative,
        ),
    ]
    .into_iter()
    .map(|(name, feature, direction)| {
        let spec = FactorDefinitionDocument {
            name,
            family: FactorFamily::DomainWeather,
            input_features: vec![feature.clone()],
            output_kind: FactorOutputKind::NormalizedScore,
            default_direction: direction,
            normalization: FactorNormalization::Rank,
            owner: "quant-pivot".to_owned(),
            quality_gates: Vec::new(),
        };
        let computer = WeatherIdentityFactor {
            spec: spec.clone(),
            feature,
        };
        (spec, Arc::new(computer) as Arc<dyn FactorComputer>)
    })
    .collect()
}

/// Shared spec shape for a crypto domain factor.
fn crypto_spec(
    name: &FactorName,
    input_features: Vec<FeatureName>,
    direction: FactorDirection,
) -> FactorDefinitionDocument {
    FactorDefinitionDocument {
        name: name.clone(),
        family: FactorFamily::DomainCrypto,
        input_features,
        output_kind: FactorOutputKind::NormalizedScore,
        default_direction: direction,
        normalization: FactorNormalization::WinsorizedZScore,
        owner: "quant-pivot".to_owned(),
        quality_gates: Vec::new(),
    }
}

/// `domain_crypto_strike_pressure`: the strike distance scaled by settlement
/// urgency — the same signed distance is a stronger YES/NO statement the
/// closer the observation instant is.
fn strike_pressure_factor() -> (FactorDefinitionDocument, Arc<dyn FactorComputer>) {
    let spec = crypto_spec(
        &DOMAIN_CRYPTO_STRIKE_PRESSURE,
        vec![
            domain_crypto::DISTANCE_TO_STRIKE,
            domain_crypto::TIME_TO_OBSERVATION,
        ],
        FactorDirection::Positive,
    );
    let computer = StrikePressureFactor { spec: spec.clone() };
    (spec, Arc::new(computer))
}

/// `domain_crypto_beta_regime`: the underlying's volatility-normalized trend
/// (momentum per unit realized vol) — the regime the market's crypto subject
/// is currently trading in.
fn beta_regime_factor() -> (FactorDefinitionDocument, Arc<dyn FactorComputer>) {
    let spec = crypto_spec(
        &DOMAIN_CRYPTO_BETA_REGIME,
        vec![
            domain_crypto::UNDERLYING_MOMENTUM,
            domain_crypto::UNDERLYING_REALIZED_VOL,
        ],
        FactorDirection::Positive,
    );
    let computer = BetaRegimeFactor { spec: spec.clone() };
    (spec, Arc::new(computer))
}

/// Whether a vector's market routes to the crypto vertical.
fn routes_to_crypto(features: &FeatureVector) -> bool {
    match features.value(&CATEGORY) {
        Some(FeatureValue::Category(category)) => {
            DomainFamily::for_category(*category) == Some(DomainFamily::Crypto)
        }
        // No readable category ⇒ the vertical structurally cannot apply.
        _ => false,
    }
}

fn routes_to_weather(features: &FeatureVector) -> bool {
    match features.value(&CATEGORY) {
        Some(FeatureValue::Category(category)) => {
            DomainFamily::for_category(*category) == Some(DomainFamily::Weather)
        }
        _ => false,
    }
}

/// One input feature's state for a domain factor computer: a genuine value,
/// a structural non-applicability (never conflated with a data gap — see
/// [`not_applicable`] vs [`domain_missing`]), or any other missing reason.
enum InputState {
    Present(Decimal),
    NotApplicable,
    Missing,
}

/// Read a decimal-valued feature across both vector slices, preserving
/// whether an absence is structural (`NullReason::NotApplicable`) or a data
/// gap — the two must never collapse into the same `RawFactor` eligibility.
fn read_input(features: &FeatureVector, name: &FeatureName) -> InputState {
    match features.cell(name) {
        Some(cell) if cell.state == FeatureCellState::NotApplicable => InputState::NotApplicable,
        Some(cell) if cell.state == FeatureCellState::Missing => InputState::Missing,
        // Absence of the domain slice for a category-routed market is a source
        // or linkage gap. Structural non-applicability is represented only by
        // an explicit `FeatureCellState::NotApplicable` cell.
        None => InputState::Missing,
        Some(cell) => cell
            .value()
            .and_then(feature_scalar)
            .map_or(InputState::Missing, InputState::Present),
    }
}

/// Assemble a raw domain factor (shared by both computers).
fn raw(
    spec: &FactorDefinitionDocument,
    raw_value: Option<Decimal>,
    eligibility: RawFactorEligibility,
    headline: String,
    drivers: Vec<FactorDriver>,
) -> RawFactor {
    let confidence = if raw_value.is_some() {
        Probability::ONE
    } else {
        Probability::ZERO
    };
    RawFactor {
        definition_id: provisional_factor_definition_id(spec.name.as_str()),
        name: spec.name.clone(),
        family: spec.family,
        raw_value,
        eligibility,
        direction: spec.default_direction,
        confidence,
        headline,
        drivers,
        input_feature_refs: spec.input_features.clone(),
    }
}

/// The structurally-not-applicable cell for markets outside the vertical.
fn not_applicable(spec: &FactorDefinitionDocument) -> RawFactor {
    raw(
        spec,
        None,
        RawFactorEligibility::NotApplicable,
        "market category maps to a different domain vertical".to_owned(),
        Vec::new(),
    )
}

/// The domain-missing cell: category-mapped but inputs absent (unresolved
/// linkage / source gap). Never a silent zero.
fn domain_missing(spec: &FactorDefinitionDocument) -> RawFactor {
    raw(
        spec,
        None,
        RawFactorEligibility::Normalizable,
        "domain inputs unavailable".to_owned(),
        Vec::new(),
    )
}

struct WeatherIdentityFactor {
    spec: FactorDefinitionDocument,
    feature: FeatureName,
}

impl FactorComputer for WeatherIdentityFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        provisional_factor_definition_id(self.spec.name.as_str())
    }

    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }

    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        if !routes_to_weather(features) {
            return Ok(not_applicable(&self.spec));
        }
        match read_input(features, &self.feature) {
            InputState::Present(value) => Ok(raw(
                &self.spec,
                Some(value),
                RawFactorEligibility::Normalizable,
                format!("{} = {value}", self.feature),
                vec![FactorDriver {
                    feature_name: self.feature.clone(),
                    contribution: value,
                }],
            )),
            InputState::NotApplicable => Ok(not_applicable(&self.spec)),
            InputState::Missing => Ok(domain_missing(&self.spec)),
        }
    }
}

struct StrikePressureFactor {
    spec: FactorDefinitionDocument,
}

impl FactorComputer for StrikePressureFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        provisional_factor_definition_id(self.spec.name.as_str())
    }

    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }

    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        if !routes_to_crypto(features) {
            return Ok(not_applicable(&self.spec));
        }
        let (distance_state, tto_state) = (
            read_input(features, &DISTANCE_TO_STRIKE),
            read_input(features, &TIME_TO_OBSERVATION),
        );
        if matches!(distance_state, InputState::NotApplicable)
            || matches!(tto_state, InputState::NotApplicable)
        {
            // The market maps to crypto, but this specific signal structurally
            // does not apply to it (e.g. a market shape this factor's inputs
            // are never computed for) — distinct from a transient data gap.
            return Ok(not_applicable(&self.spec));
        }
        let (InputState::Present(distance), InputState::Present(tto_secs)) =
            (distance_state, tto_state)
        else {
            return Ok(domain_missing(&self.spec));
        };
        // pressure = distance × sqrt(1 day / max(tto, 1 min)): the same
        // distance is more decisive with less time left. The sqrt crosses into
        // f64 and is quantized before it can enter the factor plane.
        let Some(tto) = tto_secs.to_f64() else {
            return Ok(domain_missing(&self.spec));
        };
        let urgency = (SECS_PER_DAY / tto.max(60.0)).sqrt();
        let Some(urgency) = Decimal::from_f64(urgency).filter(|value| !value.is_zero()) else {
            return Ok(domain_missing(&self.spec));
        };
        let pressure = (distance * urgency).round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(raw(
            &self.spec,
            Some(pressure),
            RawFactorEligibility::Normalizable,
            format!("strike pressure {pressure} (distance {distance}, tto {tto_secs}s)"),
            vec![
                FactorDriver {
                    feature_name: domain_crypto::DISTANCE_TO_STRIKE,
                    contribution: distance,
                },
                FactorDriver {
                    feature_name: domain_crypto::TIME_TO_OBSERVATION,
                    contribution: tto_secs,
                },
            ],
        ))
    }
}

struct BetaRegimeFactor {
    spec: FactorDefinitionDocument,
}

impl FactorComputer for BetaRegimeFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        provisional_factor_definition_id(self.spec.name.as_str())
    }

    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }

    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        if !routes_to_crypto(features) {
            return Ok(not_applicable(&self.spec));
        }
        let (momentum_state, vol_state) = (
            read_input(features, &UNDERLYING_MOMENTUM),
            read_input(features, &UNDERLYING_REALIZED_VOL),
        );
        if matches!(momentum_state, InputState::NotApplicable)
            || matches!(vol_state, InputState::NotApplicable)
        {
            return Ok(not_applicable(&self.spec));
        }
        let (InputState::Present(momentum), InputState::Present(vol)) = (momentum_state, vol_state)
        else {
            return Ok(domain_missing(&self.spec));
        };
        // Vol-normalized trend; a zero-vol window carries no regime signal
        // (missing, never a division blow-up or a fabricated extreme).
        if vol <= Decimal::ZERO {
            return Ok(domain_missing(&self.spec));
        }
        let regime = (momentum / vol).round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(raw(
            &self.spec,
            Some(regime),
            RawFactorEligibility::Normalizable,
            format!("beta regime {regime} (momentum {momentum}, vol {vol})"),
            vec![
                FactorDriver {
                    feature_name: domain_crypto::UNDERLYING_MOMENTUM,
                    contribution: momentum,
                },
                FactorDriver {
                    feature_name: domain_crypto::UNDERLYING_REALIZED_VOL,
                    contribution: vol,
                },
            ],
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use quant_pivot_models::{
        enums::{common::MarketCategory, domain::DomainFamily, quant::DataQualityStatus},
        runtime_config::DomainConfig,
        types::{MarketId, SchemaVersion, TokenId},
    };
    use rust_decimal_macros::dec;

    use super::{DomainFactorRegistry, crypto_domain_factors};
    use crate::{
        factors::value::RawFactorEligibility,
        features::{
            DomainFeatureSlice, FeatureCell, FeatureStaleness, FeatureValue, FeatureVector,
            NullReason,
            names::{
                domain_crypto::{
                    DISTANCE_TO_STRIKE, TIME_TO_OBSERVATION, UNDERLYING_MOMENTUM,
                    UNDERLYING_REALIZED_VOL,
                },
                market::CATEGORY,
            },
        },
    };

    fn vector(category: MarketCategory, domain: Option<DomainFeatureSlice>) -> FeatureVector {
        let mut generic = BTreeMap::new();
        generic.insert(
            CATEGORY,
            FeatureCell::observed(
                FeatureValue::Category(category),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        FeatureVector {
            market_id: MarketId::new("m"),
            token_id: Some(TokenId::new("t")),
            decision_at: Utc::now(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain,
            data_quality: DataQualityStatus::Fresh,
        }
    }

    fn crypto_slice() -> DomainFeatureSlice {
        let mut values = BTreeMap::new();
        values.insert(
            DISTANCE_TO_STRIKE,
            FeatureCell::observed(
                FeatureValue::Decimal(dec!(0.02)),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        values.insert(
            TIME_TO_OBSERVATION,
            FeatureCell::observed(FeatureValue::Count(86_400), None, FeatureStaleness::Unknown),
        );
        values.insert(
            UNDERLYING_MOMENTUM,
            FeatureCell::observed(
                FeatureValue::Decimal(dec!(0.01)),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        values.insert(
            UNDERLYING_REALIZED_VOL,
            FeatureCell::observed(
                FeatureValue::Decimal(dec!(0.005)),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        DomainFeatureSlice {
            family: DomainFamily::Crypto,
            schema_version: SchemaVersion::new(5),
            values,
        }
    }

    #[test]
    fn registry_routes_by_category_and_respects_family_gate() {
        let registry = DomainFactorRegistry::build(&DomainConfig::default());
        assert_eq!(registry.for_category(MarketCategory::Crypto).len(), 2);
        assert!(registry.for_category(MarketCategory::Sports).is_empty());

        let disabled = DomainFactorRegistry::build(&DomainConfig::disabled());
        assert!(disabled.is_empty());
        assert!(disabled.for_category(MarketCategory::Crypto).is_empty());
    }

    #[test]
    fn crypto_factors_compute_and_fail_closed() {
        let factors = crypto_domain_factors();

        // Mapped category + present slice → scored raw values.
        let scored = vector(MarketCategory::Crypto, Some(crypto_slice()));
        for (_, computer) in &factors {
            let raw = computer.compute_raw(&scored).expect("compute");
            assert!(raw.raw_value.is_some(), "{} must score", raw.name);
            assert_eq!(raw.eligibility, RawFactorEligibility::Normalizable);
        }

        // Mapped category, no slice → domain missing (never zero).
        let missing = vector(MarketCategory::Crypto, None);
        for (_, computer) in &factors {
            let raw = computer.compute_raw(&missing).expect("compute");
            assert!(raw.raw_value.is_none());
            assert!(raw.confidence.inner().is_zero());
            assert_eq!(raw.eligibility, RawFactorEligibility::Normalizable);
        }

        // Unmapped category → structurally not applicable.
        let sports = vector(MarketCategory::Sports, None);
        for (_, computer) in &factors {
            let raw = computer.compute_raw(&sports).expect("compute");
            assert_eq!(raw.eligibility, RawFactorEligibility::NotApplicable);
        }
    }

    #[test]
    fn structural_not_applicable_input_never_collapses_to_domain_missing() {
        // A crypto-mapped market whose strike-pressure inputs are explicitly
        // `NullReason::NotApplicable` (not merely absent/unavailable) must
        // report `RawFactorEligibility::NotApplicable` — the SAME structural
        // signal as an unmapped category, never the data-gap `Normalizable` +
        // `raw_value: None` used for a transient source outage.
        let mut values = BTreeMap::new();
        values.insert(
            DISTANCE_TO_STRIKE,
            FeatureCell::not_applicable(NullReason::NotApplicable),
        );
        values.insert(
            TIME_TO_OBSERVATION,
            FeatureCell::not_applicable(NullReason::NotApplicable),
        );
        let slice = DomainFeatureSlice {
            family: DomainFamily::Crypto,
            schema_version: SchemaVersion::new(5),
            values,
        };
        let (_, strike_pressure) = &crypto_domain_factors()[0];
        let raw = strike_pressure
            .compute_raw(&vector(MarketCategory::Crypto, Some(slice)))
            .expect("compute");
        assert_eq!(raw.eligibility, RawFactorEligibility::NotApplicable);
        assert!(raw.raw_value.is_none());
        assert!(raw.confidence.inner().is_zero());
    }

    #[test]
    fn beta_regime_is_vol_normalized() {
        let factors = crypto_domain_factors();
        let (_, beta) = &factors[1];
        let raw = beta
            .compute_raw(&vector(MarketCategory::Crypto, Some(crypto_slice())))
            .expect("compute");
        assert_eq!(raw.raw_value, Some(dec!(2)), "0.01 / 0.005");
    }
}
