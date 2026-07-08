//! Basis-cross-check exceedance detection (11.2.2 remediation R6).
//!
//! Closes the loop the design promised but never built: `domain.crypto.
//! basis_vs_resolution_source` was computed and persisted as a feature value,
//! but exceeding `domain.crypto.cross_check.max_basis_bps` never produced a
//! risk signal or a linkage-review artifact — this module is that signal.
//!
//! The feature itself is only ever `Present` for a Chainlink-oracle-settled
//! subject (a Binance-settled market's basis feature is
//! `Missing(NotApplicable)` — see `CryptoDomainFeatureBuilder`), so presence
//! alone is sufficient evidence the check applies; no separate oracle lookup
//! is needed here.

use std::{collections::HashMap, hash::BuildHasher};

use quant_pivot_models::{
    domain::NewBasisAlert,
    runtime_config::DomainConfig,
    types::{BasisAlertId, Bps, MarketId},
};
use quant_pivot_research::{
    domain::oracle_instrument,
    features::{DomainSliceInputs, FeatureValue, FeatureVector, names::domain_crypto},
};
use rust_decimal::Decimal;

/// Scan `accepted` vectors for a basis exceedance and build the alert rows to
/// persist. Pure and side-effect-free — the caller persists + alerts.
#[must_use]
pub fn detect_basis_alerts<S: BuildHasher>(
    accepted: &[FeatureVector],
    domain_inputs: &HashMap<MarketId, DomainSliceInputs, S>,
    domain: &DomainConfig,
) -> Vec<NewBasisAlert> {
    let threshold_bps = Bps::new(Decimal::from(domain.crypto.cross_check.max_basis_bps));
    accepted
        .iter()
        .filter_map(|vector| {
            let FeatureValue::Bps(basis) =
                vector.value(&domain_crypto::BASIS_VS_RESOLUTION_SOURCE)?
            else {
                return None;
            };
            if basis.abs() <= threshold_bps.inner() {
                return None;
            }
            let inputs = domain_inputs.get(&vector.market_id)?;
            let oracle_key = oracle_instrument(&inputs.binding)?;
            Some(NewBasisAlert {
                alert_id: BasisAlertId::from_v7(),
                market_id: vector.market_id.clone(),
                instrument_key: inputs.binding.instrument_key.to_string(),
                oracle_instrument_key: oracle_key.to_string(),
                basis_bps: Bps::new(*basis),
                threshold_bps,
                as_of: vector.as_of,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::detect_basis_alerts;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CryptoSubject, GroundingProof, MarketSubject, PriceComparator, ResolutionOracle,
            ResolvedBinding,
        },
        enums::{domain::DomainFamily, quant::DataQualityStatus},
        runtime_config::DomainConfig,
        types::{
            BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey,
            MarketId, SchemaVersion, TokenId,
        },
    };
    use quant_pivot_research::{
        domain::DomainObservationWindow,
        features::{DomainSliceInputs, FeatureValue, FeatureVector, names::domain_crypto},
    };
    use rust_decimal_macros::dec;
    use std::collections::{BTreeMap, HashMap};

    fn instrument() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            quant_pivot_models::enums::domain::KlineInterval::OneMinute,
        )
    }

    fn binding() -> ResolvedBinding {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_at: Some(now),
                observation_at: now,
                resolution_oracle: ResolutionOracle::ChainlinkDataStreams {
                    feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
                },
            }),
            instrument_key: instrument(),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }
    }

    fn vector(basis_bps: Option<FeatureValue>) -> FeatureVector {
        let market_id = MarketId::new("m1");
        let mut generic = BTreeMap::new();
        if let Some(value) = basis_bps {
            generic.insert(domain_crypto::BASIS_VS_RESOLUTION_SOURCE, value);
        }
        FeatureVector {
            market_id,
            token_id: Some(TokenId::new("t1")),
            as_of: Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain: None,
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
        }
    }

    fn domain_inputs_for(market_id: &MarketId) -> HashMap<MarketId, DomainSliceInputs> {
        HashMap::from([(
            market_id.clone(),
            DomainSliceInputs {
                family: DomainFamily::Crypto,
                binding: binding(),
                primary: DomainObservationWindow::default(),
                oracle: None,
            },
        )])
    }

    #[test]
    fn exceedance_produces_an_alert_with_the_governed_threshold() {
        let domain = DomainConfig::default(); // default max_basis_bps = 50
        let vec = vector(Some(FeatureValue::Bps(dec!(75))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].basis_bps.inner(), dec!(75));
        assert_eq!(alerts[0].threshold_bps.inner(), dec!(50));
        assert_eq!(alerts[0].market_id, vec.market_id);
        assert_eq!(alerts[0].oracle_instrument_key, "CHAINLINK:BTC-USD");
    }

    #[test]
    fn negative_exceedance_also_alerts_on_magnitude() {
        let domain = DomainConfig::default(); // default max_basis_bps = 50
        let vec = vector(Some(FeatureValue::Bps(dec!(-80))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert_eq!(alerts.len(), 1, "magnitude, not sign, drives the threshold");
    }

    #[test]
    fn within_threshold_never_alerts() {
        let domain = DomainConfig::default();
        let vec = vector(Some(FeatureValue::Bps(dec!(10))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert!(alerts.is_empty());
    }

    #[test]
    fn missing_basis_feature_never_alerts() {
        // Binance-settled markets carry no basis feature at all (NotApplicable).
        let domain = DomainConfig::default();
        let vec = vector(None);
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert!(alerts.is_empty());
    }

    #[test]
    fn missing_domain_inputs_fails_closed_not_panics() {
        let domain = DomainConfig::default();
        let vec = vector(Some(FeatureValue::Bps(dec!(999))));
        let alerts = detect_basis_alerts(&[vec], &HashMap::new(), &domain);
        assert!(alerts.is_empty());
    }
}
