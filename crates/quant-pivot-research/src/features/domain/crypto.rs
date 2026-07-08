//! Crypto external-vertical feature builder (Phase 11.2.2 §3.6.5).
//!
//! Five features computed from the frozen [`CryptoSubject`] and PIT domain
//! observation windows:
//!
//! | feature | inputs |
//! |---|---|
//! | `domain.crypto.distance_to_strike` | latest close + subject strike / reference price |
//! | `domain.crypto.underlying_momentum` | close series over `momentum_window_secs` |
//! | `domain.crypto.underlying_realized_vol` | close series over `volatility_window_secs` |
//! | `domain.crypto.time_to_observation` | subject `observation_at` |
//! | `domain.crypto.basis_vs_resolution_source` | latest close vs latest oracle quote |
//!
//! Every missing input is an explicit [`NullReason`] — never a silent zero.
//! For up/down markets the reference price (price-to-beat) is read from the
//! **settlement oracle's** own PIT window at the window open when available,
//! falling back to the feature source (Binance) — the basis feature then
//! carries the residual cross-source risk explicitly.

use chrono::Duration;
use quant_pivot_models::{
    domain::{CryptoSubject, MarketSubject, PriceComparator, ResolutionOracle},
    enums::{domain::DomainFamily, feature::EvidenceSourceKind},
};
use rust_decimal::Decimal;

use crate::{
    domain::DomainObservationWindow,
    features::{
        builder::RawFeature,
        domain::{DomainComputeCtx, DomainFeatureBuilder},
        generic::stats::{realized_volatility, simple_return},
        names::domain_crypto as names,
        value::{EvidenceSourceRef, FeatureValue, NullReason},
    },
};
use quant_pivot_models::enums::domain::DomainMetric;

/// Basis points per unit ratio.
const BPS_PER_UNIT: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

/// The crypto vertical's [`DomainFeatureBuilder`].
pub struct CryptoDomainFeatureBuilder;

impl DomainFeatureBuilder for CryptoDomainFeatureBuilder {
    fn family(&self) -> DomainFamily {
        DomainFamily::Crypto
    }

    fn compute(&self, ctx: &DomainComputeCtx<'_>) -> Vec<RawFeature> {
        let MarketSubject::Crypto(subject) = &ctx.binding.subject;
        vec![
            distance_to_strike(ctx, subject),
            underlying_momentum(ctx),
            underlying_realized_vol(ctx),
            time_to_observation(ctx, subject),
            basis_vs_resolution_source(ctx, subject),
        ]
    }
}

/// Evidence ref anchored on one domain observation.
fn evidence(window: &DomainObservationWindow, metric: DomainMetric) -> Option<EvidenceSourceRef> {
    window.latest(metric).map(|observation| EvidenceSourceRef {
        source_kind: EvidenceSourceKind::DomainExternal,
        reference: format!(
            "{}:{}@{}",
            observation.instrument_key,
            metric.as_str(),
            observation.observed_at.timestamp_millis()
        ),
        observed_at: observation.observed_at,
    })
}

/// Signed relative distance from the underlying to the strike, oriented so
/// positive favors YES.
fn distance_to_strike(ctx: &DomainComputeCtx<'_>, subject: &CryptoSubject) -> RawFeature {
    let name = names::DISTANCE_TO_STRIKE;
    let Some(close) = ctx.primary.latest(DomainMetric::Close) else {
        return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
    };
    let close_value = close.value;
    let ratio = match (&subject.comparator, subject.strike) {
        (PriceComparator::Above, Some(strike)) => {
            signed_relative(close_value, strike.inner(), false)
        }
        (PriceComparator::Below, Some(strike)) => {
            signed_relative(close_value, strike.inner(), true)
        }
        (PriceComparator::Between { hi }, Some(lo)) => {
            band_distance(close_value, lo.inner(), hi.inner())
        }
        (PriceComparator::UpVsReference, _) => {
            let Some(reference_at) = subject.reference_at else {
                return RawFeature::missing(name, NullReason::LinkageUnresolved);
            };
            // Price-to-beat: prefer the settlement oracle's own quote at the
            // window open (settles the market), fall back to the feature source.
            let reference = ctx
                .oracle
                .and_then(|window| window.latest_at(DomainMetric::OraclePrice, reference_at))
                .or_else(|| ctx.primary.latest_at(DomainMetric::Close, reference_at));
            let Some(reference) = reference else {
                return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
            };
            signed_relative(close_value, reference.value, false)
        }
        // A comparator that requires a strike but has none is a malformed
        // binding — fail closed, never guess.
        (_, None) => return RawFeature::missing(name, NullReason::LinkageUnresolved),
    };
    match ratio {
        Some(value) => match evidence(ctx.primary, DomainMetric::Close) {
            Some(evidence) => RawFeature::present(name, FeatureValue::Decimal(value), evidence),
            None => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
        },
        None => RawFeature::missing(name, NullReason::OutOfValidRange),
    }
}

/// `(close − anchor)/anchor`, negated when `invert` (YES = below the strike).
fn signed_relative(close: Decimal, anchor: Decimal, invert: bool) -> Option<Decimal> {
    if anchor.is_zero() {
        return None;
    }
    let ratio = (close - anchor) / anchor;
    Some(if invert { -ratio } else { ratio })
}

/// Normalized distance to the nearest band bound, positive inside `[lo, hi]`.
fn band_distance(close: Decimal, lo: Decimal, hi: Decimal) -> Option<Decimal> {
    if lo.is_zero() || hi <= lo {
        return None;
    }
    let to_lower = (close - lo) / lo;
    let to_upper = (hi - close) / lo;
    Some(to_lower.min(to_upper))
}

/// Underlying close-to-close momentum over the configured window.
fn underlying_momentum(ctx: &DomainComputeCtx<'_>) -> RawFeature {
    let name = names::UNDERLYING_MOMENTUM;
    let window_secs = i64::try_from(ctx.crypto.momentum_window_secs).unwrap_or(i64::MAX);
    let from = ctx.as_of - Duration::seconds(window_secs);
    let series = ctx.primary.series_since(DomainMetric::Close, from);
    match simple_return(&series) {
        Some(value) => match evidence(ctx.primary, DomainMetric::Close) {
            Some(evidence) => RawFeature::present(name, FeatureValue::Decimal(value), evidence),
            None => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
        },
        None if series.is_empty() => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
        None => RawFeature::missing(name, NullReason::InsufficientHistory),
    }
}

/// Underlying realized volatility over the configured window.
fn underlying_realized_vol(ctx: &DomainComputeCtx<'_>) -> RawFeature {
    let name = names::UNDERLYING_REALIZED_VOL;
    let window_secs = i64::try_from(ctx.crypto.volatility_window_secs).unwrap_or(i64::MAX);
    let from = ctx.as_of - Duration::seconds(window_secs);
    let series = ctx.primary.series_since(DomainMetric::Close, from);
    match realized_volatility(&series) {
        Some(value) => match evidence(ctx.primary, DomainMetric::Close) {
            Some(evidence) => RawFeature::present(name, FeatureValue::Decimal(value), evidence),
            None => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
        },
        None if series.is_empty() => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
        None => RawFeature::missing(name, NullReason::InsufficientHistory),
    }
}

/// Seconds from `as_of` until the subject's settlement observation (zero once
/// the observation instant has passed).
fn time_to_observation(ctx: &DomainComputeCtx<'_>, subject: &CryptoSubject) -> RawFeature {
    let name = names::TIME_TO_OBSERVATION;
    let seconds = (subject.observation_at - ctx.as_of).num_seconds().max(0);
    let value = FeatureValue::Count(u64::try_from(seconds).unwrap_or(0));
    // Intrinsically point-in-time: the anchor is the frozen subject itself.
    RawFeature::present(
        name,
        value,
        EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: format!(
                "subject:observation_at@{}",
                subject.observation_at.timestamp_millis()
            ),
            observed_at: ctx.as_of,
        },
    )
}

/// Basis (bps) between the feature source and the settlement oracle.
fn basis_vs_resolution_source(ctx: &DomainComputeCtx<'_>, subject: &CryptoSubject) -> RawFeature {
    let name = names::BASIS_VS_RESOLUTION_SOURCE;
    match &subject.resolution_oracle {
        // Feature source == settlement source: no cross-source divergence
        // exists to measure (structurally not applicable, never a zero).
        ResolutionOracle::BinanceKline { .. } => {
            RawFeature::missing(name, NullReason::NotApplicable)
        }
        ResolutionOracle::ChainlinkDataStreams { .. } => {
            let Some(oracle_window) = ctx.oracle else {
                return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
            };
            let (Some(close), Some(oracle)) = (
                ctx.primary.latest(DomainMetric::Close),
                oracle_window.latest(DomainMetric::OraclePrice),
            ) else {
                return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
            };
            if oracle.value.is_zero() {
                return RawFeature::missing(name, NullReason::OutOfValidRange);
            }
            let basis_bps = (close.value - oracle.value) / oracle.value * BPS_PER_UNIT;
            match evidence(oracle_window, DomainMetric::OraclePrice) {
                Some(evidence) => RawFeature::present(name, FeatureValue::Bps(basis_bps), evidence),
                None => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
            }
        }
        // Unrecognized oracle: we cannot cross-check — fail closed.
        ResolutionOracle::Other { .. } => {
            RawFeature::missing(name, NullReason::DomainSourceUnavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CryptoDomainFeatureBuilder, band_distance, signed_relative};
    use crate::{
        domain::DomainObservationWindow,
        features::{
            domain::{DomainComputeCtx, DomainFeatureBuilder},
            names::domain_crypto as names,
            value::{FeatureValue, NullReason},
        },
    };
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CryptoSubject, DomainObservation, GroundingProof, MarketSubject, PriceComparator,
            ResolutionOracle, ResolvedBinding,
        },
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        runtime_config::CryptoDomainConfig,
        types::{
            BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey,
            DomainSourceId, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn instrument() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn close_observation(at: DateTime<Utc>, value: Decimal) -> DomainObservation {
        DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: instrument(),
            metric: DomainMetric::Close,
            value,
            observed_at: at,
            publish_time: at,
        }
    }

    fn oracle_observation(at: DateTime<Utc>, value: Decimal) -> DomainObservation {
        DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::chainlink(),
            instrument_key: DomainInstrumentKey::chainlink_feed(
                &ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
            ),
            metric: DomainMetric::OraclePrice,
            value,
            observed_at: at,
            publish_time: at,
        }
    }

    fn binding(comparator: PriceComparator, strike: Option<Usd>) -> ResolvedBinding {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator,
                strike,
                reference_at: Some(as_of - chrono::Duration::minutes(3)),
                observation_at: as_of + chrono::Duration::minutes(2),
                resolution_oracle: ResolutionOracle::ChainlinkDataStreams {
                    feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
                },
            }),
            instrument_key: instrument(),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }
    }

    fn window(values: &[(i64, Decimal)]) -> DomainObservationWindow {
        let base = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        DomainObservationWindow {
            cutoff: base,
            observations: values
                .iter()
                .map(|(minutes_ago, value)| {
                    close_observation(base - chrono::Duration::minutes(*minutes_ago), *value)
                })
                .collect(),
        }
    }

    fn find<'a>(
        features: &'a [crate::features::builder::RawFeature],
        name: &crate::features::value::FeatureName,
    ) -> &'a crate::features::builder::RawFeature {
        features
            .iter()
            .find(|feature| &feature.name == name)
            .expect("feature present")
    }

    #[test]
    fn above_threshold_distance_is_signed_toward_yes() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(2, dec!(99000)), (1, dec!(101000))]);
        let binding = binding(PriceComparator::Above, Some(Usd::new(dec!(100000))));
        let crypto = CryptoDomainConfig::default();
        let ctx = DomainComputeCtx {
            as_of,
            binding: &binding,
            primary: &primary,
            oracle: None,
            crypto: &crypto,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &names::DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect("present"),
            &FeatureValue::Decimal(dec!(0.01)),
            "(101000 - 100000)/100000"
        );
    }

    #[test]
    fn missing_close_fails_closed() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = DomainObservationWindow::default();
        let binding = binding(PriceComparator::Above, Some(Usd::new(dec!(100000))));
        let crypto = CryptoDomainConfig::default();
        let ctx = DomainComputeCtx {
            as_of,
            binding: &binding,
            primary: &primary,
            oracle: None,
            crypto: &crypto,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &names::DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect_err("missing"),
            &NullReason::DomainSourceUnavailable
        );
    }

    #[test]
    fn basis_measures_binance_vs_chainlink_divergence() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100500))]);
        let oracle = DomainObservationWindow {
            cutoff: as_of,
            observations: vec![oracle_observation(
                as_of - chrono::Duration::minutes(1),
                dec!(100000),
            )],
        };
        let binding = binding(PriceComparator::UpVsReference, None);
        let crypto = CryptoDomainConfig::default();
        let ctx = DomainComputeCtx {
            as_of,
            binding: &binding,
            primary: &primary,
            oracle: Some(&oracle),
            crypto: &crypto,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let basis = find(&features, &names::BASIS_VS_RESOLUTION_SOURCE);
        assert_eq!(
            basis.value.as_ref().expect("present"),
            &FeatureValue::Bps(dec!(50)),
            "500/100000 = 50 bps"
        );
    }

    #[test]
    fn up_vs_reference_prefers_oracle_price_to_beat() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        // Binance ref at window open would be 100000; the oracle says 100100.
        let primary = window(&[(3, dec!(100000)), (1, dec!(100100))]);
        let oracle = DomainObservationWindow {
            cutoff: as_of,
            observations: vec![oracle_observation(
                as_of - chrono::Duration::minutes(3),
                dec!(100100),
            )],
        };
        let binding = binding(PriceComparator::UpVsReference, None);
        let crypto = CryptoDomainConfig::default();
        let ctx = DomainComputeCtx {
            as_of,
            binding: &binding,
            primary: &primary,
            oracle: Some(&oracle),
            crypto: &crypto,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &names::DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect("present"),
            &FeatureValue::Decimal(dec!(0)),
            "close 100100 vs oracle PTB 100100 → flat"
        );
    }

    #[test]
    fn band_and_signed_helpers_reject_degenerate_anchors() {
        assert!(signed_relative(dec!(1), Decimal::ZERO, false).is_none());
        assert!(band_distance(dec!(1), Decimal::ZERO, dec!(2)).is_none());
        assert!(band_distance(dec!(1), dec!(2), dec!(2)).is_none());
        assert_eq!(
            band_distance(dec!(110), dec!(100), dec!(130)),
            Some(dec!(0.1)),
            "nearest bound is lo: (110-100)/100"
        );
    }

    #[test]
    fn time_to_observation_counts_down_and_floors_at_zero() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100000))]);
        let binding = binding(PriceComparator::UpVsReference, None);
        let crypto = CryptoDomainConfig::default();
        let ctx = DomainComputeCtx {
            as_of,
            binding: &binding,
            primary: &primary,
            oracle: None,
            crypto: &crypto,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let tto = find(&features, &names::TIME_TO_OBSERVATION);
        assert_eq!(
            tto.value.as_ref().expect("present"),
            &FeatureValue::Count(120),
            "observation_at is 2 minutes out"
        );
    }
}
