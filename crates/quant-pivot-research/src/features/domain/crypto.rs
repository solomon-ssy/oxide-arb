//! Crypto external-vertical feature builder.
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
//! **settlement oracle's** own PIT window at the window open when the oracle
//! is Chainlink — never silently falling back to Binance.
//! Binance-settled markets read the feature source directly. Chainlink oracle
//! observations older than `domain.crypto.cross_check.max_oracle_staleness_secs`
//! are rejected as [`NullReason::StaleBeyondPolicy`].

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::{
        data_plane::CryptoPriceReport,
        quant::{CryptoSubject, MarketSubject, PriceComparator, ResolutionOracle},
    },
    enums::{
        domain::{DomainFamily, DomainMetric},
        feature::EvidenceSourceKind,
    },
    runtime_config::CryptoDomainConfig,
};
use rust_decimal::Decimal;

use crate::{
    domain::{CryptoPriceReportWindow, DomainObservationWindow},
    features::{
        builder::RawFeature,
        domain::{DomainComputeCtx, DomainFeatureBuilder, DomainSliceDataRef},
        generic::stats::{realized_volatility, simple_return},
        names::domain_crypto::{
            BASIS_VS_RESOLUTION_SOURCE, DISTANCE_TO_STRIKE, TIME_TO_OBSERVATION,
            UNDERLYING_MOMENTUM, UNDERLYING_REALIZED_VOL,
        },
        value::{EvidenceSourceRef, FeatureValue, NullReason},
    },
};

/// Basis points per unit ratio.
const BPS_PER_UNIT: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

/// The crypto vertical's [`DomainFeatureBuilder`].
pub struct CryptoDomainFeatureBuilder;

impl DomainFeatureBuilder for CryptoDomainFeatureBuilder {
    fn family(&self) -> DomainFamily {
        DomainFamily::Crypto
    }

    fn compute(&self, ctx: &DomainComputeCtx<'_>) -> Vec<RawFeature> {
        let MarketSubject::Crypto(subject) = &ctx.binding.subject else {
            return Vec::new();
        };
        let DomainSliceDataRef::Crypto { primary, oracle } = ctx.data else {
            return Vec::new();
        };
        let ctx = CryptoComputeCtx {
            decision_at: ctx.decision_at,
            linkage_evidence: ctx.linkage_evidence,
            primary,
            oracle,
            crypto: &ctx.domain.crypto,
        };
        vec![
            distance_to_strike(&ctx, subject),
            underlying_momentum(&ctx),
            underlying_realized_vol(&ctx),
            time_to_observation(&ctx, subject),
            basis_vs_resolution_source(&ctx, subject),
        ]
    }
}

struct CryptoComputeCtx<'a> {
    decision_at: DateTime<Utc>,
    linkage_evidence: &'a EvidenceSourceRef,
    primary: &'a DomainObservationWindow,
    oracle: Option<&'a CryptoPriceReportWindow>,
    crypto: &'a CryptoDomainConfig,
}

/// Seconds between `anchor` and `observation.observed_at`.
///
/// A future observation is invalid point-in-time evidence, not a fresh
/// observation with age zero.
/// Whether a Chainlink oracle observation exceeds the governed staleness ceiling.
fn oracle_stale(
    observation: &CryptoPriceReport,
    anchor: DateTime<Utc>,
    max_staleness_secs: u64,
) -> Option<bool> {
    u64::try_from((anchor - observation.event_time).num_seconds())
        .ok()
        .map(|age| age > max_staleness_secs)
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
        effective_at: observation.publish_time,
        available_at: observation.available_at,
    })
}

fn oracle_evidence(window: &CryptoPriceReportWindow) -> Option<EvidenceSourceRef> {
    window.latest().map(|report| EvidenceSourceRef {
        source_kind: EvidenceSourceKind::DomainExternal,
        reference: format!(
            "{}:price@{}#{}",
            report.instrument_key,
            report.event_time.timestamp_millis(),
            report.report_hash
        ),
        effective_at: report.published_at,
        available_at: Some(report.available_at),
    })
}

/// Signed relative distance from the underlying to the strike, oriented so
/// positive favors YES.
fn distance_to_strike(ctx: &CryptoComputeCtx<'_>, subject: &CryptoSubject) -> RawFeature {
    let name = DISTANCE_TO_STRIKE;
    let Some(close) = ctx.primary.latest(DomainMetric::Close) else {
        return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
    };
    let close_value = close.value;
    let ratio = match (&subject.comparator, subject.strike) {
        (PriceComparator::GreaterThan | PriceComparator::GreaterThanOrEqual, Some(strike)) => {
            signed_relative(close_value, strike.inner(), false)
        }
        (PriceComparator::LessThan | PriceComparator::LessThanOrEqual, Some(strike)) => {
            signed_relative(close_value, strike.inner(), true)
        }
        (PriceComparator::Between { hi, .. }, Some(lo)) => {
            band_distance(close_value, lo.inner(), hi.inner())
        }
        (PriceComparator::UpVsReference, _) => {
            let Some(reference_at) = subject.reference_at else {
                return RawFeature::missing(name, NullReason::LinkageUnresolved);
            };
            let max_staleness = ctx.crypto.cross_check.max_oracle_staleness_secs;
            let reference = match &subject.resolution_oracle {
                ResolutionOracle::ChainlinkDataStreams { .. } => {
                    let Some(oracle_window) = ctx.oracle else {
                        return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
                    };
                    let Some(observation) = oracle_window.latest_at(reference_at) else {
                        return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
                    };
                    match oracle_stale(observation, reference_at, max_staleness) {
                        Some(true) => {
                            return RawFeature::missing(name, NullReason::StaleBeyondPolicy);
                        }
                        Some(false) => {}
                        None => return RawFeature::missing(name, NullReason::OutOfValidRange),
                    }
                    observation.price.inner()
                }
                ResolutionOracle::BinanceKline { .. } => {
                    let Some(observation) =
                        ctx.primary.latest_at(DomainMetric::Close, reference_at)
                    else {
                        return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
                    };
                    observation.value
                }
            };
            signed_relative(close_value, reference, false)
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
fn underlying_momentum(ctx: &CryptoComputeCtx<'_>) -> RawFeature {
    let name = UNDERLYING_MOMENTUM;
    let Ok(window_secs) = i64::try_from(ctx.crypto.momentum_window_secs) else {
        return RawFeature::missing(name, NullReason::OutOfValidRange);
    };
    let from = ctx.decision_at - Duration::seconds(window_secs);
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
fn underlying_realized_vol(ctx: &CryptoComputeCtx<'_>) -> RawFeature {
    let name = UNDERLYING_REALIZED_VOL;
    let Ok(window_secs) = i64::try_from(ctx.crypto.volatility_window_secs) else {
        return RawFeature::missing(name, NullReason::OutOfValidRange);
    };
    let from = ctx.decision_at - Duration::seconds(window_secs);
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

/// Seconds from `as_of` until the subject's settlement observation.
fn time_to_observation(ctx: &CryptoComputeCtx<'_>, subject: &CryptoSubject) -> RawFeature {
    let name = TIME_TO_OBSERVATION;
    let seconds = (subject.observation_at - ctx.decision_at).num_seconds();
    let Ok(seconds) = u64::try_from(seconds) else {
        return RawFeature::missing(name, NullReason::OutOfValidRange);
    };
    let value = FeatureValue::Count(seconds);
    RawFeature::present(name, value, ctx.linkage_evidence.clone())
}

/// Basis (bps) between the feature source and the settlement oracle.
fn basis_vs_resolution_source(ctx: &CryptoComputeCtx<'_>, subject: &CryptoSubject) -> RawFeature {
    let name = BASIS_VS_RESOLUTION_SOURCE;
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
            let max_staleness = ctx.crypto.cross_check.max_oracle_staleness_secs;
            let (Some(close), Some(oracle)) = (
                ctx.primary.latest(DomainMetric::Close),
                oracle_window.latest(),
            ) else {
                return RawFeature::missing(name, NullReason::DomainSourceUnavailable);
            };
            match oracle_stale(oracle, ctx.decision_at, max_staleness) {
                Some(true) => {
                    return RawFeature::missing(name, NullReason::StaleBeyondPolicy);
                }
                Some(false) => {}
                None => return RawFeature::missing(name, NullReason::OutOfValidRange),
            }
            if oracle.price.is_zero() {
                return RawFeature::missing(name, NullReason::OutOfValidRange);
            }
            let oracle_price = oracle.price.inner();
            let basis_bps = (close.value - oracle_price) / oracle_price * BPS_PER_UNIT;
            match oracle_evidence(oracle_window) {
                Some(evidence) => RawFeature::present(name, FeatureValue::Bps(basis_bps), evidence),
                None => RawFeature::missing(name, NullReason::DomainSourceUnavailable),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            data_plane::{CryptoPriceReport, DomainObservation},
            quant::{
                CryptoSubject, GroundingProof, MarketSubject, PriceComparator, ResolutionOracle,
                ResolvedBinding, ResolvedSourceBinding,
            },
        },
        enums::{
            domain::{
                BinanceMarketSegment, DomainFamily, DomainMetric, KlineInterval, LinkageSourceRole,
            },
            feature::EvidenceSourceKind,
        },
        runtime_config::{CryptoDomainConfig, DomainConfig},
        types::{
            BinanceSymbol, ChainlinkFeedKey, ContentHash, CryptoAsset, CryptoQuote,
            DomainInstrumentKey, DomainSourceId, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{CryptoDomainFeatureBuilder, band_distance, signed_relative};
    use crate::{
        domain::{CryptoPriceReportWindow, DomainObservationWindow},
        features::{
            FeatureName, RawFeature,
            domain::{DomainComputeCtx, DomainFeatureBuilder, DomainSliceDataRef},
            names::{
                domain_crypto as names,
                domain_crypto::{
                    BASIS_VS_RESOLUTION_SOURCE, DISTANCE_TO_STRIKE, TIME_TO_OBSERVATION,
                    UNDERLYING_MOMENTUM, UNDERLYING_REALIZED_VOL,
                },
            },
            value::{EvidenceSourceRef, FeatureValue, NullReason},
        },
    };

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
            available_at: Some(at),
        }
    }

    fn oracle_observation(at: DateTime<Utc>, value: Decimal) -> CryptoPriceReport {
        CryptoPriceReport {
            source_id: DomainSourceId::chainlink_data_streams(),
            instrument_key: DomainInstrumentKey::chainlink_data_streams(
                &ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
            ),
            source_sequence: u64::try_from(at.timestamp()).expect("positive timestamp"),
            price: Usd::new(value),
            quantity: None,
            event_time: at,
            published_at: at,
            available_at: at,
            valid_from: Some(at),
            observations_timestamp: Some(at),
            expires_at: Some(at + Duration::minutes(1)),
            report_hash: content_hash(),
            raw_report: "fixture".into(),
        }
    }

    fn content_hash() -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash")
    }

    fn binding(comparator: PriceComparator, strike: Option<Usd>) -> ResolvedBinding {
        binding_with_oracle(
            comparator,
            strike,
            ResolutionOracle::ChainlinkDataStreams {
                feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
            },
        )
    }

    fn binding_with_oracle(
        comparator: PriceComparator,
        strike: Option<Usd>,
        resolution_oracle: ResolutionOracle,
    ) -> ResolvedBinding {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator,
                strike,
                reference_at: Some(as_of - Duration::minutes(3)),
                observation_at: as_of + Duration::minutes(2),
                resolution_oracle,
            }),
            source_bindings: vec![ResolvedSourceBinding {
                role: LinkageSourceRole::Feature,
                source_id: DomainSourceId::binance(),
                instrument_key: instrument(),
                available_at: as_of,
                binding_hash: content_hash(),
            }],
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
                    close_observation(base - Duration::minutes(*minutes_ago), *value)
                })
                .collect(),
        }
    }

    fn linkage_evidence(as_of: DateTime<Utc>) -> EvidenceSourceRef {
        EvidenceSourceRef {
            source_kind: EvidenceSourceKind::Linkage,
            reference: "linkage:test@blake3:test".to_owned(),
            effective_at: as_of - Duration::minutes(1),
            available_at: Some(as_of - Duration::minutes(1)),
        }
    }

    fn domain(crypto: CryptoDomainConfig) -> DomainConfig {
        DomainConfig {
            crypto,
            ..DomainConfig::default()
        }
    }

    fn find<'a>(features: &'a [RawFeature], name: &FeatureName) -> &'a RawFeature {
        features
            .iter()
            .find(|feature| &feature.name == name)
            .expect("feature present")
    }

    #[test]
    fn above_threshold_distance_is_signed_toward_yes() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(2, dec!(99000)), (1, dec!(101000))]);
        let binding = binding(
            PriceComparator::GreaterThanOrEqual,
            Some(Usd::new(dec!(100000))),
        );
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &DISTANCE_TO_STRIKE);
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
        let binding = binding(
            PriceComparator::GreaterThanOrEqual,
            Some(Usd::new(dec!(100000))),
        );
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect_err("missing"),
            &NullReason::DomainSourceUnavailable
        );
    }

    #[test]
    fn basis_measures_binance_vs_chainlink_divergence() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100500))]);
        let oracle = CryptoPriceReportWindow {
            cutoff: as_of,
            reports: vec![oracle_observation(
                as_of - chrono::Duration::minutes(1),
                dec!(100000),
            )],
        };
        let subject_binding = binding(PriceComparator::UpVsReference, None);
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &subject_binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: Some(&oracle),
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let basis = find(&features, &BASIS_VS_RESOLUTION_SOURCE);
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
        let oracle = CryptoPriceReportWindow {
            cutoff: as_of,
            reports: vec![oracle_observation(
                as_of - chrono::Duration::minutes(3),
                dec!(100100),
            )],
        };
        let binding = binding(PriceComparator::UpVsReference, None);
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: Some(&oracle),
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect("present"),
            &FeatureValue::Decimal(dec!(0)),
            "close 100100 vs oracle PTB 100100 → flat"
        );
    }

    #[test]
    fn chainlink_ptb_fails_closed_without_oracle_window() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        // Binance has a ref at window open — must not be used when oracle is Chainlink.
        let primary = window(&[(3, dec!(100000)), (1, dec!(100100))]);
        let binding = binding(PriceComparator::UpVsReference, None);
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect_err("missing"),
            &NullReason::DomainSourceUnavailable,
            "Chainlink-settled PTB must not silently fall back to Binance"
        );
    }

    #[test]
    fn binance_settled_ptb_reads_feature_source_without_oracle() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let reference_at = as_of - Duration::minutes(3);
        // PTB = Binance close at reference_at; latest close matches it.
        let primary = window(&[(3, dec!(100000)), (1, dec!(100000))]);
        let mut binding = binding_with_oracle(
            PriceComparator::UpVsReference,
            None,
            ResolutionOracle::BinanceKline {
                market: BinanceMarketSegment::Spot,
                symbol: BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                interval: KlineInterval::OneMinute,
            },
        );
        let MarketSubject::Crypto(ref mut subject) = binding.subject else {
            panic!("crypto binding")
        };
        subject.reference_at = Some(reference_at);
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let distance = find(&features, &DISTANCE_TO_STRIKE);
        assert_eq!(
            distance.value.as_ref().expect("present"),
            &FeatureValue::Decimal(dec!(0)),
            "Binance-settled PTB reads Binance close at reference_at without an oracle window"
        );
    }

    #[test]
    fn stale_chainlink_oracle_triggers_stale_beyond_policy() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100500))]);
        let oracle = CryptoPriceReportWindow {
            cutoff: as_of,
            reports: vec![oracle_observation(
                as_of - chrono::Duration::seconds(120),
                dec!(100000),
            )],
        };
        let binding = binding(PriceComparator::UpVsReference, None);
        let mut crypto = CryptoDomainConfig::default();
        crypto.cross_check.max_oracle_staleness_secs = 30;
        let domain = domain(crypto);
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: Some(&oracle),
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let basis = find(&features, &BASIS_VS_RESOLUTION_SOURCE);
        assert_eq!(
            basis.value.as_ref().expect_err("missing"),
            &NullReason::StaleBeyondPolicy,
            "oracle older than max_oracle_staleness_secs must be rejected"
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
    fn time_to_observation_counts_down_without_fabricating_elapsed_zero() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100000))]);
        let subject_binding = binding(PriceComparator::UpVsReference, None);
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &subject_binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };
        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        let tto = find(&features, &TIME_TO_OBSERVATION);
        assert_eq!(
            tto.value.as_ref().expect("present"),
            &FeatureValue::Count(120),
            "observation_at is 2 minutes out"
        );
        assert_eq!(
            tto.evidence.as_ref(),
            Some(&linkage_evidence),
            "subject-derived time must cite the exact bitemporal linkage revision"
        );

        let mut elapsed_binding = binding(PriceComparator::UpVsReference, None);
        let MarketSubject::Crypto(ref mut subject) = elapsed_binding.subject else {
            panic!("crypto binding")
        };
        subject.observation_at = as_of - Duration::seconds(1);
        let elapsed_ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &elapsed_binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };
        let elapsed = CryptoDomainFeatureBuilder.compute(&elapsed_ctx);
        assert_eq!(
            find(&elapsed, &names::TIME_TO_OBSERVATION)
                .value
                .as_ref()
                .expect_err("elapsed observation must be missing"),
            &NullReason::OutOfValidRange
        );
    }

    #[test]
    fn future_oracle_is_invalid_not_fresh_age_zero() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100500))]);
        let oracle = CryptoPriceReportWindow {
            cutoff: as_of + Duration::seconds(1),
            reports: vec![oracle_observation(
                as_of + chrono::Duration::seconds(1),
                dec!(100000),
            )],
        };
        let binding = binding(
            PriceComparator::GreaterThanOrEqual,
            Some(Usd::new(dec!(100000))),
        );
        let domain = domain(CryptoDomainConfig::default());
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: Some(&oracle),
            },
            domain: &domain,
        };

        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        assert_eq!(
            find(&features, &names::BASIS_VS_RESOLUTION_SOURCE)
                .value
                .as_ref()
                .expect_err("future oracle must be missing"),
            &NullReason::OutOfValidRange
        );
    }

    #[test]
    fn unrepresentable_feature_windows_are_explicitly_missing() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let primary = window(&[(1, dec!(100000))]);
        let binding = binding(
            PriceComparator::GreaterThanOrEqual,
            Some(Usd::new(dec!(100000))),
        );
        let domain = domain(CryptoDomainConfig {
            momentum_window_secs: u64::MAX,
            volatility_window_secs: u64::MAX,
            ..CryptoDomainConfig::default()
        });
        let linkage_evidence = linkage_evidence(as_of);
        let ctx = DomainComputeCtx {
            decision_at: as_of,
            binding: &binding,
            linkage_evidence: &linkage_evidence,
            data: DomainSliceDataRef::Crypto {
                primary: &primary,
                oracle: None,
            },
            domain: &domain,
        };

        let features = CryptoDomainFeatureBuilder.compute(&ctx);
        for name in [&UNDERLYING_MOMENTUM, &UNDERLYING_REALIZED_VOL] {
            assert_eq!(
                find(&features, name)
                    .value
                    .as_ref()
                    .expect_err("invalid window must be missing"),
                &NullReason::OutOfValidRange
            );
        }
    }
}
