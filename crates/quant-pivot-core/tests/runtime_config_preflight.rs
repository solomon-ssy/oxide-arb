//! Runtime-config activation preflight against live exposure reservations.
//!
//! Money-critical fail-closed behaviour: a candidate config whose exposure
//! ceilings fall below capital that is already committed must be rejected at
//! activation time, while loosening (or unrelated) changes pass.

use oxide_arb_core::exposure::in_memory::InMemoryExposureReservation;
use oxide_arb_models::{
    enums::common::ExecutionMode,
    runtime_config::{
        RuntimeConfig,
        validation::{RuntimePreflightContext, preflight_runtime_config},
    },
    types::{MarketId, Usd},
};
use rust_decimal_macros::dec;
use std::time::Duration;

fn reserve(exposure: &InMemoryExposureReservation, market: &str, usd: rust_decimal::Decimal) {
    exposure
        .try_reserve_sync(
            &MarketId::new(market),
            Usd::new(usd),
            Duration::from_secs(300),
        )
        .expect("reservation within default limits");
}

fn live_context(exposure: &InMemoryExposureReservation) -> RuntimePreflightContext {
    RuntimePreflightContext {
        mode: ExecutionMode::Paper,
        reserved_total_usd: exposure.total_reserved_usd_sync().inner(),
        max_market_reserved_usd: exposure.max_market_reserved_usd_sync().inner(),
    }
}

#[test]
fn preflight_rejects_total_exposure_below_reserved_capital() {
    let config = RuntimeConfig::default();
    let exposure = InMemoryExposureReservation::new(config.risk.exposure_reservation_config());
    reserve(&exposure, "m1", dec!(400));
    reserve(&exposure, "m2", dec!(300));

    let mut candidate = RuntimeConfig::default();
    candidate.risk.max_total_exposure_usd = dec!(500);

    let report = preflight_runtime_config(&candidate, &live_context(&exposure));
    assert!(
        report.has_errors(),
        "tightening total exposure below 700 reserved must fail closed"
    );
}

#[test]
fn preflight_rejects_market_exposure_below_inflight_market() {
    let config = RuntimeConfig::default();
    let exposure = InMemoryExposureReservation::new(config.risk.exposure_reservation_config());
    reserve(&exposure, "m1", dec!(450));

    let mut candidate = RuntimeConfig::default();
    candidate.risk.max_single_market_exposure_usd = dec!(100);

    let report = preflight_runtime_config(&candidate, &live_context(&exposure));
    assert!(
        report.has_errors(),
        "tightening per-market exposure below an in-flight market must fail closed"
    );
}

#[test]
fn preflight_accepts_limits_above_reserved_capital() {
    let config = RuntimeConfig::default();
    let exposure = InMemoryExposureReservation::new(config.risk.exposure_reservation_config());
    reserve(&exposure, "m1", dec!(400));

    let mut candidate = RuntimeConfig::default();
    candidate.risk.max_total_exposure_usd = dec!(6000);
    candidate.risk.max_single_market_exposure_usd = dec!(500);

    let report = preflight_runtime_config(&candidate, &live_context(&exposure));
    assert!(!report.has_errors(), "errors: {:?}", report.errors);
}

#[test]
fn reload_gates_subsequent_reservations() {
    let config = RuntimeConfig::default();
    let exposure = InMemoryExposureReservation::new(config.risk.exposure_reservation_config());
    reserve(&exposure, "m1", dec!(400));

    // Loosened per-market ceiling applies immediately to the next reservation.
    let mut loosened = RuntimeConfig::default();
    loosened.risk.max_single_market_exposure_usd = dec!(2000);
    loosened.risk.max_total_exposure_usd = dec!(10_000);
    exposure.reload(loosened.risk.exposure_reservation_config());
    reserve(&exposure, "m1", dec!(1000));

    assert_eq!(exposure.total_reserved_usd_sync(), Usd::new(dec!(1400)));
    assert_eq!(
        exposure.max_market_reserved_usd_sync(),
        Usd::new(dec!(1400))
    );
}
