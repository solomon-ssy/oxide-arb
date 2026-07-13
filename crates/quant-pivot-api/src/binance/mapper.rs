//! Map Binance kline wire rows into normalized domain observations.

use chrono::{TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    domain::DomainObservation,
    enums::domain::{DomainFamily, DomainMetric},
    types::{DomainInstrumentKey, DomainSourceId},
};

use super::wire::BinanceKlineRow;

/// Map one kline row into its Close observation (the only metric any crypto
/// domain feature consumes — see [`DomainMetric`]'s doc for why base-asset
/// volume is deliberately not modeled).
///
/// # Errors
///
/// Returns [`ApiError::Deserialize`] when `close_time_ms` is not a valid UTC instant.
pub fn into_observations(
    row: &BinanceKlineRow,
    instrument_key: &DomainInstrumentKey,
) -> QuantResult<[DomainObservation; 1]> {
    let observed_at = Utc
        .timestamp_millis_opt(row.close_time_ms)
        .single()
        .ok_or_else(|| {
            QuantError::from(ApiError::Deserialize {
                context: "binance kline row".into(),
                detail: format!("close_time_ms invalid: {}", row.close_time_ms),
            })
        })?;

    Ok([DomainObservation {
        family: DomainFamily::Crypto,
        source_id: DomainSourceId::binance(),
        instrument_key: instrument_key.clone(),
        metric: DomainMetric::Close,
        value: row.close,
        observed_at,
        publish_time: observed_at,
        available_at: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::{enums::domain::KlineInterval, types::BinanceSymbol};
    use rust_decimal_macros::dec;

    use crate::binance::wire::BinanceKlineRow;

    fn sample_row(close_time_ms: i64) -> BinanceKlineRow {
        BinanceKlineRow {
            open_time_ms: close_time_ms - 60_000,
            open: dec!(0.01),
            high: dec!(0.02),
            low: dec!(0.005),
            close: dec!(0.01577100),
            volume: dec!(148976.11427815),
            close_time_ms,
            quote_volume: dec!(2434.19055334),
            trade_count: 308,
            taker_buy_base_volume: dec!(1756.87402397),
            taker_buy_quote_volume: dec!(28.46694368),
            ignore: "0".to_owned(),
        }
    }

    #[test]
    fn maps_close_observation() {
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        let observations = into_observations(&sample_row(1_499_644_799_999), &key).expect("map");
        assert_eq!(observations[0].metric, DomainMetric::Close);
        assert_eq!(observations[0].value, dec!(0.01577100));
        assert_eq!(
            observations[0].observed_at.timestamp_millis(),
            1_499_644_799_999
        );
    }

    #[test]
    fn rejects_invalid_close_time() {
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        into_observations(&sample_row(i64::MAX), &key).expect_err("invalid ts");
    }
}
