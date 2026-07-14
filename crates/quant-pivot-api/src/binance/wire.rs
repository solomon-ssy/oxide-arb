//! Binance `GET /api/v3/klines` wire shapes.
//!
//! Official schema: fixed 12-element JSON array per row.
//! <https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints>

use std::fmt;

use rust_decimal::Decimal;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, SeqAccess, Visitor},
};

use crate::wire::decimal::parse_decimal_value;

/// Fixed field count of one Binance kline row.
pub const KLINE_FIELD_COUNT: usize = 12;

/// Binance aggregate-trade payload shared by REST `/aggTrades` and the
/// `<symbol>@aggTrade` WebSocket stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTrade {
    #[serde(rename = "e", default)]
    pub event_type: Option<String>,
    #[serde(rename = "E", default)]
    pub event_time_ms: Option<i64>,
    #[serde(rename = "s", default)]
    pub symbol: Option<String>,
    #[serde(rename = "a")]
    pub aggregate_trade_id: u64,
    #[serde(rename = "p", with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(rename = "q", with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    #[serde(rename = "T")]
    pub trade_time_ms: i64,
    #[serde(rename = "m")]
    pub buyer_is_market_maker: bool,
    #[serde(rename = "M", default)]
    pub best_price_match: Option<bool>,
}

/// One row from `GET /api/v3/klines` (12 fields, fixed order per Binance spot API).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceKlineRow {
    /// Kline open time (ms since epoch). Uniquely identifies the candle.
    pub open_time_ms: i64,
    /// Open price.
    pub open: Decimal,
    /// High price.
    pub high: Decimal,
    /// Low price.
    pub low: Decimal,
    /// Close price (feature-source price).
    pub close: Decimal,
    /// Base-asset volume.
    pub volume: Decimal,
    /// Kline close time (ms since epoch). PIT event time for closed candles.
    pub close_time_ms: i64,
    /// Quote-asset volume.
    pub quote_volume: Decimal,
    /// Number of trades in the interval.
    pub trade_count: u64,
    /// Taker buy base-asset volume.
    pub taker_buy_base_volume: Decimal,
    /// Taker buy quote-asset volume.
    pub taker_buy_quote_volume: Decimal,
    /// Unused field (ignore per Binance docs).
    pub ignore: String,
}

impl<'de> Deserialize<'de> for BinanceKlineRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(KlineRowVisitor)
    }
}

struct KlineRowVisitor;

impl<'de> Visitor<'de> for KlineRowVisitor {
    type Value = BinanceKlineRow;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a 12-element Binance kline array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let open_time_ms = next_i64(&mut seq, "open_time_ms")?;
        let open = next_decimal(&mut seq, "open")?;
        let high = next_decimal(&mut seq, "high")?;
        let low = next_decimal(&mut seq, "low")?;
        let close = next_decimal(&mut seq, "close")?;
        let volume = next_decimal(&mut seq, "volume")?;
        let close_time_ms = next_i64(&mut seq, "close_time_ms")?;
        let quote_volume = next_decimal(&mut seq, "quote_volume")?;
        let trade_count = next_u64(&mut seq, "trade_count")?;
        let taker_buy_base_volume = next_decimal(&mut seq, "taker_buy_base_volume")?;
        let taker_buy_quote_volume = next_decimal(&mut seq, "taker_buy_quote_volume")?;
        let ignore = next_string(&mut seq, "ignore")?;

        if seq.next_element::<de::IgnoredAny>()?.is_some() {
            return Err(de::Error::invalid_length(KLINE_FIELD_COUNT + 1, &self));
        }

        Ok(BinanceKlineRow {
            open_time_ms,
            open,
            high,
            low,
            close,
            volume,
            close_time_ms,
            quote_volume,
            trade_count,
            taker_buy_base_volume,
            taker_buy_quote_volume,
            ignore,
        })
    }
}

fn next_i64<'de, A: SeqAccess<'de>>(seq: &mut A, field: &'static str) -> Result<i64, A::Error> {
    let value = seq
        .next_element::<serde_json::Value>()?
        .ok_or_else(|| de::Error::invalid_length(KLINE_FIELD_COUNT, &ExpectedField(field)))?;
    value.as_i64().ok_or_else(|| {
        de::Error::custom(format!("binance kline field `{field}`: expected integer"))
    })
}

fn next_u64<'de, A: SeqAccess<'de>>(seq: &mut A, field: &'static str) -> Result<u64, A::Error> {
    let value = seq
        .next_element::<serde_json::Value>()?
        .ok_or_else(|| de::Error::invalid_length(KLINE_FIELD_COUNT, &ExpectedField(field)))?;
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|v| u64::try_from(v).ok()))
        .ok_or_else(|| {
            de::Error::custom(format!(
                "binance kline field `{field}`: expected unsigned integer"
            ))
        })
}

fn next_string<'de, A: SeqAccess<'de>>(
    seq: &mut A,
    field: &'static str,
) -> Result<String, A::Error> {
    let value = seq
        .next_element::<serde_json::Value>()?
        .ok_or_else(|| de::Error::invalid_length(KLINE_FIELD_COUNT, &ExpectedField(field)))?;
    match value {
        serde_json::Value::String(text) => Ok(text),
        other => Ok(other.to_string()),
    }
}

fn next_decimal<'de, A: SeqAccess<'de>>(
    seq: &mut A,
    field: &'static str,
) -> Result<Decimal, A::Error> {
    let value = seq
        .next_element::<serde_json::Value>()?
        .ok_or_else(|| de::Error::invalid_length(KLINE_FIELD_COUNT, &ExpectedField(field)))?;
    parse_decimal_value(&value)
        .map_err(|detail| de::Error::custom(format!("binance kline field `{field}`: {detail}")))
}

struct ExpectedField(&'static str);

impl de::Expected for ExpectedField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "binance kline field `{}`", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Official Binance doc fixture (12 fields).
    fn official_fixture_json() -> serde_json::Value {
        serde_json::json!([[
            1_499_040_000_000_i64,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1_499_644_799_999_i64,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0"
        ]])
    }

    #[test]
    fn deserializes_official_doc_fixture() {
        let rows: Vec<BinanceKlineRow> =
            serde_json::from_value(official_fixture_json()).expect("parse");
        let row = &rows[0];
        assert_eq!(row.open_time_ms, 1_499_040_000_000);
        assert_eq!(row.open, dec!(0.01634790));
        assert_eq!(row.high, dec!(0.80000000));
        assert_eq!(row.low, dec!(0.01575800));
        assert_eq!(row.close, dec!(0.01577100));
        assert_eq!(row.volume, dec!(148976.11427815));
        assert_eq!(row.close_time_ms, 1_499_644_799_999);
        assert_eq!(row.quote_volume, dec!(2434.19055334));
        assert_eq!(row.trade_count, 308);
        assert_eq!(row.taker_buy_base_volume, dec!(1756.87402397));
        assert_eq!(row.taker_buy_quote_volume, dec!(28.46694368));
        assert_eq!(row.ignore, "0");
    }

    #[test]
    fn rejects_short_row() {
        let json = serde_json::json!([[1, "0.01", "0.02"]]);
        let error = serde_json::from_value::<Vec<BinanceKlineRow>>(json).expect_err("too short");
        assert!(
            error.to_string().contains("open_time_ms")
                || error.to_string().contains("invalid length")
        );
    }

    #[test]
    fn rejects_extra_fields() {
        let json = serde_json::json!([[
            1_499_040_000_000_i64,
            "0.01",
            "0.02",
            "0.005",
            "0.01577100",
            "148976.11427815",
            1_499_644_799_999_i64,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0",
            "extra"
        ]]);
        serde_json::from_value::<Vec<BinanceKlineRow>>(json).expect_err("too long");
    }

    #[test]
    fn accepts_numeric_price() {
        let json = serde_json::json!([[
            1_494_904_000_000_i64,
            0.01,
            0.02,
            0.005,
            "0.01577100",
            "148976.11427815",
            1_499_644_799_999_i64,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0"
        ]]);
        let rows: Vec<BinanceKlineRow> = serde_json::from_value(json).expect("parse");
        assert_eq!(rows[0].close, dec!(0.01577100));
    }
}
