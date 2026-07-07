//! Lossless decimal parsing from JSON wire values (string or number, never `f64`).

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

/// Deserialize a decimal from a JSON number or string without binary `f64` loss.
pub fn de_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    parse_decimal_value(&value).map_err(serde::de::Error::custom)
}

/// Parse one JSON value into a [`Decimal`] (shared by struct fields and seq visitors).
pub fn parse_decimal_value(value: &serde_json::Value) -> Result<Decimal, String> {
    match value {
        serde_json::Value::Null => Ok(Decimal::ZERO),
        serde_json::Value::Number(number) => {
            Decimal::from_str(&number.to_string()).map_err(|error| error.to_string())
        }
        serde_json::Value::String(text) => {
            Decimal::from_str(text).map_err(|error| error.to_string())
        }
        other => Err(format!("expected decimal number or string, got {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parses_string_and_number() {
        assert_eq!(
            parse_decimal_value(&serde_json::json!("0.01577100")).expect("string"),
            dec!(0.01577100)
        );
        assert_eq!(
            parse_decimal_value(&serde_json::json!(100.0)).expect("number"),
            dec!(100)
        );
    }
}
