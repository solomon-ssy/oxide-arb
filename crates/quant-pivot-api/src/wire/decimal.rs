//! Lossless decimal parsing from JSON wire values (string or number, never `f64`).

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::Error};
use serde_json::Value;

/// Deserialize a decimal from a JSON number or string without binary `f64` loss.
pub fn de_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    parse_decimal_value(&value).map_err(Error::custom)
}

/// Parse one JSON value into a [`Decimal`] (shared by struct fields and seq visitors).
///
/// A JSON `null` is **rejected**, never silently coerced to `Decimal::ZERO`: a
/// present-but-null price/volume/PnL field is a corrupt or unexpectedly-shaped
/// upstream row, not a legitimate zero. Callers
/// that want "absent key ⇒ zero" get that for free from `#[serde(default)]`,
/// which never invokes this function for a genuinely missing key.
pub fn parse_decimal_value(value: &Value) -> Result<Decimal, String> {
    match value {
        Value::Null => Err("expected decimal number or string, got null".to_owned()),
        Value::Number(number) => {
            Decimal::from_str(&number.to_string()).map_err(|error| error.to_string())
        }
        Value::String(text) => Decimal::from_str(text).map_err(|error| error.to_string()),
        other => Err(format!("expected decimal number or string, got {other}")),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::Value;

    use super::*;

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

    #[test]
    fn null_is_rejected_not_coerced_to_zero() {
        let error = parse_decimal_value(&Value::Null).expect_err("must reject null");
        assert!(error.contains("null"));
    }
}
