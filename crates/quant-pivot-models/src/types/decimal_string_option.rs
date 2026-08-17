//! Strict decimal-string encoding for nullable wire and JSONB fields.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

pub trait OptionalDecimalRef {
    fn optional_decimal(&self) -> Option<&Decimal>;
}

impl OptionalDecimalRef for Option<Decimal> {
    fn optional_decimal(&self) -> Option<&Decimal> {
        self.as_ref()
    }
}

pub fn serialize<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: OptionalDecimalRef,
{
    value
        .optional_decimal()
        .map(ToString::to_string)
        .serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| Decimal::from_str(&value).map_err(D::Error::custom))
        .transpose()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Fixture {
        #[serde(with = "super")]
        value: Option<Decimal>,
    }

    #[test]
    fn string_null_round_trips() {
        for fixture in [
            Fixture { value: None },
            Fixture {
                value: Some(Decimal::new(125, 2)),
            },
        ] {
            let encoded = serde_json::to_value(&fixture).expect("serialize decimal option");
            let decoded: Fixture =
                serde_json::from_value(encoded).expect("deserialize decimal option");
            assert_eq!(decoded, fixture);
        }
    }

    #[test]
    fn json_number_is_rejected() {
        assert!(serde_json::from_value::<Fixture>(json!({ "value": 1.25 })).is_err());
    }
}
