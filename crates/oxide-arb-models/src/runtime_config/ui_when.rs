//! Conditional visibility/require rules for preferences form fields.
//!
//! Aligned with ng-gateway [`Operator`] / [`WhenEffect`] so the same wire
//! vocabulary can be reused across gateway driver forms and runtime-config
//! preferences.

use serde::Serialize;
use serde_json::Value;

/// Comparison operator for cross-field UI rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenOperator {
    Eq,
    /// Alias: legacy `ne` serializes to the same variant.
    #[serde(alias = "ne")]
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    Prefix,
    Suffix,
    Regex,
    In,
    NotIn,
    Between,
    NotBetween,
    NotNull,
}

/// Effect applied when a `when` rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenEffect {
    /// Structural mount gate — hidden fields skip validation and patch emission.
    If,
    /// Inverse of [`If`](Self::If).
    IfNot,
    /// CSS visibility only (preferences UI treats as [`If`](Self::If)).
    Visible,
    Invisible,
    Enable,
    Disable,
    Require,
    Optional,
}

/// One conditional rule referencing another schema leaf by dotted path.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FieldWhen {
    pub target_path: String,
    pub operator: WhenOperator,
    pub value: Value,
    pub effect: WhenEffect,
}

/// Evaluate one `when` rule against a resolved target value.
#[must_use]
pub fn evaluate_when(operator: WhenOperator, actual: Option<&Value>, expected: &Value) -> bool {
    match operator {
        WhenOperator::NotNull => actual.is_some() && !matches!(actual, Some(Value::Null)),
        WhenOperator::Eq => loosely_equal_opt(actual, expected),
        WhenOperator::Neq => !loosely_equal_opt(actual, expected),
        WhenOperator::Gt => compare(actual, expected, |ord| ord > 0),
        WhenOperator::Gte => compare(actual, expected, |ord| ord >= 0),
        WhenOperator::Lt => compare(actual, expected, |ord| ord < 0),
        WhenOperator::Lte => compare(actual, expected, |ord| ord <= 0),
        WhenOperator::Contains => contains(actual, expected),
        WhenOperator::Prefix => prefix(actual, expected),
        WhenOperator::Suffix => suffix(actual, expected),
        WhenOperator::Regex => regex(actual, expected),
        WhenOperator::In => in_list(actual, expected, true),
        WhenOperator::NotIn => in_list(actual, expected, false),
        WhenOperator::Between => between(actual, expected, true),
        WhenOperator::NotBetween => between(actual, expected, false),
    }
}

fn loosely_equal_opt(actual: Option<&Value>, expected: &Value) -> bool {
    actual.map_or_else(|| expected.is_null(), |left| loosely_equal(left, expected))
}

fn loosely_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => a.to_string() == b.to_string(),
        (Value::Null, Value::Null) => true,
        _ => match (to_string(left), to_string(right)) {
            (Some(a), Some(b)) => a == b,
            _ => left == right,
        },
    }
}

fn compare<F>(actual: Option<&Value>, expected: &Value, predicate: F) -> bool
where
    F: FnOnce(i8) -> bool,
{
    let Some(left) = actual else {
        return false;
    };
    if let (Some(a), Some(b)) = (to_f64(left), to_f64(expected)) {
        return predicate(a.partial_cmp(&b).map_or(1, |ord| match ord {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }));
    }
    if let (Some(a), Some(b)) = (to_string(left), to_string(expected)) {
        return predicate(a.cmp(&b) as i8);
    }
    if let (Some(a), Some(b)) = (to_bool(left), to_bool(expected)) {
        return predicate(i8::from(a) - i8::from(b));
    }
    false
}

fn contains(actual: Option<&Value>, expected: &Value) -> bool {
    match (actual, expected) {
        (Some(Value::String(haystack)), Value::String(needle)) => haystack.contains(needle),
        (Some(Value::Array(items)), _) => items.iter().any(|item| loosely_equal(item, expected)),
        _ => false,
    }
}

fn prefix(actual: Option<&Value>, expected: &Value) -> bool {
    match (actual, expected) {
        (Some(Value::String(haystack)), Value::String(needle)) => haystack.starts_with(needle),
        _ => false,
    }
}

fn suffix(actual: Option<&Value>, expected: &Value) -> bool {
    match (actual, expected) {
        (Some(Value::String(haystack)), Value::String(needle)) => haystack.ends_with(needle),
        _ => false,
    }
}

fn regex(actual: Option<&Value>, expected: &Value) -> bool {
    let (Some(Value::String(target)), Value::String(pattern)) = (actual, expected) else {
        return false;
    };
    regex::Regex::new(pattern)
        .ok()
        .is_some_and(|compiled| compiled.is_match(target))
}

fn in_list(actual: Option<&Value>, expected: &Value, positive: bool) -> bool {
    let Some(left) = actual else {
        return !positive;
    };
    let found = match expected {
        Value::Array(items) => items.iter().any(|item| loosely_equal(left, item)),
        Value::String(csv) => csv
            .split(',')
            .map(str::trim)
            .any(|part| to_string(left).as_deref() == Some(part)),
        _ => false,
    };
    if positive { found } else { !found }
}

fn between(actual: Option<&Value>, expected: &Value, positive: bool) -> bool {
    let Some(left) = actual else {
        return false;
    };
    let Value::Array(bounds) = expected else {
        return false;
    };
    if bounds.len() != 2 {
        return false;
    }
    let (Some(value), Some(min), Some(max)) =
        (to_f64(left), to_f64(&bounds[0]), to_f64(&bounds[1]))
    else {
        return false;
    };
    let ok = value >= min && value <= max;
    if positive { ok } else { !ok }
}

fn to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        Value::Bool(flag) => Some(f64::from(u8::from(*flag))),
        _ => None,
    }
}

fn to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn to_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        Value::Number(number) => Some(number.as_f64().unwrap_or(0.0) != 0.0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eq_and_neq_use_loose_equality() {
        assert!(evaluate_when(
            WhenOperator::Eq,
            Some(&json!("1")),
            &json!(1),
        ));
        assert!(evaluate_when(
            WhenOperator::Neq,
            Some(&json!("paper")),
            &json!("live"),
        ));
    }

    #[test]
    fn in_and_not_in_work_on_arrays() {
        assert!(evaluate_when(
            WhenOperator::In,
            Some(&json!("proxy_safe")),
            &json!(["disabled", "proxy_safe"]),
        ));
        assert!(evaluate_when(
            WhenOperator::NotIn,
            Some(&json!("live")),
            &json!(["dry_run", "paper"]),
        ));
    }

    #[test]
    fn not_null_detects_missing_and_null() {
        assert!(!evaluate_when(WhenOperator::NotNull, None, &Value::Null));
        assert!(!evaluate_when(
            WhenOperator::NotNull,
            Some(&Value::Null),
            &Value::Null
        ));
        assert!(evaluate_when(
            WhenOperator::NotNull,
            Some(&json!("x")),
            &Value::Null
        ));
    }
}
