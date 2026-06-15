//! Flat schema-field metadata for the runtime-config UI form renderer.
//!
//! Walks the generated JSON Schema once and pairs each leaf with its compiled-in
//! default from [`RuntimeConfig::default`].

use crate::domain::{JsonValueType, SchemaFieldConstraints, SchemaFieldFormat};
use crate::runtime_config::{MASKED_SECRET, RuntimeConfig, RuntimeConfigError, ui_enum};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

/// Classified JSON Schema leaf used by patch validation and UI projection.
#[derive(Debug, Clone)]
pub struct SchemaLeaf {
    pub path: String,
    pub group: String,
    pub value_type: JsonValueType,
    pub format: Option<SchemaFieldFormat>,
    pub default: Value,
    pub description: String,
    pub money_critical: bool,
    pub sensitive: bool,
    pub constraints: Option<SchemaFieldConstraints>,
    pub schema: Value,
}

/// Walk all schema leaves (including hidden / non-preferences fields).
#[must_use]
pub fn walk_schema_leaves(
    schema: &Value,
    default: &Value,
    path: &str,
    inherited_money_critical: bool,
) -> Vec<SchemaLeaf> {
    let mut leaves = Vec::with_capacity(128);
    walk_schema(schema, default, path, inherited_money_critical, &mut leaves);
    leaves
}

/// Build the flat field-metadata list (legacy helper for tests).
#[must_use]
pub fn build_schema_fields() -> Vec<SchemaLeaf> {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    walk_schema_leaves(&schema, &defaults, "", false)
}

/// Known schema leaf paths (for sparse patch validation).
#[must_use]
pub fn schema_leaf_paths() -> HashSet<String> {
    build_schema_fields()
        .into_iter()
        .map(|field| field.path)
        .collect()
}

/// Sensitive schema leaf paths.
#[must_use]
pub fn sensitive_leaf_paths() -> HashSet<String> {
    build_schema_fields()
        .into_iter()
        .filter(|field| field.sensitive)
        .map(|field| field.path)
        .collect()
}

/// Merge a sparse patch onto the live config, then typed-parse.
///
/// Paths absent from `patch` inherit the current live value (including sensitive
/// credentials). Patch keys must be known schema leaves.
pub fn apply_runtime_config_patch(
    current: &RuntimeConfig,
    patch: &BTreeMap<String, Value>,
) -> Result<RuntimeConfig, RuntimeConfigPatchError> {
    let known = schema_leaf_paths();
    for path in patch.keys() {
        if !known.contains(path) {
            return Err(RuntimeConfigPatchError::UnknownPath(path.clone()));
        }
    }
    for (path, value) in patch {
        if sensitive_leaf_paths().contains(path) {
            validate_sensitive_patch_value(path, value)?;
        }
    }

    let mut document = current.to_json();
    for (path, value) in patch {
        set_path(&mut document, path, value.clone())?;
    }
    RuntimeConfig::from_json(&document).map_err(RuntimeConfigPatchError::from)
}

/// Sparse patch validation failures.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeConfigPatchError {
    #[error("unknown runtime config path: {0}")]
    UnknownPath(String),
    #[error("sensitive path {path} must not use the mask placeholder")]
    MaskedSensitive { path: String },
    #[error(transparent)]
    Parse(#[from] RuntimeConfigError),
    #[error("invalid patch path {path}: {reason}")]
    InvalidPath { path: String, reason: String },
}

fn validate_sensitive_patch_value(
    path: &str,
    value: &Value,
) -> Result<(), RuntimeConfigPatchError> {
    let Some(text) = value.as_str() else {
        return Err(RuntimeConfigPatchError::InvalidPath {
            path: path.to_owned(),
            reason: "sensitive values must be strings".into(),
        });
    };
    if text == MASKED_SECRET {
        return Err(RuntimeConfigPatchError::MaskedSensitive {
            path: path.to_owned(),
        });
    }
    Ok(())
}

fn walk_schema(
    schema: &Value,
    default: &Value,
    path: &str,
    inherited_money_critical: bool,
    fields: &mut Vec<SchemaLeaf>,
) {
    let money_critical = inherited_money_critical || schema_flag(schema, "x-money-critical");
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, child_schema) in properties {
            let child_path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            let child_default = default.get(key).cloned().unwrap_or(Value::Null);
            walk_schema(
                child_schema,
                &child_default,
                &child_path,
                money_critical,
                fields,
            );
        }
        return;
    }

    let (value_type, format, constraints) = classify_leaf(schema, path, default);
    let group = path.split('.').next().unwrap_or("root").to_owned();

    fields.push(SchemaLeaf {
        path: path.to_owned(),
        group,
        value_type,
        format,
        default: default.clone(),
        description: schema
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or_default()
            .to_owned(),
        money_critical,
        sensitive: schema_flag(schema, "x-sensitive"),
        constraints,
        schema: schema.clone(),
    });
}

pub(crate) fn classify_leaf(
    schema: &Value,
    path: &str,
    default: &Value,
) -> (
    JsonValueType,
    Option<SchemaFieldFormat>,
    Option<SchemaFieldConstraints>,
) {
    let type_name = schema.get("type").and_then(|value| {
        value.as_str().map_or_else(
            || {
                value.as_array().and_then(|names| {
                    names
                        .iter()
                        .filter_map(Value::as_str)
                        .find(|name| *name != "null")
                        .map(str::to_owned)
                })
            },
            |name| Some(name.to_owned()),
        )
    });

    if type_name.as_deref() == Some("array") {
        return classify_array_leaf(schema);
    }
    if type_name.as_deref() == Some("object") && is_open_map_object(schema) {
        if let Some(classified) = classify_map_leaf(schema, default) {
            return classified;
        }
    }

    if let Some(enum_values) = extract_enum_values(schema) {
        return (
            JsonValueType::Enum,
            None,
            Some(SchemaFieldConstraints {
                enum_values: Some(enum_values),
                ..SchemaFieldConstraints::default()
            }),
        );
    }

    let value_type = schema_value_type(schema);
    let format = infer_format(schema, path, value_type);
    let constraints = extract_constraints(schema);
    (value_type, format, constraints)
}

fn is_open_map_object(schema: &Value) -> bool {
    schema.get("properties").is_none() && schema.get("additionalProperties").is_some()
}

fn classify_array_leaf(
    schema: &Value,
) -> (
    JsonValueType,
    Option<SchemaFieldFormat>,
    Option<SchemaFieldConstraints>,
) {
    let items = schema.get("items").unwrap_or(&Value::Null);
    if let Some(enum_values) = extract_enum_values(items) {
        return (
            JsonValueType::EnumArray,
            None,
            Some(SchemaFieldConstraints {
                enum_values: Some(enum_values),
                ..SchemaFieldConstraints::default()
            }),
        );
    }
    if items.get("type") == Some(&Value::String(String::from("string"))) {
        return (JsonValueType::StringArray, None, extract_constraints(items));
    }
    (JsonValueType::Array, None, extract_constraints(items))
}

fn classify_map_leaf(
    schema: &Value,
    _default: &Value,
) -> Option<(
    JsonValueType,
    Option<SchemaFieldFormat>,
    Option<SchemaFieldConstraints>,
)> {
    let additional = schema.get("additionalProperties")?;
    let map_key_enum = schema
        .get("x-map-key-enum")
        .and_then(Value::as_str)
        .map(str::to_owned)?;
    if additional.get("type") != Some(&Value::String(String::from("string"))) {
        return None;
    }
    let value_format = schema
        .get("x-value-format")
        .and_then(Value::as_str)
        .and_then(|raw| match raw {
            "decimal" => Some(SchemaFieldFormat::Decimal),
            _ => None,
        });
    if value_format != Some(SchemaFieldFormat::Decimal) {
        return None;
    }
    let enum_values = ui_enum::enum_items_for_id(&map_key_enum)
        .map(|items| items.into_iter().map(|item| item.key).collect())
        .filter(|values: &Vec<Value>| !values.is_empty())?;
    Some((
        JsonValueType::EnumDecimalMap,
        Some(SchemaFieldFormat::Decimal),
        Some(SchemaFieldConstraints {
            enum_values: Some(enum_values),
            ..SchemaFieldConstraints::default()
        }),
    ))
}

fn extract_enum_values(schema: &Value) -> Option<Vec<Value>> {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return Some(values.clone());
    }
    let one_of = schema.get("oneOf")?.as_array()?;
    let mut values = Vec::new();
    for variant in one_of {
        if let Some(const_value) = variant.get("const") {
            values.push(const_value.clone());
        } else if let Some(enum_values) = variant.get("enum").and_then(Value::as_array) {
            values.extend(enum_values.iter().cloned());
        } else {
            return None;
        }
    }
    (!values.is_empty()).then_some(values)
}

fn extract_constraints(schema: &Value) -> Option<SchemaFieldConstraints> {
    let constraints = SchemaFieldConstraints {
        minimum: schema.get("minimum").and_then(Value::as_f64),
        maximum: schema.get("maximum").and_then(Value::as_f64),
        exclusive_minimum: schema.get("exclusiveMinimum").and_then(Value::as_f64),
        exclusive_maximum: schema.get("exclusiveMaximum").and_then(Value::as_f64),
        min_length: schema.get("minLength").and_then(Value::as_u64),
        max_length: schema.get("maxLength").and_then(Value::as_u64),
        pattern: schema
            .get("pattern")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        enum_values: None,
    };
    let has_any = constraints.minimum.is_some()
        || constraints.maximum.is_some()
        || constraints.exclusive_minimum.is_some()
        || constraints.exclusive_maximum.is_some()
        || constraints.min_length.is_some()
        || constraints.max_length.is_some()
        || constraints.pattern.is_some();
    has_any.then_some(constraints)
}

fn infer_format(
    schema: &Value,
    _path: &str,
    _value_type: JsonValueType,
) -> Option<SchemaFieldFormat> {
    if let Some(raw) = schema.get("x-format").and_then(Value::as_str) {
        return match raw {
            "decimal" => Some(SchemaFieldFormat::Decimal),
            "integer" => Some(SchemaFieldFormat::Integer),
            "duration_ms" => Some(SchemaFieldFormat::DurationMs),
            _ => None,
        };
    }
    None
}

pub(crate) fn schema_flag(schema: &Value, flag: &str) -> bool {
    schema.get(flag).and_then(Value::as_bool) == Some(true)
}

fn schema_value_type(schema: &Value) -> JsonValueType {
    if extract_enum_values(schema).is_some() {
        return JsonValueType::Enum;
    }
    let type_name = match schema.get("type") {
        Some(Value::String(name)) => Some(name.as_str()),
        Some(Value::Array(names)) => names
            .iter()
            .filter_map(Value::as_str)
            .find(|name| *name != "null"),
        _ => None,
    };
    match type_name {
        Some("number" | "integer") => JsonValueType::Number,
        Some("boolean") => JsonValueType::Boolean,
        Some("array") => JsonValueType::Array,
        Some("object") => JsonValueType::Object,
        _ => JsonValueType::String,
    }
}

fn set_path(document: &mut Value, path: &str, value: Value) -> Result<(), RuntimeConfigPatchError> {
    let segments: Vec<&str> = path.split('.').collect();
    if segments.is_empty() {
        return Err(RuntimeConfigPatchError::InvalidPath {
            path: path.to_owned(),
            reason: "empty path".into(),
        });
    }
    let mut cursor = document;
    for segment in &segments[..segments.len() - 1] {
        let Some(next) = cursor.get_mut(*segment) else {
            return Err(RuntimeConfigPatchError::InvalidPath {
                path: path.to_owned(),
                reason: format!("missing segment '{segment}'"),
            });
        };
        if !next.is_object() {
            return Err(RuntimeConfigPatchError::InvalidPath {
                path: path.to_owned(),
                reason: format!("segment '{segment}' is not an object"),
            });
        }
        cursor = next;
    }
    let leaf = segments.last().expect("non-empty segments");
    let Some(object) = cursor.as_object_mut() else {
        return Err(RuntimeConfigPatchError::InvalidPath {
            path: path.to_owned(),
            reason: "parent is not an object".into(),
        });
    };
    object.insert((*leaf).to_owned(), value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    #[test]
    fn schema_covers_every_leaf_with_a_description() {
        let fields = build_schema_fields();
        assert!(!fields.is_empty());
        let undescribed: Vec<_> = fields
            .iter()
            .filter(|field| field.description.trim().is_empty())
            .map(|field| field.path.clone())
            .collect();
        assert!(
            undescribed.is_empty(),
            "undescribed fields: {undescribed:?}"
        );
    }

    #[test]
    fn decimal_money_field_has_decimal_format() {
        let fields = build_schema_fields();
        let daily_loss = fields
            .iter()
            .find(|field| field.path == "risk.max_daily_loss_usd")
            .expect("daily loss field");
        assert_eq!(daily_loss.value_type, JsonValueType::String);
        assert_eq!(daily_loss.format, Some(SchemaFieldFormat::Decimal));
        assert_eq!(
            daily_loss.schema.get("x-format").and_then(Value::as_str),
            Some("decimal")
        );
    }

    #[test]
    fn typed_wire_leaves_declare_x_format() {
        let missing: Vec<_> = build_schema_fields()
            .into_iter()
            .filter(|leaf| {
                matches!(leaf.value_type, JsonValueType::Number)
                    || (leaf.value_type == JsonValueType::String && leaf.format.is_some())
            })
            .filter(|leaf| leaf.schema.get("x-format").is_none())
            .map(|leaf| leaf.path)
            .collect();
        assert!(missing.is_empty(), "missing x-format: {missing:?}");
    }

    #[test]
    fn duration_ms_fields_declare_duration_format() {
        for path in [
            "market_data.staleness_fresh_ms",
            "execution.timeout.dispatcher_timeout_ms",
        ] {
            let field = build_schema_fields()
                .into_iter()
                .find(|leaf| leaf.path == path)
                .unwrap_or_else(|| panic!("missing {path}"));
            assert_eq!(field.format, Some(SchemaFieldFormat::DurationMs));
            assert_eq!(
                field.schema.get("x-format").and_then(Value::as_str),
                Some("duration_ms")
            );
        }
    }

    #[test]
    fn enum_field_exposes_enum_values() {
        let fields = build_schema_fields();
        let strategy = fields
            .iter()
            .find(|field| field.path.contains("all_sources_down"))
            .expect("enum strategy field");
        assert_eq!(strategy.value_type, JsonValueType::Enum);
        assert!(
            strategy
                .constraints
                .as_ref()
                .and_then(|c| c.enum_values.as_ref())
                .is_some()
        );
    }

    #[test]
    fn enabled_categories_is_enum_array_with_item_variants() {
        let fields = build_schema_fields();
        let field = fields
            .iter()
            .find(|field| field.path == "market_data.enabled_categories")
            .expect("enabled_categories field");
        assert_eq!(field.value_type, JsonValueType::EnumArray);
        assert_eq!(field.default, json!([]));
        let enum_values = field
            .constraints
            .as_ref()
            .and_then(|c| c.enum_values.as_ref())
            .expect("item enum values");
        assert_eq!(enum_values.len(), 10);
    }

    #[test]
    fn permanent_blacklist_fields_are_string_arrays() {
        let fields = build_schema_fields();
        for path in [
            "risk.permanent_blacklist_markets",
            "risk.permanent_blacklist_tokens",
        ] {
            let field = fields.iter().find(|field| field.path == path).expect(path);
            assert_eq!(field.value_type, JsonValueType::StringArray);
            assert_eq!(field.default, json!([]));
        }
    }

    #[test]
    fn permanent_blacklist_items_expose_validation_patterns() {
        let fields = build_schema_fields();
        let markets = fields
            .iter()
            .find(|field| field.path == "risk.permanent_blacklist_markets")
            .expect("markets");
        let tokens = fields
            .iter()
            .find(|field| field.path == "risk.permanent_blacklist_tokens")
            .expect("tokens");
        assert_eq!(
            markets
                .constraints
                .as_ref()
                .and_then(|c| c.pattern.as_deref()),
            Some("^0x[0-9a-fA-F]{64}$")
        );
        assert_eq!(
            tokens
                .constraints
                .as_ref()
                .and_then(|c| c.pattern.as_deref()),
            Some("^[0-9]+$")
        );
    }

    #[test]
    fn category_weights_is_enum_decimal_map() {
        let fields = build_schema_fields();
        let field = fields
            .iter()
            .find(|field| field.path == "detection.endgame.scorer.category_weights")
            .expect("category_weights field");
        assert_eq!(field.value_type, JsonValueType::EnumDecimalMap);
        assert_eq!(field.format, Some(SchemaFieldFormat::Decimal));
        let keys = field
            .constraints
            .as_ref()
            .and_then(|c| c.enum_values.as_ref())
            .expect("map key enum values");
        assert_eq!(keys.len(), 10);
    }

    #[test]
    fn patch_inherits_sensitive_credentials() {
        let mut current = RuntimeConfig::default();
        current.notification.telegram.bot_token = "live-token".into();
        let mut patch = BTreeMap::new();
        patch.insert("risk.max_daily_loss_usd".into(), json!("175"));
        let merged = apply_runtime_config_patch(&current, &patch).expect("patch merge");
        assert_eq!(merged.risk.max_daily_loss_usd, dec!(175));
        assert_eq!(merged.notification.telegram.bot_token, "live-token");
    }

    #[test]
    fn patch_rejects_mask_placeholder_on_sensitive_path() {
        let current = RuntimeConfig::default();
        let mut patch = BTreeMap::new();
        patch.insert(
            "notification.telegram.bot_token".into(),
            json!(MASKED_SECRET),
        );
        let error = apply_runtime_config_patch(&current, &patch).expect_err("masked rejected");
        assert!(matches!(
            error,
            RuntimeConfigPatchError::MaskedSensitive { .. }
        ));
    }
}

#[cfg(test)]
mod dump_paths {
    use super::build_schema_fields;
    #[test]
    #[ignore = "manual path dump helper for ui-catalog generator"]
    fn dump_all_paths() {
        for field in build_schema_fields() {
            eprintln!("{}|{}", field.path, field.description.replace('\n', " "));
        }
    }
}
