//! Preferences UI schema projection (`GET /runtime-config/schema`).

use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use crate::domain::{
    JsonValueType, RuntimeConfigSchemaFieldView, RuntimeConfigSchemaGroupView,
    RuntimeConfigSchemaView, SchemaFieldFormat,
};
use crate::runtime_config::RuntimeConfig;
use crate::runtime_config::ui_enum::enum_items_for_id;
use crate::runtime_config::ui_registry::{field_ui_map, groups_ui};
use crate::runtime_config::ui_widget::{FieldWhen, FieldWidget};

use super::schema_fields::walk_schema_leaves;

/// Build the preferences envelope for `GET /runtime-config/schema`.
#[must_use]
pub fn build_preferences_schema() -> RuntimeConfigSchemaView {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let leaves = walk_schema_leaves(&schema, &defaults, "", false);

    let ui_map = field_ui_map();
    let mut fields = Vec::with_capacity(leaves.len());
    let mut group_ids: HashSet<String> = HashSet::new();

    for leaf in leaves {
        let Some(ui) = ui_map.get(leaf.path.as_str()) else {
            continue;
        };
        if !ui.visible {
            continue;
        }

        group_ids.insert(leaf.group.clone());

        let enum_id = schema_string_flag(&leaf.schema, "x-enum-id")
            .or_else(|| schema_string_flag(&leaf.schema, "x-map-key-enum"));
        let enum_items = enum_id.as_deref().and_then(enum_items_for_id);

        let widget = ui
            .widget
            .or_else(|| Some(infer_widget(leaf.value_type, leaf.format, leaf.sensitive)));
        let format = leaf.format;

        fields.push(RuntimeConfigSchemaFieldView {
            path: leaf.path.clone(),
            group: leaf.group.clone(),
            order: ui.order,
            label: ui.label.clone(),
            help: ui.help.clone(),
            value_type: leaf.value_type,
            format,
            widget,
            semantics: ui.semantics,
            default: leaf.default.clone(),
            description: leaf.description.clone(),
            money_critical: leaf.money_critical,
            sensitive: leaf.sensitive,
            constraints: leaf.constraints.clone(),
            enum_items,
            when: field_when_rules(&leaf.path),
        });
    }

    fields.sort_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then_with(|| left.order.cmp(&right.order))
            .then_with(|| left.path.cmp(&right.path))
    });

    let registered_groups: BTreeMap<_, _> = groups_ui()
        .into_iter()
        .map(|group| (group.id.to_string(), group))
        .collect();

    let mut groups: Vec<RuntimeConfigSchemaGroupView> = group_ids
        .into_iter()
        .filter_map(|id| {
            registered_groups
                .get(&id)
                .map(|group| RuntimeConfigSchemaGroupView {
                    id: id.clone(),
                    label: group.label.clone(),
                    description: Some(group.description.clone()),
                    order: group.order,
                })
        })
        .collect();
    groups.sort_by_key(|group| group.order);

    RuntimeConfigSchemaView { groups, fields }
}

fn schema_string_flag(schema: &Value, flag: &str) -> Option<String> {
    schema.get(flag).and_then(Value::as_str).map(str::to_owned)
}

fn field_when_rules(path: &str) -> Option<Vec<FieldWhen>> {
    use crate::runtime_config::ui_widget::{WhenEffect, WhenOperator};

    match path {
        "settlement.redeem.proxy_safe_address" => Some(vec![
            FieldWhen {
                target_path: "settlement.redeem.route".into(),
                operator: WhenOperator::Eq,
                value: serde_json::json!("proxy_safe"),
                effect: WhenEffect::If,
            },
            FieldWhen {
                target_path: "settlement.redeem.route".into(),
                operator: WhenOperator::Eq,
                value: serde_json::json!("proxy_safe"),
                effect: WhenEffect::Require,
            },
        ]),
        "notification.telegram.bot_token" | "notification.telegram.chat_id" => Some(vec![
            FieldWhen {
                target_path: "notification.telegram.enabled".into(),
                operator: WhenOperator::Eq,
                value: serde_json::json!(true),
                effect: WhenEffect::If,
            },
            FieldWhen {
                target_path: "notification.telegram.enabled".into(),
                operator: WhenOperator::Eq,
                value: serde_json::json!(true),
                effect: WhenEffect::Require,
            },
        ]),
        "notification.webhook.url" => Some(vec![FieldWhen {
            target_path: "notification.webhook.enabled".into(),
            operator: WhenOperator::Eq,
            value: serde_json::json!(true),
            effect: WhenEffect::If,
        }]),
        _ => None,
    }
}

const fn infer_widget(
    value_type: JsonValueType,
    format: Option<SchemaFieldFormat>,
    sensitive: bool,
) -> FieldWidget {
    if sensitive {
        return FieldWidget::SecretString;
    }
    match value_type {
        JsonValueType::Boolean => FieldWidget::Boolean,
        JsonValueType::Enum => FieldWidget::EnumSelect,
        JsonValueType::EnumArray => FieldWidget::EnumSet,
        JsonValueType::StringArray => FieldWidget::StringList,
        JsonValueType::EnumDecimalMap => FieldWidget::EnumDecimalMap,
        JsonValueType::Array | JsonValueType::Object => FieldWidget::JsonTree,
        JsonValueType::Number => match format {
            Some(SchemaFieldFormat::DurationMs) => FieldWidget::DurationMs,
            _ => FieldWidget::Integer,
        },
        JsonValueType::String => match format {
            Some(SchemaFieldFormat::Decimal) => FieldWidget::DecimalString,
            Some(SchemaFieldFormat::DurationMs) => FieldWidget::DurationMs,
            Some(SchemaFieldFormat::Integer) => FieldWidget::Integer,
            _ => FieldWidget::PlainString,
        },
    }
}

/// Fail-closed check that every preferences-visible leaf has UI metadata.
#[must_use]
pub fn preferences_schema_ui_gaps() -> Vec<String> {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let leaves = walk_schema_leaves(&schema, &defaults, "", false);
    let ui_map = field_ui_map();

    leaves
        .iter()
        .filter(|leaf| leaf.path != "schema_version")
        .filter_map(|leaf| {
            if ui_map.contains_key(leaf.path.as_str()) {
                None
            } else {
                Some(leaf.path.clone())
            }
        })
        .collect()
}

/// Fail-closed locale coverage for embedded UI text.
#[must_use]
pub fn preferences_schema_locale_gaps() -> Vec<String> {
    let view = build_preferences_schema();
    let mut gaps = Vec::new();

    for group in &view.groups {
        if !group.label.has_en_and_zh() {
            gaps.push(format!("group.{}.label", group.id));
        }
        if let Some(description) = &group.description {
            if !description.has_en_and_zh() {
                gaps.push(format!("group.{}.description", group.id));
            }
        }
    }

    for field in &view.fields {
        if !field.label.has_en_and_zh() {
            gaps.push(format!("{}.label", field.path));
        }
        if !field.help.has_en_and_zh() {
            gaps.push(format!("{}.help", field.path));
        }
        if let Some(items) = &field.enum_items {
            for item in items {
                if !item.label.has_en_and_zh() {
                    gaps.push(format!("{}.enum.{item:?}", field.path));
                }
            }
        }
    }

    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::JsonValueType;
    use crate::runtime_config::ui_widget::WhenEffect;

    #[test]
    fn preferences_schema_has_no_ui_gaps() {
        let gaps = preferences_schema_ui_gaps();
        assert!(gaps.is_empty(), "missing field UI metadata: {gaps:?}");
    }

    #[test]
    fn preferences_schema_has_full_locale_coverage() {
        let gaps = preferences_schema_locale_gaps();
        assert!(gaps.is_empty(), "missing en-US/zh-CN UI text: {gaps:?}");
    }

    #[test]
    fn schema_version_is_not_in_preferences_fields() {
        let view = build_preferences_schema();
        assert!(
            view.fields
                .iter()
                .all(|field| field.path != "schema_version"),
            "schema_version must not appear in preferences schema"
        );
    }

    #[test]
    fn preferences_schema_golden_snapshot() {
        let view = build_preferences_schema();
        insta::assert_json_snapshot!("preferences_schema", view);
    }

    #[test]
    fn redeem_route_enum_items_are_complete() {
        let view = build_preferences_schema();
        let route = view
            .fields
            .iter()
            .find(|field| field.path == "settlement.redeem.route")
            .expect("redeem route field");
        let items = route.enum_items.as_ref().expect("enum items");
        assert_eq!(items.len(), 6);
        assert!(
            items
                .iter()
                .any(|item| item.key == Value::String("proxy_safe".into()))
        );
    }

    #[test]
    fn decimal_widget_fields_have_format() {
        let view = build_preferences_schema();
        for field in &view.fields {
            if field.widget == Some(FieldWidget::DecimalString) {
                assert_eq!(
                    field.format,
                    Some(SchemaFieldFormat::Decimal),
                    "{} missing decimal format",
                    field.path
                );
            }
        }
    }

    #[test]
    fn enum_fields_with_x_enum_id_have_items() {
        let view = build_preferences_schema();
        for field in &view.fields {
            if field.value_type == JsonValueType::Enum {
                assert!(
                    field
                        .enum_items
                        .as_ref()
                        .is_some_and(|items| !items.is_empty()),
                    "{} missing enum_items",
                    field.path
                );
            }
        }
    }

    #[test]
    fn proxy_safe_address_has_visible_when_rule() {
        let view = build_preferences_schema();
        let field = view
            .fields
            .iter()
            .find(|field| field.path == "settlement.redeem.proxy_safe_address")
            .expect("proxy safe address field");
        let rules = field.when.as_ref().expect("when rules");
        assert!(rules.iter().any(|rule| rule.effect == WhenEffect::If));
        assert!(rules.iter().any(|rule| rule.effect == WhenEffect::Require));
    }
}
