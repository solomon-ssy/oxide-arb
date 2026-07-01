//! Preferences UI schema projection (`GET /runtime-config/schema`).

use crate::{
    domain::{
        EnumItemView, FieldWidget, JsonValueType, RuntimeConfigSchemaFieldView,
        RuntimeConfigSchemaView, UiText,
    },
    runtime_config::RuntimeConfig,
};

use crate::schema::ui::{field_ui, field_ui_map, groups_ui};

use super::json_schema::fields::walk_schema_leaves;

/// Build the preferences envelope for `GET /runtime-config/schema`.
#[must_use]
pub fn build_preferences_schema() -> RuntimeConfigSchemaView {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let leaves = walk_schema_leaves(&schema, &defaults, "", false);

    let mut groups = groups_ui();
    groups.sort_by_key(|group| group.order);

    let mut fields = leaves
        .into_iter()
        .filter(|leaf| leaf.path != "schema_version")
        .enumerate()
        .map(|(index, leaf)| {
            let ui = field_ui(&leaf.path);
            let enum_items = leaf.constraints.as_ref().and_then(|constraints| {
                constraints.enum_values.as_ref().map(|values| {
                    values
                        .iter()
                        .map(|value| EnumItemView {
                            key: value.clone(),
                            label: UiText::plain(value.as_str().unwrap_or_default()),
                        })
                        .collect()
                })
            });
            RuntimeConfigSchemaFieldView {
                path: leaf.path.clone(),
                group: leaf.group.clone(),
                order: ui.map_or_else(
                    || u16::try_from(index).unwrap_or(u16::MAX),
                    |entry| entry.order,
                ),
                label: ui.map_or_else(
                    || UiText::plain(title(leaf.path.rsplit('.').next().unwrap_or(&leaf.path))),
                    |entry| entry.label.clone(),
                ),
                help: ui.map_or_else(
                    || UiText::plain(leaf.description.clone()),
                    |entry| entry.help.clone(),
                ),
                value_type: leaf.value_type,
                format: leaf.format,
                widget: ui
                    .and_then(|entry| entry.widget)
                    .or_else(|| Some(infer_widget(leaf.value_type, leaf.sensitive))),
                semantics: ui.and_then(|entry| entry.semantics),
                default: leaf.default,
                description: leaf.description,
                money_critical: leaf.money_critical,
                sensitive: leaf.sensitive,
                constraints: leaf.constraints,
                enum_items,
                when: None,
            }
        })
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then_with(|| left.order.cmp(&right.order))
            .then_with(|| left.path.cmp(&right.path))
    });

    RuntimeConfigSchemaView { groups, fields }
}

/// Validate hand-maintained runtime-config UI metadata against the current schema.
#[must_use]
pub fn preferences_schema_ui_gaps() -> Vec<String> {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let leaves = walk_schema_leaves(&schema, &defaults, "", false);
    let schema_paths = leaves
        .iter()
        .filter(|leaf| leaf.path != "schema_version")
        .map(|leaf| leaf.path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let ui = field_ui_map();
    let mut gaps = Vec::new();

    for path in &schema_paths {
        let Some(entry) = ui.get(path) else {
            gaps.push(format!("schema field `{path}` has no UI entry"));
            continue;
        };
        if !entry.visible {
            gaps.push(format!("schema field `{path}` is registered as hidden"));
        }
        if !entry.label.has_en_and_zh() || !entry.help.has_en_and_zh() {
            gaps.push(format!(
                "schema field `{path}` is missing bilingual UI text"
            ));
        }
    }
    for path in ui.keys() {
        if !schema_paths.contains(path) {
            gaps.push(format!(
                "UI entry `{path}` does not exist in runtime config schema"
            ));
        }
    }
    gaps.sort();
    gaps
}

const fn infer_widget(value_type: JsonValueType, sensitive: bool) -> FieldWidget {
    if sensitive {
        return FieldWidget::SecretString;
    }
    match value_type {
        JsonValueType::Boolean => FieldWidget::Boolean,
        JsonValueType::Number => FieldWidget::Integer,
        JsonValueType::Enum => FieldWidget::EnumSelect,
        JsonValueType::EnumArray => FieldWidget::EnumSet,
        JsonValueType::StringArray => FieldWidget::StringList,
        JsonValueType::EnumDecimalMap => FieldWidget::EnumDecimalMap,
        JsonValueType::String => FieldWidget::PlainString,
        _ => FieldWidget::JsonTree,
    }
}

fn title(raw: &str) -> String {
    raw.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect::<String>()
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{build_preferences_schema, preferences_schema_ui_gaps};

    #[test]
    fn preferences_schema_has_no_ui_gaps() {
        let gaps = preferences_schema_ui_gaps();
        assert!(gaps.is_empty(), "missing field UI metadata: {gaps:?}");
    }

    #[test]
    fn preferences_schema_has_full_locale_coverage() {
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

        assert!(gaps.is_empty(), "missing en-US/zh-CN UI text: {gaps:?}");
    }

    #[test]
    fn schema_version_is_not_in_preferences_fields() {
        let view = build_preferences_schema();
        assert!(
            view.fields
                .iter()
                .all(|field| field.path != "schema_version"),
            "schema_version must not appear in preferences fields"
        );
    }
}
