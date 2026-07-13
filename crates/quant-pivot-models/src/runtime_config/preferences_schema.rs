//! Preferences UI schema projection (`GET /runtime-config/schema`).
//!
//! Merges the JSON-Schema-derived leaf data (types, defaults, constraints,
//! sensitivity) with the hand-authored UI overlay in [`crate::schema::ui`] into
//! a normalized field dictionary plus a layout tree. `preferences_schema_ui_gaps`
//! enforces the two stay consistent: every schema leaf is covered by exactly one
//! tree node and one dictionary entry that carries real, bilingual `help`.

use crate::{
    domain::{
        EnumItemView, FieldWidget, JsonValueType, RuntimeConfigSchemaFieldView,
        RuntimeConfigSchemaView, UiText,
    },
    runtime_config::RuntimeConfig,
    schema::ui::FieldUiEntry,
};

use crate::schema::ui::{enum_label, field_ui, field_ui_map, schema_tree, tree_field_paths};

use super::json_schema::fields::{SchemaLeaf, walk_schema_leaves};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Build the preferences envelope for `GET /runtime-config/schema`.
#[must_use]
pub fn build_preferences_schema() -> RuntimeConfigSchemaView {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let leaves = walk_schema_leaves(&schema, &defaults, "");

    let mut fields = leaves
        .into_iter()
        .filter(|leaf| leaf.path != "schema_version")
        .map(|leaf| {
            let ui = field_ui(&leaf.path);
            let enum_items = resolve_enum_items(&leaf, ui);
            RuntimeConfigSchemaFieldView {
                path: leaf.path.clone(),
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
                ui_props: ui.and_then(|entry| entry.ui_props.clone()),
                widget: ui
                    .and_then(|entry| entry.widget)
                    .or_else(|| Some(infer_widget(leaf.value_type, leaf.sensitive))),
                semantics: ui.and_then(|entry| entry.semantics),
                model_picker: ui.and_then(|entry| entry.model_picker),
                default: leaf.default,
                description: leaf.description,
                sensitive: leaf.sensitive,
                constraints: leaf.constraints,
                enum_items,
                when: ui
                    .map(|entry| entry.when.clone())
                    .filter(|rules| !rules.is_empty()),
                array_item_type: leaf.array_item_type,
            }
        })
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.path.cmp(&right.path));

    RuntimeConfigSchemaView {
        tree: schema_tree(),
        fields,
    }
}

/// Validate hand-maintained runtime-config UI metadata against the current schema.
///
/// Reports any drift between the JSON-Schema leaves, the field dictionary, and
/// the layout tree — including missing/hidden entries, missing bilingual text,
/// `help` that merely duplicates `label`, and tree coverage gaps.
#[must_use]
pub fn preferences_schema_ui_gaps() -> Vec<String> {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let leaves = walk_schema_leaves(&schema, &defaults, "");
    let schema_paths = leaves
        .iter()
        .filter(|leaf| leaf.path != "schema_version")
        .map(|leaf| leaf.path.as_str())
        .collect::<BTreeSet<_>>();
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
        if !entry.label.has_en_and_zh() {
            gaps.push(format!(
                "schema field `{path}` label missing bilingual text"
            ));
        }
        if !entry.help.has_en_and_zh() {
            gaps.push(format!("schema field `{path}` help missing bilingual text"));
        }
        if entry.help.locales == entry.label.locales {
            gaps.push(format!(
                "schema field `{path}` help merely duplicates label (author a real help)"
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

    // Layout-tree coverage: every schema leaf appears exactly once in the tree,
    // and the tree references no unknown paths.
    let tree_paths = tree_field_paths();
    let mut seen = BTreeMap::<String, u32>::new();
    for path in &tree_paths {
        *seen.entry(path.clone()).or_default() += 1;
    }
    for (path, count) in &seen {
        if *count > 1 {
            gaps.push(format!(
                "tree references `{path}` {count} times (must be once)"
            ));
        }
        if !schema_paths.contains(path.as_str()) {
            gaps.push(format!("tree references unknown path `{path}`"));
        }
    }
    for path in &schema_paths {
        if !seen.contains_key(*path) {
            gaps.push(format!(
                "schema field `{path}` is not placed in the layout tree"
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
        JsonValueType::DecimalMap => FieldWidget::DecimalMap,
        JsonValueType::String => FieldWidget::PlainString,
        _ => FieldWidget::JsonTree,
    }
}

fn resolve_enum_items(leaf: &SchemaLeaf, ui: Option<&FieldUiEntry>) -> Option<Vec<EnumItemView>> {
    if let Some(keys) = ui.and_then(|entry| entry.static_map_keys) {
        return Some(
            keys.iter()
                .map(|name| EnumItemView {
                    key: Value::String((*name).to_owned()),
                    label: enum_label(name),
                })
                .collect(),
        );
    }
    leaf.constraints.as_ref().and_then(|constraints| {
        constraints.enum_values.as_ref().map(|values| {
            values
                .iter()
                .map(|value| EnumItemView {
                    label: enum_label(value.as_str().unwrap_or_default()),
                    key: value.clone(),
                })
                .collect()
        })
    })
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
    use crate::domain::{FieldWidget, JsonValueType, SchemaNode};

    fn walk_sections(node: &SchemaNode, gaps: &mut Vec<String>) {
        if let SchemaNode::Section(section) = node {
            if !section.label.has_en_and_zh() {
                gaps.push(format!("section {}.label", section.id));
            }
            if let Some(description) = &section.description
                && !description.has_en_and_zh()
            {
                gaps.push(format!("section {}.description", section.id));
            }
            for child in &section.children {
                walk_sections(child, gaps);
            }
        }
    }

    #[test]
    fn preferences_schema_has_no_ui_gaps() {
        let gaps = preferences_schema_ui_gaps();
        assert!(gaps.is_empty(), "runtime-config UI metadata gaps: {gaps:?}");
    }

    #[test]
    fn preferences_schema_has_full_locale_coverage() {
        let view = build_preferences_schema();
        let mut gaps = Vec::new();

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

        for node in &view.tree {
            walk_sections(node, &mut gaps);
        }

        assert!(gaps.is_empty(), "missing en-US/zh-CN UI text: {gaps:?}");
    }

    #[test]
    fn factor_weights_field_has_catalog_keys() {
        let view = build_preferences_schema();
        let field = view
            .fields
            .iter()
            .find(|field| field.path == "factors.factor_weights")
            .expect("factor_weights field");
        assert_eq!(field.value_type, JsonValueType::DecimalMap);
        assert_eq!(field.widget, Some(FieldWidget::WeightMap));
        let items = field.enum_items.as_ref().expect("enum_items");
        assert_eq!(items.len(), 12);
        assert!(
            items
                .iter()
                .any(|item| item.key.as_str() == Some("momentum_roc"))
        );
    }

    #[test]
    fn help_is_never_a_copy_of_label() {
        let view = build_preferences_schema();
        let duplicates = view
            .fields
            .iter()
            .filter(|field| field.help.locales == field.label.locales)
            .map(|field| field.path.clone())
            .collect::<Vec<_>>();
        assert!(
            duplicates.is_empty(),
            "fields whose help duplicates label: {duplicates:?}"
        );
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
