//! Link-time registries for runtime-config field and group UI metadata.

use super::ui_catalog;
use super::ui_groups;
use super::ui_text::UiText;
use super::ui_widget::{FieldSemantics, FieldWidget};
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// Per-leaf UI metadata registered at compile time.
#[derive(Clone)]
pub struct FieldUiEntry {
    pub path: &'static str,
    pub label: UiText,
    pub help: UiText,
    pub order: u16,
    pub widget: Option<FieldWidget>,
    pub semantics: Option<FieldSemantics>,
    pub visible: bool,
}

/// Per-section group metadata for the preferences drawer.
#[derive(Clone)]
pub struct GroupUiEntry {
    pub id: &'static str,
    pub label: UiText,
    pub description: UiText,
    pub order: u16,
}

/// All registered field UI entries (defined in `ui_catalog.rs`).
#[must_use]
pub fn config_field_ui() -> &'static [FieldUiEntry] {
    ui_catalog::fields()
}

/// All registered group UI entries (defined in `ui_groups.rs`).
#[must_use]
pub fn config_group_ui() -> &'static [GroupUiEntry] {
    ui_groups::groups()
}

/// Lookup field UI metadata by dotted path.
#[must_use]
pub fn field_ui(path: &str) -> Option<&'static FieldUiEntry> {
    config_field_ui().iter().find(|entry| entry.path == path)
}

/// All registered field UI entries keyed by path.
#[must_use]
pub fn field_ui_map() -> BTreeMap<&'static str, &'static FieldUiEntry> {
    config_field_ui()
        .iter()
        .map(|entry| (entry.path, entry))
        .collect()
}

/// All registered groups sorted by `order`.
#[must_use]
pub fn groups_ui() -> Vec<&'static GroupUiEntry> {
    let mut groups: Vec<_> = config_group_ui().iter().collect();
    groups.sort_by_key(|group| group.order);
    groups
}

/// Shared lazy cache for generated field catalogs.
pub(crate) fn field_catalog_lock() -> &'static OnceLock<Vec<FieldUiEntry>> {
    static LOCK: OnceLock<Vec<FieldUiEntry>> = OnceLock::new();
    &LOCK
}
