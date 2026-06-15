//! Runtime-config enum catalogs with embedded localized variant labels.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use super::ui_text::UiText;

mod all_sources_down_strategy;
mod market_category;
mod redeem_output_asset;
mod redeem_route;

/// One selectable enum wire value with localized label for the UI.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EnumItemView {
    pub key: Value,
    pub label: UiText,
}

/// Lookup enum items by schema `x-enum-id`.
#[must_use]
pub fn enum_items_for_id(enum_id: &str) -> Option<Vec<EnumItemView>> {
    match enum_id {
        "market_category" => Some(market_category::items()),
        "redeem_route" => Some(redeem_route::items()),
        "redeem_output_asset" => Some(redeem_output_asset::items()),
        "all_sources_down_strategy" => Some(all_sources_down_strategy::items()),
        _ => None,
    }
}

/// Build enum items from wire keys and en/zh label pairs.
pub(super) fn enum_items(pairs: &[(&str, (&str, &str))]) -> Vec<EnumItemView> {
    pairs
        .iter()
        .map(|(wire, (en, zh))| EnumItemView {
            key: Value::String((*wire).to_string()),
            label: {
                let mut locales = BTreeMap::new();
                locales.insert("en-US".to_string(), (*en).to_string());
                locales.insert("zh-CN".to_string(), (*zh).to_string());
                UiText::Localized { locales }
            },
        })
        .collect()
}
