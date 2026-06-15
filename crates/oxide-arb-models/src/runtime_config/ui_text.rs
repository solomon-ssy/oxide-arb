//! Localized display strings embedded in the runtime-config UI schema.

use std::collections::BTreeMap;

use serde::Serialize;

/// Wire-serialized localized or plain text for schema-driven UI labels and help.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum UiText {
    /// Single-locale fallback string.
    Simple { value: String },
    /// BCP-47 locale map (`en-US`, `zh-CN`, …).
    Localized { locales: BTreeMap<String, String> },
}

impl UiText {
    /// Resolve text for `locale` with a deterministic fallback chain.
    #[must_use]
    pub fn resolve<'a>(&'a self, locale: &str) -> &'a str {
        match self {
            Self::Simple { value } => value,
            Self::Localized { locales } => locales
                .get(locale)
                .or_else(|| locales.get("en-US"))
                .or_else(|| locales.get("zh-CN"))
                .or_else(|| locales.values().next())
                .map_or("", String::as_str),
        }
    }

    /// Whether both required operator locales are present.
    #[must_use]
    pub fn has_en_and_zh(&self) -> bool {
        match self {
            Self::Simple { .. } => false,
            Self::Localized { locales } => {
                locales.contains_key("en-US") && locales.contains_key("zh-CN")
            }
        }
    }
}

/// Build localized UI text from English and Chinese operator strings.
#[macro_export]
macro_rules! ui_text {
    (en = $en:expr, zh = $zh:expr) => {{
        let mut locales = ::std::collections::BTreeMap::new();
        locales.insert("en-US".to_string(), ($en).to_string());
        locales.insert("zh-CN".to_string(), ($zh).to_string());
        $crate::runtime_config::ui_text::UiText::Localized { locales }
    }};
    ($value:expr) => {{
        $crate::runtime_config::ui_text::UiText::Simple {
            value: ($value).to_string(),
        }
    }};
}
