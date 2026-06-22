//! Widget and semantics hints for the preferences form renderer.

use serde::Serialize;

pub use super::ui_when::{FieldWhen, WhenEffect, WhenOperator};

/// Explicit widget override for a schema leaf (server-side projection).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldWidget {
    Boolean,
    Integer,
    DecimalString,
    DurationMs,
    PlainString,
    EnumSelect,
    EnumSet,
    StringList,
    EnumDecimalMap,
    SecretString,
    JsonTree,
}

/// Domain semantics beyond raw JSON Schema type information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSemantics {
    /// Empty wire array means “all variants enabled” (e.g. trade categories).
    EmptyMeansAll,
}
