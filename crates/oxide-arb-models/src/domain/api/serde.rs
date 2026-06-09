//! Serde helpers for HTTP request bodies.

use serde::{Deserialize, Deserializer};

/// Deserialize a field into a nested option that distinguishes three states:
///
/// - field absent       → `None`        (leave unchanged)
/// - field present null → `Some(None)`  (clear to SQL `NULL`)
/// - field present `v`  → `Some(Some(v))` (set)
///
/// Pair with `#[serde(default, deserialize_with = "double_option")]` on partial-
/// update request fields. Maps directly to [`NullablePatch::from_nested_option`].
///
/// [`NullablePatch::from_nested_option`]: crate::domain::NullablePatch::from_nested_option
pub fn double_option<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}
