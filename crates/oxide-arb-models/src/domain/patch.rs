//! Patch value helpers for repository write DTOs.
//!
//! `Patch<T>` represents non-nullable partial updates. `NullablePatch<T>`
//! represents nullable partial updates and preserves all three SQL intents:
//! leave unchanged, set a value, or set `NULL`.

use sea_orm::{ActiveValue, IntoActiveValue, Value, sea_query::Nullable};

/// Partial update value for non-nullable columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Patch<T> {
    /// Do not include this column in the update.
    #[default]
    Keep,
    /// Set the column to this value.
    Set(T),
}

impl<T> Patch<T> {
    #[must_use]
    pub const fn set(value: T) -> Self {
        Self::Set(value)
    }

    /// Build from a presence-only option: `None` keeps, `Some(v)` sets. The
    /// inverse of [`Patch::into_option`], for mapping a request field where an
    /// absent field means "leave unchanged".
    #[must_use]
    pub fn from_option(value: Option<T>) -> Self {
        value.map_or(Self::Keep, Self::Set)
    }

    #[must_use]
    pub fn into_option(self) -> Option<T> {
        match self {
            Self::Keep => None,
            Self::Set(value) => Some(value),
        }
    }
}

impl<T> IntoActiveValue<T> for Patch<T>
where
    T: Into<Value>,
{
    fn into_active_value(self) -> ActiveValue<T> {
        match self {
            Self::Keep => ActiveValue::NotSet,
            Self::Set(value) => ActiveValue::Set(value),
        }
    }
}

/// Partial update value for nullable columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullablePatch<T> {
    /// Do not include this column in the update.
    #[default]
    Keep,
    /// Set the column to this non-null value.
    Set(T),
    /// Set the column to SQL `NULL`.
    Clear,
}

impl<T> NullablePatch<T> {
    #[must_use]
    pub const fn set(value: T) -> Self {
        Self::Set(value)
    }

    #[must_use]
    pub const fn clear() -> Self {
        Self::Clear
    }

    #[must_use]
    pub fn set_nullable(value: Option<T>) -> Self {
        value.map_or(Self::Clear, Self::Set)
    }

    /// Build from a nested option carrying the full three-way intent: `None`
    /// keeps, `Some(None)` clears (SQL `NULL`), `Some(Some(v))` sets. The inverse
    /// of [`NullablePatch::into_nested_option`], for mapping a request field
    /// where absence means "leave unchanged" and explicit `null` means "clear".
    #[must_use]
    pub fn from_nested_option(value: Option<Option<T>>) -> Self {
        match value {
            None => Self::Keep,
            Some(None) => Self::Clear,
            Some(Some(value)) => Self::Set(value),
        }
    }

    #[must_use]
    pub fn into_nested_option(self) -> Option<Option<T>> {
        match self {
            Self::Keep => None,
            Self::Set(value) => Some(Some(value)),
            Self::Clear => Some(None),
        }
    }
}

impl<T> IntoActiveValue<Option<T>> for NullablePatch<T>
where
    T: Into<Value> + Nullable,
{
    fn into_active_value(self) -> ActiveValue<Option<T>> {
        match self {
            Self::Keep => ActiveValue::NotSet,
            Self::Set(value) => ActiveValue::Set(Some(value)),
            Self::Clear => ActiveValue::Set(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NullablePatch, Patch};

    #[test]
    fn patch_converts_to_optional_write_intent() {
        assert_eq!(Patch::<i32>::Keep.into_option(), None);
        assert_eq!(Patch::set(7).into_option(), Some(7));
    }

    #[test]
    fn nullable_patch_preserves_keep_set_and_clear() {
        assert_eq!(NullablePatch::<i32>::Keep.into_nested_option(), None);
        assert_eq!(NullablePatch::set(7).into_nested_option(), Some(Some(7)));
        assert_eq!(
            NullablePatch::<i32>::clear().into_nested_option(),
            Some(None)
        );
    }
}
