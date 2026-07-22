//! Catalog snapshot newtypes persisted as Postgres `text[]`.

use std::{ops::Deref, string::ToString};

use sea_orm::{
    ActiveValue, ColIdx, IntoActiveValue, QueryResult, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Serialize};

use super::MarketId;

/// Gamma event catalog snapshot: ordered `condition_id`s at sync time.
///
/// Mirrors [`crate::domain::market::registry::EventRegistryInfo::market_ids`]
/// persisted for offline neg-risk leg enumeration and train-serve parity.
/// Stored as Postgres `text[]`; each element is a [`MarketId`]
/// (`condition_id`).
///
/// `SeaORM` bindings round-trip through `Vec<String>` at the wire layer because
/// `Vec<MarketId>` is not a first-class Postgres array element type in `SeaORM`
/// (`StrId` newtypes need explicit array support — see `SeaORM` #2967). Domain code
/// always sees [`MarketId`], never bare strings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogMarketIds(pub Vec<MarketId>);

impl CatalogMarketIds {
    /// Borrow the ordered catalog members.
    #[must_use]
    pub fn as_slice(&self) -> &[MarketId] {
        &self.0
    }
}

impl Deref for CatalogMarketIds {
    type Target = [MarketId];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<MarketId>> for CatalogMarketIds {
    fn from(value: Vec<MarketId>) -> Self {
        Self(value)
    }
}

impl FromIterator<MarketId> for CatalogMarketIds {
    fn from_iter<I: IntoIterator<Item = MarketId>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

fn wire_strings(ids: &CatalogMarketIds) -> Vec<String> {
    ids.0.iter().map(ToString::to_string).collect()
}

fn from_wire_strings(strings: Vec<String>) -> CatalogMarketIds {
    CatalogMarketIds(strings.into_iter().map(MarketId::new).collect())
}

impl From<CatalogMarketIds> for Value {
    #[inline]
    fn from(ids: CatalogMarketIds) -> Self {
        wire_strings(&ids).into()
    }
}

impl From<&CatalogMarketIds> for Value {
    #[inline]
    fn from(ids: &CatalogMarketIds) -> Self {
        wire_strings(ids).into()
    }
}

impl TryGetable for CatalogMarketIds {
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let raw: Vec<String> = TryGetable::try_get_by(res, index)?;
        Ok(from_wire_strings(raw))
    }
}

impl ValueType for CatalogMarketIds {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        <Vec<String> as ValueType>::try_from(v)
            .map(from_wire_strings)
            .map_err(|_| ValueTypeErr)
    }

    fn type_name() -> String {
        stringify!(CatalogMarketIds).to_owned()
    }

    fn array_type() -> ArrayType {
        <Vec<String> as ValueType>::array_type()
    }

    fn column_type() -> ColumnType {
        <Vec<String> as ValueType>::column_type()
    }
}

impl Nullable for CatalogMarketIds {
    fn null() -> Value {
        <Vec<String> as Nullable>::null()
    }
}

impl IntoActiveValue<Self> for CatalogMarketIds {
    #[inline]
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::sea_query::{Value, ValueType};

    use super::*;

    #[test]
    fn catalog_market_ids_value_type_roundtrip() {
        let ids = CatalogMarketIds(vec![MarketId::new("0xaaa"), MarketId::new("0xbbb")]);
        let value: Value = ids.clone().into();
        let back = <CatalogMarketIds as ValueType>::try_from(value).expect("roundtrip");
        assert_eq!(back, ids);
    }
}
