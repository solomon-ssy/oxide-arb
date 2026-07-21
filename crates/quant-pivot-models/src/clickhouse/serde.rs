//! ClickHouse-specific serde adapters for domain newtypes.

pub mod uuid_id {
    use ::uuid::Uuid;
    use clickhouse::serde::uuid;
    use serde::{Deserializer, Serializer};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Clone + Into<Uuid>,
        S: Serializer,
    {
        uuid::serialize(&value.clone().into(), serializer)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: From<Uuid>,
        D: Deserializer<'de>,
    {
        uuid::deserialize(deserializer).map(T::from)
    }
}
