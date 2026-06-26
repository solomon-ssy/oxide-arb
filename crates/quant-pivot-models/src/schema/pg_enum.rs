//! Postgres native enum catalog for schema migrations.

use linkme::distributed_slice;
use sea_orm::{
    DbBackend, Schema, entity::ActiveEnum, sea_query::extension::postgres::TypeCreateStatement,
};

/// Compile-time metadata for one Postgres `CREATE TYPE … AS ENUM` statement.
#[derive(Clone, Copy)]
pub struct PgEnumSpec {
    /// Postgres enum type name (e.g. `qp_market_status`).
    pub type_name: &'static str,
    /// Builds the `CREATE TYPE` statement for this enum.
    pub create_stmt: fn() -> TypeCreateStatement,
}

/// All discovered Postgres enum types, sorted by name for deterministic migrations.
#[allow(unsafe_code)]
#[distributed_slice]
pub static PG_ENUM_SPECS: [PgEnumSpec] = [..];

/// Every registered enum spec sorted by `type_name`.
pub fn specs() -> Vec<&'static PgEnumSpec> {
    let mut specs: Vec<&'static PgEnumSpec> = PG_ENUM_SPECS.iter().collect();
    specs.sort_by_key(|spec| spec.type_name);
    assert_unique_type_names(&specs);
    specs
}

/// Reverse drop order (safe after tables referencing the types are gone).
pub fn drop_order() -> Vec<&'static PgEnumSpec> {
    let mut ordered = specs();
    ordered.reverse();
    ordered
}

/// Build a Postgres `CREATE TYPE` from a [`DeriveActiveEnum`] type.
pub fn create_type<E: ActiveEnum>() -> TypeCreateStatement {
    Schema::new(DbBackend::Postgres).create_enum_from_active_enum::<E>()
}

fn assert_unique_type_names(specs: &[&PgEnumSpec]) {
    let mut seen = std::collections::BTreeSet::new();
    for spec in specs {
        assert!(
            seen.insert(spec.type_name),
            "duplicate Postgres enum type registered: {}",
            spec.type_name
        );
    }
}
