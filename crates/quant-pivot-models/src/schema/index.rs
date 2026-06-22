//! Index metadata for catalog-driven migrations.

use sea_orm::sea_query::IndexCreateStatement;

/// How an index should be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBuildMode {
    /// Safe for the greenfield initial schema and small non-hot tables.
    Transactional,
    /// Required for online additions to hot production tables.
    Concurrent,
}

/// Index creation statement stored in the schema catalog.
pub enum IndexStatement {
    SeaQuery(Box<IndexCreateStatement>),
    RawSql(&'static str),
}

/// A catalog entry for one index.
pub struct IndexSpec {
    pub name: &'static str,
    pub table_name: fn() -> String,
    pub build_mode: IndexBuildMode,
    pub statement: IndexStatement,
    pub purpose: &'static str,
}

impl IndexSpec {
    pub fn sea_query(
        name: &'static str,
        table_name: fn() -> String,
        build_mode: IndexBuildMode,
        statement: IndexCreateStatement,
        purpose: &'static str,
    ) -> Self {
        Self {
            name,
            table_name,
            build_mode,
            statement: IndexStatement::SeaQuery(Box::new(statement)),
            purpose,
        }
    }

    pub const fn raw(
        name: &'static str,
        table_name: fn() -> String,
        build_mode: IndexBuildMode,
        sql: &'static str,
        purpose: &'static str,
    ) -> Self {
        Self {
            name,
            table_name,
            build_mode,
            statement: IndexStatement::RawSql(sql),
            purpose,
        }
    }
}
