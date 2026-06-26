//! Domain enums: [`pg_enum!`] for Postgres column types, [`wire_enum!`] for JSON/wire only.

/// Postgres native enum persisted as `CREATE TYPE qp_* AS ENUM`.
///
/// Registers the type in [`crate::schema::pg_enum::PG_ENUM_SPECS`] for migrations.
#[macro_export]
macro_rules! pg_enum {
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        pub enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident $(= $discriminant:expr)? => $value:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            Copy,
            $($($extra_derive),*,)?
            PartialEq,
            Eq,
            Hash,
            serde::Serialize,
            serde::Deserialize,
            sea_orm::EnumIter,
            sea_orm::DeriveActiveEnum,
            quant_pivot_macros::IntoActiveValue,
        )]
        #[sea_orm(rs_type = "String", db_type = "Enum", enum_name = $type_name)]
        pub enum $name {
            $(
                #[sea_orm(string_value = $value)]
                #[serde(rename = $value)]
                $(#[$variant_meta])*
                $variant $(= $discriminant)?,
            )+
        }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        ::paste::paste! {
            #[allow(unsafe_code, dead_code, non_upper_case_globals)]
            #[::linkme::distributed_slice($crate::schema::pg_enum::PG_ENUM_SPECS)]
            static [< __PG_ENUM_ $name >]: $crate::schema::pg_enum::PgEnumSpec =
                $crate::schema::pg_enum::PgEnumSpec {
                    type_name: $type_name,
                    create_stmt: || $crate::schema::pg_enum::create_type::<$name>(),
                };
        }
    };
}

/// Wire/JSON enum with no Postgres `CREATE TYPE` (JSONB payloads, artifacts, runtime).
#[macro_export]
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        pub enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident $(= $discriminant:expr)? => $value:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            Copy,
            $($($extra_derive),*,)?
            PartialEq,
            Eq,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub enum $name {
            $(
                #[serde(rename = $value)]
                $(#[$variant_meta])*
                $variant $(= $discriminant)?,
            )+
        }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

pub mod clickhouse;
pub mod common;
pub mod domain;
pub mod execution;
pub mod factor;
pub mod fee;
pub mod market;
pub mod model;
pub mod operation_log;
pub mod quant;
pub mod rbac;
pub mod runtime_config;
pub mod selection;
pub mod system;
