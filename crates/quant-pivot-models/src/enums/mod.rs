//! Domain enums: [`pg_enum!`] for Postgres column types, [`wire_enum!`] for JSON/wire only.
//!
//! Both macros generate [`std::str::FromStr`] + a `${Name}ParseError` by default (canonical
//! wire-label parse). Opt out or customize with:
//!
//! - `@no_from_str` — alias / domain-specific error (e.g. [`super::common::MarketCategory`])
//! - `@from_str(trim)` — trim input before matching (e.g. Gamma tick sizes)
//! - `@from_str(err = MyError)` — tuple-struct error (`MyError(value)`)

/// Postgres native enum persisted as `CREATE TYPE qp_* AS ENUM`.
///
/// Registers the type in [`crate::schema::pg_enum::PG_ENUM_SPECS`] for migrations.
#[macro_export]
macro_rules! pg_enum {
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @no_from_str
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::pg_enum! {
            @core
            type_name = $type_name,
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
    };
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(trim)
        @from_str(err = $err:path)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::pg_enum! {
            @core
            type_name = $type_name,
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = true,
            error = $err,
            $( $variant => $value ),+
        }
    };
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(err = $err:path)
        @from_str(trim)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::pg_enum! {
            type_name = $type_name,
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            @from_str(trim)
            @from_str(err = $err)
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
    };
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(trim)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::pg_enum! {
            @core
            type_name = $type_name,
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = true,
            error = default,
            $( $variant => $value ),+
        }
    };
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(err = $err:path)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::pg_enum! {
            @core
            type_name = $type_name,
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = false,
            error = $err,
            $( $variant => $value ),+
        }
    };
    (
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::pg_enum! {
            @core
            type_name = $type_name,
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = false,
            error = default,
            $( $variant => $value ),+
        }
    };
    (
        @core
        type_name = $type_name:literal,
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
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
        @no_from_str
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::wire_enum! {
            @core
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
    };
    (
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(trim)
        @from_str(err = $err:path)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::wire_enum! {
            @core
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = true,
            error = $err,
            $( $variant => $value ),+
        }
    };
    (
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(err = $err:path)
        @from_str(trim)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::wire_enum! {
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            @from_str(trim)
            @from_str(err = $err)
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
    };
    (
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(trim)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::wire_enum! {
            @core
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = true,
            error = default,
            $( $variant => $value ),+
        }
    };
    (
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        @from_str(err = $err:path)
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::wire_enum! {
            @core
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = false,
            error = $err,
            $( $variant => $value ),+
        }
    };
    (
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
        }
    ) => {
        $crate::wire_enum! {
            @core
            $(#[$meta])*
            $(@derive($($extra_derive),*))?
            pub enum $name { $( $(#[$variant_meta])* $variant $(= $discriminant)? => $value, )+ }
        }
        $crate::__enum_from_str_impl! {
            $name,
            trim = false,
            error = default,
            $( $variant => $value ),+
        }
    };
    (
        @core
        $(#[$meta:meta])*
        $(@derive($($extra_derive:path),* $(,)?))?
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident $(= $discriminant:expr)? => $value:literal ),+ $(,)?
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

/// Internal: canonical [`FromStr`] for [`pg_enum!`] / [`wire_enum!`].
#[doc(hidden)]
#[macro_export]
macro_rules! __enum_from_str_impl {
    (
        $name:ident,
        trim = $trim:expr,
        error = default,
        $( $variant:ident => $value:literal ),+ $(,)?
    ) => {
        ::paste::paste! {
            #[derive(Debug, Clone, PartialEq, Eq, ::thiserror::Error)]
            #[error("unknown [<$name:snake>]: {0}")]
            pub struct [< $name ParseError >](pub String);

            impl std::str::FromStr for $name {
                type Err = [< $name ParseError >];

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    match if $trim { s.trim() } else { s } {
                        $( $value => Ok(Self::$variant), )+
                        other => Err([< $name ParseError >](other.to_owned())),
                    }
                }
            }
        }
    };
    (
        $name:ident,
        trim = $trim:expr,
        error = $err:path,
        $( $variant:ident => $value:literal ),+ $(,)?
    ) => {
        impl std::str::FromStr for $name {
            type Err = $err;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match if $trim { s.trim() } else { s } {
                    $( $value => Ok(Self::$variant), )+
                    other => Err($err(other.to_owned())),
                }
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
