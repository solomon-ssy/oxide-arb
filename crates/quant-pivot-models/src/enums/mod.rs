/// Declarative macro for Postgres `TEXT` enums persisted via `DeriveActiveEnum`.
///
/// Generates `as_str()`, `Display`, serde (wire value == DB `string_value`,
/// e.g. `Side::Buy` ⇄ `"BUY"`, `TickSize::Tenth` ⇄ `"0.1"`), and
/// `IntoActiveValue`.
///
/// Optional modifiers:
/// - `@derive(PartialOrd, Ord, Default, …)` — extra `#[derive]` traits
/// - Variant `#[default]` — `Default` impl target
/// - `Variant = N => "db_value"` — explicit discriminant (ordering / `as u8`)
macro_rules! active_string_enum {
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
            sea_orm::EnumIter,
            sea_orm::DeriveActiveEnum,
            quant_pivot_macros::IntoActiveValue,
        )]
        #[sea_orm(rs_type = "String", db_type = "Text")]
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
    };
}

pub mod audit;
pub mod blacklist;
pub mod calibration;
pub mod clickhouse;
pub mod common;
pub mod control_factor;
pub mod evidence;
pub mod execution;
pub mod fact;
pub mod fee;
pub mod legacy;
pub mod lifecycle;
pub mod market;
pub mod operation_log;
pub mod opportunity;
pub mod order;
pub mod pipeline;
pub mod quant;
pub mod rbac;
pub mod report;
pub mod risk;
pub mod runtime_config;
pub mod system;

pub use legacy::LegacyExecutionMode;
