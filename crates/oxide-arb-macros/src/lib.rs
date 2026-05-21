//! Procedural macros for the oxide-arb workspace.
//!
//! - [`TypedId`]: Generates a type-safe ID newtype backed by `Arc<str>`.
//! - [`IntoActiveValue`]: Generates `IntoActiveValue` impl for enums stored as
//!   strings in `SeaORM`.
//! - [`ActiveModelDefaults`]: Generates `prepare_for_insert` and
//!   `ActiveModelBehavior` for `SeaORM` entities with system-generated fields.

mod active_model_defaults;
mod into_active_value;
mod seed_unit;
mod typed_id;

use proc_macro::TokenStream;

/// Derive a type-safe ID newtype backed by `Arc<str>`.
///
/// # Usage
///
/// ```ignore
/// #[derive(TypedId)]
/// pub struct MarketId;
/// ```
///
/// This expands to a newtype `pub struct MarketId(Arc<str>)` with
/// implementations of: `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`,
/// `Display`, `FromStr`, `From<&str>`, `From<String>`, `Serialize`,
/// `Deserialize`, and full `SeaORM` bindings (`TryGetable`, `ValueType`,
/// `sea_query::ValueType`, `sea_query::Nullable`).
#[proc_macro_derive(TypedId)]
pub fn derive_typed_id(input: TokenStream) -> TokenStream {
    typed_id::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive `IntoActiveValue` for enums that should be stored as their
/// `Display` string representation in `SeaORM`.
///
/// The enum must also implement `FromStr` and `Display` (typically via
/// `strum`'s `EnumString` and `Display` derives).
///
/// # Usage
///
/// ```ignore
/// #[derive(IntoActiveValue, Display, EnumString)]
/// pub enum Side {
///     Buy,
///     Sell,
/// }
/// ```
#[proc_macro_derive(IntoActiveValue)]
pub fn derive_into_active_value(input: TokenStream) -> TokenStream {
    into_active_value::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive insert defaults and `ActiveModelBehavior` for a `SeaORM` entity model.
///
/// Requires `#[active_defaults(...)]` on the same struct. Generates
/// `ActiveModel::prepare_for_insert()` (for bulk insert paths that bypass hooks)
/// and `ActiveModelBehavior::before_save` that delegates to it on insert.
///
/// # Rules
///
/// - `generate(field, expr)` — set `field` to `expr` when `NotSet` (typically ID generation)
/// - `default(field, expr)` — set `field` to `expr` when `NotSet`
/// - `timestamp(field)` — set `field` to `Utc::now()` when `NotSet`
/// - `timestamp(field, always)` — always set `field` to `Utc::now()` on insert
/// - `on_update(timestamp(field))` — set `field` to `Utc::now()` on update
///
/// # Example
///
/// ```ignore
/// #[derive(DeriveEntityModel, ActiveModelDefaults)]
/// #[sea_orm(table_name = "trade")]
/// #[active_defaults(
///     generate(trade_id, TradeId::generate()),
///     default(outcome, TradeOutcome::Pending),
///     timestamp(created_at),
///     timestamp(updated_at, always),
///     on_update(timestamp(updated_at)),
/// )]
/// pub struct Model { /* ... */ }
/// ```
#[proc_macro_derive(ActiveModelDefaults, attributes(active_defaults))]
pub fn derive_active_model_defaults(input: TokenStream) -> TokenStream {
    active_model_defaults::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive `SeedUnit` trait implementation from `#[seed_unit(...)]` attributes.
///
/// Generates the metadata accessors (`id`, `order`, `policy`) and an
/// `execute` method that delegates to the loader function specified in the
/// `loader` attribute.
///
/// # Usage
///
/// ```ignore
/// #[derive(SeedUnit)]
/// #[seed_unit(
///     id = "trading.risk_engine_state",
///     order = 10,
///     policy = InsertIfAbsent,
///     loader = crate::seed::risk_engine_state::load,
/// )]
/// pub struct RiskEngineStateSeed;
/// ```
#[proc_macro_derive(SeedUnit, attributes(seed_unit))]
pub fn derive_seed_unit(input: TokenStream) -> TokenStream {
    seed_unit::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
