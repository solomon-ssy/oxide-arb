//! Procedural macros for the oxide-arb workspace.
//!
//! - [`TypedId`]: Generates a type-safe ID newtype backed by `Arc<str>`.
//! - [`IntoActiveValue`]: Generates `IntoActiveValue` impl for enums stored as
//!   strings in `SeaORM`.

mod into_active_value;
mod oxide_schema;
mod typed_id;

use proc_macro::TokenStream;

/// Declare a schema iden enum and register its table metadata.
#[proc_macro_attribute]
pub fn oxide_schema(args: TokenStream, input: TokenStream) -> TokenStream {
    oxide_schema::expand(args.into(), input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

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
