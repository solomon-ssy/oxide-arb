//! Procedural macros for the quant-pivot workspace.
//!
//! - [`StrId`]: Generates a type-safe string ID newtype backed by `Arc<str>`
//!   (external identifiers that are not UUIDs).
//! - [`UuidId`]: Generates a type-safe UUID ID newtype backed by `Uuid`
//!   (internal, system-generated identifiers persisted as native `uuid`).
//! - [`IntoActiveValue`]: Generates `IntoActiveValue` impl for enums stored as
//!   strings in `SeaORM`.
//! - [`NormalizePageQuery`]: Generates `normalized(self)` for list query DTOs
//!   that embed a `PageRequest` via `#[normalize_page]`.

mod into_active_value;
mod normalize_page_query;
mod str_id;
mod uuid_id;

use proc_macro::TokenStream;

/// Derive a type-safe string ID newtype backed by `Arc<str>`.
///
/// # Usage
///
/// ```ignore
/// #[derive(StrId)]
/// pub struct MarketId(Arc<str>);
/// ```
///
/// This generates implementations of: `new`, `as_str`, `Display`, `FromStr`,
/// `From<&str>`, `From<String>`, `AsRef<str>`, `Serialize`, `Deserialize`,
/// and full `SeaORM` bindings (`TryGetable`, `ValueType`, `Nullable`,
/// `IntoActiveValue`) backed by the `TEXT` column type.
///
/// Use this for externally defined identifiers that are not UUIDs, e.g.
/// Polymarket `condition_id` or CLOB decimal token ids. Internal identifiers
/// should use [`UuidId`].
#[proc_macro_derive(StrId)]
pub fn derive_str_id(input: TokenStream) -> TokenStream {
    str_id::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive a type-safe UUID ID newtype backed by `Uuid`.
///
/// # Usage
///
/// ```ignore
/// #[derive(UuidId, Clone, Copy)]
/// pub struct TradeId(Uuid);
/// ```
///
/// This generates `new`, `from_v7`, `as_uuid`, `as_uuid_ref`, `into_uuid`,
/// `Display`, `FromStr`, `Serialize`, `Deserialize`, and full `SeaORM` bindings
/// backed by the native Postgres `uuid` column type. All generated ids are
/// UUID v7 (time-ordered); there is no v4 constructor.
#[proc_macro_derive(UuidId)]
pub fn derive_uuid_id(input: TokenStream) -> TokenStream {
    uuid_id::expand(input.into())
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

/// Derive `normalized(self) -> Self` for paginated list query DTOs.
///
/// Requires exactly one field annotated with `#[normalize_page]` whose type is
/// `PageRequest`. The generated method delegates to
/// the target `PageRequest::normalized` method.
///
/// # Usage
///
/// ```ignore
/// use quant_pivot_macros::NormalizePageQuery;
///
/// #[derive(Deserialize, NormalizePageQuery)]
/// pub struct ModelVersionListQuery {
///     pub model_spec_id: Option<ModelSpecId>,
///     #[normalize_page]
///     #[serde(flatten)]
///     pub page: PageRequest,
/// }
/// ```
///
/// # Compile-time errors
///
/// - Struct without exactly one `#[normalize_page]` field
/// - Multiple `#[normalize_page]` fields
/// - `#[normalize_page]` on a non-`PageRequest` field
/// - Derived on enums or tuple/unit structs
#[proc_macro_derive(NormalizePageQuery, attributes(normalize_page))]
pub fn derive_normalize_page_query(input: TokenStream) -> TokenStream {
    normalize_page_query::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
