//! `#[derive(UuidId)]` — type-safe UUID ID newtype backed by `Arc<Uuid>`.
//!
//! Generates: `new`, `from_v7`, `as_uuid`, `into_uuid`, `Debug`, `Clone`,
//! `PartialEq`, `Eq`, `Hash`, `Display`, `FromStr`, `Serialize`,
//! `Deserialize`, and full `SeaORM` bindings backed by the native Postgres
//! `uuid` column type. All generated ids are UUID v7 (time-ordered).
//!
//! Used for internal, system-generated identifiers (`UserId`,
//! `RecommendationId`, …). Externally defined string identifiers that are not
//! UUIDs use [`crate::StrId`] instead.
//!
//! The inner value is `Arc<Uuid>` so cloning a typed id is a cheap atomic
//! reference-count bump, matching the ergonomics of the string-backed ids on
//! the hot path.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Fields, ImplGenerics, Result, TypeGenerics, Visibility, WhereClause,
};

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;
    let vis = &input.vis;

    validate_struct(&input)?;

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let core = expand_core(name, vis, &impl_generics, &ty_generics, where_clause);
    let serde = expand_serde(name, &impl_generics, &ty_generics, where_clause);
    let json_schema = expand_json_schema(name, &impl_generics, &ty_generics, where_clause);
    let seaorm = expand_seaorm(name, &impl_generics, &ty_generics, where_clause);
    let seaorm_try_getable =
        expand_seaorm_try_getable(name, &impl_generics, &ty_generics, where_clause);

    Ok(quote! {
        #core
        #serde
        #json_schema
        #seaorm
        #seaorm_try_getable
    })
}

fn expand_json_schema(
    name: &syn::Ident,
    impl_generics: &ImplGenerics<'_>,
    ty_generics: &TypeGenerics<'_>,
    where_clause: Option<&WhereClause>,
) -> TokenStream {
    quote! {
        impl #impl_generics ::schemars::JsonSchema for #name #ty_generics #where_clause {
            fn inline_schema() -> bool {
                true
            }

            fn schema_name() -> ::std::borrow::Cow<'static, str> {
                ::std::borrow::Cow::Borrowed(stringify!(#name))
            }

            fn json_schema(
                _generator: &mut ::schemars::SchemaGenerator,
            ) -> ::schemars::Schema {
                ::schemars::json_schema!({
                    "type": "string",
                    "format": "uuid"
                })
            }
        }
    }
}

fn expand_core(
    name: &syn::Ident,
    vis: &Visibility,
    impl_generics: &ImplGenerics<'_>,
    ty_generics: &TypeGenerics<'_>,
    where_clause: Option<&WhereClause>,
) -> TokenStream {
    quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            /// Wrap an existing UUID value.
            #[must_use]
            #[inline]
            #vis fn new(id: ::uuid::Uuid) -> Self {
                Self(::std::sync::Arc::new(id))
            }

            /// Generate a fresh time-ordered identifier (UUID v7).
            ///
            /// All system-generated ids use v7 so that lexicographic ordering
            /// matches creation time, keeping the backing Postgres B-tree index
            /// compact on append-heavy tables. There is intentionally no v4
            /// constructor — every internal id is time-ordered.
            #[must_use]
            #[inline]
            #vis fn from_v7() -> Self {
                Self(::std::sync::Arc::new(::uuid::Uuid::now_v7()))
            }

            /// Borrow the inner UUID value (cheap copy, `Uuid` is `Copy`).
            #[must_use]
            #[inline]
            #vis fn as_uuid(&self) -> ::uuid::Uuid {
                *self.0
            }

            /// Consume the id and return the inner UUID value.
            #[must_use]
            #[inline]
            #vis fn into_uuid(self) -> ::uuid::Uuid {
                *self.0
            }
        }

        impl #impl_generics From<::uuid::Uuid> for #name #ty_generics #where_clause {
            #[inline]
            fn from(id: ::uuid::Uuid) -> Self {
                Self::new(id)
            }
        }

        impl #impl_generics From<#name #ty_generics> for ::uuid::Uuid #where_clause {
            #[inline]
            fn from(id: #name #ty_generics) -> Self {
                *id.0
            }
        }

        impl #impl_generics From<&#name #ty_generics> for #name #ty_generics #where_clause {
            #[inline]
            fn from(id: &#name #ty_generics) -> Self {
                id.clone()
            }
        }

        impl #impl_generics ::std::fmt::Display for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl #impl_generics ::std::str::FromStr for #name #ty_generics #where_clause {
            type Err = ::uuid::Error;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                Ok(Self::new(::uuid::Uuid::parse_str(s)?))
            }
        }
    }
}

fn expand_serde(
    name: &syn::Ident,
    impl_generics: &ImplGenerics<'_>,
    ty_generics: &TypeGenerics<'_>,
    where_clause: Option<&WhereClause>,
) -> TokenStream {
    quote! {
        // Always (de)serialize as the canonical hyphenated UUID string, in every
        // serde format. This keeps the wire form a `String` for JSON, bincode /
        // bitcode caches, and ClickHouse `String` / `Array(String)` columns.
        // Postgres uses the native `uuid` column type via the `SeaORM` `Value`
        // binding below, not serde — so storage stays compact there.
        impl #impl_generics ::serde::Serialize for #name #ty_generics #where_clause {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0.as_hyphenated().to_string())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #name {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::std::result::Result<Self, D::Error> {
                let raw = <String as ::serde::Deserialize>::deserialize(deserializer)?;
                let id = ::uuid::Uuid::parse_str(&raw).map_err(::serde::de::Error::custom)?;
                Ok(Self(::std::sync::Arc::new(id)))
            }
        }
    }
}

fn expand_seaorm(
    name: &syn::Ident,
    impl_generics: &ImplGenerics<'_>,
    ty_generics: &TypeGenerics<'_>,
    where_clause: Option<&WhereClause>,
) -> TokenStream {
    quote! {
        impl #impl_generics From<#name #ty_generics> for sea_orm::sea_query::Value #where_clause {
            #[inline]
            fn from(id: #name #ty_generics) -> Self {
                sea_orm::sea_query::Value::Uuid(Some(*id.0))
            }
        }

        impl #impl_generics From<&#name #ty_generics> for sea_orm::sea_query::Value #where_clause {
            #[inline]
            fn from(id: &#name #ty_generics) -> Self {
                sea_orm::sea_query::Value::Uuid(Some(*id.0))
            }
        }

        impl #impl_generics sea_orm::sea_query::ValueType for #name #ty_generics #where_clause {
            fn try_from(v: sea_orm::sea_query::Value) -> ::std::result::Result<Self, sea_orm::sea_query::ValueTypeErr> {
                match v {
                    sea_orm::sea_query::Value::Uuid(Some(id)) => Ok(Self::new(id)),
                    _ => Err(sea_orm::sea_query::ValueTypeErr),
                }
            }

            fn type_name() -> String {
                stringify!(#name).to_owned()
            }

            fn array_type() -> sea_orm::sea_query::ArrayType {
                sea_orm::sea_query::ArrayType::Uuid
            }

            fn column_type() -> sea_orm::sea_query::ColumnType {
                sea_orm::sea_query::ColumnType::Uuid
            }
        }

        impl #impl_generics sea_orm::sea_query::Nullable for #name #ty_generics #where_clause {
            fn null() -> sea_orm::sea_query::Value {
                sea_orm::sea_query::Value::Uuid(None)
            }
        }

        impl #impl_generics sea_orm::IntoActiveValue<#name #ty_generics> for #name #ty_generics #where_clause {
            #[inline]
            fn into_active_value(self) -> sea_orm::ActiveValue<#name #ty_generics> {
                sea_orm::ActiveValue::Set(self)
            }
        }

        impl #impl_generics sea_orm::TryFromU64 for #name #ty_generics #where_clause {
            fn try_from_u64(_n: u64) -> ::std::result::Result<Self, sea_orm::DbErr> {
                Err(sea_orm::DbErr::ConvertFromU64(stringify!(#name)))
            }
        }
    }
}

fn expand_seaorm_try_getable(
    name: &syn::Ident,
    impl_generics: &ImplGenerics<'_>,
    ty_generics: &TypeGenerics<'_>,
    where_clause: Option<&WhereClause>,
) -> TokenStream {
    quote! {
        impl #impl_generics sea_orm::TryGetable for #name #ty_generics #where_clause {
            fn try_get_by<I: sea_orm::ColIdx>(
                res: &sea_orm::QueryResult,
                index: I,
            ) -> ::std::result::Result<Self, sea_orm::TryGetError> {
                let raw: ::uuid::Uuid =
                    <::uuid::Uuid as sea_orm::TryGetable>::try_get_by(res, index)?;
                Ok(Self::new(raw))
            }
        }
    }
}

fn validate_struct(input: &DeriveInput) -> Result<()> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => Ok(()),
            _ => Err(Error::new_spanned(
                input,
                "UuidId requires a tuple struct with exactly one field: `struct Foo(Arc<Uuid>)`",
            )),
        },
        _ => Err(Error::new_spanned(
            input,
            "UuidId can only be derived on structs",
        )),
    }
}
