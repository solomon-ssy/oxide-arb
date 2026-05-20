//! `#[derive(TypedId)]` — type-safe ID newtype backed by `Arc<str>`.
//!
//! Generates: `new`, `as_str`, `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`,
//! `Display`, `FromStr`, `From<&str>`, `From<String>`, `AsRef<str>`,
//! `Serialize`, `Deserialize`, and full `SeaORM` bindings.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Result};

#[allow(clippy::too_many_lines)]
pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;
    let vis = &input.vis;

    validate_struct(&input)?;

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        // ── Core trait impls ────────────────────────────────────────

        impl #impl_generics #name #ty_generics #where_clause {
            #[must_use]
            #[inline]
            #vis fn new(s: impl AsRef<str>) -> Self {
                Self(::std::sync::Arc::from(s.as_ref()))
            }

            #[must_use]
            #[inline]
            #vis fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl #impl_generics ::std::fmt::Display for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl #impl_generics ::std::str::FromStr for #name #ty_generics #where_clause {
            type Err = ::std::convert::Infallible;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                Ok(Self::new(s))
            }
        }

        impl #impl_generics AsRef<str> for #name #ty_generics #where_clause {
            #[inline]
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl #impl_generics From<String> for #name #ty_generics #where_clause {
            #[inline]
            fn from(s: String) -> Self {
                Self(::std::sync::Arc::from(s.as_str()))
            }
        }

        impl #impl_generics From<&str> for #name #ty_generics #where_clause {
            #[inline]
            fn from(s: &str) -> Self {
                Self(::std::sync::Arc::from(s))
            }
        }

        impl #impl_generics From<&#name #ty_generics> for #name #ty_generics #where_clause {
            #[inline]
            fn from(id: &#name #ty_generics) -> Self {
                id.clone()
            }
        }

        impl #impl_generics From<#name #ty_generics> for String #where_clause {
            #[inline]
            fn from(id: #name #ty_generics) -> Self {
                id.as_str().to_owned()
            }
        }

        // ── Serde ───────────────────────────────────────────────────

        impl #impl_generics ::serde::Serialize for #name #ty_generics #where_clause {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error> {
                self.0.serialize(serializer)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #name {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::std::result::Result<Self, D::Error> {
                let s = <String as ::serde::Deserialize>::deserialize(deserializer)?;
                Ok(Self(::std::sync::Arc::from(s.as_str())))
            }
        }

        // ── SeaORM bindings ─────────────────────────────────────────
        //
        // String-backed newtypes stored as TEXT columns for lossless
        // round-trips. The Arc<str> → String allocation happens only
        // at the DB boundary; hot-path clones remain O(1).

        impl #impl_generics From<#name #ty_generics> for sea_orm::sea_query::Value #where_clause {
            #[inline]
            fn from(id: #name #ty_generics) -> Self {
                sea_orm::sea_query::Value::String(Some(Box::new(id.as_str().to_owned())))
            }
        }

        impl #impl_generics From<&#name #ty_generics> for sea_orm::sea_query::Value #where_clause {
            #[inline]
            fn from(id: &#name #ty_generics) -> Self {
                sea_orm::sea_query::Value::String(Some(Box::new(id.as_str().to_owned())))
            }
        }

        impl #impl_generics sea_orm::TryGetable for #name #ty_generics #where_clause {
            fn try_get_by<I: sea_orm::ColIdx>(
                res: &sea_orm::QueryResult,
                index: I,
            ) -> ::std::result::Result<Self, sea_orm::TryGetError> {
                let raw: String =
                    <String as sea_orm::TryGetable>::try_get_by(res, index).map_err(|e| match e {
                        sea_orm::TryGetError::DbErr(sea_orm::DbErr::Type(ref msg))
                            if msg.contains("null value") =>
                        {
                            sea_orm::TryGetError::Null(format!("{:?}", index))
                        }
                        other => other,
                    })?;
                Ok(Self::from(raw))
            }
        }

        impl #impl_generics sea_orm::sea_query::ValueType for #name #ty_generics #where_clause {
            fn try_from(v: sea_orm::sea_query::Value) -> ::std::result::Result<Self, sea_orm::sea_query::ValueTypeErr> {
                match v {
                    sea_orm::sea_query::Value::String(Some(s)) => Ok(Self::from(*s)),
                    _ => Err(sea_orm::sea_query::ValueTypeErr),
                }
            }

            fn type_name() -> String {
                stringify!(#name).to_owned()
            }

            fn array_type() -> sea_orm::sea_query::ArrayType {
                sea_orm::sea_query::ArrayType::String
            }

            fn column_type() -> sea_orm::sea_query::ColumnType {
                sea_orm::sea_query::ColumnType::Text
            }
        }

        impl #impl_generics sea_orm::sea_query::Nullable for #name #ty_generics #where_clause {
            fn null() -> sea_orm::sea_query::Value {
                sea_orm::sea_query::Value::String(None)
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
    })
}

fn validate_struct(input: &DeriveInput) -> Result<()> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => Ok(()),
            _ => Err(Error::new_spanned(
                input,
                "TypedId requires a tuple struct with exactly one field: `struct Foo(Arc<str>)`",
            )),
        },
        _ => Err(Error::new_spanned(
            input,
            "TypedId can only be derived on structs",
        )),
    }
}
