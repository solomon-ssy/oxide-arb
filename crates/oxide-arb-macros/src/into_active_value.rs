//! `#[derive(IntoActiveValue)]` — auto-impl `sea_orm::IntoActiveValue` for enums.
//!
//! The generated impl wraps `self` in `ActiveValue::Set(self)`, which is
//! the standard pattern for enums that use `DeriveActiveEnum`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Result};

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;

    if !matches!(input.data, Data::Enum(_)) {
        return Err(Error::new_spanned(
            &input,
            "IntoActiveValue can only be derived on enums",
        ));
    }

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics sea_orm::IntoActiveValue<#name #ty_generics> for #name #ty_generics #where_clause {
            fn into_active_value(self) -> sea_orm::ActiveValue<#name #ty_generics> {
                sea_orm::ActiveValue::Set(self)
            }
        }
    })
}
