//! `#[oxide_schema]` — iden derive + schema catalog registration.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Result};

pub fn expand(_args: TokenStream, input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let Data::Enum(enum_data) = &input.data else {
        return Err(Error::new_spanned(
            &input,
            "oxide_schema can only be applied to iden enums",
        ));
    };

    let name = &input.ident;
    let has_table = enum_data
        .variants
        .iter()
        .any(|variant| variant.ident == "Table");
    if !has_table {
        return Err(Error::new_spanned(
            name,
            "oxide_schema iden enums must contain a `Table` variant",
        ));
    }

    let has_updated_at = enum_data
        .variants
        .iter()
        .any(|variant| variant.ident == "UpdatedAt");
    let lower = name.to_string().to_lowercase();
    let upper = name.to_string().to_uppercase();
    let table_fn = format_ident!("__oxide_schema_{lower}_table_name");
    let triggers_fn = format_ident!("__oxide_schema_{lower}_auto_triggers");
    let static_ident = format_ident!("__OXIDE_SCHEMA_{upper}_TABLE_SPEC");

    let triggers_body = if has_updated_at {
        quote! {
            ::std::vec![crate::schema::trigger::TriggerSpec::updated_at(#table_fn)]
        }
    } else {
        quote! {
            ::std::vec::Vec::new()
        }
    };

    let expanded = quote! {
        #[derive(::sea_orm::DeriveIden)]
        #input

        fn #table_fn() -> ::std::string::String {
            use ::sea_orm::Iden;
            #name::Table.to_string()
        }

        fn #triggers_fn() -> ::std::vec::Vec<crate::schema::trigger::TriggerSpec> {
            #triggers_body
        }

        #[allow(unsafe_code)]
        #[::linkme::distributed_slice(crate::schema::catalog::TABLE_SPECS)]
        static #static_ident: crate::schema::table::TableSpec = crate::schema::table::TableSpec {
            rust_type: ::std::stringify!(#name),
            table_name: #table_fn,
            table,
            indexes,
            dependencies,
            triggers: #triggers_fn,
            seed_units,
            lifecycle: crate::schema::table::TableLifecycle::Core,
        };
    };

    Ok(expanded)
}
