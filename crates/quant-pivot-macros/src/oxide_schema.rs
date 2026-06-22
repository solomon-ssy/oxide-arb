//! `#[oxide_schema]` — iden derive + schema catalog registration.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Expr, Lit, Meta, Result};

pub fn expand(args: TokenStream, input: TokenStream) -> Result<TokenStream> {
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
    let (lifecycle, is_audit) = parse_lifecycle(args)?;

    // Auto-registered triggers are derived purely from catalog metadata:
    // an `UpdatedAt` column ⇒ maintenance trigger; an `audit` lifecycle ⇒
    // append-only (WORM) guard. No hand-maintained per-table trigger lists.
    let mut trigger_inits: Vec<TokenStream> = Vec::new();
    if has_updated_at {
        trigger_inits.push(quote! {
            crate::schema::trigger::TriggerSpec::updated_at(#table_fn)
        });
    }
    if is_audit {
        trigger_inits.push(quote! {
            crate::schema::trigger::TriggerSpec::append_only(#table_fn)
        });
    }
    let triggers_body = if trigger_inits.is_empty() {
        quote! { ::std::vec::Vec::new() }
    } else {
        quote! { ::std::vec![ #(#trigger_inits),* ] }
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
            lifecycle: #lifecycle,
        };
    };

    Ok(expanded)
}

/// Parse the `lifecycle = "..."` argument into its `TableLifecycle` token and a
/// flag indicating whether it is the `audit` lifecycle (which drives append-only
/// WORM trigger registration).
fn parse_lifecycle(args: TokenStream) -> Result<(TokenStream, bool)> {
    if args.is_empty() {
        return Ok((quote! { crate::schema::table::TableLifecycle::Core }, false));
    }

    let meta: Meta = syn::parse2(args)?;
    let Meta::NameValue(name_value) = meta else {
        return Err(Error::new_spanned(
            meta,
            "expected `lifecycle = \"core|control|runtime|ledger|audit|report|seed_ledger\"`",
        ));
    };
    if !name_value.path.is_ident("lifecycle") {
        return Err(Error::new_spanned(
            name_value.path,
            "unsupported oxide_schema argument",
        ));
    }
    let Expr::Lit(expr_lit) = name_value.value else {
        return Err(Error::new_spanned(
            name_value.value,
            "lifecycle must be a string literal",
        ));
    };
    let Lit::Str(lit) = expr_lit.lit else {
        return Err(Error::new_spanned(
            expr_lit.lit,
            "lifecycle must be a string literal",
        ));
    };

    match lit.value().as_str() {
        "core" => Ok((quote! { crate::schema::table::TableLifecycle::Core }, false)),
        "control" => Ok((
            quote! { crate::schema::table::TableLifecycle::Control },
            false,
        )),
        "runtime" => Ok((
            quote! { crate::schema::table::TableLifecycle::Runtime },
            false,
        )),
        "ledger" => Ok((
            quote! { crate::schema::table::TableLifecycle::Ledger },
            false,
        )),
        "audit" => Ok((quote! { crate::schema::table::TableLifecycle::Audit }, true)),
        "report" => Ok((
            quote! { crate::schema::table::TableLifecycle::Report },
            false,
        )),
        "seed_ledger" => Ok((
            quote! { crate::schema::table::TableLifecycle::SeedLedger },
            false,
        )),
        other => Err(Error::new_spanned(
            lit,
            format!("unsupported table lifecycle `{other}`"),
        )),
    }
}
