//! `#[derive(NormalizePageQuery)]` — register list queries and implement
//! [`NormalizePageQuery`](quant_pivot_models::domain::NormalizePageQuery).
//!
//! Requires exactly one field annotated with `#[normalize_page]` whose type is
//! `PageRequest` (by final path segment).

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Ident, Result, Type};

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let Data::Struct(data_struct) = &input.data else {
        return Err(Error::new_spanned(
            name,
            "NormalizePageQuery can only be derived for structs",
        ));
    };

    let Fields::Named(fields) = &data_struct.fields else {
        return Err(Error::new_spanned(
            name,
            "NormalizePageQuery requires a struct with named fields",
        ));
    };

    let page_fields: Vec<_> = fields
        .named
        .iter()
        .filter(|field| has_normalize_page_attr(field))
        .collect();

    let page_field = match page_fields.len() {
        0 => {
            return Err(Error::new_spanned(
                name,
                "NormalizePageQuery requires exactly one field annotated with #[normalize_page]",
            ));
        }
        1 => page_fields[0],
        _ => {
            return Err(Error::new_spanned(
                page_fields[1],
                "multiple #[normalize_page] fields found; only one is allowed",
            ));
        }
    };

    let page_ident = page_field.ident.as_ref().ok_or_else(|| {
        Error::new_spanned(
            page_field,
            "#[normalize_page] cannot be applied to tuple struct fields",
        )
    })?;

    if !is_page_request_type(&page_field.ty) {
        let found = type_display(&page_field.ty);
        return Err(Error::new_spanned(
            &page_field.ty,
            format!("#[normalize_page] field must have type PageRequest (found: {found})"),
        ));
    }

    Ok(quote! {
        impl #impl_generics crate::domain::pagination::sealed::Sealed for #name #ty_generics #where_clause {}

        impl #impl_generics crate::domain::NormalizePageQuery for #name #ty_generics #where_clause {
            fn page(&self) -> &crate::domain::PageRequest {
                &self.#page_ident
            }

            #[must_use]
            fn normalized(self) -> Self {
                Self {
                    #page_ident: self.#page_ident.normalized(),
                    ..self
                }
            }
        }
    })
}

fn has_normalize_page_attr(field: &syn::Field) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("normalize_page"))
}

fn is_page_request_type(ty: &Type) -> bool {
    type_path_last_ident(ty).is_some_and(|ident| ident == "PageRequest")
}

fn type_path_last_ident(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(type_path) => type_path.path.segments.last().map(|segment| &segment.ident),
        _ => None,
    }
}

fn type_display(ty: &Type) -> String {
    quote!(#ty).to_string()
}
