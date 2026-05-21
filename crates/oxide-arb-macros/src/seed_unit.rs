//! `#[derive(SeedUnit)]` — seed metadata registration + execute delegation.
//!
//! Generates an `impl SeedUnit` block from `#[seed_unit(...)]` attributes.
//! The `execute` method delegates to a hand-written loader function specified
//! by the `loader` attribute.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, DeriveInput, Error, Ident, LitInt, LitStr, Path, Result, Token,
    parse::{Parse, ParseStream},
};

struct SeedUnitArgs {
    id: LitStr,
    order: LitInt,
    policy: Ident,
    loader: Path,
}

impl Parse for SeedUnitArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut id = None;
        let mut order = None;
        let mut policy = None;
        let mut loader = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "id" => id = Some(input.parse::<LitStr>()?),
                "order" => order = Some(input.parse::<LitInt>()?),
                "policy" => policy = Some(input.parse::<Ident>()?),
                "loader" => loader = Some(input.parse::<Path>()?),
                other => {
                    return Err(Error::new_spanned(
                        key,
                        format!(
                            "unknown seed_unit attribute `{other}`; \
                             expected: id, order, policy, loader"
                        ),
                    ));
                }
            }

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self {
            id: id.ok_or_else(|| input.error("missing required `id`"))?,
            order: order.ok_or_else(|| input.error("missing required `order`"))?,
            policy: policy.ok_or_else(|| input.error("missing required `policy`"))?,
            loader: loader.ok_or_else(|| input.error("missing required `loader`"))?,
        })
    }
}

fn find_seed_unit_attr(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs.iter().find(|a| a.path().is_ident("seed_unit"))
}

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;

    let attr = find_seed_unit_attr(&input.attrs).ok_or_else(|| {
        Error::new_spanned(&input, "SeedUnit requires #[seed_unit(...)] attribute")
    })?;

    let args: SeedUnitArgs = attr.parse_args()?;
    let ident = &input.ident;
    let id = &args.id;
    let order = &args.order;
    let policy = &args.policy;
    let loader = &args.loader;

    Ok(quote! {
        #[async_trait::async_trait]
        impl crate::seed::SeedUnit for #ident {
            fn id(&self) -> &'static str { #id }
            fn order(&self) -> i32 { #order }

            fn policy(&self) -> crate::seed::SeedConflictPolicy {
                crate::seed::SeedConflictPolicy::#policy
            }

            async fn execute(
                &self,
                db: &dyn ::sea_orm::ConnectionTrait,
                ctx: &mut crate::seed::SeedContext,
            ) -> ::std::result::Result<u64, ::sea_orm::DbErr> {
                #loader(db, ctx).await
            }
        }
    })
}
