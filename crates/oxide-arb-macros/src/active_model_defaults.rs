//! `#[derive(ActiveModelDefaults)]` — insert defaults + `ActiveModelBehavior` hooks.
//!
//! Generates `ActiveModel::prepare_for_insert()` for bulk insert paths that bypass
//! `SeaORM` lifecycle hooks, and `ActiveModelBehavior::before_save` that delegates
//! to it on insert.
//!
//! Update-time `updated_at` is NOT handled here — it is owned by a `PostgreSQL`
//! `BEFORE UPDATE` trigger (`trigger_set_updated_at`), which is the only mechanism
//! that works reliably across all `SeaORM` write paths (`ActiveModel::update`,
//! `Entity::update_many`, `Entity::insert(...).on_conflict`).

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Ident, Result, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token::Comma,
};

/// Top-level rule inside `#[active_defaults(...)]`.
enum DefaultRule {
    Generate { field: Ident, expr: Expr },
    Default { field: Ident, expr: Expr },
    Timestamp { field: Ident, always: bool },
}

struct CategorizedRules {
    generate: Vec<(Ident, Expr)>,
    default: Vec<(Ident, Expr)>,
    timestamp: Vec<(Ident, bool)>,
}

impl Parse for DefaultRule {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        syn::parenthesized!(content in input);

        match name.to_string().as_str() {
            "generate" => {
                let field: Ident = content.parse()?;
                content.parse::<Token![,]>()?;
                let expr: Expr = content.parse()?;
                Ok(Self::Generate { field, expr })
            }
            "default" => {
                let field: Ident = content.parse()?;
                content.parse::<Token![,]>()?;
                let expr: Expr = content.parse()?;
                Ok(Self::Default { field, expr })
            }
            "timestamp" => {
                let field: Ident = content.parse()?;
                let always = if content.is_empty() {
                    false
                } else {
                    content.parse::<Token![,]>()?;
                    let flag: Ident = content.parse()?;
                    if flag != "always" {
                        return Err(Error::new_spanned(
                            flag,
                            "expected `always` as the second timestamp argument",
                        ));
                    }
                    true
                };
                Ok(Self::Timestamp { field, always })
            }
            other => Err(Error::new_spanned(
                name,
                format!(
                    "unknown active_defaults rule `{other}`; \
                     expected one of: generate, default, timestamp"
                ),
            )),
        }
    }
}

struct ActiveDefaultsArgs {
    rules: Vec<DefaultRule>,
}

impl Parse for ActiveDefaultsArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let rules = Punctuated::<DefaultRule, Comma>::parse_terminated(input)?;
        Ok(Self {
            rules: rules.into_iter().collect(),
        })
    }
}

fn parse_active_defaults_args(input: &DeriveInput) -> Result<ActiveDefaultsArgs> {
    input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("active_defaults"))
        .map(Attribute::parse_args::<ActiveDefaultsArgs>)
        .transpose()?
        .ok_or_else(|| {
            Error::new_spanned(
                input,
                "ActiveModelDefaults requires #[active_defaults(...)] on the same struct",
            )
        })
}

fn categorize_rules(rules: Vec<DefaultRule>, input: &DeriveInput) -> Result<CategorizedRules> {
    let mut categorized = CategorizedRules {
        generate: Vec::new(),
        default: Vec::new(),
        timestamp: Vec::new(),
    };

    for rule in rules {
        match rule {
            DefaultRule::Generate { field, expr } => categorized.generate.push((field, expr)),
            DefaultRule::Default { field, expr } => categorized.default.push((field, expr)),
            DefaultRule::Timestamp { field, always } => categorized.timestamp.push((field, always)),
        }
    }

    if categorized.generate.is_empty()
        && categorized.default.is_empty()
        && categorized.timestamp.is_empty()
    {
        return Err(Error::new_spanned(
            input,
            "active_defaults must contain at least one rule",
        ));
    }

    Ok(categorized)
}

fn build_prepare_stmts(rules: &CategorizedRules) -> TokenStream {
    let mut prepare_stmts = TokenStream::new();

    if !rules.timestamp.is_empty() {
        prepare_stmts.extend(quote! { let now = ::chrono::Utc::now(); });
    }

    for (field, expr) in &rules.generate {
        prepare_stmts.extend(quote! {
            if self.#field.is_not_set() {
                self.#field = ::sea_orm::ActiveValue::Set(#expr);
            }
        });
    }

    for (field, expr) in &rules.default {
        prepare_stmts.extend(quote! {
            if self.#field.is_not_set() {
                self.#field = ::sea_orm::ActiveValue::Set(#expr);
            }
        });
    }

    for (field, always) in &rules.timestamp {
        if *always {
            prepare_stmts.extend(quote! {
                self.#field = ::sea_orm::ActiveValue::Set(now);
            });
        } else {
            prepare_stmts.extend(quote! {
                if self.#field.is_not_set() {
                    self.#field = ::sea_orm::ActiveValue::Set(now);
                }
            });
        }
    }

    prepare_stmts
}

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;

    if !matches!(&input.data, Data::Struct(_)) {
        return Err(Error::new_spanned(
            &input,
            "ActiveModelDefaults can only be derived on structs",
        ));
    }

    let args = parse_active_defaults_args(&input)?;
    let rules = categorize_rules(args.rules, &input)?;
    let prepare_stmts = build_prepare_stmts(&rules);

    Ok(quote! {
        impl ActiveModel {
            /// Apply insert-time defaults for fields normally set in [`ActiveModelBehavior`].
            ///
            /// Call this explicitly before `Entity::insert` or `Entity::insert_many`,
            /// which bypass `SeaORM` lifecycle hooks. The canonical example is
            /// `do_create_batch` in the trade repository.
            pub fn prepare_for_insert(mut self) -> Self {
                #prepare_stmts
                self
            }
        }

        #[async_trait::async_trait]
        impl ::sea_orm::ActiveModelBehavior for ActiveModel {
            async fn before_save<C>(self, _db: &C, insert: bool) -> ::std::result::Result<Self, ::sea_orm::DbErr>
            where
                C: ::sea_orm::ConnectionTrait,
            {
                ::std::result::Result::Ok(if insert {
                    self.prepare_for_insert()
                } else {
                    self
                })
            }
        }
    })
}
