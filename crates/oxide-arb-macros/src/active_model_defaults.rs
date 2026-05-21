//! `#[derive(ActiveModelDefaults)]` — insert defaults + `ActiveModelBehavior` hooks.
//!
//! Generates `ActiveModel::prepare_for_insert()` for bulk insert paths that bypass
//! `SeaORM` lifecycle hooks, and `ActiveModelBehavior::before_save` that delegates
//! to it on insert.

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
    OnUpdate(OnUpdateRule),
}

enum OnUpdateRule {
    Timestamp(Ident),
}

struct CategorizedRules {
    generate: Vec<(Ident, Expr)>,
    default: Vec<(Ident, Expr)>,
    timestamp: Vec<(Ident, bool)>,
    on_update: Vec<OnUpdateRule>,
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
            "on_update" => Ok(Self::OnUpdate(content.parse()?)),
            other => Err(Error::new_spanned(
                name,
                format!(
                    "unknown active_defaults rule `{other}`; expected one of: generate, default, timestamp, on_update"
                ),
            )),
        }
    }
}

impl Parse for OnUpdateRule {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        syn::parenthesized!(content in input);

        match name.to_string().as_str() {
            "timestamp" => Ok(Self::Timestamp(content.parse()?)),
            other => Err(Error::new_spanned(
                name,
                format!("unknown on_update rule `{other}`; expected: timestamp"),
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
        on_update: Vec::new(),
    };

    for rule in rules {
        match rule {
            DefaultRule::Generate { field, expr } => categorized.generate.push((field, expr)),
            DefaultRule::Default { field, expr } => categorized.default.push((field, expr)),
            DefaultRule::Timestamp { field, always } => categorized.timestamp.push((field, always)),
            DefaultRule::OnUpdate(rule) => categorized.on_update.push(rule),
        }
    }

    if categorized.generate.is_empty()
        && categorized.default.is_empty()
        && categorized.timestamp.is_empty()
        && categorized.on_update.is_empty()
    {
        return Err(Error::new_spanned(
            input,
            "active_defaults must contain at least one rule",
        ));
    }

    Ok(categorized)
}

fn build_prepare_stmts(rules: &CategorizedRules) -> TokenStream {
    let needs_now = !rules.timestamp.is_empty()
        || rules
            .on_update
            .iter()
            .any(|rule| matches!(rule, OnUpdateRule::Timestamp(_)));

    let mut prepare_stmts = TokenStream::new();
    if needs_now {
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

fn build_on_update_stmts(rules: &CategorizedRules) -> TokenStream {
    let mut on_update_stmts = TokenStream::new();
    for rule in &rules.on_update {
        match rule {
            OnUpdateRule::Timestamp(field) => {
                on_update_stmts.extend(quote! {
                    self.#field = ::sea_orm::ActiveValue::Set(::chrono::Utc::now());
                });
            }
        }
    }
    on_update_stmts
}

fn build_before_save_impl(rules: &CategorizedRules) -> TokenStream {
    if rules.on_update.is_empty() {
        quote! {
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
        }
    } else {
        let on_update_stmts = build_on_update_stmts(rules);
        quote! {
            #[async_trait::async_trait]
            impl ::sea_orm::ActiveModelBehavior for ActiveModel {
                async fn before_save<C>(mut self, _db: &C, insert: bool) -> ::std::result::Result<Self, ::sea_orm::DbErr>
                where
                    C: ::sea_orm::ConnectionTrait,
                {
                    if insert {
                        self = self.prepare_for_insert();
                    } else {
                        #on_update_stmts
                    }

                    ::std::result::Result::Ok(self)
                }
            }
        }
    }
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
    let before_save_impl = build_before_save_impl(&rules);

    Ok(quote! {
        impl ActiveModel {
            /// Apply insert-time defaults for fields normally set in [`ActiveModelBehavior`].
            ///
            /// Required for `Entity::insert_many`, which bypasses entity lifecycle hooks.
            pub fn prepare_for_insert(mut self) -> Self {
                #prepare_stmts
                self
            }
        }

        #before_save_impl
    })
}
