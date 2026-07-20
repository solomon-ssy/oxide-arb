//! Bidirectional AST audit for native runtime SQL contracts.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use quant_pivot_migration::sql_contract_registry::migration_sql_contracts;
use quant_pivot_repository::sql_contract_registry::repository_sql_contracts;
use quant_pivot_sql_contract::validate_registry;
use quant_pivot_storage::sql_contract_registry::storage_sql_contracts;
use syn::{
    Attribute, Expr, ExprCall, ExprMethodCall, ImplItemFn, Item, ItemFn, ItemMod,
    punctuated::Punctuated, token::Comma, visit::Visit,
};

use crate::{
    persistence_field_audit::{parse_rust, rust_files, workspace_root},
    sql_contract_registry::xtask_sql_contracts,
};

const SOURCE_ROOTS: &[&str] = &[
    "crates/quant-pivot-migration/src",
    "crates/quant-pivot-storage/src",
    "crates/quant-pivot-repository/src",
    "crates/quant-pivot-core/src",
    "crates/quant-pivot-web/src",
    "crates/quant-pivot-bin/src",
    "crates/quant-pivot-xtask/src",
];
const MIGRATION_REGISTRY: &str = "crates/quant-pivot-migration/src/sql_contract_registry.rs";
const STORAGE_REGISTRY: &str = "crates/quant-pivot-storage/src/sql_contract_registry.rs";
const REPOSITORY_REGISTRY: &str = "crates/quant-pivot-repository/src/sql_contract_registry.rs";
const XTASK_REGISTRY: &str = "crates/quant-pivot-xtask/src/sql_contract_registry.rs";
const CONTRACT_METHODS: &[&str] = &[
    "clickhouse_query",
    "postgres_statement",
    "postgres_query",
    "postgres_owned_query",
];

pub fn run() -> Result<()> {
    let root = workspace_root()?;
    let compiled = migration_sql_contracts()
        .iter()
        .chain(storage_sql_contracts())
        .chain(repository_sql_contracts())
        .chain(xtask_sql_contracts())
        .copied()
        .collect::<Vec<_>>();
    validate_registry(&compiled).map_err(anyhow::Error::msg)?;

    let mut declared = BTreeSet::new();
    let mut registered = BTreeSet::new();
    for registry in [
        MIGRATION_REGISTRY,
        STORAGE_REGISTRY,
        REPOSITORY_REGISTRY,
        XTASK_REGISTRY,
    ] {
        let (owner_declared, owner_registered) = registry_inventory(&root.join(registry))?;
        declared.extend(owner_declared);
        registered.extend(owner_registered);
    }
    require_same("declared", &declared, "registered", &registered)?;
    if compiled.len() != registered.len() {
        bail!(
            "compiled SQL registry has {} contracts, AST registry has {} entries",
            compiled.len(),
            registered.len()
        );
    }

    let mut usage = BTreeSet::new();
    let mut violations = Vec::new();
    for source_root in SOURCE_ROOTS {
        for path in rust_files(&root.join(source_root))? {
            let parsed = parse_rust(&path)?;
            let relative = path.strip_prefix(&root).unwrap_or(&path).to_path_buf();
            let lifecycle_source = relative.starts_with("crates/quant-pivot-migration/src")
                || relative
                    .components()
                    .any(|component| component.as_os_str() == "migration")
                || relative.ends_with("clickhouse/migration.rs");
            let mut visitor = SqlVisitor {
                path: relative,
                usage: &mut usage,
                violations: &mut violations,
                contract_depth: 0,
                loop_depth: 0,
                lifecycle_source,
            };
            visitor.visit_file(&parsed);
        }
    }
    require_same("registered", &registered, "used", &usage)?;
    if !violations.is_empty() {
        violations.sort();
        bail!("native SQL escaped SqlContract:\n{}", violations.join("\n"));
    }

    println!(
        "SQL contract audit passed: {} compiled contracts, {} AST usages, no unregistered native SQL",
        compiled.len(),
        usage.len()
    );
    Ok(())
}

fn registry_inventory(path: &Path) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let parsed = parse_rust(path)?;
    let mut declared = BTreeSet::new();
    let mut registered = BTreeSet::new();
    for item in parsed.items {
        match item {
            Item::Const(item) => {
                let ident = item.ident.to_string();
                if type_ends_with(&item.ty, "SqlContract") {
                    declared.insert(ident);
                } else if ident.ends_with("_SQL_CONTRACTS") {
                    collect_registry_expr(&item.expr, &mut registered);
                }
            }
            Item::Macro(item) if item.mac.path.is_ident("ch_contract") => {
                let arguments = item
                    .mac
                    .parse_body_with(Punctuated::<Expr, Comma>::parse_terminated)
                    .with_context(|| {
                        format!("parse ch_contract invocation in {}", path.display())
                    })?;
                let first = arguments.first().with_context(|| {
                    format!("empty ch_contract invocation in {}", path.display())
                })?;
                let Expr::Path(path) = first else {
                    bail!("ch_contract declaration must start with an identifier");
                };
                declared.insert(path_ident(&path.path)?);
            }
            _ => {}
        }
    }
    Ok((declared, registered))
}

fn type_ends_with(ty: &syn::Type, expected: &str) -> bool {
    matches!(
        ty,
        syn::Type::Path(path)
            if path.path.segments.last().is_some_and(|segment| segment.ident == expected)
    )
}

fn collect_registry_expr(expr: &Expr, output: &mut BTreeSet<String>) {
    match expr {
        Expr::Reference(reference) => collect_registry_expr(&reference.expr, output),
        Expr::Array(array) => {
            for element in &array.elems {
                if let Expr::Path(path) = element
                    && let Some(ident) = path.path.segments.last()
                {
                    output.insert(ident.ident.to_string());
                }
            }
        }
        _ => {}
    }
}

fn path_ident(path: &syn::Path) -> Result<String> {
    path.segments
        .last()
        .map(|segment| segment.ident.to_string())
        .context("Rust path has no final identifier")
}

fn require_same(
    left_name: &str,
    left: &BTreeSet<String>,
    right_name: &str,
    right: &BTreeSet<String>,
) -> Result<()> {
    let missing = left.difference(right).cloned().collect::<Vec<_>>();
    let stale = right.difference(left).cloned().collect::<Vec<_>>();
    if !missing.is_empty() || !stale.is_empty() {
        bail!(
            "SQL contract {left_name}/{right_name} mismatch: absent from {right_name}={missing:?}; absent from {left_name}={stale:?}"
        );
    }
    Ok(())
}

struct SqlVisitor<'a> {
    path: PathBuf,
    usage: &'a mut BTreeSet<String>,
    violations: &'a mut Vec<String>,
    contract_depth: usize,
    loop_depth: usize,
    lifecycle_source: bool,
}

impl SqlVisitor<'_> {
    fn record_contract(&mut self, node: &ExprMethodCall) {
        match node.receiver.as_ref() {
            Expr::Path(path) => match path_ident(&path.path) {
                Ok(ident) => {
                    self.usage.insert(ident);
                }
                Err(error) => self.violations.push(format!(
                    "{}: invalid SQL contract receiver: {error}",
                    self.path.display()
                )),
            },
            _ => self.violations.push(format!(
                "{}: SQL contract method must use a named registry constant",
                self.path.display()
            )),
        }
        if self.loop_depth > 0 && !self.lifecycle_source {
            self.violations.push(format!(
                "{}: native SQL contract executes inside an unbounded loop",
                self.path.display()
            ));
        }
    }
}

impl<'ast> Visit<'ast> for SqlVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast ItemMod) {
        if !cfg_test(&node.attrs) {
            syn::visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        if !cfg_test(&node.attrs) {
            syn::visit::visit_item_fn(self, node);
        }
    }

    fn visit_impl_item_fn(&mut self, node: &'ast ImplItemFn) {
        if !cfg_test(&node.attrs) {
            syn::visit::visit_impl_item_fn(self, node);
        }
    }

    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        let method = node.method.to_string();
        if CONTRACT_METHODS.contains(&method.as_str()) {
            self.record_contract(node);
            self.contract_depth += 1;
            syn::visit::visit_expr_method_call(self, node);
            self.contract_depth -= 1;
            return;
        }
        if method == "query" {
            self.violations.push(format!(
                "{}: direct `.query(...)` bypasses SqlContract",
                self.path.display()
            ));
        }
        if method == "execute_unprepared" && !node.args.iter().any(contains_postgres_contract) {
            self.violations.push(format!(
                "{}: direct `.execute_unprepared(...)` bypasses SqlContract",
                self.path.display()
            ));
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
        if let Expr::Path(function) = node.func.as_ref() {
            let segments = function
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            let last = segments.last().map(String::as_str).unwrap_or_default();
            if segments.first().is_some_and(|segment| segment == "sqlx")
                && matches!(last, "query" | "query_as" | "query_scalar" | "raw_sql")
                && !node.args.iter().any(contains_postgres_contract)
            {
                self.violations.push(format!(
                    "{}: direct `sqlx::{last}(...)` bypasses SqlContract",
                    self.path.display()
                ));
            }
            if segments
                .iter()
                .rev()
                .nth(1)
                .is_some_and(|segment| segment == "Statement")
                && last.starts_with("from_")
                && self.contract_depth == 0
            {
                self.violations.push(format!(
                    "{}: raw SeaORM Statement bypasses SqlContract",
                    self.path.display()
                ));
            }
        }
        syn::visit::visit_expr_call(self, node);
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.loop_depth += 1;
        syn::visit::visit_expr_for_loop(self, node);
        self.loop_depth -= 1;
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.loop_depth += 1;
        syn::visit::visit_expr_while(self, node);
        self.loop_depth -= 1;
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.loop_depth += 1;
        syn::visit::visit_expr_loop(self, node);
        self.loop_depth -= 1;
    }
}

fn contains_postgres_contract(expr: &Expr) -> bool {
    struct Detector(bool);
    impl<'ast> Visit<'ast> for Detector {
        fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
            if matches!(
                node.method.to_string().as_str(),
                "postgres_query" | "postgres_owned_query"
            ) {
                self.0 = true;
            } else {
                syn::visit::visit_expr_method_call(self, node);
            }
        }
    }
    let mut detector = Detector(false);
    detector.visit_expr(expr);
    detector.0
}

fn cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        if attribute.path().is_ident("test") {
            return true;
        }
        if !attribute.path().is_ident("cfg") {
            return false;
        }
        let mut test = false;
        let _result = attribute.parse_nested_meta(|meta| {
            test = meta.path.is_ident("test");
            Ok(())
        });
        test
    })
}
