use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use quote::ToTokens;
use serde::Deserialize;
use syn::{
    Attribute, Block, Expr, Fields, File, GenericArgument, Item, ItemMod, ItemUse, Lit, Meta,
    Path as SynPath, PathArguments, Token, Type, UseTree, Visibility,
    punctuated::Punctuated,
    visit::{self, Visit},
};
use toml::Value;

const TEST_ONLY_EXTERNAL_DEPENDENCIES: &[&str] = &[
    "actix-http",
    "actix-test",
    "criterion",
    "insta",
    "testcontainers",
    "testcontainers-modules",
    "wiremock",
];

const KNOWN_WORKSPACE_PACKAGES: &[&str] = &[
    "quant-pivot-api",
    "quant-pivot-bench",
    "quant-pivot-bin",
    "quant-pivot-core",
    "quant-pivot-error",
    "quant-pivot-macros",
    "quant-pivot-migration",
    "quant-pivot-models",
    "quant-pivot-repository",
    "quant-pivot-research",
    "quant-pivot-storage",
    "quant-pivot-system-tests",
    "quant-pivot-web",
    "quant-pivot-xtask",
];

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_root: PathBuf,
    workspace_members: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    dependencies: Vec<CargoDependency>,
}

#[derive(Debug, Deserialize)]
struct CargoDependency {
    name: String,
    rename: Option<String>,
    kind: Option<String>,
    path: Option<PathBuf>,
    features: Vec<String>,
}

pub fn run() -> Result<()> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .context("run cargo metadata for architecture check")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("decode cargo metadata")?;
    let mut violations = validate(&metadata);
    violations.extend(validate_workspace_dependency_inventory(&metadata)?);
    violations.extend(validate_public_api(&metadata.workspace_root)?);
    if violations.is_empty() {
        println!("architecture check passed");
        return Ok(());
    }

    bail!(
        "architecture check found {} violation(s):\n{}",
        violations.len(),
        violations
            .iter()
            .map(|violation| format!("- {violation}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn path_is_noncanonical(segments: &[String]) -> bool {
    const PRIMITIVE_TYPES: &[&str] = &[
        "bool", "char", "f32", "f64", "i8", "i16", "i32", "i64", "i128", "isize", "str", "u8",
        "u16", "u32", "u64", "u128", "usize",
    ];

    let tokio_function_exception = segments.len() == 3
        && segments.first().is_some_and(|segment| segment == "tokio")
        && segments
            .last()
            .and_then(|segment| segment.chars().next())
            .is_some_and(char::is_lowercase);
    let primitive_associated_item = segments
        .first()
        .is_some_and(|segment| PRIMITIVE_TYPES.contains(&segment.as_str()));
    let generic_associated_item = segments.len() > 2
        && segments.first().is_some_and(|segment| {
            segment == "Self"
                || (segment.len() == 1 && segment.chars().next().is_some_and(char::is_uppercase))
        });
    let sea_orm_relation_target = segments.len() == 3
        && segments
            .first()
            .is_some_and(|segment| segment == "super" || segment == "self" || segment == "crate")
        && segments.last().is_some_and(|segment| segment == "Entity");
    let module_owned_type_or_constant = segments.len() == 2
        && segments
            .first()
            .and_then(|segment| segment.chars().next())
            .is_some_and(char::is_lowercase)
        && segments
            .last()
            .and_then(|segment| segment.chars().next())
            .is_some_and(char::is_uppercase)
        && segments.last().is_none_or(|segment| segment != "Entity");
    (segments.len() > 2
        && !tokio_function_exception
        && !primitive_associated_item
        && !generic_associated_item
        && !sea_orm_relation_target)
        || (module_owned_type_or_constant && !primitive_associated_item)
}

fn validate_public_api(workspace_root: &Path) -> Result<Vec<String>> {
    let crates_root = workspace_root.join("crates");
    let mut source_files = Vec::new();
    collect_rust_sources(&crates_root, &mut source_files)?;
    source_files.sort();

    let mut violations = Vec::new();
    let mut parsed_sources = Vec::with_capacity(source_files.len());
    for path in source_files {
        let source = fs::read_to_string(&path)
            .with_context(|| format!("read Rust source {}", path.display()))?;
        let parsed = syn::parse_file(&source)
            .with_context(|| format!("parse Rust source {}", path.display()))?;
        let relative = path.strip_prefix(workspace_root).unwrap_or(&path);
        violations.extend(validate_public_exports(
            &parsed.items,
            relative,
            path.file_name().is_some_and(|name| name == "lib.rs"),
        ));
        violations.extend(validate_source_style(&parsed, relative));
        parsed_sources.push((relative.to_path_buf(), parsed));
    }
    violations.extend(validate_persistence_documents(&parsed_sources)?);
    violations.extend(validate_config_secret_types(&parsed_sources)?);
    Ok(violations)
}

#[derive(Debug)]
struct JsonDocumentDeclaration {
    source: PathBuf,
    derives_from_json_query_result: bool,
    has_closed_shape: bool,
}

#[derive(Default)]
struct SerdeDocumentOptions {
    deny_unknown_fields: bool,
    tagged: bool,
    transparent: bool,
}

fn validate_persistence_documents(sources: &[(PathBuf, File)]) -> Result<Vec<String>> {
    let model_source = Path::new("crates/quant-pivot-models/src");
    let entity_source = model_source.join("entities");
    let mut usages = BTreeMap::<String, Vec<String>>::new();
    let mut violations = Vec::new();

    for (source, file) in sources
        .iter()
        .filter(|(source, _)| source.starts_with(&entity_source))
    {
        for item in &file.items {
            let Item::Struct(item) = item else { continue };
            let Fields::Named(fields) = &item.fields else {
                continue;
            };
            for field in &fields.named {
                if !is_json_binary(&field.attrs)? {
                    continue;
                }
                let field_name = field
                    .ident
                    .as_ref()
                    .map_or_else(|| "<unnamed>".to_owned(), ToString::to_string);
                let usage = format!("{}::{field_name}", source.display());
                let Some(type_name) = persistence_type_name(&field.ty) else {
                    violations.push(format!(
                        "{usage} uses an unsupported JSONB field type; declare one canonical named document type"
                    ));
                    continue;
                };
                if matches!(type_name.as_str(), "Json" | "Value") {
                    violations.push(format!(
                        "{usage} exposes raw `{type_name}`; runtime JSONB fields must use a canonical typed document"
                    ));
                    continue;
                }
                usages.entry(type_name).or_default().push(usage);
            }
        }
    }

    let required_types = usages.keys().cloned().collect::<BTreeSet<_>>();
    let mut declarations = BTreeMap::<String, Vec<JsonDocumentDeclaration>>::new();
    for (source, file) in sources
        .iter()
        .filter(|(source, _)| source.starts_with(model_source))
    {
        for item in &file.items {
            let (name, attrs, fields, enum_is_unit) = match item {
                Item::Struct(item) if required_types.contains(&item.ident.to_string()) => (
                    item.ident.to_string(),
                    &item.attrs,
                    Some(&item.fields),
                    false,
                ),
                Item::Enum(item) if required_types.contains(&item.ident.to_string()) => (
                    item.ident.to_string(),
                    &item.attrs,
                    None,
                    item.variants
                        .iter()
                        .all(|variant| matches!(variant.fields, Fields::Unit)),
                ),
                _ => continue,
            };
            let serde = serde_document_options(attrs)?;
            let has_closed_shape = match fields {
                Some(Fields::Named(_)) => serde.deny_unknown_fields,
                Some(Fields::Unnamed(_)) => serde.transparent,
                Some(Fields::Unit) => false,
                None => enum_is_unit || serde.tagged,
            };
            declarations
                .entry(name)
                .or_default()
                .push(JsonDocumentDeclaration {
                    source: source.clone(),
                    derives_from_json_query_result: derives(attrs, "FromJsonQueryResult")?,
                    has_closed_shape,
                });
        }
    }

    for (type_name, type_usages) in usages {
        let Some(candidates) = declarations.get(&type_name) else {
            violations.push(format!(
                "JSONB type `{type_name}` used by {} has no canonical top-level declaration in quant-pivot-models",
                type_usages.join(", ")
            ));
            continue;
        };
        if candidates.len() != 1 {
            violations.push(format!(
                "JSONB type `{type_name}` has {} top-level declarations; persistence documents require one canonical owner",
                candidates.len()
            ));
            continue;
        }
        let declaration = &candidates[0];
        if !declaration.derives_from_json_query_result {
            violations.push(format!(
                "{} declares JSONB type `{type_name}` without `FromJsonQueryResult`",
                declaration.source.display()
            ));
        }
        if !declaration.has_closed_shape {
            violations.push(format!(
                "{} declares fail-open JSONB type `{type_name}`; named structs require `deny_unknown_fields`, tuple newtypes require `transparent`, and data enums require an explicit tag",
                declaration.source.display()
            ));
        }
    }

    Ok(violations)
}

fn is_json_binary(attrs: &[Attribute]) -> Result<bool> {
    for attribute in attrs {
        if !attribute.path().is_ident("sea_orm") {
            continue;
        }
        let options = attribute
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .context("parse sea_orm field options")?;
        for option in options {
            let Meta::NameValue(option) = option else {
                continue;
            };
            if !option.path.is_ident("column_type") {
                continue;
            }
            if let Expr::Lit(literal) = option.value
                && let Lit::Str(value) = literal.lit
                && value.value() == "JsonBinary"
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn persistence_type_name(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if matches!(segment.ident.to_string().as_str(), "Option" | "Vec")
        && let PathArguments::AngleBracketed(arguments) = &segment.arguments
    {
        return arguments.args.iter().find_map(|argument| match argument {
            GenericArgument::Type(inner) => persistence_type_name(inner),
            _ => None,
        });
    }
    Some(segment.ident.to_string())
}

fn validate_config_secret_types(sources: &[(PathBuf, File)]) -> Result<Vec<String>> {
    const SECRET_FIELDS: &[&str] = &[
        "api_key",
        "api_secret",
        "authorization",
        "bot_token",
        "password",
        "previous_signing_keys",
        "private_key",
        "signing_key",
    ];
    let config_source = Path::new("crates/quant-pivot-models/src/config");
    let mut violations = Vec::new();
    let mut secret_text_found = false;
    let mut secret_debug_found = false;

    for (source, file) in sources
        .iter()
        .filter(|(source, _)| source.starts_with(config_source))
    {
        for item in &file.items {
            if let Item::Struct(item) = item {
                if item.ident == "SecretText" {
                    secret_text_found = true;
                    let zeroizing_string = matches!(&item.fields, Fields::Unnamed(fields)
                        if fields.unnamed.len() == 1
                            && matches!(fields.unnamed[0].vis, Visibility::Inherited)
                            && fields.unnamed[0]
                                .ty
                                .to_token_stream()
                                .to_string()
                                .replace(' ', "")
                                == "Zeroizing<String>");
                    if !zeroizing_string {
                        violations.push(format!(
                            "{} must store `SecretText` as one private `Zeroizing<String>` field",
                            source.display()
                        ));
                    }
                    if derives(&item.attrs, "Serialize")? {
                        violations.push(format!(
                            "{} must not derive `Serialize` for `SecretText`",
                            source.display()
                        ));
                    }
                }
                let Fields::Named(fields) = &item.fields else {
                    continue;
                };
                for field in &fields.named {
                    let Some(field_name) = field.ident.as_ref().map(ToString::to_string) else {
                        continue;
                    };
                    let expected = if field_name == "rpc_endpoint" {
                        Some("PolygonRpcEndpoint")
                    } else if SECRET_FIELDS.contains(&field_name.as_str())
                        || (item.ident == "WebhookChannelConfig" && field_name == "url")
                    {
                        Some("SecretText")
                    } else {
                        None
                    };
                    let Some(expected) = expected else { continue };
                    let actual = persistence_type_name(&field.ty)
                        .unwrap_or_else(|| "<unsupported>".to_owned());
                    if actual != expected {
                        violations.push(format!(
                            "{}::{}::{field_name} must use `{expected}`, found `{actual}`",
                            source.display(),
                            item.ident
                        ));
                    }
                }
            }
            if let Item::Impl(item) = item
                && persistence_type_name(&item.self_ty).as_deref() == Some("SecretText")
                && let Some((_, trait_path, _)) = &item.trait_
                && let Some(trait_name) = trait_path.segments.last()
            {
                match trait_name.ident.to_string().as_str() {
                    "Debug" => secret_debug_found = true,
                    "Display" | "Serialize" => violations.push(format!(
                        "{} implements `{}` for `SecretText`; plaintext secrets must never enter formatting or serialization",
                        source.display(), trait_name.ident
                    )),
                    _ => {}
                }
            }
        }
    }

    if !secret_text_found {
        violations.push("quant-pivot-models config has no canonical `SecretText` type".to_owned());
    }
    if !secret_debug_found {
        violations
            .push("`SecretText` must own an explicit redacting `Debug` implementation".to_owned());
    }
    Ok(violations)
}

fn serde_document_options(attrs: &[Attribute]) -> Result<SerdeDocumentOptions> {
    let mut options = SerdeDocumentOptions::default();
    for attribute in attrs {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        let metas = attribute
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .context("parse serde document options")?;
        for meta in metas {
            match meta {
                Meta::Path(path) if path.is_ident("deny_unknown_fields") => {
                    options.deny_unknown_fields = true;
                }
                Meta::Path(path) if path.is_ident("transparent") => {
                    options.transparent = true;
                }
                Meta::NameValue(value) if value.path.is_ident("tag") => {
                    options.tagged = true;
                }
                _ => {}
            }
        }
    }
    Ok(options)
}

fn derives(attrs: &[Attribute], derive_name: &str) -> Result<bool> {
    for attribute in attrs {
        if !attribute.path().is_ident("derive") {
            continue;
        }
        let paths = attribute
            .parse_args_with(Punctuated::<SynPath, Token![,]>::parse_terminated)
            .context("parse derive paths")?;
        if paths.iter().any(|path| {
            path.segments
                .last()
                .is_some_and(|segment| segment.ident == derive_name)
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_source_style(file: &File, path: &Path) -> Vec<String> {
    let mut violations = validate_module_imports(&file.items, path);
    let mut visitor = BodyStyleVisitor {
        path,
        block_depth: 0,
        generated_sea_orm_entity: file.attrs.iter().any(|attribute| {
            attribute
                .meta
                .to_token_stream()
                .to_string()
                .contains("@generated by sea-orm-codegen")
        }),
        violations: Vec::new(),
    };
    visitor.visit_file(file);
    violations.extend(visitor.violations);
    violations
}

fn validate_module_imports(items: &[Item], path: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    let mut substantive_item_seen = false;
    let mut imports_by_root = BTreeMap::<(String, String, String), usize>::new();

    for item in items {
        if let Item::Mod(item_mod) = item
            && let Some((_, nested_items)) = &item_mod.content
        {
            violations.extend(validate_module_imports(nested_items, path));
        }

        let Item::Use(item_use) = item else {
            if !matches!(
                item,
                Item::Mod(_)
                    | Item::ExternCrate(_)
                    | Item::Macro(syn::ItemMacro { ident: Some(_), .. })
            ) {
                substantive_item_seen = true;
            }
            continue;
        };

        if substantive_item_seen {
            violations.push(format!(
                "{} contains a `use` after a substantive module item; imports must stay in the module preamble",
                path.display()
            ));
        }

        let mut roots = Vec::new();
        use_tree_roots(&item_use.tree, &mut roots);
        roots.sort();
        roots.dedup();
        if roots.len() != 1 || matches!(item_use.tree, UseTree::Group(_)) {
            violations.push(format!(
                "{} contains a rootless or multi-root import; every `use` must start from one explicit root",
                path.display()
            ));
            continue;
        }

        let mut renames = Vec::new();
        collect_use_renames(&item_use.tree, &mut Vec::new(), &mut renames);
        for (import_path, alias) in renames {
            if alias_repeats_internal_path(&import_path, &alias) {
                violations.push(format!(
                    "{} uses mechanical import alias `{alias}`; name the conflicting role or domain concept instead of encoding the full module path",
                    path.display()
                ));
            }
        }

        let visibility = item_use.vis.to_token_stream().to_string();
        let attributes = item_use
            .attrs
            .iter()
            .map(|attribute| attribute.meta.to_token_stream().to_string())
            .collect::<Vec<_>>()
            .join("|");
        let key = (visibility, attributes, roots[0].clone());
        let count = imports_by_root.entry(key).or_default();
        *count += 1;
        if *count > 1 {
            violations.push(format!(
                "{} splits the `{}` import root with identical visibility/attributes; merge it into one tree-shaped `use`",
                path.display(), roots[0]
            ));
        }
    }

    violations
}

fn collect_use_renames(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    output: &mut Vec<(Vec<String>, String)>,
) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use_renames(&path.tree, prefix, output);
            prefix.pop();
        }
        UseTree::Rename(rename) => {
            let mut import_path = prefix.clone();
            import_path.push(rename.ident.to_string());
            output.push((import_path, rename.rename.to_string()));
        }
        UseTree::Group(group) => {
            for nested in &group.items {
                collect_use_renames(nested, prefix, output);
            }
        }
        UseTree::Name(_) | UseTree::Glob(_) => {}
    }
}

fn alias_repeats_internal_path(import_path: &[String], alias: &str) -> bool {
    let Some(root) = import_path.first() else {
        return false;
    };
    let internal_root =
        matches!(root.as_str(), "crate" | "self" | "super") || root.starts_with("quant_pivot_");
    internal_root && alias.starts_with(&pascal_identifier(root))
}

fn pascal_identifier(identifier: &str) -> String {
    identifier
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(char::to_uppercase)
                .into_iter()
                .flatten()
                .chain(chars)
                .collect::<String>()
        })
        .collect()
}

struct BodyStyleVisitor<'a> {
    path: &'a Path,
    block_depth: usize,
    generated_sea_orm_entity: bool,
    violations: Vec<String>,
}

impl<'ast> Visit<'ast> for BodyStyleVisitor<'_> {
    fn visit_attribute(&mut self, _attribute: &'ast Attribute) {
        // Attribute and derive macro paths are invocation syntax, not body references. Importing
        // them can change macro resolution and breaks canonical framework forms such as
        // `#[sea_orm::model]`, `#[tokio::test]`, and `#[serde(with = "...")]`.
    }

    fn visit_block(&mut self, block: &'ast Block) {
        self.block_depth += 1;
        visit::visit_block(self, block);
        self.block_depth -= 1;
    }

    fn visit_item_mod(&mut self, item_mod: &'ast ItemMod) {
        let previous_depth = self.block_depth;
        self.block_depth = 0;
        visit::visit_item_mod(self, item_mod);
        self.block_depth = previous_depth;
    }

    fn visit_item_use(&mut self, item_use: &'ast ItemUse) {
        if self.block_depth > 0 {
            self.violations.push(format!(
                "{} contains a block-local `use`; import it at the owning module preamble",
                self.path.display()
            ));
        }
        // `UseTree` paths are intentionally excluded from body path-depth checks.
        let _ = item_use;
    }

    fn visit_path(&mut self, path: &'ast SynPath) {
        let segments = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        if self
            .path
            .starts_with("crates/quant-pivot-repository/src/postgres")
            && segments
                .last()
                .is_some_and(|segment| segment == "insert_many")
            && !matches!(
                self.path.to_str(),
                Some(
                    "crates/quant-pivot-repository/src/postgres/write.rs"
                        | "crates/quant-pivot-repository/src/postgres/rbac/casbin/adapter.rs"
                )
            )
        {
            self.violations.push(format!(
                "{} bypasses the bind-budgeted batch-write boundary with direct `insert_many`; use postgres::write helpers",
                self.path.display()
            ));
        }
        if !self.generated_sea_orm_entity && path_is_noncanonical(&segments) {
            let rendered = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::");
            self.violations.push(format!(
                "{} uses non-canonical path `{rendered}` outside an import; import types/constants and keep non-Tokio item paths to at most one `::`",
                self.path.display()
            ));
        }
        visit::visit_path(self, path);
    }
}

fn collect_rust_sources(directory: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read source directory {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", directory.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, output)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
    Ok(())
}

fn validate_public_exports(items: &[Item], path: &Path, crate_root: bool) -> Vec<String> {
    let mut violations = Vec::new();
    for item in items {
        if let Item::Mod(item_mod) = item
            && let Some((_, nested_items)) = &item_mod.content
        {
            violations.extend(validate_public_exports(nested_items, path, false));
        }
        let Item::Use(item_use) = item else { continue };
        if !matches!(item_use.vis, Visibility::Public(_)) {
            continue;
        }
        if crate_root {
            violations.push(format!(
                "{} contains a crate-root public re-export; callers must use the canonical bounded-context module",
                path.display()
            ));
        }
        if use_tree_contains_glob(&item_use.tree) {
            violations.push(format!(
                "{} contains a public glob re-export",
                path.display()
            ));
        }
        let mut roots = Vec::new();
        use_tree_roots(&item_use.tree, &mut roots);
        for root in roots {
            if matches!(root.as_str(), "crate" | "self" | "super")
                || root.starts_with("quant_pivot_")
            {
                violations.push(format!(
                    "{} forwards public API through `{root}`; bounded-context facades may export only their directly owned child modules",
                    path.display()
                ));
            }
        }
    }
    violations
}

fn use_tree_contains_glob(tree: &UseTree) -> bool {
    match tree {
        UseTree::Glob(_) => true,
        UseTree::Group(group) => group.items.iter().any(use_tree_contains_glob),
        UseTree::Path(path) => use_tree_contains_glob(&path.tree),
        UseTree::Name(_) | UseTree::Rename(_) => false,
    }
}

fn use_tree_roots(tree: &UseTree, output: &mut Vec<String>) {
    match tree {
        UseTree::Path(path) => output.push(path.ident.to_string()),
        UseTree::Name(name) => output.push(name.ident.to_string()),
        UseTree::Rename(rename) => output.push(rename.ident.to_string()),
        UseTree::Group(group) => {
            for tree in &group.items {
                use_tree_roots(tree, output);
            }
        }
        UseTree::Glob(_) => {}
    }
}

fn validate(metadata: &CargoMetadata) -> Vec<String> {
    let workspace_packages = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| (package.name.as_str(), package))
        .collect::<BTreeMap<_, _>>();
    let mut violations = Vec::new();

    for package in workspace_packages.values() {
        if package.name.starts_with("quant-pivot-")
            && !KNOWN_WORKSPACE_PACKAGES.contains(&package.name.as_str())
        {
            violations.push(format!(
                "{} has no declared architecture role",
                package.name
            ));
        }
        for dependency in package
            .dependencies
            .iter()
            .filter(|dependency| dependency.kind.is_none())
        {
            validate_test_boundary(package, dependency, &mut violations);
            validate_entity_visibility(package, dependency, &mut violations);
            if dependency.path.is_some()
                && dependency.name.starts_with("quant-pivot-")
                && !allowed_workspace_dependencies(&package.name)
                    .contains(&dependency.name.as_str())
            {
                violations.push(format!(
                    "{} has forbidden normal dependency on {}",
                    package.name, dependency.name
                ));
            }
        }
    }

    violations
}

fn validate_workspace_dependency_inventory(metadata: &CargoMetadata) -> Result<Vec<String>> {
    let manifest_path = metadata.workspace_root.join("Cargo.toml");
    let manifest = fs::read_to_string(&manifest_path)
        .with_context(|| format!("read {}", manifest_path.display()))?;
    let manifest: Value =
        toml::from_str(&manifest).with_context(|| format!("parse {}", manifest_path.display()))?;
    let declared = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Value::as_table)
        .context("workspace.dependencies must be a TOML table")?;
    let used = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .flat_map(|package| package.dependencies.iter())
        .map(|dependency| dependency.rename.as_deref().unwrap_or(&dependency.name))
        .collect::<BTreeSet<_>>();

    Ok(declared
        .keys()
        .filter(|dependency| !used.contains(dependency.as_str()))
        .map(|dependency| {
            format!("workspace dependency {dependency} is not inherited by any member manifest")
        })
        .collect())
}

fn validate_test_boundary(
    package: &CargoPackage,
    dependency: &CargoDependency,
    violations: &mut Vec<String>,
) {
    if !is_production_package(&package.name) {
        return;
    }
    if TEST_ONLY_EXTERNAL_DEPENDENCIES.contains(&dependency.name.as_str()) {
        violations.push(format!(
            "{} exposes test-only dependency {} in its production dependency graph",
            package.name, dependency.name
        ));
    }
}

fn validate_entity_visibility(
    package: &CargoPackage,
    dependency: &CargoDependency,
    violations: &mut Vec<String>,
) {
    if dependency.name != "quant-pivot-models"
        || !dependency
            .features
            .iter()
            .any(|feature| feature == "persistence-entities")
    {
        return;
    }
    if !matches!(
        package.name.as_str(),
        "quant-pivot-repository" | "quant-pivot-storage"
    ) {
        violations.push(format!(
            "{} enables quant-pivot-models/persistence-entities outside the persistence boundary",
            package.name
        ));
    }
}

fn is_production_package(name: &str) -> bool {
    !matches!(
        name,
        "quant-pivot-bench" | "quant-pivot-system-tests" | "quant-pivot-xtask"
    )
}

fn allowed_workspace_dependencies(package: &str) -> &'static [&'static str] {
    match package {
        "quant-pivot-models" => &["quant-pivot-error", "quant-pivot-macros"],
        "quant-pivot-api" | "quant-pivot-research" => &["quant-pivot-error", "quant-pivot-models"],
        "quant-pivot-migration" | "quant-pivot-storage" => {
            &["quant-pivot-error", "quant-pivot-models"]
        }
        "quant-pivot-repository" => &[
            "quant-pivot-error",
            "quant-pivot-models",
            "quant-pivot-storage",
        ],
        "quant-pivot-web" => &[
            "quant-pivot-error",
            "quant-pivot-migration",
            "quant-pivot-models",
            "quant-pivot-repository",
            "quant-pivot-storage",
        ],
        "quant-pivot-core" => &[
            "quant-pivot-api",
            "quant-pivot-error",
            "quant-pivot-migration",
            "quant-pivot-models",
            "quant-pivot-repository",
            "quant-pivot-research",
            "quant-pivot-storage",
            "quant-pivot-web",
        ],
        "quant-pivot-bin" => &["quant-pivot-core", "quant-pivot-models"],
        "quant-pivot-bench" => &[
            "quant-pivot-core",
            "quant-pivot-models",
            "quant-pivot-research",
        ],
        "quant-pivot-system-tests" => &[
            "quant-pivot-api",
            "quant-pivot-core",
            "quant-pivot-error",
            "quant-pivot-migration",
            "quant-pivot-models",
            "quant-pivot-repository",
            "quant-pivot-research",
            "quant-pivot-storage",
        ],
        "quant-pivot-xtask" => &[
            "quant-pivot-api",
            "quant-pivot-error",
            "quant-pivot-migration",
            "quant-pivot-models",
            "quant-pivot-repository",
            "quant-pivot-storage",
            "quant-pivot-system-tests",
        ],
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
    };

    use super::{
        CargoDependency, CargoMetadata, CargoPackage, validate, validate_config_secret_types,
        validate_persistence_documents, validate_public_exports, validate_source_style,
    };

    fn dependency(name: &str, kind: Option<&str>, features: &[&str]) -> CargoDependency {
        CargoDependency {
            name: name.to_owned(),
            rename: None,
            kind: kind.map(str::to_owned),
            path: Some(PathBuf::from("/workspace/crates").join(name)),
            features: features
                .iter()
                .map(|feature| (*feature).to_owned())
                .collect(),
        }
    }

    fn metadata(package: CargoPackage) -> CargoMetadata {
        CargoMetadata {
            workspace_members: BTreeSet::from([package.id.clone()]),
            packages: vec![package],
            workspace_root: PathBuf::from("/workspace"),
        }
    }

    #[test]
    fn rejects_upward_production_dependency() {
        let metadata = metadata(CargoPackage {
            id: "models-id".to_owned(),
            name: "quant-pivot-models".to_owned(),
            dependencies: vec![dependency("quant-pivot-core", None, &[])],
        });

        assert_eq!(
            validate(&metadata),
            ["quant-pivot-models has forbidden normal dependency on quant-pivot-core"]
        );
    }

    #[test]
    fn ignores_dev_only_dependency_direction() {
        let metadata = metadata(CargoPackage {
            id: "models-id".to_owned(),
            name: "quant-pivot-models".to_owned(),
            dependencies: vec![dependency("quant-pivot-core", Some("dev"), &[])],
        });

        assert!(validate(&metadata).is_empty());
    }

    #[test]
    fn restricts_persistence_entity_visibility() {
        let metadata = metadata(CargoPackage {
            id: "web-id".to_owned(),
            name: "quant-pivot-web".to_owned(),
            dependencies: vec![dependency(
                "quant-pivot-models",
                None,
                &["persistence-entities"],
            )],
        });

        assert_eq!(
            validate(&metadata),
            [
                "quant-pivot-web enables quant-pivot-models/persistence-entities outside the persistence boundary"
            ]
        );
    }

    #[test]
    fn permits_explicit_bounded_context_exports() {
        let source =
            syn::parse_file("pub use child::{OwnedType, owned_function};").expect("parse fixture");

        assert!(
            validate_public_exports(&source.items, Path::new("src/child/mod.rs"), false).is_empty()
        );
    }

    #[test]
    fn rejects_forwarding_glob_and_crate_root_exports() {
        let source = syn::parse_file(
            "pub use quant_pivot_models::Thing; pub use crate::owned::Other; pub use child::*;",
        )
        .expect("parse fixture");

        let violations = validate_public_exports(&source.items, Path::new("src/lib.rs"), true);

        assert_eq!(violations.len(), 6);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("quant_pivot_models"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("public glob"))
        );
    }

    #[test]
    fn source_style_accepts_tree_imports_and_one_qualifier_body_paths() {
        let source = syn::parse_file(
            r"
            use std::{cmp::Ordering, panic::{self, AssertUnwindSafe}};
            use anyhow::Error as AnyhowError;
            use quant_pivot_models::enums::Side;

            fn run<D>() -> Result<(), AnyhowError> {
                let _ = Ordering::Equal;
                let _ = panic::catch_unwind(AssertUnwindSafe(|| Side::Buy));
                let _ = tokio::task::spawn_blocking(run);
                let _ = tokio::time::timeout;
                let _ = i64::MAX;
                let _ = D::Error::custom;
                Ok(())
            }
            ",
        )
        .expect("parse fixture");

        let violations = validate_source_style(&source, Path::new("src/example.rs"));
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn source_style_accepts_framework_attribute_and_sea_orm_relation_paths() {
        let source = syn::parse_file(
            r"
            use super::market;
            use sea_orm::entity::prelude::*;

            #[sea_orm::model]
            #[derive(Clone, sea_orm::entity::prelude::DeriveEntityModel)]
            struct Model {
                market: BelongsTo<market::Entity>,
            }
            ",
        )
        .expect("parse fixture");

        assert!(validate_source_style(&source, Path::new("src/entity.rs")).is_empty());
    }

    #[test]
    fn source_style_preserves_codegen_paths_but_still_rejects_local_imports() {
        let generated = syn::parse_file(
            r"
            //! `SeaORM` Entity, @generated by sea-orm-codegen 2.0.0
            use sea_orm::entity::prelude::*;

            #[sea_orm::model]
            struct Model {
                relation: BelongsTo<super::market::Entity>,
                status: super::sea_orm_active_enums::QpStatus,
            }
            ",
        )
        .expect("parse fixture");
        assert!(validate_source_style(&generated, Path::new("src/generated.rs")).is_empty());

        let generated_with_local_use = syn::parse_file(
            r"
            //! `SeaORM` Entity, @generated by sea-orm-codegen 2.0.0
            fn run() { use std::cmp::Ordering; let _ = Ordering::Equal; }
            ",
        )
        .expect("parse fixture");
        let violations =
            validate_source_style(&generated_with_local_use, Path::new("src/generated.rs"));
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("block-local `use`"));
    }

    #[test]
    fn source_style_rejects_split_same_root_imports() {
        let source =
            syn::parse_file("use std::cmp::Ordering; use std::panic::{self, AssertUnwindSafe};")
                .expect("parse fixture");

        let violations = validate_source_style(&source, Path::new("src/example.rs"));

        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("splits the `std` import root"));
    }

    #[test]
    fn source_style_rejects_block_imports_and_deep_paths() {
        let source = syn::parse_file(
            r"
            fn run() {
                use std::cmp::Ordering;
                let _ = Ordering::Equal;
                let _ = tokio_util::sync::CancellationToken::new();
                let _ = quant_pivot_models::enums::Side::Buy;
            }
            ",
        )
        .expect("parse fixture");

        let violations = validate_source_style(&source, Path::new("src/example.rs"));

        assert_eq!(violations.len(), 3);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("block-local `use`"))
        );
        assert_eq!(
            violations
                .iter()
                .filter(|violation| violation.contains("uses non-canonical path"))
                .count(),
            2
        );
    }

    #[test]
    fn source_style_rejects_imports_after_module_items() {
        let source = syn::parse_file("const LIMIT: usize = 1; use std::cmp::Ordering;")
            .expect("parse fixture");

        let violations = validate_source_style(&source, Path::new("src/example.rs"));

        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("module preamble"));
    }

    #[test]
    fn source_style_accepts_macro_rules_reexport_in_preamble() {
        let source =
            syn::parse_file("macro_rules! stable_name { () => {}; } pub(crate) use stable_name;")
                .expect("parse fixture");

        assert!(validate_source_style(&source, Path::new("src/naming.rs")).is_empty());
    }

    #[test]
    fn source_style_rejects_mechanical_internal_aliases() {
        let source = syn::parse_file(
            "use quant_pivot_models::entities::market::Entity as QuantPivotModelsEntitiesMarketEntity;",
        )
        .expect("parse fixture");

        let violations = validate_source_style(&source, Path::new("src/repository.rs"));
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("mechanical import alias"));
    }

    #[test]
    fn source_style_accepts_semantic_conflict_aliases() {
        let source = syn::parse_file(
            r"
            use anyhow::Error as AnyhowError;
            use crate::entities::market::Entity as MarketEntity;
            use std::time::Duration as StdDuration;
            ",
        )
        .expect("parse fixture");

        assert!(validate_source_style(&source, Path::new("src/repository.rs")).is_empty());
    }

    #[test]
    fn persistence_documents_require_typed_fail_closed_shapes() {
        let entity = syn::parse_file(
            r#"
            #[sea_orm::model]
            struct Model {
                #[sea_orm(column_type = "JsonBinary")]
                typed: ClosedDocument,
                #[sea_orm(column_type = "JsonBinary", nullable)]
                raw: Option<Json>,
            }
            "#,
        )
        .expect("parse entity fixture");
        let document = syn::parse_file(
            r"
            #[derive(FromJsonQueryResult)]
            #[serde(deny_unknown_fields)]
            struct ClosedDocument { value: String }
            ",
        )
        .expect("parse document fixture");

        let violations = validate_persistence_documents(&[
            (
                PathBuf::from("crates/quant-pivot-models/src/entities/example.rs"),
                entity,
            ),
            (
                PathBuf::from("crates/quant-pivot-models/src/types/example.rs"),
                document,
            ),
        ])
        .expect("validate fixture");

        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("exposes raw `Json`"));
    }

    #[test]
    fn persistence_documents_reject_open_or_non_decodable_types() {
        let entity = syn::parse_file(
            r#"
            #[sea_orm::model]
            struct Model {
                #[sea_orm(column_type = "JsonBinary")]
                payload: OpenDocument,
            }
            "#,
        )
        .expect("parse entity fixture");
        let document = syn::parse_file("struct OpenDocument { value: String }")
            .expect("parse document fixture");

        let violations = validate_persistence_documents(&[
            (
                PathBuf::from("crates/quant-pivot-models/src/entities/example.rs"),
                entity,
            ),
            (
                PathBuf::from("crates/quant-pivot-models/src/types/example.rs"),
                document,
            ),
        ])
        .expect("validate fixture");

        assert_eq!(violations.len(), 2);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("without `FromJsonQueryResult`"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("fail-open JSONB"))
        );
    }

    #[test]
    fn repository_direct_batch_writes_are_restricted_to_semantic_owners() {
        let source = syn::parse_file("async fn write() { Entity::insert_many(rows).await; }")
            .expect("parse repository fixture");

        let violation = validate_source_style(
            &source,
            Path::new("crates/quant-pivot-repository/src/postgres/quant/example.rs"),
        );
        assert_eq!(violation.len(), 1);
        assert!(violation[0].contains("bind-budgeted batch-write boundary"));

        assert!(
            validate_source_style(
                &source,
                Path::new("crates/quant-pivot-repository/src/postgres/write.rs"),
            )
            .is_empty()
        );
        assert!(
            validate_source_style(
                &source,
                Path::new("crates/quant-pivot-repository/src/postgres/rbac/casbin/adapter.rs",),
            )
            .is_empty()
        );
    }

    #[test]
    fn config_secrets_require_the_non_serializable_zeroizing_boundary() {
        let valid = syn::parse_file(
            r"
            struct SecretText(Zeroizing<String>);
            impl Debug for SecretText {}
            struct Config {
                password: SecretText,
                private_key: Option<SecretText>,
                previous_signing_keys: Vec<SecretText>,
                rpc_endpoint: PolygonRpcEndpoint,
                api_key_address: Option<String>,
            }
            ",
        )
        .expect("parse valid secret fixture");
        assert!(
            validate_config_secret_types(&[(
                PathBuf::from("crates/quant-pivot-models/src/config/secret.rs"),
                valid,
            )])
            .expect("validate secret fixture")
            .is_empty()
        );

        let invalid = syn::parse_file(
            r"
            #[derive(Serialize)]
            struct SecretText(pub String);
            impl Display for SecretText {}
            struct Config { api_secret: String, rpc_endpoint: String }
            struct WebhookChannelConfig { url: String }
            ",
        )
        .expect("parse invalid secret fixture");
        let violations = validate_config_secret_types(&[(
            PathBuf::from("crates/quant-pivot-models/src/config/secret.rs"),
            invalid,
        )])
        .expect("validate secret fixture");
        assert_eq!(violations.len(), 7);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("api_secret"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("must not derive `Serialize`"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("implements `Display`"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("WebhookChannelConfig::url"))
        );
    }
}
