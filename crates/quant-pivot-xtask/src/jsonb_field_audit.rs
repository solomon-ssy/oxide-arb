//! AST-backed audit of every runtime `PostgreSQL` `JSONB` persistence field.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
use syn::{Attribute, Fields, Item, LitStr};

use crate::persistence_field_audit::{
    file_stem, named_fields, parse_rust, persistence_dto_entity_type, persistence_target,
    rust_files, rust_type, workspace_root,
};

const ENTITY_SOURCE: &str = "crates/quant-pivot-models/src/entities";
const SNAPSHOT_SOURCE: &str = "crates/quant-pivot-migration/src/snapshots/v1";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionRegistry {
    schema_version: u16,
    group: Vec<DecisionGroup>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionGroup {
    disposition: Disposition,
    owner: String,
    access: String,
    lifecycle: String,
    rationale: String,
    members: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum Disposition {
    #[serde(rename = "typed_document")]
    Typed,
    #[serde(rename = "external_document")]
    External,
    #[serde(rename = "controlled_open_document")]
    ControlledOpen,
}

#[derive(Debug)]
struct FieldDecision {
    expected_type: String,
    disposition: Disposition,
}

#[derive(Debug)]
enum DocumentShape {
    NamedStruct,
    TransparentNewtype,
    TaggedEnum,
    UnitEnum,
}

#[derive(Debug)]
struct DocumentDeclaration {
    derives_from_json_query_result: bool,
    shape: DocumentShape,
}

pub fn run(registry_path: &Path, print_candidates: bool) -> Result<()> {
    let root = workspace_root()?;
    let runtime = jsonb_fields(&root.join(ENTITY_SOURCE), false)?;
    if print_candidates {
        for (field, rust_type) in &runtime {
            println!("{field}\t{rust_type}");
        }
        return Ok(());
    }

    let source = fs::read_to_string(registry_path)
        .with_context(|| format!("read JSONB decision registry {}", registry_path.display()))?;
    let registry: DecisionRegistry = toml::from_str(&source)
        .with_context(|| format!("decode JSONB decision registry {}", registry_path.display()))?;
    if registry.schema_version != 1 {
        bail!("JSONB decision registry schema_version must be 1");
    }
    let decisions = decisions(&registry)?;
    require_same_fields("runtime JSONB", &runtime, "decision", &decisions)?;

    for (key, decision) in &decisions {
        let actual = runtime
            .get(key)
            .with_context(|| format!("stale JSONB decision points to missing field `{key}`"))?;
        if actual != &decision.expected_type {
            bail!(
                "JSONB decision `{key}` expects `{}`, entity declares `{actual}`",
                decision.expected_type
            );
        }
        validate_disposition(key, actual, decision.disposition)?;
    }
    audit_document_declarations(&root, &decisions)?;
    audit_persistence_dtos(&root, &runtime)?;

    let snapshot = jsonb_fields(&root.join(SNAPSHOT_SOURCE), true)?;
    require_same_fields("runtime JSONB", &runtime, "fresh-boot v1 JSONB", &snapshot)?;
    for (key, active_type) in &runtime {
        let snapshot_type = snapshot
            .get(key)
            .with_context(|| format!("fresh-boot v1 is missing JSONB field `{key}`"))?;
        let expected = if active_type.starts_with("Option<") {
            "Option<Json>"
        } else {
            "Json"
        };
        if snapshot_type != expected {
            bail!(
                "fresh-boot v1 JSONB `{key}` uses `{snapshot_type}`, expected `{expected}` for runtime `{active_type}`"
            );
        }
    }

    let external = decisions
        .values()
        .filter(|decision| matches!(decision.disposition, Disposition::External))
        .count();
    let controlled_open = decisions
        .values()
        .filter(|decision| matches!(decision.disposition, Disposition::ControlledOpen))
        .count();
    println!(
        "JSONB field audit passed: {} fields, {} external boundaries, {} controlled-open boundaries",
        runtime.len(),
        external,
        controlled_open
    );
    Ok(())
}

fn decisions(registry: &DecisionRegistry) -> Result<BTreeMap<String, FieldDecision>> {
    let mut output = BTreeMap::new();
    for group in &registry.group {
        if group.owner.trim().is_empty()
            || group.access.trim().is_empty()
            || group.lifecycle.trim().is_empty()
            || group.rationale.trim().is_empty()
            || group.members.is_empty()
        {
            bail!(
                "every JSONB decision group requires owner, access, lifecycle, rationale, and members"
            );
        }
        for member in &group.members {
            let (field, expected_type) = member.split_once('=').with_context(|| {
                format!("JSONB decision member must be `<table>.<field>=<RustType>`: {member}")
            })?;
            if field.trim().is_empty() || expected_type.trim().is_empty() {
                bail!("JSONB decision member contains an empty field or type: {member}");
            }
            if output
                .insert(
                    field.to_owned(),
                    FieldDecision {
                        expected_type: expected_type.to_owned(),
                        disposition: group.disposition,
                    },
                )
                .is_some()
            {
                bail!("duplicate JSONB field decision for `{field}`");
            }
        }
    }
    Ok(output)
}

fn validate_disposition(key: &str, rust_type: &str, disposition: Disposition) -> Result<()> {
    match disposition {
        Disposition::Typed
            if rust_type.contains("ExternalJsonDocument")
                || rust_type.contains("OperationDetailDocument")
                || rust_type == "Json"
                || rust_type == "Option<Json>"
                || rust_type.contains("Value") =>
        {
            bail!("typed JSONB `{key}` exposes non-canonical type `{rust_type}`")
        }
        Disposition::External
            if !matches!(
                rust_type,
                "ExternalJsonDocument" | "Option<ExternalJsonDocument>"
            ) =>
        {
            bail!("external JSONB `{key}` must use ExternalJsonDocument, got `{rust_type}`")
        }
        Disposition::ControlledOpen if rust_type != "OperationDetailDocument" => {
            bail!(
                "controlled-open JSONB `{key}` must use OperationDetailDocument, got `{rust_type}`"
            )
        }
        _ => Ok(()),
    }
}

fn audit_document_declarations(
    root: &Path,
    decisions: &BTreeMap<String, FieldDecision>,
) -> Result<()> {
    let required_types = decisions
        .values()
        .filter(|decision| matches!(decision.disposition, Disposition::Typed))
        .map(|decision| {
            decision
                .expected_type
                .strip_prefix("Option<")
                .and_then(|inner| inner.strip_suffix('>'))
                .unwrap_or(&decision.expected_type)
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    let mut declarations = BTreeMap::new();
    for path in rust_files(&root.join("crates/quant-pivot-models/src"))? {
        let parsed = parse_rust(&path)?;
        for item in parsed.items {
            let declaration = match item {
                Item::Struct(item) => {
                    let options = serde_options(&item.attrs)?;
                    let shape = match item.fields {
                        Fields::Named(_) if options.deny_unknown_fields => {
                            DocumentShape::NamedStruct
                        }
                        Fields::Unnamed(_) if options.transparent => {
                            DocumentShape::TransparentNewtype
                        }
                        Fields::Named(_) | Fields::Unnamed(_) | Fields::Unit => continue,
                    };
                    (
                        item.ident.to_string(),
                        DocumentDeclaration {
                            derives_from_json_query_result: derives(
                                &item.attrs,
                                "FromJsonQueryResult",
                            )?,
                            shape,
                        },
                    )
                }
                Item::Enum(item) => {
                    let options = serde_options(&item.attrs)?;
                    let shape = if options.tagged {
                        DocumentShape::TaggedEnum
                    } else if item
                        .variants
                        .iter()
                        .all(|variant| matches!(variant.fields, Fields::Unit))
                    {
                        DocumentShape::UnitEnum
                    } else {
                        continue;
                    };
                    (
                        item.ident.to_string(),
                        DocumentDeclaration {
                            derives_from_json_query_result: derives(
                                &item.attrs,
                                "FromJsonQueryResult",
                            )?,
                            shape,
                        },
                    )
                }
                _ => continue,
            };
            if !required_types.contains(&declaration.0) {
                continue;
            }
            if declarations
                .insert(declaration.0.clone(), declaration.1)
                .is_some()
            {
                bail!(
                    "JSONB document type `{}` has ambiguous top-level declarations",
                    declaration.0
                );
            }
        }
    }

    let mut failures = Vec::new();
    for (key, decision) in decisions {
        if !matches!(decision.disposition, Disposition::Typed) {
            continue;
        }
        let type_name = decision
            .expected_type
            .strip_prefix("Option<")
            .and_then(|inner| inner.strip_suffix('>'))
            .unwrap_or(&decision.expected_type);
        let Some(declaration) = declarations.get(type_name) else {
            failures.push(format!(
                "`{key}` uses `{type_name}` without a closed top-level declaration"
            ));
            continue;
        };
        if !declaration.derives_from_json_query_result {
            failures.push(format!(
                "`{key}` declaration `{type_name}` does not derive FromJsonQueryResult"
            ));
        }
        match declaration.shape {
            DocumentShape::NamedStruct
            | DocumentShape::TransparentNewtype
            | DocumentShape::TaggedEnum
            | DocumentShape::UnitEnum => {}
        }
    }
    if !failures.is_empty() {
        bail!(
            "typed JSONB declarations are not fail-closed (named structs require serde deny_unknown_fields, tuple newtypes require transparent, and data enums require an explicit tag):\n- {}",
            failures.join("\n- ")
        );
    }
    Ok(())
}

fn audit_persistence_dtos(root: &Path, runtime: &BTreeMap<String, String>) -> Result<()> {
    let models_root = root.join("crates/quant-pivot-models/src");
    let entities_root = models_root.join("entities");
    for path in rust_files(&models_root)? {
        if path.starts_with(&entities_root) {
            continue;
        }
        let parsed = parse_rust(&path)?;
        for item in parsed.items {
            let Item::Struct(item) = item else {
                continue;
            };
            let Some(target) = persistence_target(&item.attrs)? else {
                continue;
            };
            for (field, rust_type) in named_fields(&item)? {
                let key = format!("{target}.{field}");
                let Some(entity_type) = runtime.get(&key) else {
                    continue;
                };
                let dto_type = persistence_dto_entity_type(&rust_type);
                if &dto_type != entity_type {
                    bail!(
                        "persistence DTO {}::{} exposes JSONB `{key}` as `{rust_type}` (storage `{dto_type}`), entity requires `{entity_type}`",
                        path.display(),
                        item.ident
                    );
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct SerdeOptions {
    deny_unknown_fields: bool,
    tagged: bool,
    transparent: bool,
}

fn serde_options(attrs: &[Attribute]) -> Result<SerdeOptions> {
    let mut options = SerdeOptions::default();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("deny_unknown_fields") {
                options.deny_unknown_fields = true;
            } else if meta.path.is_ident("transparent") {
                options.transparent = true;
            } else if meta.path.is_ident("tag") {
                options.tagged = true;
                let value = meta.value()?;
                let _: syn::Expr = value.parse()?;
            } else if meta.input.peek(syn::Token![=]) {
                let value = meta.value()?;
                let _: syn::Expr = value.parse()?;
            }
            Ok(())
        })?;
    }
    Ok(options)
}

fn derives(attrs: &[Attribute], derive_name: &str) -> Result<bool> {
    let mut found = false;
    for attr in attrs {
        if !attr.path().is_ident("derive") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == derive_name)
            {
                found = true;
            }
            Ok(())
        })?;
    }
    Ok(found)
}

fn require_same_fields<T, U>(
    left_name: &str,
    left: &BTreeMap<String, T>,
    right_name: &str,
    right: &BTreeMap<String, U>,
) -> Result<()> {
    let left_keys = left.keys().cloned().collect::<BTreeSet<_>>();
    let right_keys = right.keys().cloned().collect::<BTreeSet<_>>();
    let missing = left_keys
        .difference(&right_keys)
        .cloned()
        .collect::<Vec<_>>();
    let stale = right_keys
        .difference(&left_keys)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() || !stale.is_empty() {
        bail!(
            "{left_name}/{right_name} field drift: missing [{}], stale [{}]",
            missing.join(", "),
            stale.join(", ")
        );
    }
    Ok(())
}

fn jsonb_fields(root: &Path, snapshot: bool) -> Result<BTreeMap<String, String>> {
    let mut output = BTreeMap::new();
    for path in rust_files(root)? {
        if path.file_name().and_then(|name| name.to_str()) == Some("mod.rs") {
            continue;
        }
        let table = file_stem(&path)?;
        let parsed = parse_rust(&path)?;
        let Some(model) = parsed.items.iter().find_map(|item| match item {
            Item::Struct(item) if item.ident == "Model" => Some(item),
            _ => None,
        }) else {
            continue;
        };
        let Fields::Named(fields) = &model.fields else {
            bail!("entity Model in {} must use named fields", path.display());
        };
        for field in &fields.named {
            if !is_json_binary(&field.attrs)? {
                continue;
            }
            let field_name = field
                .ident
                .as_ref()
                .context("named entity field has no identifier")?;
            let key = format!("{table}.{field_name}");
            let field_type = rust_type(&field.ty)?;
            if output.insert(key.clone(), field_type).is_some() {
                bail!("duplicate JSONB field `{key}`");
            }
        }
    }
    if output.is_empty() {
        let source = if snapshot {
            "fresh-boot snapshot"
        } else {
            "runtime"
        };
        bail!("{source} contains no JSONB fields under {}", root.display());
    }
    Ok(output)
}

fn is_json_binary(attrs: &[Attribute]) -> Result<bool> {
    let mut json_binary = false;
    for attr in attrs {
        if !attr.path().is_ident("sea_orm") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("column_type") {
                let value = meta.value()?;
                let literal: LitStr = value.parse()?;
                json_binary = literal.value() == "JsonBinary";
            } else if meta.input.peek(syn::Token![=]) {
                let value = meta.value()?;
                let _: syn::Expr = value.parse()?;
            }
            Ok(())
        })?;
    }
    Ok(json_binary)
}
