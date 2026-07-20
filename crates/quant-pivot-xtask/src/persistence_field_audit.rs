//! AST-backed audit of semantic primitive fields at the persistence boundary.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use syn::{Attribute, Fields, Item, ItemStruct, LitStr, Type};

const MODELS_SOURCE: &str = "crates/quant-pivot-models/src";
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
    validation: String,
    rationale: String,
    members: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Disposition {
    ActiveEnum,
    ValidatedNewtype,
    TypedId,
    TaggedReference,
    FreeText,
    ExternalProtocolValue,
    OpaqueSnapshotLabel,
    Remove,
}

#[derive(Debug)]
struct FieldDecision {
    expected_type: String,
    disposition: Disposition,
}

#[derive(Debug)]
struct PersistenceDto {
    source: PathBuf,
    target_entity: String,
    item: ItemStruct,
}

pub fn run(registry_path: &Path, print_candidates: bool) -> Result<()> {
    let root = workspace_root()?;
    let models_root = root.join(MODELS_SOURCE);
    let entity_root = models_root.join("entities");
    let entity_files = rust_files(&entity_root)?;
    let mut entity_fields = BTreeMap::new();
    let mut candidates = BTreeMap::new();

    for path in entity_files {
        if path.file_name().and_then(|name| name.to_str()) == Some("mod.rs") {
            continue;
        }
        let table = file_stem(&path)?;
        let parsed = parse_rust(&path)?;
        let model = parsed.items.iter().find_map(|item| match item {
            Item::Struct(item) if item.ident == "Model" => Some(item),
            _ => None,
        });
        let Some(model) = model else {
            continue;
        };
        for (field, rust_type) in named_fields(model)? {
            let key = format!("{table}.{field}");
            entity_fields.insert(key.clone(), rust_type.clone());
            if primitive_type(&rust_type) {
                candidates.insert(key, rust_type);
            }
        }
    }

    if print_candidates {
        for (field, rust_type) in &candidates {
            println!("{field}\t{rust_type}");
        }
        return Ok(());
    }

    let registry_source = fs::read_to_string(registry_path).with_context(|| {
        format!(
            "read persistence decision registry {}",
            registry_path.display()
        )
    })?;
    let registry: DecisionRegistry = toml::from_str(&registry_source).with_context(|| {
        format!(
            "decode persistence decision registry {}",
            registry_path.display()
        )
    })?;
    if registry.schema_version != 1 {
        bail!("persistence decision registry schema_version must be 1");
    }

    let decisions = decisions(&registry)?;
    let candidate_keys = candidates.keys().cloned().collect::<BTreeSet<_>>();
    let decision_keys = decisions.keys().cloned().collect::<BTreeSet<_>>();
    let missing = candidate_keys
        .difference(&decision_keys)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "primitive persistence fields have no decision: {}",
            missing.join(", ")
        );
    }

    for (key, decision) in &decisions {
        let actual = entity_fields.get(key).with_context(|| {
            format!("stale persistence decision points to missing field `{key}`")
        })?;
        if actual != &decision.expected_type {
            bail!(
                "persistence decision `{key}` expects `{}`, entity declares `{actual}`",
                decision.expected_type
            );
        }
        validate_disposition(key, actual, decision.disposition)?;
    }

    audit_persistence_dtos(&models_root, &entity_fields, &decisions)?;
    audit_snapshots(&root.join(SNAPSHOT_SOURCE), &entity_fields, &decisions)?;

    println!(
        "Persistence field audit passed: {} primitive entity fields, {} explicit decisions",
        candidates.len(),
        decisions.len()
    );
    Ok(())
}

pub fn workspace_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("quant-pivot-xtask is not two levels below the workspace root")
}

fn decisions(registry: &DecisionRegistry) -> Result<BTreeMap<String, FieldDecision>> {
    let mut decisions = BTreeMap::new();
    for group in &registry.group {
        if group.owner.trim().is_empty()
            || group.validation.trim().is_empty()
            || group.rationale.trim().is_empty()
            || group.members.is_empty()
        {
            bail!(
                "every persistence decision group requires owner, validation, rationale, and members"
            );
        }
        for member in &group.members {
            let (field, expected_type) = member.split_once('=').with_context(|| {
                format!(
                    "persistence decision member must be `<table>.<field>=<RustType>`: {member}"
                )
            })?;
            if field.trim().is_empty() || expected_type.trim().is_empty() {
                bail!("persistence decision member contains an empty field or type: {member}");
            }
            if decisions
                .insert(
                    field.to_owned(),
                    FieldDecision {
                        expected_type: expected_type.to_owned(),
                        disposition: group.disposition,
                    },
                )
                .is_some()
            {
                bail!("duplicate persistence field decision for `{field}`");
            }
        }
    }
    Ok(decisions)
}

fn validate_disposition(key: &str, rust_type: &str, disposition: Disposition) -> Result<()> {
    let remains_primitive = primitive_type(rust_type);
    match disposition {
        Disposition::ActiveEnum | Disposition::ValidatedNewtype | Disposition::TypedId
            if remains_primitive =>
        {
            bail!("`{key}` is classified as {disposition:?} but remains primitive `{rust_type}`")
        }
        Disposition::TaggedReference
            if remains_primitive
                && !(key == "quant_research_job.result_ref" && rust_type == "Option<Uuid>") =>
        {
            bail!("`{key}` is not a supported discriminator-backed tagged reference")
        }
        Disposition::Remove => bail!("`{key}` is classified for removal but still exists"),
        Disposition::FreeText
        | Disposition::ExternalProtocolValue
        | Disposition::OpaqueSnapshotLabel
            if !remains_primitive =>
        {
            bail!(
                "`{key}` is classified as {disposition:?} but declares non-primitive `{rust_type}`"
            )
        }
        _ => Ok(()),
    }
}

fn audit_persistence_dtos(
    models_root: &Path,
    entity_fields: &BTreeMap<String, String>,
    decisions: &BTreeMap<String, FieldDecision>,
) -> Result<()> {
    for path in rust_files(models_root)? {
        if path.starts_with(models_root.join("entities")) {
            continue;
        }
        let parsed = parse_rust(&path)?;
        for item in parsed.items {
            let Item::Struct(item) = item else {
                continue;
            };
            let Some(target_entity) = persistence_target(&item.attrs)? else {
                continue;
            };
            let dto = PersistenceDto {
                source: path.clone(),
                target_entity,
                item,
            };
            for (field, rust_type) in named_fields(&dto.item)? {
                let key = format!("{}.{}", dto.target_entity, field);
                let dto_entity_type = persistence_dto_entity_type(&rust_type);
                let Some(entity_type) = entity_fields.get(&key) else {
                    bail!(
                        "persistence DTO {}::{} contains field `{key}` absent from its entity",
                        dto.source.display(),
                        dto.item.ident
                    );
                };
                if &dto_entity_type != entity_type
                    && (primitive_type(&dto_entity_type) || primitive_type(entity_type))
                {
                    bail!(
                        "persistence DTO {}::{} declares `{key}` as `{rust_type}` (storage `{dto_entity_type}`) but entity uses `{entity_type}`",
                        dto.source.display(),
                        dto.item.ident
                    );
                }
                if primitive_type(&dto_entity_type) && !decisions.contains_key(&key) {
                    bail!(
                        "persistence DTO {}::{} exposes primitive `{rust_type}` without entity decision `{key}`",
                        dto.source.display(),
                        dto.item.ident
                    );
                }
            }
        }
    }
    Ok(())
}

pub fn persistence_dto_entity_type(rust_type: &str) -> String {
    if let Some(inner) = rust_type
        .strip_prefix("Patch<")
        .and_then(|inner| inner.strip_suffix('>'))
    {
        return inner.to_owned();
    }
    if let Some(inner) = rust_type
        .strip_prefix("NullablePatch<")
        .and_then(|inner| inner.strip_suffix('>'))
    {
        return format!("Option<{inner}>");
    }
    rust_type.to_owned()
}

fn audit_snapshots(
    snapshot_root: &Path,
    entity_fields: &BTreeMap<String, String>,
    decisions: &BTreeMap<String, FieldDecision>,
) -> Result<()> {
    let mut snapshot_fields = BTreeMap::new();
    for path in rust_files(snapshot_root)? {
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
        for (field, rust_type) in named_fields(model)? {
            let key = format!("{table}.{field}");
            snapshot_fields.insert(key.clone(), rust_type.clone());
            if primitive_type(&rust_type)
                && entity_fields
                    .get(&key)
                    .is_some_and(|active_type| primitive_type(active_type))
                && !decisions.contains_key(&key)
            {
                bail!(
                    "v1 snapshot {} exposes primitive `{rust_type}` without entity decision `{key}`",
                    path.display()
                );
            }
        }
    }
    for (key, decision) in decisions {
        let active_type = entity_fields
            .get(key)
            .with_context(|| format!("decision `{key}` has no runtime entity field"))?;
        let snapshot_type = snapshot_fields
            .get(key)
            .with_context(|| format!("fresh-boot v1 snapshot is missing decided field `{key}`"))?;
        if !snapshot_representation_matches(active_type, snapshot_type, decision.disposition) {
            bail!(
                "fresh-boot v1 snapshot `{key}` uses `{snapshot_type}`, incompatible with runtime `{active_type}` and {:?}",
                decision.disposition
            );
        }
    }
    Ok(())
}

fn snapshot_representation_matches(
    active_type: &str,
    snapshot_type: &str,
    disposition: Disposition,
) -> bool {
    if active_type == snapshot_type {
        return true;
    }
    match disposition {
        Disposition::ActiveEnum => {
            let active_optional = active_type.starts_with("Option<");
            let snapshot_optional = snapshot_type.starts_with("Option<Qp");
            (active_optional && snapshot_optional)
                || (!active_optional && snapshot_type.starts_with("Qp"))
        }
        Disposition::ValidatedNewtype => {
            if active_type.starts_with("Option<") {
                snapshot_type == "Option<String>"
            } else {
                snapshot_type == "String"
            }
        }
        Disposition::TypedId => {
            if active_type.starts_with("Option<") {
                snapshot_type == "Option<Uuid>"
            } else {
                snapshot_type == "Uuid"
            }
        }
        Disposition::TaggedReference => snapshot_type == "Option<Uuid>",
        Disposition::FreeText
        | Disposition::ExternalProtocolValue
        | Disposition::OpaqueSnapshotLabel => active_type == snapshot_type,
        Disposition::Remove => false,
    }
}

pub fn persistence_target(attrs: &[Attribute]) -> Result<Option<String>> {
    let mut target = None;
    for attr in attrs {
        if !attr.path().is_ident("sea_orm") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("active_model") || meta.path.is_ident("entity") {
                let value = meta.value()?;
                let literal: LitStr = value.parse()?;
                let path = literal.value();
                if let Some(entity) = path
                    .split("entities::")
                    .nth(1)
                    .and_then(|tail| tail.split("::").next())
                {
                    target = Some(entity.to_owned());
                }
            }
            Ok(())
        })?;
    }
    Ok(target)
}

pub fn named_fields(item: &ItemStruct) -> Result<Vec<(String, String)>> {
    let Fields::Named(fields) = &item.fields else {
        bail!("persistence struct `{}` must use named fields", item.ident);
    };
    fields
        .named
        .iter()
        .map(|field| {
            let name = field
                .ident
                .as_ref()
                .context("named field has no identifier")?;
            Ok((name.to_string(), rust_type(&field.ty)?))
        })
        .collect()
}

pub fn rust_type(ty: &Type) -> Result<String> {
    let Type::Path(path) = ty else {
        bail!("persistence field uses unsupported non-path Rust type");
    };
    let segment = path
        .path
        .segments
        .last()
        .context("Rust type path is empty")?;
    let container = segment.ident.to_string();
    if matches!(container.as_str(), "Option" | "Patch" | "NullablePatch") {
        let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
            bail!("Option persistence field has no type argument");
        };
        let inner = arguments
            .args
            .first()
            .context("Option persistence field is empty")?;
        let syn::GenericArgument::Type(inner) = inner else {
            bail!("Option persistence field has a non-type argument");
        };
        return Ok(format!("{container}<{}>", rust_type(inner)?));
    }
    Ok(segment.ident.to_string())
}

fn primitive_type(rust_type: &str) -> bool {
    matches!(
        rust_type,
        "String" | "Option<String>" | "Uuid" | "Option<Uuid>"
    )
}

pub fn parse_rust(path: &Path) -> Result<syn::File> {
    let source = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    syn::parse_file(&source).with_context(|| format!("parse Rust AST for {}", path.display()))
}

pub fn rust_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    collect_rust_files(root, &mut output)?;
    output.sort();
    Ok(output)
}

fn collect_rust_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("read directory {}", root.display()))? {
        let entry = entry.with_context(|| format!("read entry under {}", root.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspect {}", path.display()))?;
        if file_type.is_dir() {
            collect_rust_files(&path, output)?;
        } else if file_type.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("rs")
        {
            output.push(path);
        }
    }
    Ok(())
}

pub fn file_stem(path: &Path) -> Result<&str> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .with_context(|| format!("path has no UTF-8 file stem: {}", path.display()))
}
