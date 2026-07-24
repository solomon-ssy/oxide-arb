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
    Attribute, Block, Expr, ExprMethodCall, Fields, File, GenericArgument, ImplItemFn, Item,
    ItemFn, ItemMod, ItemUse, Lit, Meta, Path as SynPath, PathArguments, Token, Type, UseTree,
    Visibility,
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
    "quant-pivot-allocator",
    "quant-pivot-api",
    "quant-pivot-bench",
    "quant-pivot-bin",
    "quant-pivot-compute",
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
    violations.extend(validate_performance_contracts(&metadata)?);
    violations.extend(validate_public_api(&metadata.workspace_root)?);
    violations.extend(validate_settlement_runtime_reachability(
        &metadata.workspace_root,
    )?);
    violations.extend(validate_phase_11_9_ledger(&metadata.workspace_root)?);
    violations.extend(validate_removed_phase_11_9_attribution_contract(
        &metadata.workspace_root,
    )?);
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

fn validate_settlement_runtime_reachability(workspace_root: &Path) -> Result<Vec<String>> {
    let mut violations = validate_settlement_runtime_wiring(workspace_root)?;
    violations.extend(validate_settlement_persistence_boundaries(workspace_root)?);
    violations.extend(validate_settlement_transport_boundaries(workspace_root)?);
    Ok(violations)
}

fn validate_settlement_runtime_wiring(workspace_root: &Path) -> Result<Vec<String>> {
    let (bootstrap_path, bootstrap) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/app/bootstrap.rs",
    )?;
    let (workers_path, workers) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/app/settlement_workers.rs",
    )?;
    let (bundle_path, bundle) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/app/bundles/execution.rs",
    )?;
    let (executor_path, executor) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/execution/settlement_executor.rs",
    )?;
    let mut violations = Vec::new();
    require_exact_occurrences(
        &mut violations,
        &bootstrap_path,
        &bootstrap,
        "ctx.register_settlement_workers(&mut runner);",
        1,
        "production bootstrap must register the settlement worker set exactly once",
    );
    require_exact_occurrences(
        &mut violations,
        &bootstrap_path,
        &bootstrap,
        "ctx.register_runtime_control_sync(&mut runner);",
        1,
        "production bootstrap must register runtime-control convergence exactly once",
    );
    require_exact_occurrences(
        &mut violations,
        &bundle_path,
        &bundle,
        "ProductionSettlementExecutor::connect(",
        1,
        "production execution bundle must construct exactly one settlement executor",
    );
    require_exact_occurrences(
        &mut violations,
        &executor_path,
        &executor,
        "impl SettlementSubmissionExecutor for ProductionSettlementExecutor",
        1,
        "the money-moving settlement executor must have exactly one production implementation",
    );
    for task in [
        "SettlementDiscovery",
        "SettlementPreflight",
        "SettlementExecution",
        "SettlementExternalObservation",
        "SettlementGovernedAction",
    ] {
        let needle = format!("TaskId::{task}");
        require_exact_occurrences(
            &mut violations,
            &workers_path,
            &workers,
            &needle,
            1,
            "each settlement worker identity must be registered exactly once",
        );
    }
    Ok(violations)
}

fn validate_settlement_persistence_boundaries(workspace_root: &Path) -> Result<Vec<String>> {
    let (discovery_path, discovery) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/execution/settlement_discovery.rs",
    )?;
    let (external_path, external) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/execution/settlement_external.rs",
    )?;
    let (repository_trait_path, repository_trait) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-repository/src/traits/quant/settlement_redeem.rs",
    )?;
    let (repository_impl_path, repository_impl) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-repository/src/postgres/quant/settlement_redeem.rs",
    )?;
    let mut violations = Vec::new();
    for (path, source) in [
        (&repository_trait_path, repository_trait.as_str()),
        (&repository_impl_path, repository_impl.as_str()),
    ] {
        require_exact_occurrences(
            &mut violations,
            path,
            source,
            "async fn insert_submission(",
            0,
            "settlement submissions must enter through guarded prepared/external persistence commands",
        );
    }
    for (path, source, needle, invariant) in [
        (
            &discovery_path,
            discovery.as_str(),
            ".insert_discovered_case(",
            "durable settlement case discovery must have exactly one production producer",
        ),
        (
            &external_path,
            external.as_str(),
            ".persist_scan(",
            "external settlement observation must journal cursor and evidence through one production boundary",
        ),
        (
            &external_path,
            external.as_str(),
            "kind: SettlementSubmissionKind::ExternallyObserved,",
            "externally observed settlement identity must have exactly one production constructor",
        ),
    ] {
        require_exact_occurrences(&mut violations, path, source, needle, 1, invariant);
    }
    Ok(violations)
}

fn validate_settlement_transport_boundaries(workspace_root: &Path) -> Result<Vec<String>> {
    let (control_path, control) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-core/src/app/ports/settlement_control.rs",
    )?;
    let (web_routes_path, web_routes) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-web/src/routes/settlement_redeems.rs",
    )?;
    let (clob_path, clob) =
        read_architecture_source(workspace_root, "crates/quant-pivot-api/src/clob/mod.rs")?;
    let (wallet_path, wallet) =
        read_architecture_source(workspace_root, "crates/quant-pivot-api/src/wallet/mod.rs")?;
    let (relayer_path, relayer) = read_architecture_source(
        workspace_root,
        "crates/quant-pivot-api/src/settlement/relayer.rs",
    )?;
    let mut violations = Vec::new();
    for (path, source, needle, invariant) in [
        (
            &control_path,
            control.as_str(),
            "async fn apply_governed_action(",
            "governed settlement apply must have exactly one production control implementation",
        ),
        (
            &web_routes_path,
            web_routes.as_str(),
            ".apply_governed_action(request, actor_id, Utc::now())",
            "the governed settlement HTTP apply boundary must journal through the control port exactly once",
        ),
        (
            &clob_path,
            clob.as_str(),
            ".signature_type(topology.signature_type)",
            "CLOB production orders must consume the verified wallet signature type exactly once",
        ),
        (
            &wallet_path,
            wallet.as_str(),
            "ExecutionWalletKind::DepositWallet => SignatureType::Poly1271",
            "Deposit Wallet topology must map to POLY_1271 exactly once",
        ),
        (
            &relayer_path,
            relayer.as_str(),
            "tx_type: \"WALLET\".to_owned(),",
            "Deposit Wallet settlement must construct exactly one canonical WALLET request body",
        ),
    ] {
        require_exact_occurrences(&mut violations, path, source, needle, 1, invariant);
    }
    Ok(violations)
}

fn read_architecture_source(
    workspace_root: &Path,
    relative_path: &str,
) -> Result<(PathBuf, String)> {
    let path = workspace_root.join(relative_path);
    let source = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    Ok((path, source))
}

fn validate_removed_phase_11_9_attribution_contract(workspace_root: &Path) -> Result<Vec<String>> {
    const REMOVED_PATHS: &[&str] = &[
        "crates/quant-pivot-core/src/app/attribution_worker.rs",
        "crates/quant-pivot-core/src/execution/attribution.rs",
        "crates/quant-pivot-core/src/observability/attribution_fact_writer.rs",
        "crates/quant-pivot-migration/src/snapshots/v1/quant_recommendation_attribution.rs",
        "crates/quant-pivot-models/src/domain/quant/attribution.rs",
        "crates/quant-pivot-models/src/entities/quant_recommendation_attribution.rs",
        "crates/quant-pivot-models/src/types/attribution_payload.rs",
        "crates/quant-pivot-repository/src/postgres/quant/attribution.rs",
        "crates/quant-pivot-repository/src/traits/quant/attribution.rs",
        "ui/apps/web-antdv-next/src/views/quant/recommendations/modules/widgets/recommendation-attribution.vue",
    ];
    const REMOVED_TOKENS: &[&str] = &[
        "RecommendationStatus::Attributed",
        "RecommendationAttribution",
        "quant_recommendation_attribution",
        "recommendation-attribution",
        "AttributionPolicy",
        "AttributionRepository",
        "AttributionService",
        "AttributionWorker",
        "AttributionEventWriter",
        "attribution_fact_writer",
        "attribution_worker",
        "InsertFinalOutcome",
        "TrainingSampleSource::LiveAttribution",
        "live_attribution",
        "LIVE_ATTRIBUTION_SAMPLE_LIMIT",
        "eligible_for_attribution",
        "find_attribution_candidates",
        "find_unfilled_attribution_candidates",
        "recommendation_blocks_final_attribution",
        "blocks_attribution",
        "ATTRIBUTION_ELIGIBLE",
        "get_recommendation_attribution",
        "getRecommendationAttribution",
        "recommendations/{id}/attribution",
        "recommendations/${id}/attribution",
        "recommendationAttribution",
        "quantRecommendations.attribution",
        "#[sea_orm(string_value = \"attributed\")]",
        "attributed: 'attributed'",
        "RECOMMENDATION_STATUSES.attributed",
    ];

    let mut violations = Vec::new();
    for relative in REMOVED_PATHS {
        let path = workspace_root.join(relative);
        if path.exists() {
            violations.push(format!(
                "{} retains a removed Phase 11.9 attribution source path",
                path.display()
            ));
        }
    }

    let mut source_paths = Vec::new();
    for relative in [
        "crates",
        "schema",
        "config",
        "ui/apps/web-antdv-next/src",
        "ui/packages/types/src",
    ] {
        collect_contract_sources(&workspace_root.join(relative), &mut source_paths)?;
    }
    source_paths.sort();
    let gate_source = workspace_root.join("crates/quant-pivot-xtask/src/architecture.rs");
    for path in source_paths {
        if path == gate_source {
            continue;
        }
        let source =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        for token in REMOVED_TOKENS {
            if source.contains(token) {
                violations.push(format!(
                    "{} retains removed Phase 11.9 attribution contract `{token}`",
                    path.display()
                ));
            }
        }
    }

    let postgres_manifest = workspace_root.join("schema/postgres/manifest.json");
    let postgres_schema = fs::read_to_string(&postgres_manifest)
        .with_context(|| format!("read {}", postgres_manifest.display()))?;
    if postgres_schema.contains("\"attributed\"") {
        violations.push(format!(
            "{} retains the removed recommendation status value `attributed`",
            postgres_manifest.display()
        ));
    }

    Ok(violations)
}

fn collect_contract_sources(directory: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read source directory {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", directory.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_contract_sources(&path, output)?;
        } else if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| {
                matches!(
                    extension,
                    "json" | "rs" | "sql" | "toml" | "ts" | "vue" | "yaml" | "yml"
                )
            })
        {
            output.push(path);
        }
    }
    Ok(())
}

fn require_exact_occurrences(
    violations: &mut Vec<String>,
    path: &Path,
    source: &str,
    needle: &str,
    expected: usize,
    invariant: &str,
) {
    let actual = source.matches(needle).count();
    if actual != expected {
        violations.push(format!(
            "{} contains `{needle}` {actual} time(s), expected {expected}: {invariant}",
            path.display()
        ));
    }
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
        test_depth: 0,
        function_name: None,
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

fn parallel_kernel_allowed(path: &Path, function_name: Option<&str>) -> bool {
    matches!(
        (path.to_str(), function_name),
        (
            Some("crates/quant-pivot-core/src/service/cpcv_backtest.rs"),
            Some("run_trial_grid")
        ) | (
            Some("crates/quant-pivot-research/src/features/builder.rs"),
            Some("build_batch")
        ) | (
            Some("crates/quant-pivot-research/src/parallel.rs"),
            Some("par_try_map" | "par_map_with_index" | "par_try_map_with_index")
        ) | (
            Some("crates/quant-pivot-research/src/validation/cpcv.rs"),
            Some("run")
        ) | (
            Some("crates/quant-pivot-research/src/validation/pbo.rs"),
            Some("probability_of_backtest_overfitting")
        )
    )
}

struct BodyStyleVisitor<'a> {
    path: &'a Path,
    block_depth: usize,
    test_depth: usize,
    function_name: Option<String>,
    generated_sea_orm_entity: bool,
    violations: Vec<String>,
}

fn type_path_has_direct_argument(ty: &Type, owner: &str, argument: &str) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    let Some(segment) = type_path.path.segments.last() else {
        return false;
    };
    if segment.ident != owner {
        return false;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    arguments.args.iter().any(|generic| {
        let GenericArgument::Type(Type::Path(argument_path)) = generic else {
            return false;
        };
        argument_path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == argument)
    })
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
        let is_test = item_mod.ident == "tests"
            || item_mod.attrs.iter().any(|attribute| {
                attribute
                    .meta
                    .to_token_stream()
                    .to_string()
                    .contains("test")
            });
        self.test_depth += usize::from(is_test);
        self.block_depth = 0;
        visit::visit_item_mod(self, item_mod);
        self.block_depth = previous_depth;
        self.test_depth -= usize::from(is_test);
    }

    fn visit_item_fn(&mut self, item_fn: &'ast ItemFn) {
        let previous = self.function_name.replace(item_fn.sig.ident.to_string());
        visit::visit_item_fn(self, item_fn);
        self.function_name = previous;
    }

    fn visit_impl_item_fn(&mut self, item_fn: &'ast ImplItemFn) {
        let previous = self.function_name.replace(item_fn.sig.ident.to_string());
        visit::visit_impl_item_fn(self, item_fn);
        self.function_name = previous;
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

    fn visit_type(&mut self, ty: &'ast Type) {
        if type_path_has_direct_argument(ty, "Arc", "Uuid") {
            self.violations.push(format!(
                "{} heap-shares a 16-byte UUID as `Arc<Uuid>`; use the Copy UUID newtype value",
                self.path.display()
            ));
        }
        if self.path.starts_with("crates/quant-pivot-web/src/ws")
            && type_path_has_direct_argument(ty, "Sender", "String")
        {
            self.violations.push(format!(
                "{} uses `Sender<String>` in WebSocket delivery; encode once and fan out shared `ByteString` frames",
                self.path.display()
            ));
        }
        visit::visit_type(self, ty);
    }

    fn visit_path(&mut self, path: &'ast SynPath) {
        let segments = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        if self.test_depth == 0
            && segments
                .last()
                .is_some_and(|segment| segment == "spawn_blocking")
        {
            self.violations.push(format!(
                "{} directly calls `spawn_blocking` in production code; route CPU work through ComputeExecutor and keep bounded blocking I/O inside its owned boundary",
                self.path.display()
            ));
        }
        if self.test_depth == 0
            && segments
                .iter()
                .any(|segment| segment == "ThreadPoolBuilder")
            && !self.path.starts_with("crates/quant-pivot-compute/src")
            && !self.path.starts_with("crates/quant-pivot-bench")
        {
            self.violations.push(format!(
                "{} constructs a production Rayon pool outside ComputeExecutor",
                self.path.display()
            ));
        }
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

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        let method = call.method.to_string();
        if self.test_depth == 0
            && matches!(
                method.as_str(),
                "par_iter" | "par_iter_mut" | "into_par_iter" | "build_global"
            )
            && !self.path.starts_with("crates/quant-pivot-compute/src")
            && !self.path.starts_with("crates/quant-pivot-bench")
            && !(method != "build_global"
                && parallel_kernel_allowed(self.path, self.function_name.as_deref()))
        {
            self.violations.push(format!(
                "{} uses governed compute primitive `{method}` outside ComputeExecutor or an approved pure parallel kernel",
                self.path.display()
            ));
        }
        visit::visit_expr_method_call(self, call);
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
        if !matches!(
            package.name.as_str(),
            "quant-pivot-allocator" | "quant-pivot-macros"
        ) && !package.dependencies.iter().any(|dependency| {
            dependency.kind.is_none() && dependency.name == "quant-pivot-allocator"
        }) {
            violations.push(format!(
                "{} does not link the target-process jemalloc policy crate",
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
                && dependency.name != "quant-pivot-allocator"
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

fn validate_performance_contracts(metadata: &CargoMetadata) -> Result<Vec<String>> {
    let root = &metadata.workspace_root;
    let mut violations = Vec::new();
    validate_allocator_contract(root, &mut violations)?;
    validate_deployment_contract(root, &mut violations)?;
    validate_runtime_contract(root, &mut violations)?;
    validate_performance_evidence_contract(root, &mut violations)?;
    validate_removed_l2_contract(root, &mut violations)?;
    Ok(violations)
}

fn validate_performance_evidence_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    for (relative, fragments) in [
        (
            "crates/quant-pivot-system-tests/src/performance/mod.rs",
            &[
                "const REQUIRED_RUNNER: &str = \"quant-pivot-perf-8c16g\";",
                "const ACTIVE_TOKENS: usize = 2_000;",
                "const FULL_RUN_COUNT: u16 = 3;",
                "const MAX_RUNNER_VARIATION_PERCENT: f64 = 3.0;",
                "warmup: Duration::from_mins(5),",
                "sustained: Duration::from_mins(30),",
                "sustained_rate: 10_000,",
                "burst: Duration::from_secs(10),",
                "burst_rate: 50_000,",
                "sustained: Duration::from_hours(2),",
                "churn_interval: Some(Duration::from_mins(5)),",
                "record_correct(value, expected)",
                "all_durable_publications",
                "profile == PerformanceProfile::Smoke || hard_slo_passed",
            ][..],
        ),
        (
            "crates/quant-pivot-system-tests/src/performance/evidence.rs",
            &[
                "pub const PERFORMANCE_EVIDENCE_SCHEMA_VERSION: u16 = 1;",
                "pub struct PerformanceEvidenceV1",
                "coordinated_omission_expected_interval_us",
                "sha256: sha256_hex(&bytes)",
                "VmHWM:",
            ][..],
        ),
        (
            "crates/quant-pivot-xtask/src/performance.rs",
            &[
                "const FULL_KERNEL_REPETITIONS: u16 = 10;",
                "name: \"training_matrix_gate\"",
                "name: \"cpcv_orchestration_gate\"",
                "name: \"report_compute_gate\"",
                "name: \"model_train_replay_gate\"",
                "peak_rss_bytes: Option<u64>",
            ][..],
        ),
        (
            "crates/quant-pivot-bench/src/bin/training_matrix_gate.rs",
            &[
                "const MAX_RSS_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;",
                "enforce_linux_peak_rss(peak_rss, MAX_RSS_BYTES, \"training matrix\")?;",
            ][..],
        ),
        (
            "crates/quant-pivot-bench/src/bin/model_train_replay_gate.rs",
            &[
                "const MAX_RSS_BYTES: u64 = 10 * 1_024 * 1_024 * 1_024;",
                "enforce_linux_peak_rss(peak_rss, MAX_RSS_BYTES, \"model train/replay\")?;",
            ][..],
        ),
        (
            ".github/workflows/performance.yml",
            &[
                "runs-on: [self-hosted, quant-pivot-perf-8c16g]",
                "QUANT_PIVOT_PERF_RUNNER: quant-pivot-perf-8c16g",
                "profile=soak",
                "profile=full",
                "actions/upload-artifact@v4",
                "! -name artifact-manifest.sha256 -print0",
            ][..],
        ),
    ] {
        let path = root.join(relative);
        let source = fs::read_to_string(&path)
            .with_context(|| format!("read performance evidence contract {}", path.display()))?;
        for fragment in fragments {
            if !source.contains(fragment) {
                violations.push(format!(
                    "{} is missing governed performance evidence contract `{fragment}`",
                    path.display()
                ));
            }
        }
    }

    let legacy_gate = root.join("crates/quant-pivot-bench/src/bin/cpcv_gate.rs");
    if legacy_gate.exists() {
        violations.push(format!(
            "{} retains the superseded ambiguous CPCV gate name",
            legacy_gate.display()
        ));
    }
    Ok(())
}

fn validate_allocator_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    let allocator_source_path = root.join("crates/quant-pivot-allocator/src/lib.rs");
    let allocator_source = fs::read_to_string(&allocator_source_path)
        .with_context(|| format!("read {}", allocator_source_path.display()))?;
    for fragment in [
        "#[global_allocator]",
        "use tikv_jemallocator::Jemalloc;",
        "static GLOBAL_ALLOCATOR: Jemalloc = Jemalloc;",
        "pub const NAME: &str = \"tikv-jemalloc\";",
    ] {
        if !allocator_source.contains(fragment) {
            violations.push(format!(
                "{} must contain fixed allocator contract `{fragment}`",
                allocator_source_path.display()
            ));
        }
    }
    let package_roots = [
        "api",
        "bench",
        "bin",
        "compute",
        "core",
        "error",
        "migration",
        "models",
        "repository",
        "research",
        "storage",
        "system-tests",
        "web",
        "xtask",
    ];
    for package in package_roots {
        let crate_root = if matches!(package, "bin" | "xtask") {
            root.join(format!("crates/quant-pivot-{package}/src/main.rs"))
        } else {
            root.join(format!("crates/quant-pivot-{package}/src/lib.rs"))
        };
        let source = fs::read_to_string(&crate_root)
            .with_context(|| format!("read {}", crate_root.display()))?;
        if !source.contains("use quant_pivot_allocator as _;") {
            violations.push(format!(
                "{} must force-link the target-process jemalloc policy",
                crate_root.display()
            ));
        }
    }
    for manifest in [
        "Cargo.toml",
        "crates/quant-pivot-api/Cargo.toml",
        "crates/quant-pivot-bench/Cargo.toml",
        "crates/quant-pivot-bin/Cargo.toml",
        "crates/quant-pivot-core/Cargo.toml",
        "crates/quant-pivot-research/Cargo.toml",
    ] {
        let path = root.join(manifest);
        let source =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        for forbidden in ["mimalloc", "allocator-jemalloc", "allocator-mimalloc"] {
            if source.contains(forbidden) {
                violations.push(format!(
                    "{} contains allocator fallback/selection token `{forbidden}`; jemalloc is unconditional",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

fn validate_deployment_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    for (manifest, required_features) in [
        (
            "crates/quant-pivot-bin/Cargo.toml",
            &["serving", "research-jobs", "domain-chainlink"][..],
        ),
        (
            "crates/quant-pivot-core/Cargo.toml",
            &["serving", "research-jobs", "domain-chainlink"][..],
        ),
        (
            "crates/quant-pivot-research/Cargo.toml",
            &["research-jobs"][..],
        ),
        (
            "crates/quant-pivot-api/Cargo.toml",
            &["domain-chainlink"][..],
        ),
    ] {
        let path = root.join(manifest);
        let source =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: Value =
            toml::from_str(&source).with_context(|| format!("parse {}", path.display()))?;
        let features = parsed
            .get("features")
            .and_then(Value::as_table)
            .with_context(|| format!("{} must declare a feature table", path.display()))?;
        for feature in required_features {
            if !features.contains_key(*feature) {
                violations.push(format!(
                    "{} is missing deployment feature `{feature}`",
                    path.display()
                ));
            }
        }
        if features.contains_key("dataframe") {
            violations.push(format!(
                "{} retains the removed `dataframe` compatibility feature",
                path.display()
            ));
        }
    }
    Ok(())
}

fn validate_runtime_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    validate_compute_runtime_contract(root, violations)?;
    validate_data_plane_runtime_contract(root, violations)
}

fn validate_compute_runtime_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    let runtime_path = root.join("crates/quant-pivot-bin/src/main.rs");
    let runtime = fs::read_to_string(&runtime_path)
        .with_context(|| format!("read {}", runtime_path.display()))?;
    for fragment in [
        "const TOKIO_WORKER_THREADS: usize = 3;",
        "const TOKIO_MAX_BLOCKING_THREADS: usize = 4;",
        ".worker_threads(TOKIO_WORKER_THREADS)",
        ".max_blocking_threads(TOKIO_MAX_BLOCKING_THREADS)",
        "let compute = Arc::new(ComputeExecutor::new()?);",
        "bootstrap::run(deploy, compute)",
    ] {
        if !runtime.contains(fragment) {
            violations.push(format!(
                "{} is missing governed runtime budget `{fragment}`",
                runtime_path.display()
            ));
        }
    }

    let compute_path = root.join("crates/quant-pivot-compute/src/lib.rs");
    let compute = fs::read_to_string(&compute_path)
        .with_context(|| format!("read {}", compute_path.display()))?;
    for fragment in [
        "pub const SERVING_THREADS: usize = 2;",
        "pub const OFFLINE_THREADS: usize = 2;",
        "pub const OFFLINE_MEMORY_BYTES: usize = 10 * 1024 * 1024 * 1024;",
        "offline_jobs: Arc<Semaphore>",
        "offline_memory: Arc<Semaphore>",
        "run_offline_cancellable",
        "run_serving_scoped",
    ] {
        if !compute.contains(fragment) {
            violations.push(format!(
                "{} is missing governed compute contract `{fragment}`",
                compute_path.display()
            ));
        }
    }

    for (relative, fragments) in [
        (
            "crates/quant-pivot-core/src/service/feature_pipeline.rs",
            &["ComputeExecutor", "run_serving_scoped"][..],
        ),
        (
            "crates/quant-pivot-core/src/service/factor_pipeline.rs",
            &["ComputeExecutor", "run_serving"][..],
        ),
        (
            "crates/quant-pivot-core/src/service/model_training.rs",
            &["ComputeExecutor", "run_offline_cancellable"][..],
        ),
        (
            "crates/quant-pivot-core/src/service/training_dataset.rs",
            &["ComputeExecutor", "run_offline_cancellable"][..],
        ),
        (
            "crates/quant-pivot-core/src/service/backtest.rs",
            &["ComputeExecutor", "run_offline_cancellable"][..],
        ),
        (
            "crates/quant-pivot-core/src/service/cpcv_backtest.rs",
            &["ComputeExecutor", "run_offline_cancellable"][..],
        ),
    ] {
        let path = root.join(relative);
        let source =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        for fragment in fragments {
            if !source.contains(fragment) {
                violations.push(format!(
                    "{} bypasses the centralized compute contract `{fragment}`",
                    path.display()
                ));
            }
        }
    }

    let web_path = root.join("crates/quant-pivot-web/src/lib.rs");
    let web =
        fs::read_to_string(&web_path).with_context(|| format!("read {}", web_path.display()))?;
    for fragment in [".workers(1)", ".worker_max_blocking_threads(1)"] {
        if !web.contains(fragment) {
            violations.push(format!(
                "{} is missing governed Actix budget `{fragment}`",
                web_path.display()
            ));
        }
    }
    Ok(())
}

fn validate_data_plane_runtime_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    let session_hub_path = root.join("crates/quant-pivot-web/src/ws/mod.rs");
    let session_hub = fs::read_to_string(&session_hub_path)
        .with_context(|| format!("read {}", session_hub_path.display()))?;
    for fragment in [
        "const HUB_CONTROL_CAPACITY: usize = 1_024;",
        "const HUB_RELIABLE_CAPACITY: usize = 2_048;",
        "const HUB_BEST_EFFORT_TOPIC_CAPACITY: usize = 8_192;",
        "const HUB_FRAME_BUDGET_BYTES: usize = 64 * 1_024 * 1_024;",
        "const HUB_MAX_FRAME_BYTES: usize = 1_024 * 1_024;",
        "biased;",
        "control: Receiver<SessionControlCommand>",
        "reliable: Receiver<ReliableFanout>",
        "best_effort: Arc<BestEffortCoalescer>",
        "_byte_permit: OwnedSemaphorePermit",
        "self.fail_closed.cancel();",
    ] {
        if !session_hub.contains(fragment) {
            violations.push(format!(
                "{} is missing governed SessionHub contract `{fragment}`",
                session_hub_path.display()
            ));
        }
    }
    if session_hub.contains("WebSocketHubConfig") {
        violations.push(format!(
            "{} exposes fixed SessionHub qualification budgets as pseudo-configuration",
            session_hub_path.display()
        ));
    }

    let data_pipeline_path = root.join("crates/quant-pivot-core/src/ingest/data_pipeline.rs");
    let data_pipeline = fs::read_to_string(&data_pipeline_path)
        .with_context(|| format!("read {}", data_pipeline_path.display()))?;
    for fragment in [
        "Retire {",
        "retire_transport_tokens",
        "take_retired_mutable_book",
        "mutable_book_count",
    ] {
        if !data_pipeline.contains(fragment) {
            violations.push(format!(
                "{} is missing governed token-retirement contract `{fragment}`",
                data_pipeline_path.display()
            ));
        }
    }

    let ledger_path = root.join("crates/quant-pivot-core/src/observability/ledger_persistence.rs");
    let ledger = fs::read_to_string(&ledger_path)
        .with_context(|| format!("read {}", ledger_path.display()))?;
    if !ledger.contains("pub const LEDGER_PARTITION_COUNT: usize = 8;") {
        violations.push(format!(
            "{} must retain exactly eight durable partition cursors",
            ledger_path.display()
        ));
    }

    let book_store_path = root.join("crates/quant-pivot-core/src/ingest/book_store.rs");
    let book_store = fs::read_to_string(&book_store_path)
        .with_context(|| format!("read {}", book_store_path.display()))?;
    let read_body = book_store
        .split_once("pub fn read_fresh<R>")
        .and_then(|(_, tail)| tail.split_once("/// Load an owned fresh snapshot"))
        .map(|(body, _)| body);
    if read_body.is_none_or(|body| {
        !body.contains("slot.snapshot_with_freshness()")
            || !body.contains("is_epoch_active")
            || body.contains("load_full")
    }) {
        violations.push(format!(
            "{} must keep the synchronous Fresh reader on one coherent ArcSwap/seqlock guard with an active-session fence and without `load_full()`",
            book_store_path.display()
        ));
    }
    for forbidden in [
        "pub fn read<",
        "pub fn load_owned(",
        "pub fn load_by_id(",
        "pub fn load_pair(",
        "pub fn published_snapshots(",
    ] {
        if book_store.contains(forbidden) {
            violations.push(format!(
                "{} exposes forbidden raw book API `{forbidden}`; use FreshBook or diagnostic LastKnownBook",
                book_store_path.display()
            ));
        }
    }
    Ok(())
}

fn validate_removed_l2_contract(root: &Path, violations: &mut Vec<String>) -> Result<()> {
    let mut production_sources = Vec::new();
    for package in [
        "api",
        "bin",
        "core",
        "error",
        "macros",
        "migration",
        "models",
        "repository",
        "research",
        "storage",
        "web",
    ] {
        collect_rust_sources(
            &root.join(format!("crates/quant-pivot-{package}/src")),
            &mut production_sources,
        )?;
    }
    for path in production_sources {
        let source =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        for legacy in [
            "BookL2EventRow",
            "BookL2CheckpointRow",
            "quant_book_l2_event",
            "quant_book_l2_checkpoint",
            "bids_json",
            "asks_json",
        ] {
            if source.contains(legacy) {
                violations.push(format!(
                    "{} contains removed L2 compatibility contract `{legacy}`",
                    path.display()
                ));
            }
        }
        let l2_path = path.starts_with(root.join("crates/quant-pivot-core/src/ingest"))
            || path.starts_with(root.join("crates/quant-pivot-core/src/observability"))
            || path.starts_with(root.join("crates/quant-pivot-models/src/clickhouse"));
        if l2_path && source.contains("serde_json_canonicalizer") {
            violations.push(format!(
                "{} uses JCS in the L2 production path; hash typed fixed-width fields directly",
                path.display()
            ));
        }
        if source.contains("#[global_allocator]") {
            violations.push(format!(
                "{} declares a second global allocator; only quant-pivot-allocator owns that policy",
                path.display()
            ));
        }
    }
    let bootstrap_path = root.join("crates/quant-pivot-storage/src/clickhouse/sql/bootstrap.sql");
    let bootstrap = fs::read_to_string(&bootstrap_path)
        .with_context(|| format!("read {}", bootstrap_path.display()))?;
    for legacy in ["quant_book_l2_event", "quant_book_l2_checkpoint"] {
        if bootstrap.contains(legacy) {
            violations.push(format!(
                "{} contains removed L2 schema `{legacy}`",
                bootstrap_path.display()
            ));
        }
    }
    Ok(())
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
        "quant-pivot-compute" => &["quant-pivot-error"],
        "quant-pivot-api" => &[
            "quant-pivot-compute",
            "quant-pivot-error",
            "quant-pivot-models",
        ],
        "quant-pivot-research" => &["quant-pivot-error", "quant-pivot-models"],
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
            "quant-pivot-compute",
            "quant-pivot-error",
            "quant-pivot-migration",
            "quant-pivot-models",
            "quant-pivot-repository",
            "quant-pivot-research",
            "quant-pivot-storage",
            "quant-pivot-web",
        ],
        "quant-pivot-bin" => &[
            "quant-pivot-compute",
            "quant-pivot-core",
            "quant-pivot-models",
        ],
        "quant-pivot-bench" => &[
            "quant-pivot-compute",
            "quant-pivot-core",
            "quant-pivot-error",
            "quant-pivot-models",
            "quant-pivot-research",
        ],
        "quant-pivot-system-tests" => &[
            "quant-pivot-api",
            "quant-pivot-compute",
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
            "quant-pivot-compute",
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

const PHASE_11_9_LEDGER_PATH: &str =
    "docs/plans/quant-pivot/phase-11/11.9-attribution-feedback-and-auto-retraining.md";
const PHASE_11_9_LEDGER_STATES: &[&str] = &[
    "TODO",
    "IN_PROGRESS",
    "BLOCKED",
    "PAUSED",
    "DONE",
    "SUPERSEDED",
];
const PHASE_11_9_CHECKPOINT_FIELDS: &[&str] = &[
    "baseline_branch",
    "baseline_head",
    "working_tree_summary",
    "current_task_id",
    "current_task_objective",
    "dependency_status",
    "changed_paths",
    "last_passed_command",
    "last_failed_command_and_root_cause",
    "evidence_artifacts_and_hashes",
    "active_long_running_command",
    "exact_resume_command",
    "next_single_action",
    "updated_at",
];

#[derive(Debug)]
struct PhaseLedgerTask {
    status: String,
    dependencies: Vec<String>,
    detail: String,
}

fn validate_phase_11_9_ledger(workspace_root: &Path) -> Result<Vec<String>> {
    let path = workspace_root.join(PHASE_11_9_LEDGER_PATH);
    let source = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    Ok(validate_phase_11_9_ledger_source(&source)
        .into_iter()
        .map(|violation| format!("{}: {violation}", path.display()))
        .collect())
}

fn validate_phase_11_9_ledger_source(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let Some((before_implementation, implementation_and_after)) =
        source.split_once("## 11. Implementation Ledger")
    else {
        return vec!["missing `## 11. Implementation Ledger`".to_owned()];
    };
    let Some((implementation, evidence_and_after)) =
        implementation_and_after.split_once("## 12. Evidence Ledger")
    else {
        return vec!["missing `## 12. Evidence Ledger`".to_owned()];
    };
    let Some((evidence, _decisions)) = evidence_and_after.split_once("## 13. 决策账本") else {
        return vec!["missing `## 13. 决策账本`".to_owned()];
    };

    let mut checkpoint = BTreeMap::new();
    for field in PHASE_11_9_CHECKPOINT_FIELDS {
        let marker = format!("- `{field}`:");
        let value = before_implementation
            .lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix(&marker)
                    .map(|value| value.trim().trim_matches('`').to_owned())
            })
            .unwrap_or_default();
        if value.is_empty() {
            violations.push(format!("checkpoint field `{field}` is missing or empty"));
        }
        checkpoint.insert(*field, value);
    }

    let mut tasks = BTreeMap::<String, PhaseLedgerTask>::new();
    for line in implementation.lines() {
        let Some(cells) = markdown_cells(line) else {
            continue;
        };
        let Some(id) = cells.first() else {
            continue;
        };
        if !is_phase_ledger_task_id(id) || cells.len() < 4 {
            continue;
        }
        if tasks.contains_key(*id) {
            violations.push(format!("task ID `{id}` is duplicated"));
            continue;
        }
        let status = cells[1].to_owned();
        if !PHASE_11_9_LEDGER_STATES.contains(&status.as_str()) {
            violations.push(format!("task `{id}` uses unsupported status `{status}`"));
        }
        let (dependencies, detail_start) = if cells.len() >= 5 {
            (
                parse_phase_ledger_dependencies(cells[2], id, &mut violations),
                3,
            )
        } else {
            (Vec::new(), 2)
        };
        tasks.insert(
            (*id).to_owned(),
            PhaseLedgerTask {
                status,
                dependencies,
                detail: cells[detail_start..].join(" | "),
            },
        );
    }
    if tasks.is_empty() {
        violations.push("implementation ledger contains no task rows".to_owned());
        return violations;
    }

    let in_progress = tasks
        .iter()
        .filter_map(|(id, task)| (task.status == "IN_PROGRESS").then_some(id.as_str()))
        .collect::<Vec<_>>();
    if in_progress.len() > 1 {
        violations.push(format!(
            "multiple tasks are IN_PROGRESS: {}",
            in_progress.join(", ")
        ));
    }
    let checkpoint_task = checkpoint
        .get("current_task_id")
        .map(String::as_str)
        .unwrap_or_default();
    match (checkpoint_task, in_progress.as_slice()) {
        ("无" | "none", []) => {}
        (_, [active]) if checkpoint_task == *active => {}
        _ => violations.push(format!(
            "checkpoint current_task_id `{checkpoint_task}` does not match IN_PROGRESS tasks [{}]",
            in_progress.join(", ")
        )),
    }

    let passed_tasks = evidence
        .lines()
        .filter_map(markdown_cells)
        .filter(|cells| cells.len() >= 4 && cells[2].starts_with("PASS"))
        .flat_map(|cells| extract_phase_ledger_task_ids(cells[1]))
        .collect::<BTreeSet<_>>();

    for (id, task) in &tasks {
        if task.status == "DONE" && !passed_tasks.contains(id) {
            violations.push(format!("DONE task `{id}` has no PASS evidence row"));
        }
        if task.status == "SUPERSEDED"
            && !task.detail.contains("replacement=")
            && !task.detail.contains("§13")
        {
            violations.push(format!(
                "SUPERSEDED task `{id}` lacks `replacement=` or a §13 decision reference"
            ));
        }
        if task.status == "BLOCKED"
            && (!task.detail.contains("blocker=")
                || !task.detail.contains("unblock=")
                || !task.detail.contains("resume="))
        {
            violations.push(format!(
                "BLOCKED task `{id}` must record blocker=, unblock=, and resume="
            ));
        }
        validate_phase_ledger_task_dependencies(id, task, &tasks, &mut violations);
    }

    let status_line = source
        .lines()
        .find(|line| line.starts_with("> 状态："))
        .unwrap_or_default();
    if (status_line.contains("Phase 11.9 已完成") || status_line.contains("完整双垂直生产激活"))
        && tasks
            .values()
            .any(|task| !matches!(task.status.as_str(), "DONE" | "SUPERSEDED"))
    {
        violations.push(
            "phase status claims completion while implementation tasks remain open".to_owned(),
        );
    }

    violations
}

fn markdown_cells(line: &str) -> Option<Vec<&str>> {
    let line = line.trim();
    if !line.starts_with('|') || !line.ends_with('|') {
        return None;
    }
    Some(line.trim_matches('|').split('|').map(str::trim).collect())
}

fn is_phase_ledger_task_id(value: &str) -> bool {
    let Some(value) = value.strip_prefix('W') else {
        return false;
    };
    let Some((wave, task)) = value.split_once('-') else {
        return false;
    };
    !wave.is_empty()
        && wave.chars().all(|character| character.is_ascii_digit())
        && !task.is_empty()
        && task
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn extract_phase_ledger_task_ids(value: &str) -> Vec<String> {
    value
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '-'))
        .filter(|token| is_phase_ledger_task_id(token))
        .map(str::to_owned)
        .collect()
}

fn parse_phase_ledger_dependencies(
    value: &str,
    task_id: &str,
    violations: &mut Vec<String>,
) -> Vec<String> {
    if matches!(value, "" | "无") {
        return Vec::new();
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|dependency| !dependency.is_empty())
        .filter_map(|dependency| {
            let reference = dependency.strip_suffix('*').unwrap_or(dependency);
            if is_phase_ledger_task_id(reference)
                || (dependency.ends_with('*') && is_phase_ledger_task_prefix(reference))
            {
                Some(dependency.to_owned())
            } else {
                violations.push(format!(
                    "task `{task_id}` has invalid dependency expression `{dependency}`"
                ));
                None
            }
        })
        .collect()
}

fn is_phase_ledger_task_prefix(value: &str) -> bool {
    let Some(value) = value.strip_prefix('W') else {
        return false;
    };
    let Some((wave, prefix)) = value.split_once('-') else {
        return false;
    };
    !wave.is_empty()
        && wave.chars().all(|character| character.is_ascii_digit())
        && !prefix.is_empty()
        && prefix
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn validate_phase_ledger_task_dependencies(
    task_id: &str,
    task: &PhaseLedgerTask,
    tasks: &BTreeMap<String, PhaseLedgerTask>,
    violations: &mut Vec<String>,
) {
    for dependency in &task.dependencies {
        let dependency_ids = dependency.strip_suffix('*').map_or_else(
            || vec![dependency.as_str()],
            |prefix| {
                tasks
                    .keys()
                    .filter(|id| id.starts_with(prefix))
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            },
        );
        if dependency_ids.is_empty() {
            violations.push(format!(
                "task `{task_id}` dependency `{dependency}` matches no task"
            ));
            continue;
        }
        for dependency_id in dependency_ids {
            if dependency_id == task_id {
                violations.push(format!("task `{task_id}` depends on itself"));
                continue;
            }
            let Some(dependency_task) = tasks.get(dependency_id) else {
                violations.push(format!(
                    "task `{task_id}` references unknown dependency `{dependency_id}`"
                ));
                continue;
            };
            if matches!(task.status.as_str(), "IN_PROGRESS" | "DONE")
                && !matches!(dependency_task.status.as_str(), "DONE" | "SUPERSEDED")
            {
                violations.push(format!(
                    "task `{task_id}` is {} while dependency `{dependency_id}` is {}",
                    task.status, dependency_task.status
                ));
            }
        }
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
        validate_persistence_documents, validate_phase_11_9_ledger_source, validate_public_exports,
        validate_source_style,
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

    fn metadata(mut package: CargoPackage) -> CargoMetadata {
        package
            .dependencies
            .push(dependency("quant-pivot-allocator", None, &[]));
        CargoMetadata {
            workspace_members: BTreeSet::from([package.id.clone()]),
            packages: vec![package],
            workspace_root: PathBuf::from("/workspace"),
        }
    }

    fn phase_11_9_ledger_fixture(implementation: &str, evidence: &str) -> String {
        format!(
            r"
> 状态：**Phase 11.9 执行中**

## 0. 文档使用规则与恢复协议

- `baseline_branch`: `quant-pivot`
- `baseline_head`: `abc123`
- `working_tree_summary`: clean
- `current_task_id`: `W2-00`
- `current_task_objective`: ledger
- `dependency_status`: `W1-01=DONE`
- `changed_paths`: docs
- `last_passed_command`: tests
- `last_failed_command_and_root_cause`: 无
- `evidence_artifacts_and_hashes`: none
- `active_long_running_command`: 无
- `exact_resume_command`: cargo test
- `next_single_action`: validate
- `updated_at`: `2026-07-24`

## 11. Implementation Ledger

{implementation}

## 12. Evidence Ledger

| 日期 | Item | 结果 | 证据 |
|---|---|---|---|
{evidence}

## 13. 决策账本
"
        )
    }

    #[test]
    fn phase_11_9_ledger_accepts_one_active_task_and_verified_dependencies() {
        let source = phase_11_9_ledger_fixture(
            r"
| ID | 状态 | 依赖 | 任务 | 完成证据 |
|---|---|---|---|---|
| W0-01 | SUPERSEDED | 无 | old plan | replacement=W2-00 |
| W1-01 | DONE | 无 | baseline | tests |
| W2-00 | IN_PROGRESS | W1-01 | ledger | tests |
| W2-A01 | TODO | W2-00 | outcome | tests |
| W4-E01 | TODO | W2-A* | exit | tests |
",
            "| 2026-07-24 | W1-01 | PASS | verified |",
        );

        assert!(validate_phase_11_9_ledger_source(&source).is_empty());
    }

    #[test]
    fn phase_11_9_ledger_rejects_drifted_state_and_missing_evidence() {
        let source = phase_11_9_ledger_fixture(
            r"
| ID | 状态 | 依赖 | 任务 | 完成证据 |
|---|---|---|---|---|
| W1-01 | DONE | 无 | baseline | tests |
| W2-00 | IN_PROGRESS | W1-01 | ledger | tests |
| W2-A01 | IN_PROGRESS | W2-00 | outcome | tests |
| W2-A02 | DONE | W2-A01 | payout | tests |
| W3-01 | SUPERSEDED | 无 | old route | no replacement |
",
            "",
        );

        let violations = validate_phase_11_9_ledger_source(&source);

        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("multiple tasks are IN_PROGRESS"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("W1-01") && violation.contains("no PASS"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("W2-A02") && violation.contains("dependency"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("W3-01") && violation.contains("replacement"))
        );
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
    fn blocking_tasks_are_restricted_to_executor_or_tests() {
        let source = syn::parse_file("fn run() { let _ = tokio::task::spawn_blocking(work); }")
            .expect("parse fixture");

        let violations = validate_source_style(&source, Path::new("src/unbudgeted.rs"));
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("directly calls `spawn_blocking`"));

        let test_source = syn::parse_file(
            "#[cfg(test)] mod tests { fn run() { let _ = tokio::task::spawn_blocking(work); } }",
        )
        .expect("parse test fixture");
        assert!(validate_source_style(&test_source, Path::new("src/test_owner.rs")).is_empty());
    }

    #[test]
    fn rayon_primitives_are_restricted_to_exact_governed_kernels() {
        let pool = syn::parse_file(
            "fn run() { let _ = ThreadPoolBuilder::new().num_threads(9).build(); }",
        )
        .expect("parse pool fixture");
        let violations = validate_source_style(&pool, Path::new("src/unbudgeted.rs"));
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("production Rayon pool"));

        let unapproved = syn::parse_file("fn surprise() { let _ = rows.par_iter(); }")
            .expect("parse parallel fixture");
        let builder_path = Path::new("crates/quant-pivot-research/src/features/builder.rs");
        let violations = validate_source_style(&unapproved, builder_path);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("governed compute primitive `par_iter`"));

        let approved =
            syn::parse_file("impl Builder { fn build_batch(&self) { let _ = rows.par_iter(); } }")
                .expect("parse approved kernel fixture");
        assert!(validate_source_style(&approved, builder_path).is_empty());
    }

    #[test]
    fn source_style_rejects_heap_uuid_and_ws_string_sender() {
        let uuid = syn::parse_file("struct Id(Arc<Uuid>);").expect("parse fixture");
        let violations = validate_source_style(&uuid, Path::new("src/id.rs"));
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("`Arc<Uuid>`"));

        let sender = syn::parse_file("struct Outbound(Sender<String>);").expect("parse fixture");
        let violations = validate_source_style(
            &sender,
            Path::new("crates/quant-pivot-web/src/ws/session.rs"),
        );
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("`Sender<String>`"));
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
