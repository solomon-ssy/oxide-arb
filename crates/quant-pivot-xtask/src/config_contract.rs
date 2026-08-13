//! Generated Deploy Config templates and descriptor-backed CI audits.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    path::Path,
};

use anyhow::{Context, Result, ensure};
use quant_pivot_models::{
    config::{
        ArtifactStoreKind, DeployConfig, DeployConfigDescriptor, DeployConfigFieldDescriptor,
        DeployConfigTemplate, DeploySensitivity, DeployValueKind, DomainCacheConfig,
        PolygonRpcEndpoint, secret::SecretText,
    },
    types::DeploymentEnvironment,
};
use serde_json::Value as JsonValue;

const DEVELOPMENT_FILE: &str = "config/quant-pivot.toml";
const PRODUCTION_FILE: &str = "config/quant-pivot.production.example.toml";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateKind {
    Development,
    Production,
}

impl TemplateKind {
    const fn title(self) -> &'static str {
        match self {
            Self::Development => "Development",
            Self::Production => "Production Example",
        }
    }
}

/// Render both descriptor-owned TOML files, or fail when `--check` detects drift.
pub fn render(workspace_root: &Path, check: bool) -> Result<()> {
    let descriptor = audited_descriptor()?;
    for (kind, relative_path) in [
        (TemplateKind::Development, DEVELOPMENT_FILE),
        (TemplateKind::Production, PRODUCTION_FILE),
    ] {
        let rendered = render_template(kind, &descriptor)?;
        audit_text(kind, &rendered, &descriptor)?;
        let path = workspace_root.join(relative_path);
        if check {
            let current = fs::read_to_string(&path)
                .with_context(|| format!("read generated config {}", path.display()))?;
            ensure!(
                current == rendered,
                "{} is not canonical; run `cargo xtask config render`",
                path.display()
            );
        } else {
            fs::write(&path, rendered)
                .with_context(|| format!("write generated config {}", path.display()))?;
            println!("rendered {}", path.display());
        }
    }
    Ok(())
}

/// Audit descriptors, strict parsing, comments, inventory, and generated-file drift.
pub fn audit(workspace_root: &Path) -> Result<()> {
    let descriptor = audited_descriptor()?;
    for (kind, relative_path) in [
        (TemplateKind::Development, DEVELOPMENT_FILE),
        (TemplateKind::Production, PRODUCTION_FILE),
    ] {
        let path = workspace_root.join(relative_path);
        let text = fs::read_to_string(&path)
            .with_context(|| format!("read Deploy Config {}", path.display()))?;
        audit_text(kind, &text, &descriptor)
            .with_context(|| format!("audit Deploy Config {}", path.display()))?;
        let expected = render_template(kind, &descriptor)?;
        ensure!(
            text == expected,
            "{} differs from the descriptor-owned render",
            path.display()
        );
    }
    println!(
        "Deploy Config audit passed: {} descriptors, two strict generated TOML files",
        descriptor.fields.len()
    );
    Ok(())
}

fn audited_descriptor() -> Result<DeployConfigDescriptor> {
    let descriptor = DeployConfigDescriptor::generate();
    let failures = descriptor.audit();
    ensure!(
        failures.is_empty(),
        "Deploy Config descriptor audit failed:\n{}",
        failures.join("\n")
    );
    Ok(descriptor)
}

fn render_template(kind: TemplateKind, descriptor: &DeployConfigDescriptor) -> Result<String> {
    let config = kind.config()?;
    let body = toml::to_string_pretty(&DeployConfigTemplate::from(&config))
        .context("serialize safe Deploy Config template")?;
    let observed = observed_fields(&body, descriptor);
    let mut documented = documented_fields(descriptor, &observed);
    let mut output = String::new();
    output.push_str(&kind.header());
    let mut current_table = String::new();
    let mut active_array = None::<String>;
    for raw_line in body.lines() {
        let line = raw_line.trim();
        if let Some(path) = table_header(line, "[[", "]]") {
            append_documented(kind, &current_table, &mut documented, &mut output);
            current_table = format!("{path}.*");
            active_array = Some(path.to_owned());
            output.push('\n');
            output.push_str(raw_line);
            output.push('\n');
            continue;
        }
        if let Some(path) = table_header(line, "[", "]") {
            append_documented(kind, &current_table, &mut documented, &mut output);
            current_table = array_aware_path(path, active_array.as_deref());
            if !current_table.contains(".*") {
                active_array = None;
            }
            output.push('\n');
            if let Some(field) = find_field(descriptor, &current_table) {
                output.push_str(&field_comment(kind, field));
            }
            output.push_str(raw_line);
            output.push('\n');
            continue;
        }
        if line.is_empty() {
            continue;
        }
        let Some((raw_key, _)) = raw_line.split_once('=') else {
            continue;
        };
        let key = raw_key.trim().trim_matches('"');
        let actual_path = join_path(&current_table, key);
        if let Some(field) = find_field(descriptor, &actual_path) {
            output.push_str(&field_comment(kind, field));
            output.push_str(&assignment(kind, field, raw_line));
        } else {
            output.push_str(&union_assignment(kind, &actual_path, raw_line));
        }
        output.push('\n');
    }
    append_documented(kind, &current_table, &mut documented, &mut output);
    ensure!(
        documented.is_empty(),
        "Deploy Config renderer could not place documented assignments under their owning TOML tables: {}",
        documented.keys().cloned().collect::<Vec<_>>().join(", ")
    );
    append_variant_examples(kind, &mut output);
    Ok(output)
}

fn observed_fields(body: &str, descriptor: &DeployConfigDescriptor) -> BTreeSet<String> {
    let mut observed = BTreeSet::new();
    let mut current_table = String::new();
    let mut active_array = None::<String>;
    for raw_line in body.lines() {
        let line = raw_line.trim();
        if let Some(path) = table_header(line, "[[", "]]") {
            current_table = format!("{path}.*");
            active_array = Some(path.to_owned());
            continue;
        }
        if let Some(path) = table_header(line, "[", "]") {
            current_table = array_aware_path(path, active_array.as_deref());
            if !current_table.contains(".*") {
                active_array = None;
            }
            if let Some(field) = find_field(descriptor, &current_table) {
                observed.insert(field.toml_path.clone());
            }
            continue;
        }
        let Some((raw_key, _)) = raw_line.split_once('=') else {
            continue;
        };
        let key = raw_key.trim().trim_matches('"');
        if let Some(field) = find_field(descriptor, &join_path(&current_table, key)) {
            observed.insert(field.toml_path.clone());
        }
    }
    observed
}

impl TemplateKind {
    fn config(self) -> Result<DeployConfig> {
        let mut config = DeployConfig::default();
        config.cache.domains = HashMap::from([(
            "market".to_owned(),
            DomainCacheConfig {
                timeout_ms: Some(config.cache.operation_timeout_ms),
                fail_open: Some(config.cache.fail_open),
                disabled: false,
            },
        )]);
        if self == Self::Development {
            return Ok(config);
        }
        config.deployment.environment =
            DeploymentEnvironment::parse("production").context("production environment")?;
        config.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::Protected {
            url: SecretText::from("template-only"),
        };
        config.db.postgres.host = String::from("postgres.internal.example.invalid");
        config.db.postgres.user = String::from("quant_pivot_runtime");
        config.db.clickhouse.deployment_id = String::from("production-primary");
        config.db.clickhouse.cluster_id = String::from("clickhouse-service-id");
        config.db.clickhouse.url = String::from("https://clickhouse.internal.example.invalid:8443");
        config.db.clickhouse.user = String::from("quant_pivot_runtime");
        config.cache.redis.host = String::from("redis.internal.example.invalid");
        config.cache.redis.user = String::from("quant_pivot_runtime");
        config.observability.log_json = true;
        config.web.serve_static_ui = true;
        config.quant.account.funder = Some(String::from("REPLACE_WITH_POLYMARKET_FUNDER_ADDRESS"));
        config.keys.private_key = Some(SecretText::from("template-only"));
        config.research.artifact_store.kind = ArtifactStoreKind::S3;
        config.research.artifact_store.bucket = String::from("quant-pivot-production-artifacts");
        config.research.artifact_store.prefix = String::from("evidence/");
        config.research.artifact_store.endpoint =
            Some(String::from("https://s3.internal.example.invalid"));
        config.research.artifact_store.path_style = false;
        config.research.artifact_store.require_object_lock = true;
        config.research.artifact_store.require_versioning = true;
        Ok(config)
    }

    fn header(self) -> String {
        format!(
            "# quant-pivot — Deploy Configuration ({})\n\
# GENERATED BY `cargo xtask config render`; DO NOT EDIT BY HAND.\n\
# Every process-bound field is derived from the Rust DeployConfig descriptor.\n\
# Loading requires `--config-file <absolute-path>` and `--expected-environment <name>`.\n\
# There is no default path, directory discovery, environment source, overlay, or default fill.\n\
# Copy this file to an untracked 0600 file before adding any real secret.\n",
            self.title()
        )
    }
}

fn field_comment(kind: TemplateKind, field: &DeployConfigFieldDescriptor) -> String {
    let requirement = if field.dynamic && field.required {
        "Required within each configured dynamic binding; the binding map may be empty"
    } else if field.required {
        "Required"
    } else {
        "Optional"
    };
    let default = if kind == TemplateKind::Production && field.toml_path == "deployment.environment"
    {
        "\"production\"".to_owned()
    } else {
        field
            .default
            .as_ref()
            .map_or_else(|| "no implicit default".to_owned(), ValueText::render)
    };
    let unit = field.unit.map_or_else(
        || "unitless".to_owned(),
        |unit| format!("{unit:?}").to_lowercase(),
    );
    let bounds = if field.constraints.is_empty() {
        "no additional scalar restriction beyond the serialized Rust type".to_owned()
    } else {
        field.constraints.join(", ")
    };
    let validation = if field.validation_rules.is_empty() {
        "No additional semantic or cross-field rule beyond the scalar and type contract.".to_owned()
    } else {
        field
            .validation_rules
            .iter()
            .map(|rule| {
                format!(
                    "[{}] When {}: {}",
                    rule.rule_id, rule.condition, rule.requirement
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    let variants = if field.variants.is_empty() {
        "not applicable".to_owned()
    } else {
        field.variants.join(", ")
    };
    let sensitivity = match field.sensitivity {
        DeploySensitivity::Public => "public",
        DeploySensitivity::SensitiveEndpoint => "sensitive_endpoint",
        DeploySensitivity::SensitiveIdentifier => "sensitive_identifier",
        DeploySensitivity::Secret => "secret",
    };
    format!(
        "# Field: {}\n# Purpose: {}\n# Requirement: {}; recommended value: {}.\n# Type/unit/range: {:?}; {}; {}.\n# Validation/cross-field contract: {}\n# Operational impact: {}\n# Consumer: {}\n# Apply: process restart required; startup validation is fail-closed.\n# Sensitivity/redaction: {}; deployment projection never exposes protected literals.\n# Variants: {}.\n",
        field.toml_path,
        single_line(&field.purpose),
        requirement,
        default,
        field.value_kind,
        unit,
        bounds,
        single_line(&validation),
        field.operational_impact,
        field.consumer,
        sensitivity,
        variants,
    )
}

struct ValueText;

impl ValueText {
    fn render(value: &JsonValue) -> String {
        let rendered = value.to_string();
        if rendered.len() > 120 {
            "the canonical Rust template value".to_owned()
        } else {
            rendered
        }
    }
}

fn assignment(kind: TemplateKind, field: &DeployConfigFieldDescriptor, original: &str) -> String {
    if kind == TemplateKind::Production
        && field.sensitivity == DeploySensitivity::Secret
        && active_production_secret(&field.toml_path)
    {
        let key = original
            .split_once('=')
            .map_or("value", |(key, _)| key.trim());
        if field.value_kind == DeployValueKind::StringArray {
            return format!("{key} = [\"{}\"]", placeholder(&field.toml_path));
        }
        return format!("{key} = \"{}\"", placeholder(&field.toml_path));
    }
    original.to_owned()
}

fn union_assignment(kind: TemplateKind, actual_path: &str, original: &str) -> String {
    if kind == TemplateKind::Production && actual_path == "polymarket.onchain.rpc_endpoint.url" {
        return "url = \"REPLACE_WITH_AUTHENTICATED_POLYGON_RPC_URL\"".to_owned();
    }
    original.to_owned()
}

fn active_production_secret(path: &str) -> bool {
    matches!(
        path,
        "db.postgres.password"
            | "db.clickhouse.password"
            | "cache.redis.password"
            | "keys.private_key"
            | "web.jwt.signing_key"
            | "research.evidence_attestation.signing_key"
    )
}

fn placeholder(path: &str) -> String {
    format!(
        "REPLACE_WITH_{}",
        path.chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect::<String>()
    )
}

fn documented_fields<'a>(
    descriptor: &'a DeployConfigDescriptor,
    observed: &BTreeSet<String>,
) -> BTreeMap<String, Vec<&'a DeployConfigFieldDescriptor>> {
    let mut documented = BTreeMap::<String, Vec<&DeployConfigFieldDescriptor>>::new();
    for field in descriptor
        .fields
        .iter()
        .filter(|field| !observed.contains(&field.toml_path))
    {
        documented
            .entry(documented_anchor(&field.toml_path))
            .or_default()
            .push(field);
    }
    documented
}

fn append_documented(
    kind: TemplateKind,
    current_table: &str,
    documented: &mut BTreeMap<String, Vec<&DeployConfigFieldDescriptor>>,
    output: &mut String,
) {
    let Some(fields) = documented.remove(current_table) else {
        return;
    };
    let mut dynamic = BTreeMap::<String, Vec<&DeployConfigFieldDescriptor>>::new();
    for field in fields.iter().filter(|field| !field.dynamic) {
        output.push_str("\n# Optional assignment in this owning table. Uncomment only after satisfying its cross-field contract.\n");
        output.push_str(&field_comment(kind, field));
        let key = leaf_key(&field.toml_path);
        let value = optional_value(kind, field);
        output.push_str("# ");
        output.push_str(key);
        output.push_str(" = ");
        output.push_str(&value);
        output.push('\n');
    }
    for field in fields.into_iter().filter(|field| field.dynamic) {
        dynamic
            .entry(documented_table(&field.toml_path))
            .or_default()
            .push(field);
    }
    for (table, fields) in dynamic {
        output.push_str(
            "\n# Canonical dynamic binding. Uncomment this table header and every required assignment in the block as one unit.\n",
        );
        output.push_str("# [");
        output.push_str(&table);
        output.push_str("]\n");
        for field in fields {
            output.push_str(&field_comment(kind, field));
            let key = leaf_key(&field.toml_path);
            let value = optional_value(kind, field);
            output.push_str("# ");
            output.push_str(key);
            output.push_str(" = ");
            output.push_str(&value);
            output.push('\n');
        }
    }
}

fn optional_value(kind: TemplateKind, field: &DeployConfigFieldDescriptor) -> String {
    if field.sensitivity == DeploySensitivity::Secret {
        return format!("\"{}\"", placeholder(&field.toml_path));
    }
    match field.toml_path.as_str() {
        "domain_sources.chainlink_data_streams.feeds.*.decimals" => return "8".to_owned(),
        "domain_sources.chainlink_data_streams.feeds.*.feed_id" => {
            return "\"REPLACE_WITH_SUBSCRIPTION_V3_FEED_ID\"".to_owned();
        }
        "polymarket.relayer.api_key_address" => {
            return "\"REPLACE_WITH_RELAYER_SIGNER_EOA_ADDRESS\"".to_owned();
        }
        "quant.account.funder" => {
            return "\"REPLACE_WITH_POLYMARKET_FUNDER_ADDRESS\"".to_owned();
        }
        "research.artifact_store.endpoint" => {
            return if kind == TemplateKind::Production {
                "\"https://s3.internal.example.invalid\"".to_owned()
            } else {
                "\"http://127.0.0.1:9000\"".to_owned()
            };
        }
        _ => {}
    }
    if let Some(value) = field.example.as_ref().or(field.default.as_ref()) {
        return value.to_string();
    }
    match field.value_kind {
        DeployValueKind::Boolean => "false".to_owned(),
        DeployValueKind::Integer | DeployValueKind::Decimal => "1".to_owned(),
        DeployValueKind::StringArray | DeployValueKind::ScalarArray => "[]".to_owned(),
        DeployValueKind::Variant => "{}".to_owned(),
        DeployValueKind::String => "\"REPLACE_WITH_VALUE\"".to_owned(),
    }
}

fn documented_anchor(path: &str) -> String {
    let segments = path.split('.').collect::<Vec<_>>();
    if let Some(wildcard) = segments.iter().position(|segment| *segment == "*") {
        return segments[..wildcard.saturating_sub(1)].join(".");
    }
    segments[..segments.len().saturating_sub(1)].join(".")
}

fn documented_table(path: &str) -> String {
    path.split('.')
        .take(path.split('.').count().saturating_sub(1))
        .map(|segment| {
            if segment == "*" {
                "\"example\"".to_owned()
            } else {
                segment.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn leaf_key(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

fn append_variant_examples(kind: TemplateKind, output: &mut String) {
    output.push_str("\n# Complete mutually exclusive tagged-union examples\n");
    output.push_str("# Select exactly one Polygon RPC source shape:\n");
    output.push_str("# polymarket.onchain.rpc_endpoint = { source = \"public\", url = \"https://polygon-rpc.com\" }\n");
    let protected = if kind == TemplateKind::Production {
        "REPLACE_WITH_AUTHENTICATED_POLYGON_RPC_URL"
    } else {
        "https://provider.example.invalid/credential"
    };
    output.push_str("# polymarket.onchain.rpc_endpoint = { source = \"protected\", url = \"");
    output.push_str(protected);
    output.push_str("\" }\n");
    output.push_str(
        "# Tornado scope variants are both instantiated in the canonical binding list:\n",
    );
    output.push_str("# scope = { kind = \"united_states\" }\n");
    output.push_str(
        "# scope = { kind = \"state\", spc_state_code = \"OK\", ncei_state_name = \"OKLAHOMA\" }\n",
    );
    output.push_str(
        "# Chainlink feed identifiers are subscription-specific; no fake fallback is active:\n",
    );
    output.push_str("# [domain_sources.chainlink_data_streams.feeds.\"BTC-USD\"]\n");
    output.push_str("# feed_id = \"REPLACE_WITH_SUBSCRIPTION_V3_FEED_ID\"\n");
    output.push_str("# decimals = 8\n");
}

fn audit_text(_kind: TemplateKind, text: &str, descriptor: &DeployConfigDescriptor) -> Result<()> {
    ensure!(
        !text.contains("quant-pivot.local.toml")
            && !text.contains("QUANT_PIVOT_CONFIG_DIR")
            && !text.contains("--config-dir"),
        "generated config contains a deleted loading path"
    );
    toml::from_str::<DeployConfig>(text).context("strictly parse generated Deploy Config")?;
    let mut markers = BTreeMap::<&str, usize>::new();
    for line in text.lines() {
        if let Some(path) = line.strip_prefix("# Field: ") {
            *markers.entry(path).or_default() += 1;
        }
    }
    for field in &descriptor.fields {
        let count = markers.get(field.toml_path.as_str()).copied().unwrap_or(0);
        if field.dynamic {
            ensure!(
                count > 0,
                "dynamic descriptor {} has no example",
                field.toml_path
            );
        } else {
            ensure!(
                count == 1,
                "static descriptor {} appears {count} times",
                field.toml_path
            );
        }
    }
    for path in markers.keys() {
        ensure!(
            descriptor
                .fields
                .iter()
                .any(|field| field.toml_path == *path),
            "rendered comment references unknown descriptor {path}"
        );
    }
    ensure!(
        text.ends_with('\n'),
        "generated TOML must end with a newline"
    );
    Ok(())
}

fn table_header<'a>(line: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    line.strip_prefix(prefix)?.strip_suffix(suffix)
}

fn array_aware_path(path: &str, active_array: Option<&str>) -> String {
    if let Some(array) = active_array
        && let Some(suffix) = path.strip_prefix(array)
        && (suffix.is_empty() || suffix.starts_with('.'))
    {
        return format!("{array}.*{suffix}");
    }
    path.to_owned()
}

fn join_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_owned()
    } else {
        format!("{parent}.{child}")
    }
}

fn find_field<'a>(
    descriptor: &'a DeployConfigDescriptor,
    actual_path: &str,
) -> Option<&'a DeployConfigFieldDescriptor> {
    descriptor.fields.iter().find(|field| {
        let expected = field.toml_path.split('.').collect::<Vec<_>>();
        let actual = actual_path.split('.').collect::<Vec<_>>();
        expected.len() == actual.len()
            && expected
                .iter()
                .zip(actual)
                .all(|(left, right)| *left == "*" || *left == right)
    })
}

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use quant_pivot_models::config::DeployConfig;

    use super::{TemplateKind, audit_text, audited_descriptor, render_template};

    #[test]
    fn both_templates_are_auditable() {
        let descriptor = audited_descriptor().expect("valid descriptor");
        for kind in [TemplateKind::Development, TemplateKind::Production] {
            let rendered = render_template(kind, &descriptor).expect("render template");
            audit_text(kind, &rendered, &descriptor).expect("audit template");
        }
    }

    #[test]
    fn documented_assignments_parse_tables() {
        let descriptor = audited_descriptor().expect("valid descriptor");
        let documented = descriptor
            .fields
            .iter()
            .filter(|field| field.default.is_none())
            .map(|field| field.toml_path.as_str())
            .collect::<BTreeSet<_>>();
        let rendered = render_template(TemplateKind::Development, &descriptor)
            .expect("render development template");
        let mut active_field = None::<&str>;
        let activated = rendered
            .lines()
            .map(|line| {
                if line == "# [domain_sources.chainlink_data_streams.feeds.\"example\"]" {
                    return line.trim_start_matches("# ").to_owned();
                }
                if let Some(path) = line.strip_prefix("# Field: ") {
                    active_field = documented.contains(path).then_some(path);
                    return line.to_owned();
                }
                if active_field.is_some()
                    && line.starts_with("# ")
                    && line.contains(" = ")
                    && !line.contains(": ")
                {
                    active_field = None;
                    return line.trim_start_matches("# ").to_owned();
                }
                line.to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n");
        toml::from_str::<DeployConfig>(&activated)
            .expect("every documented optional assignment must parse in its owning table");
        assert!(!rendered.contains("# keys.private_key ="));
        assert!(rendered.contains("[keys]\n"));
        assert!(rendered.contains("# private_key = \"REPLACE_WITH_KEYS_PRIVATE_KEY\""));
    }
}
