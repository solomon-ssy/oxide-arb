//! Strongly typed Deploy Config metadata shared by rendering, audit, and projections.

use std::collections::{BTreeMap, BTreeSet};

use schemars::{JsonSchema, Schema, SchemaGenerator, generate::SchemaSettings};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use toml::Value as TomlValue;

use super::{
    DeployConfig, DeployConfigTemplate,
    validation_contract::{DeployValidationRuleContract, DeployValidationRuleDescriptor},
};

/// The audited leaf count at the clean-break descriptor boundary.
pub const DEPLOY_CONFIG_EXPECTED_LEAF_COUNT: usize = 315;

/// Every `SecretText` descriptor path. New secret fields must update this exhaustive set.
pub const DEPLOY_SECRET_PATHS: [&str; 13] = [
    "cache.redis.password",
    "db.clickhouse.password",
    "db.postgres.password",
    "domain_sources.chainlink_data_streams.api_key",
    "domain_sources.chainlink_data_streams.api_secret",
    "keys.private_key",
    "notifications.telegram.bot_token",
    "notifications.webhook.authorization",
    "notifications.webhook.url",
    "polymarket.relayer.api_key",
    "research.evidence_attestation.previous_signing_keys",
    "research.evidence_attestation.signing_key",
    "web.jwt.signing_key",
];

/// TOML value category exposed to documentation and inventory tooling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeployValueKind {
    Boolean,
    Integer,
    Decimal,
    String,
    StringArray,
    ScalarArray,
    Variant,
}

/// Unit rendered in generated comments and safe deployment projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeployFieldUnit {
    BasisPoints,
    Blocks,
    Bytes,
    ChainId,
    Count,
    Days,
    DecimalPlaces,
    Degrees,
    Hours,
    Index,
    Meters,
    Milliseconds,
    Months,
    Percent,
    Port,
    Ratio,
    RequestWeightPerMinute,
    Seconds,
    Shares,
    Usd,
    Years,
}

/// Confidentiality policy for one deploy leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploySensitivity {
    Public,
    SensitiveEndpoint,
    SensitiveIdentifier,
    Secret,
}

/// Apply semantics for static process configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeployApplyEffect {
    ProcessRestartRequired,
}

/// Exact numeric constraints represented without a floating-point conversion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployFieldBounds {
    pub minimum: Option<String>,
    pub exclusive_minimum: Option<String>,
    pub maximum: Option<String>,
    pub exclusive_maximum: Option<String>,
}

/// Complete metadata for one static leaf or one wildcard dynamic-binding leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployConfigFieldDescriptor {
    pub toml_path: String,
    pub json_pointer: String,
    pub title: String,
    pub purpose: String,
    pub value_kind: DeployValueKind,
    pub unit: Option<DeployFieldUnit>,
    pub required: bool,
    pub default: Option<Value>,
    pub example: Option<Value>,
    pub bounds: DeployFieldBounds,
    pub enum_values: Vec<String>,
    pub variants: Vec<String>,
    pub dynamic: bool,
    pub sensitivity: DeploySensitivity,
    pub operational_impact: String,
    pub consumer: String,
    pub apply_effect: DeployApplyEffect,
    pub constraints: Vec<String>,
    pub validation_rules: Vec<DeployValidationRuleDescriptor>,
    pub documentation_url: String,
}

/// Canonical descriptor inventory for the complete Deploy Config tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployConfigDescriptor {
    pub fields: Vec<DeployConfigFieldDescriptor>,
}

impl DeployConfigDescriptor {
    /// Generate the one canonical descriptor inventory from the Rust type graph.
    #[must_use]
    pub fn generate() -> Self {
        let settings = SchemaSettings::default().with(|settings| settings.inline_subschemas = true);
        let schema = SchemaGenerator::new(settings).into_root_schema_for::<DeployConfig>();
        let mut descriptor = Self::from_schema(&schema);
        let defaults = DeployConfig::default();
        if let Ok(template) = TomlValue::try_from(DeployConfigTemplate::from(&defaults)) {
            descriptor.attach_defaults(&template, "");
        }
        descriptor
    }

    /// Generate a descriptor from an inline schema for deterministic tests.
    #[must_use]
    pub fn from_schema(schema: &Schema) -> Self {
        let mut collector = DeployDescriptorCollector::default();
        collector.collect_node(schema.as_value(), "", true, &[]);
        let mut fields = collector.fields.into_values().collect::<Vec<_>>();
        fields.sort_by(|left, right| left.toml_path.cmp(&right.toml_path));
        Self { fields }
    }

    /// Return all structural or metadata failures consumed by `cargo xtask config audit`.
    #[must_use]
    pub fn audit(&self) -> Vec<String> {
        let mut failures = Vec::new();
        if self.fields.len() != DEPLOY_CONFIG_EXPECTED_LEAF_COUNT {
            failures.push(format!(
                "Deploy Config descriptor leaf count changed: expected {DEPLOY_CONFIG_EXPECTED_LEAF_COUNT}, found {}",
                self.fields.len()
            ));
        }
        let mut paths = BTreeSet::new();
        let mut rule_ids = BTreeSet::new();
        for rule in DeployValidationRuleContract::ALL {
            if !rule_ids.insert(rule.rule_id) {
                failures.push(format!(
                    "duplicate Deploy Config validation rule `{}`",
                    rule.rule_id
                ));
            }
            if rule.condition.trim().is_empty() || rule.requirement.trim().is_empty() {
                failures.push(format!(
                    "{} has incomplete validation metadata",
                    rule.rule_id
                ));
            }
            for scope in rule.scopes {
                if !self.fields.iter().any(|field| {
                    DeployValidationRuleContract::scope_matches(scope, &field.toml_path)
                }) {
                    failures.push(format!(
                        "{} scope `{scope}` matches no Deploy Config descriptor",
                        rule.rule_id
                    ));
                }
            }
        }
        for field in &self.fields {
            if field.toml_path.trim().is_empty() {
                failures.push("deploy descriptor contains an empty TOML path".to_owned());
            }
            if field.json_pointer.is_empty() || !field.json_pointer.starts_with('/') {
                failures.push(format!("{} has an invalid JSON pointer", field.toml_path));
            }
            if !paths.insert(field.toml_path.as_str()) {
                failures.push(format!(
                    "duplicate Deploy Config path `{}`",
                    field.toml_path
                ));
            }
            if field.title.trim().is_empty() || field.purpose.trim().is_empty() {
                failures.push(format!(
                    "{} has incomplete purpose metadata",
                    field.toml_path
                ));
            }
            if field.purpose.contains("Configure `") || field.purpose.len() < 32 {
                failures.push(format!(
                    "{} has non-semantic purpose metadata: {}",
                    field.toml_path, field.purpose
                ));
            }
            if field.operational_impact.trim().is_empty() || field.consumer.trim().is_empty() {
                failures.push(format!(
                    "{} has incomplete consumer metadata",
                    field.toml_path
                ));
            }
            if field.documentation_url.trim().is_empty() {
                failures.push(format!("{} has no documentation URL", field.toml_path));
            }
            if matches!(
                field.value_kind,
                DeployValueKind::Integer | DeployValueKind::Decimal
            ) && field.unit.is_none()
            {
                failures.push(format!(
                    "{} is numeric but has no explicit unit contract",
                    field.toml_path
                ));
            }
            if field.sensitivity == DeploySensitivity::Secret
                && !field.constraints.iter().any(|value| value == "write_only")
            {
                failures.push(format!("{} secret is not write-only", field.toml_path));
            }
        }
        let descriptor_secrets = self
            .fields
            .iter()
            .filter(|field| field.sensitivity == DeploySensitivity::Secret)
            .map(|field| field.toml_path.as_str())
            .collect::<BTreeSet<_>>();
        let declared_secrets = DEPLOY_SECRET_PATHS.into_iter().collect::<BTreeSet<_>>();
        if descriptor_secrets != declared_secrets {
            failures.push(format!(
                "secret descriptor inventory mismatch: descriptor={descriptor_secrets:?}, declared={declared_secrets:?}"
            ));
        }
        failures
    }

    fn attach_defaults(&mut self, value: &TomlValue, path: &str) {
        if let Some(index) = self.field_index(path)
            && self.fields[index].value_kind == DeployValueKind::Variant
        {
            if let Ok(value) = serde_json::to_value(value) {
                self.fields[index].default = Some(value.clone());
                self.fields[index].example = Some(value);
            }
            return;
        }
        match value {
            TomlValue::Table(table) => {
                for (name, child) in table {
                    let child_path = Self::join_path(path, name);
                    self.attach_defaults(child, &child_path);
                }
            }
            TomlValue::Array(values)
                if values
                    .iter()
                    .all(|value| matches!(value, TomlValue::Table(_))) =>
            {
                for child in values {
                    let child_path = Self::join_path(path, "*");
                    self.attach_defaults(child, &child_path);
                }
            }
            _ => {
                if let Some(index) = self.field_index(path)
                    && self.fields[index].default.is_none()
                    && let Ok(value) = serde_json::to_value(value)
                {
                    self.fields[index].default = Some(value.clone());
                    self.fields[index].example = Some(value);
                }
            }
        }
    }

    fn field_index(&self, actual_path: &str) -> Option<usize> {
        self.fields
            .iter()
            .position(|field| Self::path_matches(&field.toml_path, actual_path))
    }

    fn path_matches(pattern: &str, actual: &str) -> bool {
        let pattern = pattern.split('.').collect::<Vec<_>>();
        let actual = actual.split('.').collect::<Vec<_>>();
        pattern.len() == actual.len()
            && pattern
                .iter()
                .zip(actual)
                .all(|(expected, value)| *expected == "*" || *expected == value)
    }

    fn join_path(parent: &str, child: &str) -> String {
        if parent.is_empty() {
            child.to_owned()
        } else {
            format!("{parent}.{child}")
        }
    }
}

#[derive(Default)]
struct DeployDescriptorCollector {
    fields: BTreeMap<String, DeployConfigFieldDescriptor>,
}

impl DeployDescriptorCollector {
    fn collect_node(&mut self, node: &Value, pointer: &str, required: bool, variants: &[String]) {
        let Some(schema) = node.as_object() else {
            return;
        };
        if let Some(branches) = Self::branches(schema) {
            if branches.len() == 1 {
                self.collect_node(branches[0], pointer, required, variants);
                return;
            }
            if !pointer.is_empty() {
                let mut branch_variants = variants.to_vec();
                branch_variants.extend(
                    branches
                        .iter()
                        .enumerate()
                        .map(|(index, branch)| Self::variant_name(branch, index)),
                );
                self.insert_leaf(schema, pointer, required, &branch_variants);
            }
            return;
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            let required_names = Self::required_names(schema);
            for (name, child) in properties {
                let child_pointer = Self::child_pointer(pointer, name);
                self.collect_node(
                    child,
                    &child_pointer,
                    required_names.contains(name.as_str()),
                    variants,
                );
            }
            return;
        }
        if let Some(items) = schema.get("items")
            && Self::is_structured(items)
        {
            self.collect_node(items, &Self::child_pointer(pointer, "*"), true, variants);
            return;
        }
        if let Some(additional) = schema.get("additionalProperties")
            && Self::is_structured(additional)
        {
            self.collect_node(
                additional,
                &Self::child_pointer(pointer, "*"),
                true,
                variants,
            );
            return;
        }
        if pointer.is_empty() || Self::is_null(schema) {
            return;
        }
        self.insert_leaf(schema, pointer, required, variants);
    }

    fn insert_leaf(
        &mut self,
        schema: &Map<String, Value>,
        pointer: &str,
        required: bool,
        variants: &[String],
    ) {
        let toml_path = Self::toml_path(pointer);
        let descriptor = self
            .fields
            .entry(toml_path)
            .or_insert_with(|| Self::leaf_descriptor(schema, pointer, required, variants));
        descriptor.required &= required;
        descriptor.variants.extend(variants.iter().cloned());
        descriptor.variants.sort();
        descriptor.variants.dedup();
    }

    fn leaf_descriptor(
        schema: &Map<String, Value>,
        pointer: &str,
        required: bool,
        variants: &[String],
    ) -> DeployConfigFieldDescriptor {
        let toml_path = Self::toml_path(pointer);
        let title = schema
            .get("title")
            .and_then(Value::as_str)
            .map_or_else(|| Self::path_title(&toml_path), str::to_owned);
        let purpose = schema
            .get("description")
            .and_then(Value::as_str)
            .map_or_else(String::new, str::to_owned);
        let sensitivity = Self::sensitivity(schema, &toml_path);
        let mut constraints = Self::constraints(schema);
        if sensitivity == DeploySensitivity::Secret {
            constraints.push("write_only".to_owned());
        }
        DeployConfigFieldDescriptor {
            toml_path: toml_path.clone(),
            json_pointer: pointer.to_owned(),
            title,
            purpose,
            value_kind: Self::value_kind(schema, &toml_path),
            unit: Self::unit(&toml_path),
            required,
            default: schema.get("default").cloned(),
            example: schema
                .get("examples")
                .and_then(Value::as_array)
                .and_then(|examples| examples.first())
                .cloned()
                .or_else(|| schema.get("default").cloned()),
            bounds: Self::bounds(schema),
            enum_values: Self::enum_values(schema),
            variants: variants.to_vec(),
            dynamic: pointer.split('/').any(|segment| segment == "*"),
            sensitivity,
            operational_impact: Self::operational_impact(&toml_path),
            consumer: Self::consumer(&toml_path).to_owned(),
            apply_effect: DeployApplyEffect::ProcessRestartRequired,
            constraints,
            validation_rules: DeployValidationRuleContract::ALL
                .iter()
                .copied()
                .filter(|rule| rule.applies_to(&toml_path))
                .map(DeployValidationRuleContract::descriptor)
                .collect(),
            documentation_url:
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#4-deploy-config-descriptor"
                    .to_owned(),
        }
    }

    fn branches(schema: &Map<String, Value>) -> Option<Vec<&Value>> {
        let branches = schema
            .get("oneOf")
            .or_else(|| schema.get("anyOf"))?
            .as_array()?;
        let non_null = branches
            .iter()
            .filter(|branch| !branch.as_object().is_some_and(Self::is_null))
            .collect::<Vec<_>>();
        (!non_null.is_empty()).then_some(non_null)
    }

    fn variant_name(branch: &Value, index: usize) -> String {
        let object = branch.as_object();
        object
            .and_then(|schema| schema.get("properties"))
            .and_then(Value::as_object)
            .and_then(|properties| {
                properties.values().find_map(|property| {
                    property
                        .as_object()
                        .and_then(|schema| schema.get("const"))
                        .and_then(Value::as_str)
                })
            })
            .or_else(|| {
                object
                    .and_then(|schema| schema.get("title"))
                    .and_then(Value::as_str)
            })
            .map_or_else(|| format!("variant_{index}"), str::to_owned)
    }

    fn is_structured(value: &Value) -> bool {
        value.as_object().is_some_and(|schema| {
            schema.contains_key("properties")
                || schema.contains_key("oneOf")
                || schema.contains_key("anyOf")
                || schema.contains_key("additionalProperties")
                || schema.get("type").and_then(Value::as_str) == Some("object")
        })
    }

    fn is_null(schema: &Map<String, Value>) -> bool {
        schema.get("type").and_then(Value::as_str) == Some("null")
    }

    fn required_names(schema: &Map<String, Value>) -> BTreeSet<&str> {
        schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect()
    }

    fn child_pointer(parent: &str, name: &str) -> String {
        let escaped = name.replace('~', "~0").replace('/', "~1");
        format!("{parent}/{escaped}")
    }

    fn toml_path(pointer: &str) -> String {
        pointer
            .trim_start_matches('/')
            .split('/')
            .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
            .collect::<Vec<_>>()
            .join(".")
    }

    fn root(path: &str) -> &str {
        path.split('.').next().unwrap_or("deploy")
    }

    fn path_title(path: &str) -> String {
        path.rsplit('.')
            .next()
            .unwrap_or(path)
            .split('_')
            .filter(|part| !part.is_empty() && *part != "*")
            .map(|part| {
                let mut chars = part.chars();
                chars.next().map_or_else(String::new, |first| {
                    first.to_uppercase().collect::<String>() + chars.as_str()
                })
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn operational_impact(path: &str) -> String {
        format!(
            "Changes {}; the process must restart and pass fail-closed startup validation before the new value is consumed.",
            Self::consumer(path)
        )
    }

    fn value_kind(schema: &Map<String, Value>, path: &str) -> DeployValueKind {
        if schema.contains_key("oneOf") || schema.contains_key("anyOf") {
            return DeployValueKind::Variant;
        }
        if matches!(
            path.rsplit('.').next(),
            Some("latitude" | "longitude" | "elevation_meters")
        ) {
            return DeployValueKind::Decimal;
        }
        match Self::schema_type(schema) {
            Some("boolean") => DeployValueKind::Boolean,
            Some("integer") => DeployValueKind::Integer,
            Some("number") => DeployValueKind::Decimal,
            Some("array") => {
                let item_type = schema
                    .get("items")
                    .and_then(Value::as_object)
                    .and_then(Self::schema_type);
                if item_type == Some("string") {
                    DeployValueKind::StringArray
                } else {
                    DeployValueKind::ScalarArray
                }
            }
            _ => DeployValueKind::String,
        }
    }

    fn schema_type(schema: &Map<String, Value>) -> Option<&str> {
        let schema_type = schema.get("type")?;
        if let Some(value) = schema_type.as_str() {
            return Some(value);
        }
        let mut non_null = schema_type
            .as_array()?
            .iter()
            .filter_map(Value::as_str)
            .filter(|value| *value != "null");
        let value = non_null.next()?;
        non_null.next().is_none().then_some(value)
    }

    fn unit(path: &str) -> Option<DeployFieldUnit> {
        let leaf = path.rsplit('.').next().unwrap_or(path);
        if leaf == "chain_id" {
            Some(DeployFieldUnit::ChainId)
        } else if leaf == "decimals" {
            Some(DeployFieldUnit::DecimalPlaces)
        } else if matches!(leaf, "latitude" | "longitude") {
            Some(DeployFieldUnit::Degrees)
        } else if leaf == "database" && path == "cache.redis.database" {
            Some(DeployFieldUnit::Index)
        } else if leaf == "weight_budget_per_min" {
            Some(DeployFieldUnit::RequestWeightPerMinute)
        } else if leaf == "size_threshold" && path == "market_data.data_api.size_threshold" {
            Some(DeployFieldUnit::Shares)
        } else if Self::block_unit(leaf) {
            Some(DeployFieldUnit::Blocks)
        } else if leaf.ends_with("_bps") {
            Some(DeployFieldUnit::BasisPoints)
        } else if leaf.ends_with("_bytes") || leaf.ends_with("_working_set_bytes") {
            Some(DeployFieldUnit::Bytes)
        } else if leaf.ends_with("_ms") {
            Some(DeployFieldUnit::Milliseconds)
        } else if leaf.ends_with("_secs") {
            Some(DeployFieldUnit::Seconds)
        } else if leaf.ends_with("_hours") {
            Some(DeployFieldUnit::Hours)
        } else if leaf.ends_with("_days") {
            Some(DeployFieldUnit::Days)
        } else if leaf.ends_with("_months") {
            Some(DeployFieldUnit::Months)
        } else if leaf.ends_with("_years") {
            Some(DeployFieldUnit::Years)
        } else if leaf.ends_with("_meters") {
            Some(DeployFieldUnit::Meters)
        } else if leaf.ends_with("_usd") {
            Some(DeployFieldUnit::Usd)
        } else if leaf.ends_with("_pct") {
            Some(DeployFieldUnit::Percent)
        } else if leaf.ends_with("_ratio") {
            Some(DeployFieldUnit::Ratio)
        } else if leaf == "port" || leaf.ends_with("_port") {
            Some(DeployFieldUnit::Port)
        } else if Self::count_unit(leaf) {
            Some(DeployFieldUnit::Count)
        } else {
            None
        }
    }

    fn block_unit(leaf: &str) -> bool {
        matches!(leaf, "confirmations" | "external_scan_block_span")
            || leaf.contains("blocks_per_")
            || leaf.ends_with("_block_span")
    }

    fn count_unit(leaf: &str) -> bool {
        matches!(leaf, "threads" | "max_top_n")
            || leaf.contains("concurrency")
            || leaf.contains("capacity")
            || matches!(leaf, "batch_size" | "page_size")
            || leaf.ends_with("_batch_size")
            || leaf.ends_with("_page_size")
            || leaf.ends_with("_limit")
            || leaf.ends_with("_count")
            || leaf.ends_with("_connections")
            || leaf.ends_with("_contracts")
            || leaf.ends_with("_in_flight")
            || leaf.ends_with("_inserts")
            || leaf.ends_with("_loads")
            || leaf.ends_with("_markets")
            || leaf.ends_with("_observations")
            || leaf.ends_with("_pages")
            || leaf.ends_with("_requests")
            || leaf.ends_with("_rows")
            || leaf.ends_with("_samples")
            || leaf.ends_with("_scenarios")
            || leaf.ends_with("_slices")
            || leaf.ends_with("_subscriptions")
            || leaf.ends_with("_threads")
            || leaf.ends_with("_tiers")
            || leaf.ends_with("_tokens")
            || leaf.ends_with("_attempts")
            || matches!(
                leaf,
                "max_claims_per_tick"
                    | "max_spine_samples"
                    | "max_subscriptions_per_connection"
                    | "pool_size"
            )
    }

    fn sensitivity(schema: &Map<String, Value>, path: &str) -> DeploySensitivity {
        if schema
            .get("writeOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || matches!(
                path,
                "polymarket.onchain.rpc_endpoint.url"
                    | "polymarket.relayer.api_key"
                    | "domain_sources.chainlink_data_streams.api_key"
                    | "domain_sources.chainlink_data_streams.api_secret"
                    | "notifications.telegram.bot_token"
                    | "notifications.webhook.url"
                    | "notifications.webhook.authorization"
                    | "db.postgres.password"
                    | "db.clickhouse.password"
                    | "cache.redis.password"
                    | "keys.private_key"
                    | "web.jwt.signing_key"
                    | "research.evidence_attestation.signing_key"
                    | "research.evidence_attestation.previous_signing_keys"
            )
        {
            DeploySensitivity::Secret
        } else if path == "polymarket.onchain.rpc_endpoint"
            || path.ends_with("host")
            || path.ends_with("url")
            || path.ends_with("endpoint")
            || path.contains("_url")
        {
            DeploySensitivity::SensitiveEndpoint
        } else if path.rsplit('.').next() == Some("user")
            || path.ends_with(".api_key_address")
            || path.ends_with(".chat_id")
            || path.ends_with(".funder")
            || path.ends_with(".deployment_id")
            || path.ends_with(".cluster_id")
            || path.ends_with(".issuer")
            || path.ends_with(".audience")
            || path.ends_with(".cors_allowed_origins")
            || path == "research.artifact_store.bucket"
            || path == "research.artifact_store.prefix"
        {
            DeploySensitivity::SensitiveIdentifier
        } else {
            DeploySensitivity::Public
        }
    }

    fn consumer(path: &str) -> &'static str {
        if path.starts_with("quant.portfolio_solver.") {
            "quant-pivot-research global HiGHS MILP optimizer and exact post-solve verifier"
        } else if path.starts_with("quant.research_jobs.") {
            "quant-pivot-core durable research-job workers and 15-stage feedback coordinator"
        } else if path.starts_with("quant.workers.") {
            "quant-pivot-core bounded report, reconciliation, and execution workers"
        } else if path.starts_with("quant.account.") {
            "quant-pivot-core credential-gated venue account snapshot and report capital freeze"
        } else if path.starts_with("research.artifact_store.") {
            "quant-pivot-research immutable evidence and model artifact store"
        } else if path.starts_with("research.evidence_attestation.") {
            "quant-pivot-research evidence signing and verification boundary"
        } else if path.starts_with("research.model_serving_registry.") {
            "quant-pivot-research route-owned champion and shadow model registry"
        } else if path.starts_with("polymarket.relayer.") {
            "quant-pivot-api Polymarket relayer authentication and submission adapter"
        } else if path.starts_with("polymarket.onchain.") {
            "quant-pivot-api Polygon settlement and on-chain evidence adapter"
        } else if path.starts_with("polymarket.") {
            "quant-pivot-api Polymarket CLOB, Gamma, WebSocket, and settlement clients"
        } else if path.starts_with("market_data.") {
            "quant-pivot-core market-data ingestion, durability, and reconciliation pipeline"
        } else if path.starts_with("domain_sources.") {
            "quant-pivot-api typed external-domain adapter and quant-pivot-research PIT feature builder"
        } else if path.starts_with("db.postgres.") {
            "quant-pivot-storage PostgreSQL transaction and repository pools"
        } else if path.starts_with("db.clickhouse.") {
            "quant-pivot-storage ClickHouse fact, lineage, and analytics pools"
        } else if path.starts_with("cache.redis.") {
            "quant-pivot-storage Redis cache and quant-pivot-web revocation store"
        } else if path.starts_with("cache.") {
            "quant-pivot-storage bounded cache facade and domain cache policies"
        } else if path.starts_with("notifications.telegram.") {
            "quant-pivot-core Telegram operator notification adapter"
        } else if path.starts_with("notifications.webhook.") {
            "quant-pivot-core authenticated operator webhook adapter"
        } else if path.starts_with("web.jwt.") || path.starts_with("web.password_crypto.") {
            "quant-pivot-web authentication, token verification, and password-crypto boundary"
        } else {
            match Self::root(path) {
                "deployment" => "quant-pivot-models secure loader and destructive-operation guards",
                "observability" => "quant-pivot-bin tracing and metrics bootstrap",
                "notifications" => "quant-pivot-core operator notification dispatcher",
                "db" => "quant-pivot-storage database bootstrap",
                "keys" => "quant-pivot-api keystore and venue identity bootstrap",
                "web" => "quant-pivot-web HTTP, WebSocket, authorization, and static UI server",
                "quant" => "quant-pivot-core process-bound quant runtime",
                "research" => "quant-pivot-research process-bound research runtime",
                _ => "quant-pivot process bootstrap",
            }
        }
    }

    fn bounds(schema: &Map<String, Value>) -> DeployFieldBounds {
        DeployFieldBounds {
            minimum: schema.get("minimum").map(Self::scalar),
            exclusive_minimum: schema.get("exclusiveMinimum").map(Self::scalar),
            maximum: schema.get("maximum").map(Self::scalar),
            exclusive_maximum: schema.get("exclusiveMaximum").map(Self::scalar),
        }
    }

    fn enum_values(schema: &Map<String, Value>) -> Vec<String> {
        let mut values = schema
            .get("enum")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(Self::scalar)
            .collect::<Vec<_>>();
        if let Some(value) = schema.get("const") {
            values.push(Self::scalar(value));
        }
        values.sort();
        values.dedup();
        values
    }

    fn constraints(schema: &Map<String, Value>) -> Vec<String> {
        let bounds = Self::bounds(schema);
        let mut values = Vec::new();
        if let Some(minimum) = bounds.minimum {
            values.push(format!("minimum={minimum}"));
        }
        if let Some(minimum) = bounds.exclusive_minimum {
            values.push(format!("exclusive_minimum={minimum}"));
        }
        if let Some(maximum) = bounds.maximum {
            values.push(format!("maximum={maximum}"));
        }
        if let Some(maximum) = bounds.exclusive_maximum {
            values.push(format!("exclusive_maximum={maximum}"));
        }
        let enum_values = Self::enum_values(schema);
        if !enum_values.is_empty() {
            values.push(format!("enum={}", enum_values.join("|")));
        }
        values
    }

    fn scalar(value: &Value) -> String {
        value
            .as_str()
            .map_or_else(|| value.to_string(), str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEPLOY_CONFIG_EXPECTED_LEAF_COUNT, DeployConfigDescriptor, DeployFieldUnit, DeployValueKind,
    };

    #[test]
    fn descriptor_inventory_is_complete() {
        let descriptor = DeployConfigDescriptor::generate();
        assert_eq!(descriptor.audit(), Vec::<String>::new());
        if descriptor.fields.len() != DEPLOY_CONFIG_EXPECTED_LEAF_COUNT {
            eprintln!(
                "{}",
                descriptor
                    .fields
                    .iter()
                    .map(|field| field.toml_path.as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        assert_eq!(descriptor.fields.len(), DEPLOY_CONFIG_EXPECTED_LEAF_COUNT);
    }

    #[test]
    fn semantic_units_are_exact() {
        let descriptor = DeployConfigDescriptor::generate();
        let unit = |path: &str| {
            descriptor
                .fields
                .iter()
                .find(|field| field.toml_path == path)
                .and_then(|field| field.unit)
        };
        assert_eq!(
            unit("market_data.websocket.engine_subscription_window_hours"),
            Some(DeployFieldUnit::Hours)
        );
        assert_eq!(
            unit("domain_sources.ghcnh.calibration_years"),
            Some(DeployFieldUnit::Years)
        );
        assert_eq!(
            unit("domain_sources.hko_open_data.daily_temperature_lookback_months"),
            Some(DeployFieldUnit::Months)
        );
        assert_eq!(
            unit("domain_sources.weather_stations.*.latitude"),
            Some(DeployFieldUnit::Degrees)
        );
        assert_eq!(
            unit("polymarket.settlement.external_scan_block_span"),
            Some(DeployFieldUnit::Blocks)
        );
        let kind = |path: &str| {
            descriptor
                .fields
                .iter()
                .find(|field| field.toml_path == path)
                .map(|field| field.value_kind)
        };
        assert_eq!(
            kind("domain_sources.weather_stations.*.latitude"),
            Some(DeployValueKind::Decimal)
        );
        assert_eq!(
            kind("cache.domains.*.fail_open"),
            Some(DeployValueKind::Boolean)
        );
    }
}
