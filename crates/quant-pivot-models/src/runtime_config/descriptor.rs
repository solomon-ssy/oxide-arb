//! Machine-readable metadata for governed Runtime Config editors.

use std::collections::{BTreeMap, BTreeSet};

use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use self::RuntimeFieldUnit::{
    BasisPoints, Blocks, Count, Generation, Milliseconds, Ratio, Revision, Seconds, Usd,
};
use crate::enums::runtime_config::{
    ConfigResourceKind,
    ConfigResourceKind::{
        ExecutionAutomationPolicy, ExecutionRiskPolicy, ModelRouting, OperationsPolicy,
        RecommendationPolicy,
    },
    PolicyApplyBoundary,
};

/// UI control selected from the exact Rust/JSON-Schema field shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeFieldControl {
    Toggle,
    Integer,
    Decimal,
    Money,
    Probability,
    Text,
    Select,
    MultiSelect,
    Duration,
    ArtifactPicker,
    ArtifactMapping,
    ScheduleList,
    CapitalTimeBuckets,
    Variant,
}

/// Human unit rendered beside one Runtime Config control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeFieldUnit {
    Usd,
    BasisPoints,
    Blocks,
    Milliseconds,
    Seconds,
    Hours,
    UsdHours,
    Ratio,
    Count,
    Generation,
    Revision,
}

/// Operator-risk classification for one field mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeFieldRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// Exact numeric bounds represented without floating-point conversion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFieldBounds {
    pub minimum: Option<String>,
    pub exclusive_minimum: Option<String>,
    pub maximum: Option<String>,
    pub exclusive_maximum: Option<String>,
}

/// Typed condition controlling whether a dependent editor is visible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeVisibilityCondition {
    pub pointer: String,
    pub equals: Value,
}

/// Complete metadata for one editable or explicitly read-only Runtime leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFieldDescriptor {
    pub pointer: String,
    pub title: String,
    pub description: String,
    pub unit: Option<RuntimeFieldUnit>,
    pub format: Option<String>,
    pub required: bool,
    pub default: Option<Value>,
    pub example: Option<Value>,
    pub bounds: RuntimeFieldBounds,
    pub enum_values: Vec<String>,
    pub control: RuntimeFieldControl,
    pub group: String,
    pub order: u32,
    pub risk_level: RuntimeFieldRiskLevel,
    pub apply_effect: PolicyApplyBoundary,
    pub read_only: bool,
    pub write_only: bool,
    pub visibility_condition: Option<RuntimeVisibilityCondition>,
    pub documentation_url: String,
}

/// Descriptor set for one strongly typed Runtime resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeResourceDescriptor {
    pub resource: ConfigResourceKind,
    pub fields: Vec<RuntimeFieldDescriptor>,
}

impl RuntimeResourceDescriptor {
    /// Derive a canonical, pointer-sorted descriptor set from an inline schema.
    #[must_use]
    pub fn from_schema(resource: ConfigResourceKind, schema: &Schema) -> Self {
        let mut collector = DescriptorCollector {
            resource,
            fields: Vec::new(),
        };
        collector.collect_node(schema.as_value(), "", true);
        collector
            .fields
            .sort_by(|left, right| left.pointer.cmp(&right.pointer));
        for (index, field) in collector.fields.iter_mut().enumerate() {
            field.order = u32::try_from(index).unwrap_or(u32::MAX);
        }
        Self {
            resource,
            fields: collector.fields,
        }
    }

    /// Return structural audit failures for CI and component-contract tests.
    #[must_use]
    pub fn audit(&self) -> Vec<String> {
        let mut failures = Vec::new();
        let mut pointers = BTreeSet::new();
        for field in &self.fields {
            if !field.pointer.starts_with('/') {
                failures.push(format!("invalid RFC 6901 pointer `{}`", field.pointer));
            }
            if !pointers.insert(field.pointer.as_str()) {
                failures.push(format!("duplicate pointer `{}`", field.pointer));
            }
            if field.title.trim().is_empty() {
                failures.push(format!("{} has no title", field.pointer));
            }
            if field.description.trim().is_empty() {
                failures.push(format!("{} has no description", field.pointer));
            }
            if field.documentation_url.trim().is_empty() {
                failures.push(format!("{} has no documentation URL", field.pointer));
            }
            if field.group == "__unclassified__" {
                failures.push(format!("{} has no exact group contract", field.pointer));
            }
            if matches!(
                field.control,
                RuntimeFieldControl::Integer
                    | RuntimeFieldControl::Decimal
                    | RuntimeFieldControl::Money
                    | RuntimeFieldControl::Probability
                    | RuntimeFieldControl::Duration
            ) && field.unit.is_none()
            {
                failures.push(format!(
                    "{} is a numeric control without an explicit unit contract",
                    field.pointer
                ));
            }
            if field.control == RuntimeFieldControl::Variant && field.enum_values.is_empty() {
                failures.push(format!(
                    "{} is an editable variant without explicit choices",
                    field.pointer
                ));
            }
        }
        let contracts = RUNTIME_GROUP_CONTRACTS
            .iter()
            .filter(|contract| contract.resource == self.resource)
            .collect::<Vec<_>>();
        let mut prefixes = BTreeSet::new();
        for contract in contracts {
            if !prefixes.insert(contract.pointer_prefix) {
                failures.push(format!(
                    "{} has duplicate group prefix {}",
                    self.resource, contract.pointer_prefix
                ));
            }
            if !self
                .fields
                .iter()
                .any(|field| Self::contract_matches(contract, &field.pointer))
            {
                failures.push(format!(
                    "{} has stale group contract {}",
                    self.resource, contract.pointer_prefix
                ));
            }
        }
        failures
    }

    fn contract_matches(contract: &RuntimeGroupContract, pointer: &str) -> bool {
        pointer == contract.pointer_prefix
            || pointer
                .strip_prefix(contract.pointer_prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

struct DescriptorCollector {
    resource: ConfigResourceKind,
    fields: Vec<RuntimeFieldDescriptor>,
}

#[derive(Clone, Copy)]
struct RuntimeGroupContract {
    resource: ConfigResourceKind,
    pointer_prefix: &'static str,
    group: &'static str,
    risk_level: RuntimeFieldRiskLevel,
}

const RUNTIME_GROUP_CONTRACTS: &[RuntimeGroupContract] = &[
    RuntimeGroupContract {
        resource: ConfigResourceKind::RecommendationPolicy,
        pointer_prefix: "/selection",
        group: "selection",
        risk_level: RuntimeFieldRiskLevel::Medium,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::RecommendationPolicy,
        pointer_prefix: "/data_quality",
        group: "data_quality",
        risk_level: RuntimeFieldRiskLevel::Medium,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::RecommendationPolicy,
        pointer_prefix: "/reports",
        group: "reports",
        risk_level: RuntimeFieldRiskLevel::Medium,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/portfolio/budget",
        group: "portfolio/budget",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/portfolio/admission",
        group: "portfolio/admission",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/portfolio/exposure_limits",
        group: "portfolio/exposure_limits",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/portfolio/tail_risk",
        group: "portfolio/tail_risk",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/entry_order_policy",
        group: "entry_order_policy",
        risk_level: RuntimeFieldRiskLevel::High,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/reconciliation",
        group: "reconciliation",
        risk_level: RuntimeFieldRiskLevel::High,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/exit_monitor",
        group: "exit_monitor",
        risk_level: RuntimeFieldRiskLevel::High,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionRiskPolicy,
        pointer_prefix: "/breaker",
        group: "breaker",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ModelRouting,
        pointer_prefix: "/model/buy_routes",
        group: "model/buy_routes",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ModelRouting,
        pointer_prefix: "/model/portfolio_scenario_model_bindings",
        group: "model/portfolio_scenario_model_bindings",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ModelRouting,
        pointer_prefix: "/model/calibration",
        group: "model/calibration",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ModelRouting,
        pointer_prefix: "/model/shadow_diff_threshold",
        group: "model/shadow_diff_threshold",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ModelRouting,
        pointer_prefix: "/model/active_exit_model_version_id",
        group: "model/active_exit_model_version_id",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ReportSchedule,
        pointer_prefix: "/schedules",
        group: "schedules",
        risk_level: RuntimeFieldRiskLevel::Medium,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::OperationsPolicy,
        pointer_prefix: "/outcome_reconciliation",
        group: "outcome_reconciliation",
        risk_level: RuntimeFieldRiskLevel::High,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::OperationsPolicy,
        pointer_prefix: "/entry_condition",
        group: "entry_condition",
        risk_level: RuntimeFieldRiskLevel::High,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::OperationsPolicy,
        pointer_prefix: "/notifications",
        group: "notifications",
        risk_level: RuntimeFieldRiskLevel::Medium,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::OperationsPolicy,
        pointer_prefix: "/kill_switch",
        group: "kill_switch",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionAutomationPolicy,
        pointer_prefix: "/semi_auto",
        group: "semi_auto",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
    RuntimeGroupContract {
        resource: ConfigResourceKind::ExecutionAutomationPolicy,
        pointer_prefix: "/auto_execution",
        group: "auto_execution",
        risk_level: RuntimeFieldRiskLevel::Critical,
    },
];

impl DescriptorCollector {
    fn collect_node(&mut self, node: &Value, pointer: &str, required: bool) {
        let Some(object) = node.as_object() else {
            return;
        };
        if object
            .get("x-ui-visible")
            .and_then(Value::as_bool)
            .is_some_and(|visible| !visible)
        {
            return;
        }
        let properties = object.get("properties").and_then(Value::as_object);
        if let Some(properties) = properties.filter(|properties| !properties.is_empty()) {
            let required_names = Self::required_names(object);
            let ordered = properties.iter().collect::<BTreeMap<_, _>>();
            for (name, child) in ordered {
                let child_pointer = Self::child_pointer(pointer, name);
                self.collect_node(
                    child,
                    &child_pointer,
                    required_names.contains(name.as_str()),
                );
            }
            return;
        }
        if pointer.is_empty() {
            return;
        }
        self.fields
            .push(self.leaf_descriptor(object, pointer, required));
    }

    fn leaf_descriptor(
        &self,
        schema: &Map<String, Value>,
        pointer: &str,
        required: bool,
    ) -> RuntimeFieldDescriptor {
        let title = schema
            .get("title")
            .and_then(Value::as_str)
            .map_or_else(|| Self::pointer_title(pointer), str::to_owned);
        let description = schema
            .get("description")
            .and_then(Value::as_str)
            .map_or_else(String::new, str::to_owned);
        let format = schema
            .get("x-format")
            .or_else(|| schema.get("format"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let default = schema.get("default").cloned();
        let example = schema
            .get("examples")
            .and_then(Value::as_array)
            .and_then(|examples| examples.first())
            .cloned()
            .or_else(|| default.clone());
        let group_contract = self.group_contract(pointer);
        let unit = Self::unit(self.resource, pointer);
        let read_only = Self::read_only(self.resource, schema);
        RuntimeFieldDescriptor {
            pointer: pointer.to_owned(),
            title,
            description,
            unit,
            format: format.clone(),
            required,
            default,
            example,
            bounds: Self::bounds(schema),
            enum_values: Self::enum_values(schema),
            control: Self::control(
                self.resource,
                schema,
                pointer,
                format.as_deref(),
                unit,
                read_only,
            ),
            group: group_contract
                .map_or("__unclassified__", |contract| contract.group)
                .to_owned(),
            order: 0,
            risk_level: group_contract.map_or(RuntimeFieldRiskLevel::Critical, |contract| {
                contract.risk_level
            }),
            apply_effect: self.resource.apply_boundary(),
            read_only,
            write_only: schema
                .get("writeOnly")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            visibility_condition: None,
            documentation_url: Self::documentation_url(self.resource).to_owned(),
        }
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

    fn pointer_title(pointer: &str) -> String {
        pointer
            .rsplit('/')
            .next()
            .unwrap_or(pointer)
            .split('_')
            .filter(|part| !part.is_empty())
            .map(|part| {
                let mut chars = part.chars();
                chars.next().map_or_else(String::new, |first| {
                    first.to_uppercase().collect::<String>() + chars.as_str()
                })
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn bounds(schema: &Map<String, Value>) -> RuntimeFieldBounds {
        RuntimeFieldBounds {
            minimum: schema.get("minimum").map(Self::scalar),
            exclusive_minimum: schema.get("exclusiveMinimum").map(Self::scalar),
            maximum: schema.get("maximum").map(Self::scalar),
            exclusive_maximum: schema.get("exclusiveMaximum").map(Self::scalar),
        }
    }

    fn scalar(value: &Value) -> String {
        value
            .as_str()
            .map_or_else(|| value.to_string(), str::to_owned)
    }

    fn enum_values(schema: &Map<String, Value>) -> Vec<String> {
        let mut values = schema
            .get("enum")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(Self::scalar)
            .collect::<Vec<_>>();
        if let Some(items) = schema.get("items").and_then(Value::as_object) {
            values.extend(Self::enum_values(items));
        }
        if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
            values.extend(variants.iter().filter_map(|variant| {
                variant
                    .as_object()
                    .and_then(|object| object.get("const"))
                    .map(Self::scalar)
            }));
        }
        values.sort();
        values.dedup();
        values
    }

    fn group_contract(&self, pointer: &str) -> Option<&'static RuntimeGroupContract> {
        RUNTIME_GROUP_CONTRACTS.iter().find(|contract| {
            contract.resource == self.resource
                && RuntimeResourceDescriptor::contract_matches(contract, pointer)
        })
    }

    fn unit(resource: ConfigResourceKind, pointer: &str) -> Option<RuntimeFieldUnit> {
        match (resource, pointer) {
            (
                RecommendationPolicy,
                "/data_quality/max_book_age_ms" | "/data_quality/max_ingest_lag_ms",
            )
            | (
                OperationsPolicy,
                "/entry_condition/backstop_interval_ms"
                | "/entry_condition/next_evaluation_delay_ms",
            ) => Some(Milliseconds),
            (
                RecommendationPolicy,
                "/data_quality/max_domain_observation_age_secs"
                | "/data_quality/max_feature_bucket_age_secs"
                | "/data_quality/max_trade_tape_age_secs"
                | "/reports/ad_hoc_default_knowledge_lag_secs"
                | "/selection/max_time_to_resolution_secs"
                | "/selection/min_time_to_resolution_secs",
            )
            | (
                ExecutionRiskPolicy,
                "/breaker/cooldown_secs"
                | "/breaker/venue_window_secs"
                | "/exit_monitor/monitor_secs"
                | "/exit_monitor/signal_recheck_secs"
                | "/reconciliation/interval_secs"
                | "/reconciliation/stale_open_secs",
            )
            | (ModelRouting, "/model/calibration/embargo_secs")
            | (
                OperationsPolicy,
                "/entry_condition/lease_duration_secs"
                | "/entry_condition/lease_renew_interval_secs"
                | "/outcome_reconciliation/sweep_secs",
            )
            | (ExecutionAutomationPolicy, "/semi_auto/approval_ttl_secs") => Some(Seconds),
            (
                RecommendationPolicy,
                "/data_quality/max_stale_book_ratio_bps" | "/selection/max_spread_bps",
            )
            | (
                ExecutionRiskPolicy,
                "/breaker/venue_error_rate_bps_to_halt"
                | "/entry_order_policy/max_slippage_bps"
                | "/portfolio/admission/liquidity_buffer_bps"
                | "/portfolio/admission/max_probability_interval_width_bps"
                | "/portfolio/admission/min_profit_probability_bps"
                | "/portfolio/tail_risk/cvar_confidence_bps",
            )
            | (OperationsPolicy, "/kill_switch/emergency_exit/max_slippage_bps") => {
                Some(BasisPoints)
            }
            (
                RecommendationPolicy,
                "/selection/min_liquidity_usd" | "/selection/min_volume_24h_usd",
            )
            | (
                ExecutionRiskPolicy,
                "/breaker/daily_realized_loss_cap_usd"
                | "/entry_order_policy/min_entry_book_depth_usd"
                | "/portfolio/admission/min_nominal_expected_net_usd"
                | "/portfolio/admission/min_robust_expected_net_usd"
                | "/portfolio/budget/cash_reserve_usd"
                | "/portfolio/budget/max_open_capital_usd"
                | "/portfolio/budget/total_budget_usd"
                | "/portfolio/exposure_limits/max_category_exposure_usd"
                | "/portfolio/exposure_limits/max_event_exposure_usd"
                | "/portfolio/exposure_limits/max_market_exposure_usd"
                | "/portfolio/exposure_limits/max_route_exposure_usd"
                | "/portfolio/exposure_limits/max_single_recommendation_usd"
                | "/portfolio/tail_risk/max_cvar_usd"
                | "/portfolio/tail_risk/max_drawdown_usd"
                | "/portfolio/tail_risk/max_scenario_loss_usd",
            )
            | (ExecutionAutomationPolicy, "/auto_execution/max_total_usd_per_report") => Some(Usd),
            (RecommendationPolicy, "/reports/entry_window_ratio")
            | (ModelRouting, "/model/calibration/ci_confidence" | "/model/shadow_diff_threshold") => {
                Some(Ratio)
            }
            (
                RecommendationPolicy,
                "/reports/ad_hoc_default_top_n"
                | "/reports/hard_candidate_ceiling"
                | "/reports/max_top_n",
            )
            | (
                ExecutionRiskPolicy,
                "/breaker/venue_consecutive_failures_to_degrade"
                | "/breaker/venue_consecutive_failures_to_halt"
                | "/breaker/venue_min_window_samples"
                | "/portfolio/exposure_limits/max_open_recommendations",
            )
            | (ModelRouting, "/model/calibration/min_samples_isotonic")
            | (
                OperationsPolicy,
                "/entry_condition/expiry_batch_limit"
                | "/entry_condition/pass_limit"
                | "/outcome_reconciliation/candidate_batch_size",
            )
            | (ExecutionAutomationPolicy, "/auto_execution/max_orders_per_report") => Some(Count),
            (ModelRouting, pointer) if pointer.ends_with("/config_revision") => Some(Revision),
            (ModelRouting, pointer) if pointer.ends_with("/generation") => Some(Generation),
            (OperationsPolicy, "/outcome_reconciliation/source_block_span") => Some(Blocks),
            _ => None,
        }
    }

    fn control(
        resource: ConfigResourceKind,
        schema: &Map<String, Value>,
        pointer: &str,
        format: Option<&str>,
        unit: Option<RuntimeFieldUnit>,
        read_only: bool,
    ) -> RuntimeFieldControl {
        if resource == ConfigResourceKind::ReportSchedule && pointer == "/schedules" {
            return RuntimeFieldControl::ScheduleList;
        }
        if resource == ConfigResourceKind::ExecutionRiskPolicy
            && pointer == "/portfolio/tail_risk/capital_time_buckets"
        {
            return RuntimeFieldControl::CapitalTimeBuckets;
        }
        if resource == ConfigResourceKind::RecommendationPolicy
            && pointer == "/selection/enabled_categories"
        {
            return RuntimeFieldControl::MultiSelect;
        }
        if resource == ConfigResourceKind::ModelRouting
            && (pointer == "/model/portfolio_scenario_model_bindings"
                || pointer.ends_with("/source")
                || pointer.ends_with("/shadow"))
        {
            return RuntimeFieldControl::ArtifactMapping;
        }
        if resource == ConfigResourceKind::ModelRouting
            && (pointer == "/model/active_exit_model_version_id"
                || pointer.ends_with("/model_version_id"))
        {
            return RuntimeFieldControl::ArtifactPicker;
        }
        let enum_values = Self::enum_values(schema);
        if !enum_values.is_empty() {
            return if schema.get("type").and_then(Value::as_str) == Some("array") {
                RuntimeFieldControl::MultiSelect
            } else {
                RuntimeFieldControl::Select
            };
        }
        if schema.contains_key("oneOf") || schema.contains_key("anyOf") {
            return if read_only {
                RuntimeFieldControl::ArtifactMapping
            } else {
                RuntimeFieldControl::Variant
            };
        }
        match schema.get("type").and_then(Value::as_str) {
            Some("boolean") => RuntimeFieldControl::Toggle,
            Some("integer" | "number")
                if matches!(
                    unit,
                    Some(
                        RuntimeFieldUnit::Milliseconds
                            | RuntimeFieldUnit::Seconds
                            | RuntimeFieldUnit::Hours
                    )
                ) =>
            {
                RuntimeFieldControl::Duration
            }
            Some("integer" | "number") => RuntimeFieldControl::Integer,
            Some("string") if unit == Some(RuntimeFieldUnit::Usd) => RuntimeFieldControl::Money,
            Some("string")
                if resource == ConfigResourceKind::ModelRouting
                    && pointer == "/model/calibration/ci_confidence" =>
            {
                RuntimeFieldControl::Probability
            }
            Some("string") if format == Some("decimal") => RuntimeFieldControl::Decimal,
            Some("array") => RuntimeFieldControl::Variant,
            _ => RuntimeFieldControl::Text,
        }
    }

    fn read_only(resource: ConfigResourceKind, schema: &Map<String, Value>) -> bool {
        resource == ConfigResourceKind::ModelRouting
            || schema
                .get("readOnly")
                .and_then(Value::as_bool)
                .unwrap_or(false)
    }

    const fn documentation_url(resource: ConfigResourceKind) -> &'static str {
        match resource {
            ConfigResourceKind::RecommendationPolicy => {
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#recommendation-policy"
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#execution-risk-policy"
            }
            ConfigResourceKind::ModelRouting => {
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#model-routing"
            }
            ConfigResourceKind::ReportSchedule => {
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#report-schedule"
            }
            ConfigResourceKind::OperationsPolicy => {
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#operations-policy"
            }
            ConfigResourceKind::ExecutionAutomationPolicy => {
                "docs/plans/quant-pivot/06-config-deploy-and-ops.md#execution-automation-policy"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeFieldControl, RuntimeFieldUnit, RuntimeResourceDescriptor};
    use crate::{
        enums::runtime_config::ConfigResourceKind, runtime_config::DecisionPolicySnapshot,
    };

    #[test]
    fn all_resources_have_descriptors() {
        for resource in ConfigResourceKind::ALL {
            let schema = DecisionPolicySnapshot::resource_json_schema(resource);
            let descriptor = RuntimeResourceDescriptor::from_schema(resource, &schema);
            assert!(!descriptor.fields.is_empty(), "{resource} has no fields");
            assert_eq!(descriptor.audit(), Vec::<String>::new(), "{resource}");
        }
    }

    #[test]
    fn risk_fields_are_required() {
        let resource = ConfigResourceKind::ExecutionRiskPolicy;
        let schema = DecisionPolicySnapshot::resource_json_schema(resource);
        let descriptor = RuntimeResourceDescriptor::from_schema(resource, &schema);
        assert!(
            descriptor.fields.iter().all(|field| field.required),
            "execution-risk documents must never be completed by serde defaults"
        );
        assert!(
            descriptor
                .fields
                .iter()
                .all(|field| field.pointer != "/schema_version")
        );
        let capital_buckets = descriptor
            .fields
            .iter()
            .find(|field| field.pointer == "/portfolio/tail_risk/capital_time_buckets")
            .expect("capital-time bucket descriptor");
        assert_eq!(
            capital_buckets.control,
            RuntimeFieldControl::CapitalTimeBuckets
        );
        assert_eq!(capital_buckets.group, "portfolio/tail_risk");
    }

    #[test]
    fn category_multiselect_covers_enum() {
        let resource = ConfigResourceKind::RecommendationPolicy;
        let schema = DecisionPolicySnapshot::resource_json_schema(resource);
        let descriptor = RuntimeResourceDescriptor::from_schema(resource, &schema);
        let categories = descriptor
            .fields
            .iter()
            .find(|field| field.pointer == "/selection/enabled_categories")
            .expect("enabled-categories descriptor");
        assert_eq!(categories.control, RuntimeFieldControl::MultiSelect);
        assert_eq!(categories.enum_values.len(), 10);
    }

    #[test]
    fn metadata_is_semantic() {
        let risk = RuntimeResourceDescriptor::from_schema(
            ConfigResourceKind::ExecutionRiskPolicy,
            &DecisionPolicySnapshot::resource_json_schema(ConfigResourceKind::ExecutionRiskPolicy),
        );
        let cvar = risk
            .fields
            .iter()
            .find(|field| field.pointer == "/portfolio/tail_risk/cvar_confidence_bps")
            .expect("CVaR confidence descriptor");
        assert_eq!(cvar.unit, Some(RuntimeFieldUnit::BasisPoints));
        assert_eq!(cvar.group, "portfolio/tail_risk");

        let routing = RuntimeResourceDescriptor::from_schema(
            ConfigResourceKind::ModelRouting,
            &DecisionPolicySnapshot::resource_json_schema(ConfigResourceKind::ModelRouting),
        );
        assert!(routing.fields.iter().all(|field| field.read_only));
        let scenarios = routing
            .fields
            .iter()
            .find(|field| field.pointer == "/model/portfolio_scenario_model_bindings")
            .expect("scenario binding descriptor");
        assert_eq!(scenarios.control, RuntimeFieldControl::ArtifactMapping);
    }
}
