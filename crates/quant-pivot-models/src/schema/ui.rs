//! Runtime-config v3 UI metadata used by the preferences schema projection.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::domain::{FieldSemantics, FieldWidget, RuntimeConfigSchemaGroupView, UiText};

/// Per-leaf UI metadata registered at compile time.
#[derive(Clone)]
pub struct FieldUiEntry {
    pub path: &'static str,
    pub label: UiText,
    pub help: UiText,
    pub order: u16,
    pub widget: Option<FieldWidget>,
    pub semantics: Option<FieldSemantics>,
    pub visible: bool,
}

/// All registered groups sorted by `order`.
#[must_use]
pub fn groups_ui() -> Vec<RuntimeConfigSchemaGroupView> {
    let mut groups = groups().to_vec();
    groups.sort_by_key(|group| group.order);
    groups
}

/// Lookup field UI metadata by dotted path.
#[must_use]
pub fn field_ui(path: &str) -> Option<&'static FieldUiEntry> {
    fields().iter().find(|entry| entry.path == path)
}

/// All registered field UI entries keyed by path.
#[must_use]
pub fn field_ui_map() -> BTreeMap<&'static str, &'static FieldUiEntry> {
    fields().iter().map(|entry| (entry.path, entry)).collect()
}

fn groups() -> &'static [RuntimeConfigSchemaGroupView] {
    static GROUPS: OnceLock<Vec<RuntimeConfigSchemaGroupView>> = OnceLock::new();
    GROUPS.get_or_init(|| {
        vec![
            group(
                "universe",
                10,
                "Universe",
                "市场池",
                "Market universe selection policy.",
            ),
            group(
                "data_quality",
                20,
                "Data quality",
                "数据质量",
                "Freshness and quality gates.",
            ),
            group(
                "features",
                30,
                "Features",
                "特征",
                "Feature families and windows.",
            ),
            group(
                "factors",
                40,
                "Factors",
                "因子",
                "Factor families and scoring weights.",
            ),
            group(
                "model",
                50,
                "Model",
                "模型",
                "Active model and shadow model gates.",
            ),
            group(
                "reports",
                60,
                "Reports",
                "报告",
                "Report schedules and publication policy.",
            ),
            group(
                "portfolio",
                70,
                "Portfolio",
                "组合",
                "Budget and exposure constraints.",
            ),
            group(
                "execution",
                80,
                "Execution",
                "执行",
                "Order-intent and execution policy.",
            ),
            group(
                "notification",
                90,
                "Notification",
                "通知",
                "Operator notification channels.",
            ),
        ]
    })
}

fn group(
    id: impl Into<String>,
    order: u16,
    en: impl Into<String>,
    zh_cn: impl Into<String>,
    description: impl Into<String>,
) -> RuntimeConfigSchemaGroupView {
    RuntimeConfigSchemaGroupView {
        id: id.into(),
        label: UiText::localized(en, zh_cn),
        description: Some(UiText::plain(description)),
        order,
    }
}

fn fields() -> &'static [FieldUiEntry] {
    static FIELDS: OnceLock<Vec<FieldUiEntry>> = OnceLock::new();
    FIELDS.get_or_init(build_fields)
}

fn build_fields() -> Vec<FieldUiEntry> {
    [
        universe_fields(),
        data_quality_fields(),
        feature_fields(),
        factor_fields(),
        model_fields(),
        report_fields(),
        portfolio_fields(),
        execution_fields(),
        notification_fields(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn universe_fields() -> Vec<FieldUiEntry> {
    vec![
        entry(
            "universe.enabled_categories",
            "Enabled categories",
            "启用分类",
            10,
            Some(FieldWidget::EnumSet),
            Some(FieldSemantics::EmptyMeansAll),
        ),
        entry(
            "universe.excluded_market_ids",
            "Excluded market ids",
            "排除市场 ID",
            20,
            Some(FieldWidget::StringList),
            None,
        ),
        entry(
            "universe.included_market_ids",
            "Included market ids",
            "包含市场 ID",
            30,
            Some(FieldWidget::StringList),
            None,
        ),
        money(
            "universe.min_liquidity_usd",
            "Minimum liquidity USD",
            "最低流动性",
            40,
        ),
        money(
            "universe.min_volume_24h_usd",
            "Minimum 24h volume USD",
            "最低 24h 成交量",
            50,
        ),
        integer(
            "universe.max_spread_bps",
            "Maximum spread bps",
            "最大价差 bps",
            60,
        ),
        boolean(
            "universe.allow_near_resolution",
            "Allow near resolution",
            "允许临近结算",
            70,
        ),
        integer(
            "universe.min_time_to_resolution_secs",
            "Minimum time to resolution",
            "最短结算剩余秒数",
            80,
        ),
        integer(
            "universe.max_time_to_resolution_secs",
            "Maximum time to resolution",
            "最长结算剩余秒数",
            90,
        ),
        integer(
            "universe.max_universe_size",
            "Maximum universe size",
            "最大市场池规模",
            100,
        ),
    ]
}

fn data_quality_fields() -> Vec<FieldUiEntry> {
    vec![
        duration(
            "data_quality.max_book_age_ms",
            "Maximum book age",
            "最大订单簿年龄",
            10,
        ),
        integer(
            "data_quality.max_fact_lag_secs",
            "Maximum fact lag",
            "最大事实延迟",
            20,
        ),
        money(
            "data_quality.min_book_depth_usd",
            "Minimum book depth USD",
            "最低订单簿深度",
            30,
        ),
        boolean(
            "data_quality.allow_degraded_domain_features",
            "Allow degraded features",
            "允许降级特征",
            40,
        ),
        boolean(
            "data_quality.reject_crossed_books",
            "Reject crossed books",
            "拒绝交叉订单簿",
            50,
        ),
        boolean(
            "data_quality.reject_empty_books",
            "Reject empty books",
            "拒绝空订单簿",
            60,
        ),
        integer(
            "data_quality.source_delay_secs",
            "Source delay",
            "数据源延迟秒数",
            70,
        ),
        entry(
            "data_quality.feature_staleness_policy",
            "Feature staleness policy",
            "特征新鲜度策略",
            80,
            Some(FieldWidget::EnumSelect),
            None,
        ),
    ]
}

fn feature_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "features.feature_schema_version",
            "Feature schema version",
            "特征 schema 版本",
            10,
        ),
        entry(
            "features.enabled_feature_families",
            "Enabled feature families",
            "启用特征族",
            20,
            Some(FieldWidget::EnumSet),
            None,
        ),
        entry(
            "features.required_features",
            "Required features",
            "必需特征",
            30,
            Some(FieldWidget::StringList),
            None,
        ),
        entry(
            "features.domain_feature_policy",
            "Domain feature policy",
            "领域特征策略",
            40,
            Some(FieldWidget::EnumSelect),
            None,
        ),
        entry(
            "features.bar_windows_secs",
            "Bar windows",
            "K 线窗口秒数",
            50,
            Some(FieldWidget::JsonTree),
            None,
        ),
        entry(
            "features.momentum_windows_secs",
            "Momentum windows",
            "动量窗口秒数",
            60,
            Some(FieldWidget::JsonTree),
            None,
        ),
        entry(
            "features.volatility_windows_secs",
            "Volatility windows",
            "波动率窗口秒数",
            70,
            Some(FieldWidget::JsonTree),
            None,
        ),
        entry(
            "features.depth_levels",
            "Depth levels",
            "订单簿深度层级",
            80,
            Some(FieldWidget::JsonTree),
            None,
        ),
    ]
}

fn factor_fields() -> Vec<FieldUiEntry> {
    vec![
        entry(
            "factors.enabled_factor_families",
            "Enabled factor families",
            "启用因子族",
            10,
            Some(FieldWidget::StringList),
            None,
        ),
        entry(
            "factors.factor_weights",
            "Factor weights",
            "因子权重",
            20,
            Some(FieldWidget::EnumDecimalMap),
            None,
        ),
        entry(
            "factors.min_factor_confidence",
            "Minimum factor confidence",
            "最低因子置信度",
            30,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "factors.missing_factor_policy",
            "Missing factor policy",
            "缺失因子策略",
            40,
            Some(FieldWidget::EnumSelect),
            None,
        ),
        entry(
            "factors.published_factor_set_id",
            "Published factor set",
            "已发布因子集",
            50,
            Some(FieldWidget::PlainString),
            None,
        ),
        entry(
            "factors.shadow_factor_set_id",
            "Shadow factor set",
            "影子因子集",
            60,
            Some(FieldWidget::PlainString),
            None,
        ),
    ]
}

fn model_fields() -> Vec<FieldUiEntry> {
    vec![
        entry(
            "model.active_model_version_id",
            "Active model version",
            "活动模型版本",
            10,
            Some(FieldWidget::PlainString),
            None,
        ),
        entry(
            "model.shadow_model_version_id",
            "Shadow model version",
            "影子模型版本",
            20,
            Some(FieldWidget::PlainString),
            None,
        ),
        entry(
            "model.min_model_confidence",
            "Minimum model confidence",
            "最低模型置信度",
            30,
            Some(FieldWidget::DecimalString),
            None,
        ),
        integer(
            "model.min_quality_gate_age_secs",
            "Minimum quality gate age",
            "质量门最大年龄秒数",
            40,
        ),
        integer(
            "model.prediction_horizon_secs",
            "Prediction horizon",
            "预测周期秒数",
            50,
        ),
        entry(
            "model.candidate_score_floor",
            "Candidate score floor",
            "候选分数下限",
            60,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "model.shadow_diff_threshold",
            "Shadow diff threshold",
            "影子差异阈值",
            70,
            Some(FieldWidget::DecimalString),
            None,
        ),
    ]
}

fn report_fields() -> Vec<FieldUiEntry> {
    vec![
        entry(
            "reports.schedules",
            "Schedules",
            "报告计划",
            10,
            Some(FieldWidget::JsonTree),
            None,
        ),
        integer("reports.default_top_n", "Default TopN", "默认 TopN", 20),
        integer("reports.max_top_n", "Maximum TopN", "最大 TopN", 30),
        integer(
            "reports.report_horizon_secs",
            "Report horizon",
            "报告周期秒数",
            40,
        ),
        boolean(
            "reports.publish_empty_reports",
            "Publish empty reports",
            "发布空报告",
            50,
        ),
        integer("reports.report_ttl_secs", "Report TTL", "报告 TTL 秒数", 60),
        boolean(
            "reports.ad_hoc_report_enabled",
            "Ad-hoc reports",
            "临时报告",
            70,
        ),
        entry(
            "reports.delivery_policy",
            "Delivery policy",
            "投递策略",
            80,
            Some(FieldWidget::EnumSelect),
            None,
        ),
    ]
}

fn portfolio_fields() -> Vec<FieldUiEntry> {
    vec![
        money(
            "portfolio.total_budget_usd",
            "Total budget USD",
            "总预算",
            10,
        ),
        money(
            "portfolio.max_single_recommendation_usd",
            "Max recommendation USD",
            "单建议最大金额",
            20,
        ),
        money(
            "portfolio.max_market_exposure_usd",
            "Max market exposure USD",
            "单市场最大敞口",
            30,
        ),
        money(
            "portfolio.max_event_exposure_usd",
            "Max event exposure USD",
            "单事件最大敞口",
            40,
        ),
        money(
            "portfolio.max_category_exposure_usd",
            "Max category exposure USD",
            "单分类最大敞口",
            50,
        ),
        money(
            "portfolio.max_correlated_exposure_usd",
            "Max correlated exposure USD",
            "最大相关敞口",
            60,
        ),
        money(
            "portfolio.min_recommendation_usd",
            "Minimum recommendation USD",
            "最小建议金额",
            70,
        ),
        entry(
            "portfolio.liquidity_usage_cap_pct",
            "Liquidity usage cap",
            "流动性使用上限",
            80,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "portfolio.confidence_size_curve",
            "Confidence size curve",
            "置信度规模曲线",
            90,
            Some(FieldWidget::EnumSelect),
            None,
        ),
        entry(
            "portfolio.drawdown_multiplier",
            "Drawdown multiplier",
            "回撤乘数策略",
            100,
            Some(FieldWidget::EnumSelect),
            None,
        ),
    ]
}

fn execution_fields() -> Vec<FieldUiEntry> {
    let mut fields = vec![entry(
        "execution.runtime_mode",
        "Runtime mode",
        "运行模式",
        10,
        Some(FieldWidget::EnumSelect),
        Some(FieldSemantics::RuntimeMode),
    )];
    fields.extend(execution_semi_auto_fields());
    fields.extend(execution_auto_fields());
    fields.extend(execution_policy_fields());
    fields
}

fn execution_semi_auto_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "execution.semi_auto.approval_ttl_secs",
            "Approval TTL",
            "审批 TTL 秒数",
            20,
        ),
        entry(
            "execution.semi_auto.required_role",
            "Required role",
            "审批角色",
            30,
            Some(FieldWidget::PlainString),
            None,
        ),
        boolean(
            "execution.semi_auto.allow_size_reduction",
            "Allow size reduction",
            "允许减少下单规模",
            40,
        ),
    ]
}

fn execution_auto_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.auto_execution.enabled",
            "Auto execution enabled",
            "启用自动执行",
            50,
        ),
        integer(
            "execution.auto_execution.max_orders_per_report",
            "Max orders per report",
            "单报告最大订单数",
            60,
        ),
        money(
            "execution.auto_execution.max_total_usd_per_report",
            "Max auto USD per report",
            "单报告最大自动执行金额",
            70,
        ),
        entry(
            "execution.auto_execution.min_score",
            "Minimum auto score",
            "自动执行最低分",
            80,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "execution.auto_execution.min_confidence",
            "Minimum auto confidence",
            "自动执行最低置信度",
            90,
            Some(FieldWidget::DecimalString),
            None,
        ),
        boolean(
            "execution.auto_execution.require_shadow_passed",
            "Require shadow passed",
            "要求影子验证通过",
            100,
        ),
    ]
}

fn execution_policy_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "execution.entry_order_policy.max_slippage_bps",
            "Entry max slippage",
            "入场最大滑点",
            110,
        ),
        boolean(
            "execution.entry_order_policy.allow_market_orders",
            "Allow market orders",
            "允许市价单",
            120,
        ),
        boolean(
            "execution.exit_order_policy.allow_reduce_only",
            "Exit reduce only",
            "退出仅减仓",
            130,
        ),
        integer(
            "execution.exit_order_policy.max_slippage_bps",
            "Exit max slippage",
            "退出最大滑点",
            140,
        ),
        entry(
            "execution.admission.min_score",
            "Admission min score",
            "准入最低分",
            150,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "execution.admission.min_confidence",
            "Admission min confidence",
            "准入最低置信度",
            160,
            Some(FieldWidget::DecimalString),
            None,
        ),
        boolean(
            "execution.admission.require_fresh_features",
            "Require fresh features",
            "要求新鲜特征",
            170,
        ),
        boolean(
            "execution.kill_switch.enabled",
            "Kill switch",
            "执行熔断",
            180,
        ),
        entry(
            "execution.kill_switch.reason",
            "Kill switch reason",
            "熔断原因",
            190,
            Some(FieldWidget::PlainString),
            None,
        ),
        money(
            "execution.capital.max_reserved_usd",
            "Max reserved USD",
            "最大预留金额",
            200,
        ),
        integer(
            "execution.capital.max_open_intents",
            "Max open intents",
            "最大打开意图",
            210,
        ),
        boolean(
            "execution.reconciliation.enabled",
            "Reconciliation enabled",
            "启用对账",
            220,
        ),
        integer(
            "execution.reconciliation.interval_secs",
            "Reconciliation interval",
            "对账间隔秒数",
            230,
        ),
    ]
}

fn notification_fields() -> Vec<FieldUiEntry> {
    vec![
        entry(
            "notification.telegram.bot_token",
            "Telegram bot token",
            "Telegram Bot Token",
            10,
            Some(FieldWidget::SecretString),
            Some(FieldSemantics::Credential),
        ),
        entry(
            "notification.telegram.chat_id",
            "Telegram chat id",
            "Telegram Chat ID",
            20,
            Some(FieldWidget::PlainString),
            None,
        ),
        entry(
            "notification.webhook.url",
            "Webhook URL",
            "Webhook URL",
            30,
            Some(FieldWidget::SecretString),
            Some(FieldSemantics::Credential),
        ),
        boolean(
            "notification.policies.report_published",
            "Notify report published",
            "报告发布通知",
            40,
        ),
        boolean(
            "notification.policies.execution_halted",
            "Notify execution halted",
            "执行停止通知",
            50,
        ),
        boolean(
            "notification.policies.config_activated",
            "Notify config activated",
            "配置激活通知",
            60,
        ),
    ]
}

fn money(path: &'static str, en: &'static str, zh_cn: &'static str, order: u16) -> FieldUiEntry {
    entry(
        path,
        en,
        zh_cn,
        order,
        Some(FieldWidget::DecimalString),
        Some(FieldSemantics::Money),
    )
}

fn integer(path: &'static str, en: &'static str, zh_cn: &'static str, order: u16) -> FieldUiEntry {
    entry(path, en, zh_cn, order, Some(FieldWidget::Integer), None)
}

fn duration(path: &'static str, en: &'static str, zh_cn: &'static str, order: u16) -> FieldUiEntry {
    entry(path, en, zh_cn, order, Some(FieldWidget::DurationMs), None)
}

fn boolean(path: &'static str, en: &'static str, zh_cn: &'static str, order: u16) -> FieldUiEntry {
    entry(path, en, zh_cn, order, Some(FieldWidget::Boolean), None)
}

fn entry(
    path: &'static str,
    en: &'static str,
    zh_cn: &'static str,
    order: u16,
    widget: Option<FieldWidget>,
    semantics: Option<FieldSemantics>,
) -> FieldUiEntry {
    FieldUiEntry {
        path,
        label: UiText::localized(en, zh_cn),
        help: UiText::localized(en, zh_cn),
        order,
        widget,
        semantics,
        visible: true,
    }
}
