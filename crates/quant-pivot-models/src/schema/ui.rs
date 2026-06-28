//! Runtime-config v5 UI metadata used by the preferences schema projection.

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
                "selection",
                10,
                "Selection",
                "市场池",
                "Market selection selection policy.",
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
                "training",
                55,
                "Training",
                "训练",
                "Offline training-dataset build parameters.",
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
        selection_fields(),
        data_quality_fields(),
        feature_fields(),
        factor_fields(),
        model_fields(),
        quality_gate_fields(),
        training_fields(),
        report_fields(),
        portfolio_fields(),
        execution_fields(),
        notification_fields(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn selection_fields() -> Vec<FieldUiEntry> {
    vec![
        entry(
            "selection.enabled_categories",
            "Enabled categories",
            "启用分类",
            10,
            Some(FieldWidget::EnumSet),
            Some(FieldSemantics::EmptyMeansAll),
        ),
        entry(
            "selection.excluded_market_ids",
            "Excluded market ids",
            "排除市场 ID",
            20,
            Some(FieldWidget::StringList),
            None,
        ),
        entry(
            "selection.included_market_ids",
            "Included market ids",
            "包含市场 ID",
            30,
            Some(FieldWidget::StringList),
            None,
        ),
        money(
            "selection.min_liquidity_usd",
            "Minimum liquidity USD",
            "最低流动性",
            40,
        ),
        money(
            "selection.min_volume_24h_usd",
            "Minimum 24h volume USD",
            "最低 24h 成交量",
            50,
        ),
        integer(
            "selection.max_spread_bps",
            "Maximum spread bps",
            "最大价差 bps",
            60,
        ),
        boolean(
            "selection.allow_near_resolution",
            "Allow near resolution",
            "允许临近结算",
            70,
        ),
        integer(
            "selection.min_time_to_resolution_secs",
            "Minimum time to resolution",
            "最短结算剩余秒数",
            80,
        ),
        integer(
            "selection.max_time_to_resolution_secs",
            "Maximum time to resolution",
            "最长结算剩余秒数",
            90,
        ),
        integer(
            "selection.max_selection_size",
            "Maximum selection size",
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
        integer(
            "data_quality.max_stale_book_ratio_bps",
            "Max stale-book ratio (bps)",
            "最大陈旧订单簿比例（bps）",
            90,
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
        integer(
            "features.max_concurrent_market_resolves",
            "Max concurrent market resolves",
            "特征 resolve 最大并发",
            90,
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
            Some(FieldWidget::EnumSet),
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

fn quality_gate_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "quality_gate.min_sample_count",
            "Minimum sample count",
            "最小样本数",
            10,
        ),
        entry(
            "quality_gate.min_label_coverage",
            "Minimum label coverage",
            "最低标签覆盖率",
            20,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "quality_gate.min_critical_feature_coverage",
            "Minimum critical-feature coverage",
            "最低关键特征覆盖率",
            30,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "quality_gate.max_drawdown",
            "Maximum drawdown",
            "最大回撤",
            40,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "quality_gate.min_liquidity_exit_feasibility",
            "Minimum liquidity-exit feasibility",
            "最低流动性退出可行性",
            50,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "quality_gate.min_shadow_overlap_stability",
            "Minimum shadow overlap stability",
            "最低影子重叠稳定性",
            60,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "quality_gate.min_rank_ic",
            "Minimum rank IC (soft)",
            "最低排序 IC（软）",
            70,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "quality_gate.max_category_concentration",
            "Maximum category concentration (soft)",
            "最大类别集中度（软）",
            80,
            Some(FieldWidget::DecimalString),
            None,
        ),
        integer(
            "quality_gate.required_shadow_window_secs",
            "Required shadow window",
            "所需影子窗口秒数",
            90,
        ),
    ]
}

fn training_fields() -> Vec<FieldUiEntry> {
    vec![
        duration(
            "training.max_book_staleness_ms",
            "Historical max book staleness",
            "历史 PIT 最大订单簿陈旧度",
            10,
        ),
        money(
            "training.min_exit_depth_usd",
            "Minimum exit depth USD",
            "流动性退出标签最低深度",
            20,
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
            "reports.fallback_horizon_secs",
            "Fallback horizon",
            "回退预测周期秒数",
            40,
        ),
        boolean(
            "reports.publish_empty_reports",
            "Publish empty reports",
            "发布空报告",
            50,
        ),
        entry(
            "reports.entry_window_ratio",
            "Entry window ratio",
            "进场窗口比例",
            60,
            Some(FieldWidget::DecimalString),
            None,
        ),
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
            "portfolio.budget.total_budget_usd",
            "Total budget USD",
            "总预算",
            10,
        ),
        money(
            "portfolio.budget.min_recommendation_usd",
            "Minimum recommendation USD",
            "最小建议金额",
            20,
        ),
        money(
            "portfolio.budget.max_single_recommendation_usd",
            "Max recommendation USD",
            "单建议最大金额",
            30,
        ),
        money(
            "portfolio.constraints.max_market_exposure_usd",
            "Max market exposure USD",
            "单市场最大敞口",
            40,
        ),
        money(
            "portfolio.constraints.max_event_exposure_usd",
            "Max event exposure USD",
            "单事件最大敞口",
            50,
        ),
        money(
            "portfolio.constraints.max_category_exposure_usd",
            "Max category exposure USD",
            "单分类最大敞口",
            60,
        ),
        money(
            "portfolio.constraints.max_correlated_exposure_usd",
            "Max correlated exposure USD",
            "最大相关敞口",
            70,
        ),
        entry(
            "portfolio.constraints.liquidity_usage_cap_pct",
            "Liquidity usage cap",
            "流动性使用上限",
            80,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "portfolio.sizing.kelly_fraction",
            "Kelly fraction",
            "Kelly 分数",
            90,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "portfolio.sizing.max_position_pct",
            "Max position pct",
            "单仓上限占比",
            100,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "portfolio.sizing.target_reward_multiple",
            "Target reward multiple",
            "目标盈亏倍数",
            110,
            Some(FieldWidget::DecimalString),
            None,
        ),
        entry(
            "portfolio.sizing.confidence_weighting",
            "Confidence weighting",
            "置信度收缩曲线",
            120,
            Some(FieldWidget::EnumSelect),
            None,
        ),
        entry(
            "portfolio.sizing.drawdown_scaling",
            "Drawdown scaling",
            "回撤缩放策略",
            130,
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
        boolean(
            "execution.semi_auto.allow_size_reduction",
            "Allow size reduction",
            "允许减少下单规模",
            30,
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
        integer(
            "execution.entry_order_policy.confirmation_window_secs",
            "Entry confirmation window",
            "限价确认窗口秒数",
            125,
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
        entry(
            "execution.kill_switch.emergency_exit.kind",
            "Emergency exit",
            "紧急退出策略",
            190,
            Some(FieldWidget::EnumSelect),
            None,
        ),
        integer(
            "execution.kill_switch.emergency_exit.max_slippage_bps",
            "Emergency exit max slippage",
            "紧急退出最大滑点",
            195,
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
    ]
    .into_iter()
    .chain(execution_exit_monitor_fields())
    .chain(execution_reconciliation_fields())
    .chain(execution_breaker_fields())
    .collect()
}

fn execution_exit_monitor_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.exit_monitor.enabled",
            "Exit monitor enabled",
            "启用退出监控",
            142,
        ),
        integer(
            "execution.exit_monitor.monitor_secs",
            "Exit monitor interval",
            "退出监控间隔秒数",
            144,
        ),
        integer(
            "execution.exit_monitor.signal_recheck_secs",
            "Exit signal re-check interval",
            "信号重算间隔秒数",
            146,
        ),
        entry(
            "execution.exit_monitor.signal_invalidation_ratio",
            "Signal invalidation ratio",
            "信号失效比率",
            148,
            Some(FieldWidget::DecimalString),
            None,
        ),
        boolean(
            "execution.exit_monitor.signal_reinference.enabled",
            "Signal re-inference enabled",
            "启用信号再推理",
            150,
        ),
        boolean(
            "execution.exit_monitor.signal_reinference.shadow_mode",
            "Signal re-inference shadow mode",
            "信号再推理影子模式（只审计不触发）",
            152,
        ),
    ]
}

fn execution_reconciliation_fields() -> Vec<FieldUiEntry> {
    vec![
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
        integer(
            "execution.reconciliation.stale_open_secs",
            "Reconciliation stale-open deadline",
            "对账强制终态秒数",
            235,
        ),
    ]
}

fn execution_breaker_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "execution.breaker.venue_consecutive_failures_to_degrade",
            "Breaker degrade threshold (consecutive)",
            "熔断器降级阈值（连续失败）",
            240,
        ),
        integer(
            "execution.breaker.venue_consecutive_failures_to_halt",
            "Breaker halt threshold (consecutive)",
            "熔断器熔断阈值（连续失败）",
            250,
        ),
        integer(
            "execution.breaker.venue_error_rate_bps_to_halt",
            "Breaker halt error rate (bps)",
            "熔断器熔断错误率（bps）",
            260,
        ),
        integer(
            "execution.breaker.venue_min_window_samples",
            "Breaker min window samples",
            "熔断器窗口最小样本数",
            270,
        ),
        integer(
            "execution.breaker.venue_window_secs",
            "Breaker window seconds",
            "熔断器滚动窗口秒数",
            280,
        ),
        integer(
            "execution.breaker.cooldown_secs",
            "Breaker cooldown seconds",
            "熔断器冷却秒数",
            290,
        ),
        money(
            "execution.breaker.daily_realized_loss_cap_usd",
            "Breaker daily realized-loss cap (USD)",
            "熔断器日内已实现亏损上限（USD）",
            295,
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
