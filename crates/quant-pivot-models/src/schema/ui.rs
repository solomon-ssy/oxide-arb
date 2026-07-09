//! Runtime-config v10 UI metadata.
//!
//! Two artifacts, one authored source of truth:
//!
//! 1. A **field dictionary** ([`fields`]) — per-leaf label, authored bilingual
//!    rich `help` (distinct from `label`), render `widget`, behavioral
//!    `semantics`, presentation [`UiProps`], and conditional `when` rules.
//! 2. A **layout tree** ([`schema_tree`]) — nested [`SchemaSection`]s, field
//!    references, and discriminated [`SchemaUnion`]s describing how the fields
//!    are grouped and gated. Each section may declare an Iconify `icon` string
//!    (same convention as RBAC menu seeds, e.g. `lucide:wallet`).
//!
//! [`crate::runtime_config::preferences_schema`] merges the dictionary with the
//! JSON-Schema-derived type/default/constraint data and enforces (via
//! `preferences_schema_ui_gaps`) that every schema leaf is covered exactly once
//! and carries a real, bilingual `help`.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::domain::{
    FieldSemantics, FieldWhen, FieldWidget, ModelPickerProps, ModelPickerSide, SchemaFieldRef,
    SchemaNode, SchemaSection, SchemaUnion, SchemaUnionCase, UiProps, UiText,
};
use crate::enums::common::MarketCategory;
use crate::schema::factor_names::GENERIC_SCORING_FACTOR_NAMES;
use serde_json::Value;

/// Per-leaf UI metadata registered at compile time.
#[derive(Clone)]
pub struct FieldUiEntry {
    pub path: &'static str,
    pub label: UiText,
    pub help: UiText,
    pub widget: Option<FieldWidget>,
    pub semantics: Option<FieldSemantics>,
    pub ui_props: Option<UiProps>,
    pub model_picker: Option<ModelPickerProps>,
    /// Fixed map keys for decimal-map / weight-map widgets (declared in UI overlay).
    pub static_map_keys: Option<&'static [&'static str]>,
    pub visible: bool,
    pub when: Vec<FieldWhen>,
}

impl FieldUiEntry {
    /// Mark the field as governance-critical (danger confirmation on mutation).
    #[must_use]
    const fn critical(mut self) -> Self {
        self.semantics = Some(FieldSemantics::GovernanceCritical);
        self
    }

    /// Mark the field as a masked credential (empty patch keeps the stored secret).
    #[must_use]
    const fn credential(mut self) -> Self {
        self.widget = Some(FieldWidget::SecretString);
        self.semantics = Some(FieldSemantics::Credential);
        self
    }

    /// Mark the field as "empty selection means all".
    #[must_use]
    const fn empty_means_all(mut self) -> Self {
        self.semantics = Some(FieldSemantics::EmptyMeansAll);
        self
    }

    /// Override the render widget.
    #[must_use]
    const fn widget(mut self, widget: FieldWidget) -> Self {
        self.widget = Some(widget);
        self
    }

    /// Attach a static unit suffix (USD / bps / s / % …).
    #[must_use]
    fn suffix(mut self, unit: &'static str) -> Self {
        let mut props = self.ui_props.take().unwrap_or_default();
        props.suffix = Some(unit.to_owned());
        self.ui_props = Some(props);
        self
    }

    /// Grid width in 24-column units (`1..=24`); default is full row when unset.
    #[must_use]
    fn col_span(mut self, span: u8) -> Self {
        let mut props = self.ui_props.take().unwrap_or_default();
        props.col_span = Some(span.clamp(1, 24));
        self.ui_props = Some(props);
        self
    }

    /// Slider range for [`FieldWidget::RatioSlider`] fields.
    #[must_use]
    fn slider_range(mut self, min: f64, max: f64, step: f64) -> Self {
        let mut props = self.ui_props.take().unwrap_or_default();
        props.slider_min = Some(min);
        props.slider_max = Some(max);
        props.slider_step = Some(step);
        self.ui_props = Some(props);
        self
    }

    /// Fixed string keys for open decimal-map editors (e.g. factor-weight rows).
    #[must_use]
    const fn map_keys(mut self, keys: &'static [&'static str]) -> Self {
        self.static_map_keys = Some(keys);
        self
    }

    /// Attach conditional visibility rules (hidden until they all match).
    #[must_use]
    fn visible_when(mut self, rules: Vec<FieldWhen>) -> Self {
        self.when = rules;
        self
    }

    /// Render as a governed model-version picker, filtered to `category` /
    /// `side` (11.2.2 remediation R8).
    #[must_use]
    const fn model_version_select(
        mut self,
        category: Option<MarketCategory>,
        side: ModelPickerSide,
    ) -> Self {
        self.widget = Some(FieldWidget::ModelVersionSelect);
        self.model_picker = Some(ModelPickerProps { category, side });
        self
    }
}

/// One `if target == true` visibility rule (parent toggle gates its children).
fn enabled(target: &'static str) -> Vec<FieldWhen> {
    vec![FieldWhen::visible_when_eq(target, Value::Bool(true))]
}

/// Two `if target == true` rules (nested toggle: both ancestors must be on).
fn enabled2(outer: &'static str, inner: &'static str) -> Vec<FieldWhen> {
    vec![
        FieldWhen::visible_when_eq(outer, Value::Bool(true)),
        FieldWhen::visible_when_eq(inner, Value::Bool(true)),
    ]
}

// ---------------------------------------------------------------------------
// Field-entry constructors
// ---------------------------------------------------------------------------

/// Base field entry with authored bilingual label + rich help.
fn f(
    path: &'static str,
    label_en: &'static str,
    label_zh: &'static str,
    help_en: &'static str,
    help_zh: &'static str,
) -> FieldUiEntry {
    FieldUiEntry {
        path,
        label: UiText::localized(label_en, label_zh),
        help: UiText::localized(help_en, help_zh),
        widget: None,
        semantics: None,
        ui_props: None,
        model_picker: None,
        static_map_keys: None,
        visible: true,
        when: Vec::new(),
    }
}

fn usd(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::DecimalString)
        .suffix("USD")
}

fn integer(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz).widget(FieldWidget::Integer)
}

fn secs(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::Integer)
        .suffix("s")
}

fn millis(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::DurationMs)
        .suffix("ms")
}

fn bps(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::Integer)
        .suffix("bps")
}

/// A `[0, 1]` ratio decimal (help states the range; slider UX).
fn ratio(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::RatioSlider)
        .slider_range(0.0, 1.0, 0.01)
}

/// A `(0, 1]` ratio decimal — slider minimum is 0.01 so zero is unreachable.
fn ratio_half_open(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::RatioSlider)
        .slider_range(0.01, 1.0, 0.01)
}

/// A bounded decimal slider over an explicit `[min, max]` range — for
/// non-ratio decimals that still benefit from slider UX (e.g. a shrink
/// coefficient), matching `validation::bounded_decimal`'s range exactly so
/// the UI never offers a value the backend would reject.
#[allow(clippy::too_many_arguments)]
fn decimal_bounded(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
    min: f64,
    max: f64,
    step: f64,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::RatioSlider)
        .slider_range(min, max, step)
}

/// A two-sided confidence level, strictly within `(0.5, 1)` (matches
/// `validation::bounded_decimal`/the `(0.5, 1)` checks for both
/// `factors.structural.favorite_longshot.ci_confidence` and
/// `model.calibration.ci_confidence` — a Wilson interval degenerates at or
/// below `0.5` confidence, so the slider never offers a value validation
/// would reject).
fn ratio_confidence(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz)
        .widget(FieldWidget::RatioSlider)
        .slider_range(0.51, 0.99, 0.01)
}

fn decimal(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz).widget(FieldWidget::DecimalString)
}

fn boolean(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz).widget(FieldWidget::Boolean)
}

fn enum_select(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz).widget(FieldWidget::EnumSelect)
}

fn plain(
    path: &'static str,
    le: &'static str,
    lz: &'static str,
    he: &'static str,
    hz: &'static str,
) -> FieldUiEntry {
    f(path, le, lz, he, hz).widget(FieldWidget::PlainString)
}

// ---------------------------------------------------------------------------
// Public accessors
// ---------------------------------------------------------------------------

/// The runtime-config layout tree (top-level sections in display order).
#[must_use]
pub fn schema_tree() -> Vec<SchemaNode> {
    build_tree()
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

/// Every dotted path referenced by the layout tree (fields, union-case children).
#[must_use]
pub fn tree_field_paths() -> Vec<String> {
    let mut paths = Vec::new();
    for node in schema_tree() {
        collect_paths(&node, &mut paths);
    }
    paths
}

fn collect_paths(node: &SchemaNode, out: &mut Vec<String>) {
    match node {
        SchemaNode::Field(field) => out.push(field.path.clone()),
        SchemaNode::Section(section) => {
            for child in &section.children {
                collect_paths(child, out);
            }
        }
        SchemaNode::Union(union) => {
            for case in &union.cases {
                for child in &case.children {
                    collect_paths(child, out);
                }
            }
        }
    }
}

/// Localized label for a scalar enum wire value (`snake_case` slug).
///
/// Curated bilingual labels for the small policy enums; domain enums (market
/// categories, feature / factor families) fall back to a humanized slug that
/// reads acceptably in both locales.
#[must_use]
pub fn enum_label(value: &str) -> UiText {
    match value {
        // FeatureStalenessPolicy
        "reject_stale_required" => UiText::localized("Reject when stale", "陈旧则拒绝"),
        "allow_degraded" => UiText::localized("Allow degraded", "允许降级"),
        // MissingFactorPolicy
        "zero_weight" => UiText::localized("Treat as zero weight", "按零权重处理"),
        "reject_candidate" => UiText::localized("Reject candidate", "剔除候选"),
        // SmallCrossSectionPolicy
        "indeterminate" => UiText::localized("Indeterminate", "标记为不确定"),
        "historical_quantile" => UiText::localized("Historical quantile", "历史分位归一化"),
        // NeutralizeDimension
        "category" => UiText::localized("Market category", "市场分类"),
        // ReportDeliveryPolicy
        "store_and_notify" => UiText::localized("Store & notify", "存储并通知"),
        "store_only" => UiText::localized("Store only", "仅存储"),
        // ConfidenceSizeCurve
        "linear" => UiText::localized("Linear", "线性"),
        "step" => UiText::localized("Step", "阶梯"),
        // DrawdownMultiplierPolicy
        "fixed" => UiText::localized("Fixed", "固定"),
        "conservative" => UiText::localized("Conservative", "保守"),
        // PortfolioSolverKind
        "microlp" => UiText::localized("microlp (pure Rust)", "microlp（纯 Rust）"),
        "highs" => UiText::localized("HiGHS (native)", "HiGHS（原生）"),
        // Learning-to-rank training objective (simplex surrogates, not GBDT LambdaMART)
        "rank_ic_weighted_ranknet" => {
            UiText::localized("RankIC-weighted RankNet", "RankIC 加权 RankNet")
        }
        "pairwise_ranknet" => UiText::localized("Pairwise RankNet", "Pairwise RankNet"),
        "argmin" => UiText::localized("argmin refinement", "argmin 精修"),
        "coordinate_search" => UiText::localized("Coordinate search", "坐标搜索"),
        // EmergencyExitKind
        "liquidate_all" => UiText::localized("Liquidate all", "全部平仓"),
        "manual_only" => UiText::localized("Manual only", "仅人工处理"),
        // FeatureFamily
        "market_metadata" => UiText::localized("Market metadata", "市场元数据"),
        "price_book" => UiText::localized("Price & book", "价格与订单簿"),
        "time_series" => UiText::localized("Time series", "时间序列"),
        // FactorFamily / FeatureFamily overlap
        "microstructure" => UiText::localized("Microstructure", "微观结构"),
        "structural" => UiText::localized("Structural", "结构性"),
        "liquidity" => UiText::localized("Liquidity", "流动性"),
        "momentum" => UiText::localized("Momentum", "动量"),
        "mean_reversion" => UiText::localized("Mean reversion", "均值回归"),
        "volatility" => UiText::localized("Volatility", "波动率"),
        "activity" => UiText::localized("Activity", "活跃度"),
        "resolution" => UiText::localized("Resolution proximity", "结算临近度"),
        "data_quality" => UiText::localized("Data quality", "数据质量"),
        "domain_sports" => UiText::localized("Domain: sports", "垂直：体育"),
        "domain_politics" => UiText::localized("Domain: politics", "垂直：政治"),
        "domain_crypto" => UiText::localized("Domain: crypto", "垂直：加密"),
        "domain_weather" => UiText::localized("Domain: weather", "垂直：天气"),
        "domain_geopolitics" => UiText::localized("Domain: geopolitics", "垂直：地缘政治"),
        // MarketCategory
        "geopolitics" => UiText::localized("Geopolitics", "地缘政治"),
        "sports" => UiText::localized("Sports", "体育"),
        "politics" => UiText::localized("Politics", "政治"),
        "finance" => UiText::localized("Finance", "金融"),
        "tech" => UiText::localized("Tech", "科技"),
        "culture" => UiText::localized("Culture", "文化"),
        "weather" => UiText::localized("Weather", "天气"),
        "economics" => UiText::localized("Economics", "经济"),
        "crypto" => UiText::localized("Crypto", "加密"),
        "other" => UiText::localized("Other", "其他"),
        slug => {
            let humanized = humanize(slug);
            UiText::localized(humanized.clone(), humanized)
        }
    }
}

/// Title-case a `snake_case` slug for a fallback enum label.
fn humanize(slug: &str) -> String {
    let mut out = String::with_capacity(slug.len());
    for (index, part) in slug.split('_').filter(|p| !p.is_empty()).enumerate() {
        if index > 0 {
            out.push(' ');
        }
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    out
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
        domain_fields(),
        factor_fields(),
        model_fields(),
        quality_gate_fields(),
        research_training_fields(),
        research_validation_fields(),
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

// ---------------------------------------------------------------------------
// Field dictionary — Selection
// ---------------------------------------------------------------------------

fn selection_fields() -> Vec<FieldUiEntry> {
    vec![
        f(
            "selection.enabled_categories",
            "Enabled categories",
            "启用分类",
            "Market categories eligible for quant reports. Leave empty to allow every category; adding categories restricts selection to only those, shrinking the candidate pool and biasing the book toward those verticals.",
            "允许进入 quant 报告的市场分类。留空表示允许全部分类；一旦选择分类，则只保留这些分类，会缩小候选池并使持仓偏向这些垂直领域。",
        )
        .widget(FieldWidget::EnumSet)
        .empty_means_all(),
        usd(
            "selection.min_liquidity_usd",
            "Minimum liquidity",
            "最低流动性",
            "Markets whose displayed liquidity is below this are excluded from selection. Raising it improves executability but discards thin markets; 0 disables the floor.",
            "显示流动性低于此值的市场将被剔除。调高可提升可成交性但会丢弃浅市场；填 0 表示不设下限。",
        ),
        usd(
            "selection.min_volume_24h_usd",
            "Minimum 24h volume",
            "最低 24h 成交量",
            "Minimum trailing 24-hour traded volume for a market to qualify. Higher values favor active, price-discovered markets and drop stale ones; 0 disables.",
            "市场进入候选所需的过去 24 小时成交量下限。调高更偏好活跃、价格充分发现的市场，剔除冷清市场；填 0 表示不限制。",
        ),
        bps(
            "selection.max_spread_bps",
            "Maximum spread",
            "最大价差",
            "Reject markets whose top-of-book spread exceeds this width (basis points). Tighter caps improve entry quality but shrink the candidate pool.",
            "拒绝盘口价差超过该宽度（基点）的市场。收紧可提升入场质量但会缩小候选池。",
        ),
        boolean(
            "selection.allow_near_resolution",
            "Allow near-resolution markets",
            "允许临近结算市场",
            "When off, markets closer to resolution than the minimum time-to-resolution are excluded. Enabling admits late-cycle markets, which carry higher settlement / gap risk.",
            "关闭时，剩余结算时间低于下限的市场会被剔除。开启则纳入临近结算的市场，其结算/跳空风险更高。",
        ),
        secs(
            "selection.min_time_to_resolution_secs",
            "Minimum time to resolution",
            "最短结算剩余时间",
            "Markets resolving sooner than this are excluded (unless near-resolution is allowed). Guards against entering positions with too little runway to realize edge.",
            "结算剩余时间短于此值的市场会被剔除（除非允许临近结算）。避免进入没有足够时间兑现 edge 的仓位。",
        ),
        secs(
            "selection.max_time_to_resolution_secs",
            "Maximum time to resolution",
            "最长结算剩余时间",
            "Markets resolving later than this are excluded. Caps capital lock-up in very long-dated markets; must be greater than the minimum.",
            "结算剩余时间长于此值的市场会被剔除。限制资金被超长周期市场占用；必须大于下限。",
        ),
        integer(
            "selection.max_selection_size",
            "Maximum selection size",
            "最大市场池规模",
            "Hard cap on the number of markets carried into the feature / scoring pipeline per run. Lower values cut compute cost but may drop viable candidates; must be > 0.",
            "每次运行进入特征/打分管线的市场数量硬上限。调低可降低计算开销，但可能丢弃可行候选；必须大于 0。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Data quality
// ---------------------------------------------------------------------------

fn data_quality_fields() -> Vec<FieldUiEntry> {
    vec![
        millis(
            "data_quality.max_book_age_ms",
            "Maximum book age",
            "最大订单簿年龄",
            "A book snapshot older than this is treated as stale by the staleness ladder and rejected for feature generation and execution admission. Lower values are safer but reject more during venue lag; must be > 0.",
            "订单簿快照超过该年龄即被陈旧度阶梯判为陈旧，特征生成与执行准入都会拒绝。调低更安全，但在场馆延迟时会拒绝更多；必须大于 0。",
        ),
        millis(
            "data_quality.max_ingest_lag_ms",
            "Maximum ingest pipeline lag",
            "最大入库管道滞后",
            "Backpressure ceiling on the live ingest pipeline (enqueue → ClickHouse flush-ack). Above it, execution admission defers and market selection tightens. This is live-plane health only, not venue book age; must be > 0.",
            "实时入库管道（入队 → ClickHouse flush-ack）的背压上限。超过后执行准入会延后、选市会收紧。这是实时链路健康度，与场馆订单簿年龄无关；必须大于 0。",
        ),
        secs(
            "data_quality.max_feature_bucket_age_secs",
            "Maximum feature bucket age",
            "最大特征桶年龄",
            "Oldest acceptable materialized feature bucket at decision time. Governs online/offline feature staleness (independent of live ingest lag); above it, features are treated as stale per the staleness policy. Must be > 0.",
            "决策时可接受的最旧物化特征桶年龄。控制在线/离线特征陈旧度（与实时入库滞后独立）；超过后按特征陈旧策略处理。必须大于 0。",
        ),
        secs(
            "data_quality.max_trade_tape_age_secs",
            "Maximum trade-tape age",
            "最大成交带年龄",
            "Oldest acceptable on-chain trade-tape print at decision time. Governs structural participant-concentration staleness; above it, trade-tape features are treated as stale per the staleness policy. Must be > 0.",
            "决策时可接受的最旧链上成交带打印时间。控制结构性参与者集中度特征的陈旧度；超过后按特征陈旧策略处理成交带特征。必须大于 0。",
        ),
        secs(
            "data_quality.max_domain_observation_age_secs",
            "Maximum domain-observation age",
            "最大域观测年龄",
            "Oldest acceptable external domain observation (Binance kline / Chainlink oracle) at decision time. Above it, domain features fail closed or degrade per the feature staleness policy. Must be > 0.",
            "决策时可接受的最旧外部域观测（Binance K 线 / Chainlink 预言机）。超过后域特征按特征陈旧策略 fail-closed 或降级。必须大于 0。",
        ),
        boolean(
            "data_quality.reject_crossed_books",
            "Reject crossed books",
            "拒绝交叉订单簿",
            "Drop books whose bid ≥ ask (crossed / locked) before feature generation. Recommended on; disabling admits structurally invalid books and pollutes features.",
            "在特征生成前丢弃买价≥卖价（交叉/锁定）的订单簿。建议开启；关闭会纳入结构非法的订单簿并污染特征。",
        ),
        boolean(
            "data_quality.reject_empty_books",
            "Reject empty books",
            "拒绝空订单簿",
            "Drop books with no visible depth on a side before feature generation. Recommended on; disabling lets empty books through and distorts spread / depth features.",
            "在特征生成前丢弃某一侧无可见深度的订单簿。建议开启；关闭会放行空订单簿并扭曲价差/深度特征。",
        ),
        enum_select(
            "data_quality.feature_staleness_policy",
            "Feature staleness policy",
            "特征新鲜度策略",
            "How the feature plane handles a required feature that is stale/missing: 'Reject when stale' fails the candidate closed (safest); 'Allow degraded' keeps scoring with a confidence penalty.",
            "特征平面对陈旧/缺失的必需特征的处理方式：『陈旧则拒绝』失败关闭该候选（最安全）；『允许降级』则带置信度折减继续打分。",
        ),
        bps(
            "data_quality.max_stale_book_ratio_bps",
            "Maximum stale-book ratio",
            "最大陈旧订单簿比例",
            "Execution admission (data-quality check) denies when the fraction of stale tokens across the live book plane exceeds this (basis points, ≤ 10000 = 100%). Lower is stricter about system-wide freshness.",
            "当实时订单簿平面中陈旧 token 的比例超过该值（基点，≤10000 即 100%）时，执行准入的数据质量检查会拒绝。调低对全局新鲜度要求更严。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Features
// ---------------------------------------------------------------------------

fn feature_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "features.feature_schema_version",
            "Feature schema version",
            "特征 schema 版本",
            "Identity of the active feature schema. Bumping it changes the feature-schema hash, so the model factory rejects artifacts trained under the old semantics (forcing rematerialize + retrain) instead of silently scoring different features. Must be positive.",
            "当前特征 schema 的身份标识。修改会改变特征 schema 哈希，模型工厂将拒绝在旧语义下训练的产物（强制重新物化+重训），而非静默用不同特征打分。必须为正。",
        ),
        f(
            "features.enabled_feature_families",
            "Enabled feature families",
            "启用特征族",
            "Which feature-builder groups the feature plane computes. Disabling a family removes its features from scoring and can invalidate models that require them; domain families are routed by market category.",
            "特征平面计算哪些特征构建组。停用某族会将其特征移出打分，并可能使依赖它们的模型失效；垂直（domain）族由市场分类路由。",
        )
        .widget(FieldWidget::EnumSet),
        f(
            "features.required_features",
            "Required features",
            "必需特征",
            "Feature names that must be present (and fresh) for a candidate to be scored. Each must exist in the active feature schema and be unique; a missing required feature fails the candidate closed.",
            "候选被打分所必须存在（且新鲜）的特征名。每项都必须存在于当前特征 schema 且唯一；缺失任一必需特征会使候选失败关闭。",
        )
        .widget(FieldWidget::StringList),
        f(
            "features.bar_windows_secs",
            "Bar windows",
            "K 线窗口",
            "Aggregation windows (seconds) for bar features. Each window is prefetched from historical data; more/longer windows increase compute and lookback cost. Must contain at least one positive value.",
            "K 线类特征的聚合窗口（秒）。每个窗口都会从历史数据预取；更多/更长的窗口会增加计算与回看成本。至少包含一个正值。",
        )
        .widget(FieldWidget::JsonTree),
        f(
            "features.momentum.roc_windows_secs",
            "Momentum ROC windows",
            "动量 ROC 窗口",
            "Lookback windows (seconds) for the lag-skipped rate-of-change momentum. Each must exceed the ROC lag; more/longer windows increase prefetch cost.",
            "跳过近端的变化率(ROC)动量的回看窗口（秒）。每个窗口都必须大于 ROC lag；更多/更长的窗口会增加预取成本。",
        )
        .widget(FieldWidget::JsonTree),
        secs(
            "features.momentum.roc_lag_secs",
            "Momentum ROC lag",
            "动量 ROC 跳过窗口",
            "Seconds skipped at the near edge of each ROC window (classic 12-1 momentum: exclude the recent reversal-prone segment). Must be smaller than every ROC window.",
            "每个 ROC 窗口近端跳过的秒数（经典 12-1 动量：排除近端易反转段）。必须小于所有 ROC 窗口。",
        ),
        secs(
            "features.momentum.ema_fast_secs",
            "MACD fast EMA half-life",
            "MACD 快线 EMA 半衰期",
            "Fast EMA half-life (seconds) for the MACD fast leg and the EMA-slope estimator: an observation's weight halves every N seconds of elapsed time (a true duration, not a point count). Must be strictly less than the slow half-life.",
            "MACD 快腿与 EMA 斜率估计的快 EMA 半衰期（秒）：观测权重每经过 N 秒真实时间减半（真实时长，而非点数）。必须严格小于慢线半衰期。",
        ),
        secs(
            "features.momentum.ema_slow_secs",
            "MACD slow EMA half-life",
            "MACD 慢线 EMA 半衰期",
            "Slow EMA half-life (seconds) for the MACD slow leg; same duration semantics as the fast half-life. Must be strictly greater than the fast half-life.",
            "MACD 慢腿的慢 EMA 半衰期（秒）；与快线半衰期同为真实时长语义。必须严格大于快线半衰期。",
        ),
        f(
            "features.momentum.slope_windows_secs",
            "EMA-slope windows",
            "EMA 斜率窗口",
            "Lookback windows (seconds) for the EMA-slope momentum estimator. Must contain at least one positive value.",
            "EMA 斜率动量估计的回看窗口（秒）。至少包含一个正值。",
        )
        .widget(FieldWidget::JsonTree),
        f(
            "features.volatility_windows_secs",
            "Volatility windows",
            "波动率窗口",
            "Lookback windows (seconds) for volatility features. Drives historical prefetch; must contain at least one positive value.",
            "波动率类特征的回看窗口（秒）。决定历史预取；至少包含一个正值。",
        )
        .widget(FieldWidget::JsonTree),
        f(
            "features.depth_levels",
            "Depth levels",
            "订单簿深度层级",
            "Order-book depth levels inspected by book / microstructure features (e.g. top-1 / 3 / 5). Deeper levels capture more structure at higher compute cost.",
            "订单簿/微观结构特征检查的深度层级（如 top-1/3/5）。更深的层级捕捉更多结构，但计算成本更高。",
        )
        .widget(FieldWidget::JsonTree),
        integer(
            "features.max_concurrent_market_resolves",
            "Max concurrent market resolves",
            "特征 resolve 最大并发",
            "Upper bound on concurrent per-market point-in-time resolves in the feature pipeline. Higher throughput at the cost of DB / CPU pressure.",
            "特征管线中每市场 point-in-time resolve 的并发上限。调高吞吐更快，但会增加数据库/CPU 压力。",
        ),
    ]
    .into_iter()
    .chain(structural_feature_fields())
    .collect()
}

// ---------------------------------------------------------------------------
// Field dictionary — Domain (external verticals)
// ---------------------------------------------------------------------------

fn domain_fields() -> Vec<FieldUiEntry> {
    vec![
        f(
            "domain.enabled_by_family",
            "Enabled domain families",
            "启用的域垂直族",
            "Per-vertical enablement for the external domain plane. A disabled family fails closed (`domain: None`) for its categories; only enabled families may serve linkage-backed domain features.",
            "外部域平面的逐垂直启用开关。禁用的族对其分类 fail-closed（`domain: None`）；仅启用的族可提供联动支撑的域特征。",
        )
        .widget(FieldWidget::JsonTree),
        secs(
            "domain.crypto.source_delay_secs",
            "Crypto source visibility delay",
            "加密源可见性延迟",
            "PIT delay applied to domain observations: only rows with `event_time <= as_of - delay` are visible to crypto domain features. Mirrors report `source_delay_secs`; must be ≥ 0.",
            "应用于域观测的 PIT 延迟：仅 `event_time <= as_of - delay` 的行对加密域特征可见。镜像报告 `source_delay_secs`；必须 ≥ 0。",
        ),
        integer(
            "domain.crypto.backfill_days",
            "Crypto backfill window",
            "加密回填窗口",
            "Days of history the domain ingest worker backfills on bootstrap before switching to incremental polling. Exact for Binance klines; a ceiling for Chainlink, additionally capped by the deploy-config `max_round_backscan` round count (a Chainlink feed may backfill fewer days when its round cadence is sparse). Larger windows cost more Binance weight budget.",
            "域摄取 worker 在启动时回填的历史天数，之后切换为增量轮询。对 Binance K 线是精确值；对 Chainlink 是上限——另受部署配置 `max_round_backscan` 轮次数量的约束（若某个 feed 的轮次节奏较稀疏，实际回填天数可能更短）。窗口越大消耗的 Binance weight 预算越多。",
        ),
        secs(
            "domain.crypto.momentum_window_secs",
            "Crypto momentum lookback",
            "加密动量回看",
            "Lookback (seconds) for the underlying momentum feature fed into crypto domain builders. Must be > 0.",
            "加密域构建器所用底层动量特征的回看窗口（秒）。必须 > 0。",
        ),
        secs(
            "domain.crypto.volatility_window_secs",
            "Crypto volatility lookback",
            "加密波动率回看",
            "Lookback (seconds) for the underlying realized-volatility feature fed into crypto domain builders. Must be > 0.",
            "加密域构建器所用底层已实现波动率特征的回看窗口（秒）。必须 > 0。",
        ),
        bps(
            "domain.crypto.cross_check.max_basis_bps",
            "Maximum Binance–Chainlink basis",
            "Binance–Chainlink 最大基差",
            "When the settlement oracle is Chainlink, basis between Binance and Chainlink PIT quotes above this (bps) raises a cross-check risk signal and appends a durable, operator-acknowledgeable alert to the basis-alert queue — never silently clamps a feature.",
            "结算预言机为 Chainlink 时，Binance 与 Chainlink PIT 报价基差超过此值（bps）会触发交叉核验风险信号，并写入 basis 告警队列供操作员确认处理——绝不静默钳制特征值。",
        ),
        secs(
            "domain.crypto.cross_check.alert_cooldown_secs",
            "Basis-alert cooldown",
            "基差告警冷却期",
            "Minimum seconds between two persisted basis-exceedance alerts for the same market. A market whose basis persistently exceeds the threshold raises one alert per cooldown window, not one per report round.",
            "同一市场两次持久化基差超限告警之间的最小间隔（秒）。持续超限的市场每个冷却窗口只触发一次告警，而非每轮报告都触发。",
        ),
        secs(
            "domain.crypto.cross_check.max_oracle_staleness_secs",
            "Max Chainlink oracle staleness",
            "Chainlink 预言机最大滞后",
            "Risk control for the freshness gap between on-chain Chainlink Data Feeds (push, deviation/heartbeat cadence) and Polymarket's Data Streams settlement path. Oracle observations older than this (seconds) are rejected as stale in basis and price-to-beat features — mitigates but does not eliminate cross-source drift; true Data Streams ingest requires a paid subscription (Phase 11.2.3).",
            "缓解链上 Chainlink Data Feeds（推送式、按偏差/心跳更新）与 Polymarket Data Streams 结算路径之间新鲜度差异的风险控制。超过此秒数的预言机观测在基差与 PTB 特征中被拒绝为滞后——缓解而非消除跨源偏离；真正的 Data Streams 接入需付费订阅（11.2.3 阶段）。",
        ),
    ]
}

/// Structural feature-family windows (Phase 11.2.1).
fn structural_feature_fields() -> Vec<FieldUiEntry> {
    vec![
        secs(
            "features.structural.shock_window_secs",
            "Structural shock window",
            "结构冲击窗口",
            "Lookback (seconds) for the shock ratio / realized-vol estimator that gates `struct.reversal_after_shock`. Drives historical prefetch for the structural feature plane.",
            "结构冲击比率/已实现波动估计的回看窗口（秒），用于门控 `struct.reversal_after_shock`。决定结构特征平面的历史预取。",
        ),
        secs(
            "features.structural.book_churn_window_secs",
            "Book-churn window",
            "订单簿 churn 窗口",
            "Lookback (seconds) for the book-churn intensity proxy (delta-to-update ratio over the microstructure window). NOT true maker concentration (which needs trade-tape; see Phase 11.2.1.1).",
            "订单簿 churn 强度代理（微观结构窗口内 delta/update 比率）的回看窗口（秒）。非真·做市集中度（需 trade-tape，见 Phase 11.2.1.1）。",
        ),
        secs(
            "features.structural.trade_tape_window_secs",
            "Trade-tape window",
            "Trade tape 窗口",
            "Lookback (seconds) for participant-concentration features. The PIT query reads fill-side trade tape over `[as_of - delay - window, as_of - delay)`.",
            "参与者集中度特征的回看窗口（秒）。PIT 查询读取 `[as_of - delay - window, as_of - delay)` 内的 fill-side trade tape。",
        ),
        integer(
            "features.structural.trade_tape_min_unique_participants",
            "Trade-tape minimum trades",
            "Trade tape 最小成交数",
            "Minimum fill count required before participant-concentration features can score. Below this the feature emits an explicit insufficient-trade-tape null reason.",
            "参与者集中度特征可打分前要求的最小成交笔数。低于此值时特征输出明确的 insufficient-trade-tape 空值原因。",
        ),
        usd(
            "features.structural.trade_tape_min_notional_usd",
            "Trade-tape minimum notional",
            "Trade tape 最小名义额",
            "Minimum USD notional required before participant-concentration features can score. Keeps sparse dust fills from creating a concentration signal.",
            "参与者集中度特征可打分前要求的最小 USD 名义额。防止稀疏 dust 成交生成集中度信号。",
        ),
        ratio(
            "features.structural.trade_tape_min_coverage_ratio",
            "Trade-tape minimum coverage",
            "Trade tape 最小覆盖率",
            "Minimum source coverage ratio in [0, 1] required before participant-concentration features can score; missing coverage fails closed instead of becoming zero.",
            "参与者集中度特征可打分前要求的最小数据源覆盖率（[0,1]）；覆盖不足会 fail closed，而不是写成 0。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Factors
// ---------------------------------------------------------------------------

fn factor_fields() -> Vec<FieldUiEntry> {
    vec![
        f(
            "factors.enabled_factor_families",
            "Enabled factor families",
            "启用因子族",
            "Generic and platform-internal structural factor families computed online (must contain at least one). External vertical/domain families are routed by market category and must not appear here; disabling a family drops its factors from scoring.",
            "在线计算的通用与平台内结构因子族（至少一个）。外部垂直/domain 族由市场分类路由，不得在此出现；停用某族会将其因子移出打分。",
        )
        .widget(FieldWidget::EnumSet),
        f(
            "factors.factor_weights",
            "Factor weights",
            "因子权重",
            "Per-factor scoring weights. IMPORTANT: this overlay applies ONLY to Candidate / Shadow model versions and as the training seed — a Published active model always scores with its frozen artifact weights, so changing this does not affect production scoring until a new version is trained and published.",
            "各因子的打分权重。重要：该覆盖仅作用于候选(Candidate)/影子(Shadow)模型版本以及训练初值——已发布(Published)的活动模型始终使用其冻结的产物权重，因此修改此项在训练并发布新版本之前不会影响生产打分。",
        )
        .widget(FieldWidget::WeightMap)
        .map_keys(GENERIC_SCORING_FACTOR_NAMES),
        ratio(
            "factors.min_factor_confidence",
            "Minimum factor confidence",
            "最低因子置信度",
            "A factor contributes to scoring only when its confidence is at least this ([0, 1]). Higher values suppress low-evidence factors; too high can zero out most factors.",
            "因子只有在置信度不低于此值时才参与打分（[0,1]）。调高会压制证据不足的因子；过高会使多数因子归零。",
        ),
        enum_select(
            "factors.missing_factor_policy",
            "Missing factor policy",
            "缺失因子策略",
            "How a missing factor is handled: 'Treat as zero weight' keeps the candidate with reduced signal; 'Reject candidate' drops any candidate missing the factor (stricter).",
            "缺失因子的处理方式：『按零权重处理』保留候选但信号减弱；『剔除候选』则丢弃任何缺失该因子的候选（更严格）。",
        ),
        decimal(
            "factors.normalization.default_winsor_p",
            "Default winsorize percentile",
            "默认 winsorize 分位",
            "Cross-sectional winsorize tail fraction in (0, 0.5) applied before z-scoring (e.g. 0.01 clips the 1st / 99th percentiles). Larger values tame outliers more aggressively.",
            "z-score 前对截面做 winsorize 的尾部占比，取值 (0, 0.5)（如 0.01 表示裁剪 1%/99% 分位）。调大对离群点抑制更强。",
        ),
        decimal(
            "factors.normalization.default_clamp_sigma",
            "Default sigma clamp",
            "默认 sigma 截断",
            "Standardized scores are clamped to ±this many standard deviations before mapping into [0, 1] (e.g. 3). Smaller values compress the tails harder.",
            "标准化分数在映射到 [0,1] 前被截断到 ±该标准差数（如 3）。调小对尾部压缩更强。",
        ),
        f(
            "factors.normalization.per_factor",
            "Per-factor normalization overrides",
            "逐因子归一化覆盖",
            "Per-factor overrides of the normalization method and parameters, keyed by factor name (e.g. data_quality → min/max bounds). Factors without an entry use their declared method with the section defaults.",
            "按因子名对归一化方法与参数的覆盖（如 data_quality 使用 min/max 界）。没有条目的因子使用其声明的方法与本段默认参数。",
        )
        .widget(FieldWidget::JsonTree),
        integer(
            "factors.cross_section.min_size",
            "Cross-section minimum size",
            "截面最小规模",
            "Minimum number of present markets for cross-sectional normalization (winsorized z-score / rank). Below it the small-cross-section policy applies; never a silent neutral.",
            "进行截面归一化（winsorized z-score / rank）所需的最小在场市场数。低于此值时启用小样本策略；绝不静默中性化。",
        ),
        enum_select(
            "factors.cross_section.small_cross_section_policy",
            "Small-cross-section policy",
            "小样本策略",
            "What to do when the present cross-section is below the minimum: 'Indeterminate' emits a reasoned no-score; 'Historical quantile' normalizes against the factor's rolling history.",
            "在场截面低于最小规模时的处理：『不确定』给出带原因的无分数；『历史分位』则用该因子的滚动历史归一化。",
        ),
        secs(
            "factors.cross_section.historical_lookback_secs",
            "Historical quantile lookback",
            "历史分位回看窗口",
            "Rolling lookback (seconds) used by the 'Historical quantile' small-cross-section policy to build the factor's reference distribution.",
            "『历史分位』小样本策略用于构建该因子参考分布的滚动回看窗口（秒）。",
        ),
        ratio(
            "factors.orthogonalize.max_correlation",
            "Max factor correlation",
            "因子最大相关",
            "Absolute Spearman correlation tolerance between factors ([0, 1]). Pairs above it are flagged as collinear in the analysis report (a hard publish gate lands in a later phase).",
            "因子间 Spearman 绝对相关容忍度（[0,1]）。超过者在分析报告中被标记为共线（硬发布门禁在后续阶段落地）。",
        ),
        f(
            "factors.orthogonalize.neutralize_by",
            "Neutralize dimensions",
            "中性化维度",
            "Dimensions each factor is residualized against before normalization (e.g. market category) to remove structural exposure. Empty disables neutralization.",
            "归一化前对每个因子做残差化的维度（如市场分类），以移除结构性暴露。留空则关闭中性化。",
        )
        .widget(FieldWidget::EnumSet),
    ]
    .into_iter()
    .chain(structural_factor_fields())
    .collect()
}

/// Structural factor-plane parameters (Phase 11.2.1).
fn structural_factor_fields() -> Vec<FieldUiEntry> {
    vec![
        decimal(
            "factors.structural.reversal_after_shock.shock_k",
            "Reversal shock threshold",
            "反转冲击阈值",
            "Shock threshold k: `struct.reversal_after_shock` only fires when `|ret| / realized_vol > k`. Below it the factor is inert (never a fabricated neutral).",
            "冲击阈值 k：`struct.reversal_after_shock` 仅当 `|ret| / realized_vol > k` 时触发。低于此值因子保持 inert（绝不伪造中性值）。",
        ),
        decimal(
            "factors.structural.reversal_after_shock.shock_cap",
            "Reversal shock cap",
            "反转冲击上限",
            "Cap on the reported shock magnitude for `struct.reversal_after_shock` (bounds an extreme normalized signal).",
            "`struct.reversal_after_shock` 报告的冲击幅度上限（限制极端归一化信号）。",
        ),
        integer(
            "factors.structural.negrisk.min_legs",
            "Neg-risk minimum legs",
            "Neg-risk 最少腿数",
            "Minimum resolved YES legs for neg-risk structural factors to compute. Below this the factor is Indeterminate, never a silent value.",
            "Neg-risk 结构因子计算所需的最少已解析 YES 腿数。低于此值因子为 Indeterminate，绝不静默取值。",
        ),
    ]
    .into_iter()
    .chain(participant_concentration_factor_fields())
    .chain(favorite_longshot_factor_fields())
    .collect()
}

fn participant_concentration_factor_fields() -> Vec<FieldUiEntry> {
    vec![
        decimal(
            "factors.structural.participant_concentration.gini_weight",
            "Participant Gini weight",
            "参与者 Gini 权重",
            "Non-negative composite weight applied to `struct.participant_gini` when building the neutral participant-concentration risk/regime factor.",
            "构建中性参与者集中度风险/状态因子时应用在 `struct.participant_gini` 上的非负复合权重。",
        ),
        decimal(
            "factors.structural.participant_concentration.cr1_share_weight",
            "Participant top-1 weight",
            "参与者 Top-1 权重",
            "Non-negative composite weight applied to `struct.participant_cr1_share`; at least one participant-concentration weight must be positive.",
            "应用在 `struct.participant_cr1_share` 上的非负复合权重；参与者集中度权重至少有一个必须为正。",
        ),
        decimal(
            "factors.structural.participant_concentration.hhi_weight",
            "Participant HHI weight",
            "参与者 HHI 权重",
            "Non-negative composite weight applied to `struct.participant_hhi`. The resulting factor is neutral and does not imply YES/NO direction.",
            "应用在 `struct.participant_hhi` 上的非负复合权重。生成的因子为中性，不表达 YES/NO 方向。",
        ),
    ]
}

fn favorite_longshot_factor_fields() -> Vec<FieldUiEntry> {
    vec![
        plain(
            "factors.structural.favorite_longshot.bias_table_ref",
            "Favorite-longshot bias table",
            "Favorite-longshot 偏差表",
            "Content-addressed bias-table artifact id (UUID). `None` keeps `struct.favorite_longshot` inert — never a fabricated constant. Fit via the bias-table catalog, then activate here.",
            "内容寻址偏差表产物 id（UUID）。`None` 时 `struct.favorite_longshot` 保持 inert——绝不伪造常数。通过偏差表目录拟合后在此激活。",
        ),
        integer(
            "factors.structural.favorite_longshot.bins",
            "Bias-table price bins",
            "偏差表价格分桶",
            "Number of equal-width price buckets over `(0, 1)` the bias-table fit uses.",
            "偏差表拟合在 `(0, 1)` 上使用的等宽价格分桶数。",
        ),
        f(
            "factors.structural.favorite_longshot.ttr_bucket_bounds_secs",
            "Bias-table ttr buckets",
            "偏差表 ttr 分桶",
            "Ascending time-to-resolution bucket boundaries (seconds); `n` bounds define `n+1` conditioning buckets. The favorite-longshot bias is conditioned on residual time to resolution as well as category.",
            "升序的距结算时间分桶边界（秒）；`n` 个边界定义 `n+1` 个条件化分桶。favorite-longshot 偏差按距结算时间与分类共同条件化。",
        )
        .widget(FieldWidget::JsonTree),
        integer(
            "factors.structural.favorite_longshot.min_bin_samples",
            "Bias-table min bin samples",
            "偏差表分桶最小样本",
            "Minimum samples per `(category, ttr_bucket, price_bucket)` bin for a usable bias estimate (fail-closed below this).",
            "每个 `(category, ttr_bucket, price_bucket)` 分桶的可用偏差估计最小样本数（低于此 fail-closed）。",
        ),
        integer(
            "factors.structural.favorite_longshot.min_curve_samples",
            "Bias-table min curve samples",
            "偏差表曲线最小样本",
            "Minimum samples per `(category, ttr_bucket)` curve for it to be retained (fail-closed below this).",
            "每个 `(category, ttr_bucket)` 曲线保留所需的最小样本数（低于此 fail-closed）。",
        ),
        ratio_confidence(
            "factors.structural.favorite_longshot.ci_confidence",
            "Bias-table CI confidence",
            "偏差表置信区间",
            "Two-sided confidence level for the Wilson interval and the IC significance test during bias-table fit (e.g. 0.95). Must be within (0.5, 1) — a Wilson interval degenerates at or below 0.5.",
            "偏差表拟合 Wilson 区间与 IC 显著性检验的双侧置信水平（如 0.95）。必须严格落在 (0.5, 1) 之间——置信水平不高于 0.5 时 Wilson 区间会退化。",
        ),
        decimal(
            "factors.structural.favorite_longshot.ic_significance_min",
            "Bias-table IC floor",
            "偏差表 IC 下限",
            "Absolute `|IC|` floor a `(category, ttr_bucket)` curve must clear in addition to the Student-t significance test.",
            "`(category, ttr_bucket)` 曲线在 Student-t 显著性检验之外还须清过的 `|IC|` 绝对下限。",
        ),
        secs(
            "factors.structural.favorite_longshot.fit_sample_stride_secs",
            "Bias-table fit sample stride",
            "偏差表拟合采样步长",
            "Spacing between the point-in-time sample instants the fit draws over each market's lifecycle. The fit samples across the whole life (not a single pre-resolution lead), matching the served distribution.",
            "拟合在每个市场生命周期上抽取 PIT 采样点的间隔（秒）。拟合覆盖整个生命周期（非单一结算前提前量），匹配服务分布。",
        ),
        boolean(
            "factors.structural.per_category_ic_gate",
            "Per-category IC gate",
            "逐分类 IC 门控",
            "When true, disable a category's bias curve whose IC is not significant (soft gate; hard publish-gate is Phase 11.5).",
            "为 true 时，关闭 IC 不显著的分类偏差曲线（软门控；硬发布门禁在 Phase 11.5）。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Model
// ---------------------------------------------------------------------------

fn model_fields() -> Vec<FieldUiEntry> {
    vec![
        plain(
            "model.active_model_version_id",
            "Active model version",
            "活动模型版本",
            "The published model version used for online entry scoring. Changing it swaps the live scorer on the next report run — a high-impact governance action requiring the model to have cleared its quality gate. Empty means no active model (report generation is degraded).",
            "用于在线入场打分的已发布模型版本。修改会在下次报告运行时切换生产打分器——高影响治理动作，要求该模型已通过质量门。留空表示没有活动模型（报告生成降级）。",
        )
        .critical()
        .model_version_select(None, ModelPickerSide::Buy),
        plain(
            "model.shadow_model_version_id",
            "Shadow model version",
            "影子模型版本",
            "An optional model scored in parallel for comparison only (never drives entries). Used to measure divergence from the active model before promotion. Empty disables shadow scoring.",
            "可选的并行打分模型，仅用于对比（绝不驱动入场）。用于在晋升前衡量与活动模型的偏离。留空则关闭影子打分。",
        )
        .model_version_select(None, ModelPickerSide::Buy),
        plain(
            "model.active_exit_model_version_id",
            "Active exit (Sell) scorer version",
            "活动退出(卖出)评分模型版本",
            "The published hold-vs-exit Sell scorer loaded by the opportunistic-sell evaluator. A separate pointer from the entry model so Buy and Sell models are governed independently. Empty disables model-driven opportunistic exits.",
            "机会性卖出评估器加载的、已发布的『持有 vs 退出』卖出评分模型。与入场模型是独立指针，使买、卖模型分别治理。留空则关闭由模型驱动的机会性退出。",
        )
        .model_version_select(None, ModelPickerSide::Sell),
    ]
    .into_iter()
    .chain(category_model_pointer_fields())
    .chain([
        ratio(
            "model.min_model_confidence",
            "Minimum model confidence",
            "最低模型置信度",
            "Candidates whose model confidence is below this ([0, 1]) are dropped before portfolio construction. Higher values trade coverage for conviction.",
            "模型置信度低于此值（[0,1]）的候选会在组合构建前被剔除。调高以覆盖度换取确定性。",
        ),
        secs(
            "model.min_quality_gate_age_secs",
            "Maximum quality-gate report age",
            "质量门报告最大年龄",
            "Governance denies loading / publishing a model whose latest quality-gate report is older than this. Guards against activating models whose evidence has gone stale.",
            "治理层会拒绝加载/发布『最新质量门报告年龄超过此值』的模型。防止激活证据已过期的模型。",
        ),
        ratio(
            "model.candidate_score_floor",
            "Candidate score floor",
            "候选分数下限",
            "Minimum composite score to enter portfolio pruning. Candidates scoring below are discarded before sizing; raising it concentrates capital on stronger signals.",
            "进入组合裁剪的最低综合分。低于此分的候选在定量前被丢弃；调高会将资金集中到更强信号上。",
        ),
        decimal(
            "model.shadow_diff_threshold",
            "Shadow diff threshold",
            "影子差异阈值",
            "Absolute active-vs-shadow score divergence above which a divergence alert is raised and recorded. Lower values surface drift earlier (more alerts).",
            "活动与影子模型分数的绝对偏离超过该阈值时，会触发并记录偏离告警。调低会更早暴露漂移（告警更多）。",
        ),
    ])
    .chain(model_calibration_fields())
    .collect()
}

fn model_calibration_fields() -> Vec<FieldUiEntry> {
    vec![
        enum_select(
            "model.calibration.method",
            "Calibrator method",
            "校准方法",
            "Default probability calibrator for model-score fits: isotonic (monotone, needs enough samples) or Platt (sigmoid, small-sample friendly).",
            "模型分数校准默认方法：等张（单调，需足够样本）或 Platt（sigmoid，小样本友好）。",
        ),
        integer(
            "model.calibration.min_samples_isotonic",
            "Minimum isotonic samples",
            "等张最小样本数",
            "Minimum calibration-split samples required to select isotonic; below this the fit must use Platt (fail-closed, never silent downgrade).",
            "选用等张校准所需的最小校准集样本数；低于此值必须使用 Platt（fail-closed，禁止静默降级）。",
        ),
        secs(
            "model.calibration.embargo_secs",
            "Calibration embargo gap",
            "校准 embargo 间隔",
            "Minimum seconds between a model's training-dataset window end and its calibration-dataset window start (purge + embargo).",
            "模型训练集窗口结束与校准集窗口开始之间的最小间隔秒数（purge + embargo）。",
        ),
        boolean(
            "model.calibration.require_for_publish",
            "Require calibration for publish",
            "发布强制要求已校准",
            "Hard-gate GateId::CalibrationRequired: when on (the production default), Publish/AutoExecution on a Buy model requires a Calibrated return model. Disabling is an auditable, operator-governed cold-start bootstrap window — never disable outside one.",
            "硬门禁 GateId::CalibrationRequired：开启时（生产默认），Buy 模型的发布/自动执行需要已校准的收益模型。关闭是可审计、由运营方治理的冷启动窗口——除该窗口外禁止关闭。",
        )
        .critical(),
        ratio_confidence(
            "model.calibration.ci_confidence",
            "Reliability CI confidence",
            "可靠性置信水平",
            "Two-sided confidence level for Wilson intervals in reliability bins (edge-uncertainty shrink source). Must be within (0.5, 1).",
            "可靠性分箱 Wilson 区间的双侧置信水平（edge 不确定性收缩来源）。必须严格落在 (0.5, 1) 之间。",
        ),
    ]
}

fn category_model_pointer_fields() -> Vec<FieldUiEntry> {
    vec![
        plain(
            "model.category_model_pointers.crypto",
            "Crypto category model",
            "加密分类模型",
            "Published Buy-side model for Crypto markets (may consume the crypto domain slice). Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Crypto.",
            "Crypto 市场的已发布 Buy 侧模型（可消费加密域切片）。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Crypto 的版本。",
        )
        .model_version_select(Some(MarketCategory::Crypto), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.sports",
            "Sports category model",
            "体育分类模型",
            "Published Buy-side model for Sports markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Sports.",
            "Sports 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Sports 的版本。",
        )
        .model_version_select(Some(MarketCategory::Sports), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.politics",
            "Politics category model",
            "政治分类模型",
            "Published Buy-side model for Politics markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Politics.",
            "Politics 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Politics 的版本。",
        )
        .model_version_select(Some(MarketCategory::Politics), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.finance",
            "Finance category model",
            "金融分类模型",
            "Published Buy-side model for Finance markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Finance.",
            "Finance 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Finance 的版本。",
        )
        .model_version_select(Some(MarketCategory::Finance), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.tech",
            "Tech category model",
            "科技分类模型",
            "Published Buy-side model for Tech markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Tech.",
            "Tech 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Tech 的版本。",
        )
        .model_version_select(Some(MarketCategory::Tech), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.culture",
            "Culture category model",
            "文化分类模型",
            "Published Buy-side model for Culture markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Culture.",
            "Culture 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Culture 的版本。",
        )
        .model_version_select(Some(MarketCategory::Culture), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.weather",
            "Weather category model",
            "天气分类模型",
            "Published Buy-side model for Weather markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Weather.",
            "Weather 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Weather 的版本。",
        )
        .model_version_select(Some(MarketCategory::Weather), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.economics",
            "Economics category model",
            "经济分类模型",
            "Published Buy-side model for Economics markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Economics.",
            "Economics 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Economics 的版本。",
        )
        .model_version_select(Some(MarketCategory::Economics), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.geopolitics",
            "Geopolitics category model",
            "地缘政治分类模型",
            "Published Buy-side model for Geopolitics markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Geopolitics.",
            "Geopolitics 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Geopolitics 的版本。",
        )
        .model_version_select(Some(MarketCategory::Geopolitics), ModelPickerSide::Buy),
        plain(
            "model.category_model_pointers.other",
            "Other category model",
            "其他分类模型",
            "Published Buy-side model for Other markets. Empty falls back to the generic active model. The picker rejects a version whose artifact declares a category_scope other than Other.",
            "Other 市场的已发布 Buy 侧模型。留空则回落到通用活动模型。选择器会拒绝其 artifact 声明的 category_scope 不是 Other 的版本。",
        )
        .model_version_select(Some(MarketCategory::Other), ModelPickerSide::Buy),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Quality gate
// ---------------------------------------------------------------------------

fn quality_gate_fields() -> Vec<FieldUiEntry> {
    let mut fields = vec![
        integer(
            "quality_gate.min_sample_count",
            "Minimum sample count",
            "最小样本数",
            "Fewest resolved samples a model / dataset needs to clear the gate. Higher values demand more evidence before publish / promotion (stricter, slower iteration).",
            "模型/数据集通过质量门所需的最少已结算样本数。调高会在发布/晋升前要求更多证据（更严格、迭代更慢）。",
        ),
        ratio(
            "quality_gate.min_label_coverage",
            "Minimum label coverage",
            "最低标签覆盖率",
            "Minimum fraction of samples that carry a usable label ([0, 1]). Below it the gate fails: too many unlabeled rows make the metrics untrustworthy.",
            "带可用标签的样本最低占比（[0,1]）。低于此值门禁失败：未标注行过多会使指标不可信。",
        ),
        ratio(
            "quality_gate.min_critical_feature_coverage",
            "Minimum critical-feature coverage",
            "最低关键特征覆盖率",
            "Minimum fraction of build rows with all critical features present ([0, 1]). Guards against training on sparsely-featured data.",
            "关键特征齐全的构建行最低占比（[0,1]）。防止在特征稀疏的数据上训练。",
        ),
        ratio(
            "quality_gate.max_drawdown",
            "Maximum drawdown",
            "最大回撤",
            "Largest backtest drawdown tolerated ([0, 1]). A model whose backtest drawdown exceeds this fails the gate. Lower is more risk-averse.",
            "可容忍的最大回测回撤（[0,1]）。回测回撤超过此值的模型无法通过门禁。调低更厌恶风险。",
        ),
        ratio(
            "quality_gate.min_liquidity_exit_feasibility",
            "Minimum liquidity-exit feasibility",
            "最低流动性退出可行性",
            "Minimum fraction of positions that could realistically be exited into available liquidity ([0, 1]). A hard auto-execution publish gate: too low means positions can be entered but not cleanly exited.",
            "在可用流动性下可现实退出的仓位最低占比（[0,1]）。自动执行发布的硬门禁：过低意味着能进场但无法干净退出。",
        ),
        ratio(
            "quality_gate.min_shadow_overlap_stability",
            "Minimum shadow overlap stability",
            "最低影子重叠稳定性",
            "Minimum stability of the shadow-vs-active pick overlap ([0, 1]) required before publish. Guards against promoting a model whose selections are erratic relative to the incumbent.",
            "发布前所需的『影子与活动模型选择重叠』最低稳定性（[0,1]）。防止晋升选择相对现役模型不稳定的模型。",
        ),
        ratio(
            "quality_gate.max_category_concentration",
            "Maximum category concentration (soft)",
            "最大类别集中度（软）",
            "Soft ceiling on per-category sample concentration ([0, 1]); above it raises a soft warning. Guards against a model whose evidence is dominated by one category.",
            "各类别样本集中度的软上限（[0,1]）；超过时给出软告警。防止模型证据被单一类别主导。",
        ),
        secs(
            "quality_gate.required_shadow_window_secs",
            "Required shadow window",
            "所需影子观察窗口",
            "Minimum elapsed shadow-comparison window before a shadow model may be published. Ensures enough live comparison time; must be > 0.",
            "影子模型可发布前所需的最短影子对比观察时长。确保有足够的实盘对比时间；必须大于 0。",
        ),
    ];
    fields.extend(sell_quality_gate_fields());
    fields
}

fn sell_quality_gate_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "quality_gate.sell.min_sample_count",
            "Sell minimum sample count",
            "卖出侧最小样本数",
            "Fewest ExitDecision samples a Sell scorer needs to clear the Sell-side gate. Higher demands more exit evidence before the exit model can be published.",
            "卖出评分模型通过卖出侧门禁所需的最少 ExitDecision 样本数。调高会在发布退出模型前要求更多退出证据。",
        ),
        ratio(
            "quality_gate.sell.min_label_coverage",
            "Sell minimum label coverage",
            "卖出侧最低标签覆盖率",
            "Minimum fraction of Sell-side samples carrying a usable hold-vs-exit label ([0, 1]).",
            "带可用『持有 vs 退出』标签的卖出侧样本最低占比（[0,1]）。",
        ),
        decimal(
            "quality_gate.sell.min_exit_alpha_rank_ic",
            "Sell minimum exit-alpha rank IC",
            "卖出侧最低 exit-alpha rank IC",
            "Soft floor on the Sell scorer's exit-alpha rank IC (correlation in [-1, 1]). Measures how well the scorer ranks exit timing.",
            "卖出评分模型 exit-alpha rank IC 的软下限（相关系数，[-1,1]）。衡量其对退出时机的排序能力。",
        ),
        ratio(
            "quality_gate.sell.min_l2_book_fidelity_ratio",
            "Sell minimum L2 book fidelity",
            "卖出侧最低 L2 订单簿保真度",
            "Minimum fraction of ExitDecision rows simulated from full L2 books rather than microstructure fallback ([0, 1]). Higher demands more faithful exit simulation.",
            "从完整 L2 订单簿（而非微观结构回退）模拟的 ExitDecision 行最低占比（[0,1]）。调高要求更真实的退出模拟。",
        ),
        ratio(
            "quality_gate.sell.max_fallback_ratio",
            "Sell maximum fallback ratio",
            "卖出侧最高回退比例",
            "Maximum fraction of ExitDecision rows allowed to use the microstructure fallback ([0, 1]). Above it the Sell gate fails: too much fallback means unreliable exit fills.",
            "允许使用微观结构回退的 ExitDecision 行最高占比（[0,1]）。超过则卖出门禁失败：回退过多意味着退出成交不可靠。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Research
// ---------------------------------------------------------------------------

fn research_training_fields() -> Vec<FieldUiEntry> {
    vec![
        enum_select(
            "research.training.rank_loss",
            "Rank loss",
            "排序损失",
            "Ranking loss optimized within each same-as_of cross-section. RankIC-weighted RankNet is a simplex surrogate (not GBDT LambdaRankIC): RankNet pairs weighted by RankIC swap impact. Pairwise RankNet uses unweighted label-order pairs.",
            "同一 as_of 横截面内优化的排序损失。RankIC 加权 RankNet 是 simplex 代理（不是 GBDT LambdaRankIC）：按 RankIC 交换影响加权 RankNet pair。Pairwise RankNet 使用未加权的标签有序 pair。",
        )
        .col_span(12),
        enum_select(
            "research.training.optimizer",
            "Optimizer",
            "优化器",
            "Simplex-weight optimizer for weighted factor models. coordinate_search is the deterministic default. argmin requires the optimize feature and fails closed when unavailable — it never silently falls back.",
            "加权因子模型的 simplex 权重优化器。coordinate_search 是确定性默认。argmin 需要 optimize feature，不可用时 fail-closed，绝不静默回退。",
        )
        .col_span(12),
        decimal(
            "research.training.lambda_tail",
            "Tail penalty weight (λ)",
            "尾部惩罚权重 (λ)",
            "Non-negative multiplier on lower-tail TopN pseudo-portfolio return loss (optimization proxy, not the authoritative backtest capital path).",
            "下行尾部 TopN 伪组合收益损失的非负乘子（优化代理，不是权威回测资金路径）。",
        )
        .col_span(12),
        ratio_half_open(
            "research.training.tail_fraction",
            "Tail fraction",
            "尾部样本比例",
            "Worst fraction of grouped TopN pseudo-portfolio returns used for the tail penalty, in (0, 1]. 0.10 means the worst decile.",
            "尾部惩罚使用的最差 group TopN 伪组合收益比例，范围 (0,1]。0.10 表示最差十分位。",
        )
        .col_span(12),
        decimal(
            "research.training.lambda_turnover",
            "Turnover penalty weight (λ)",
            "换手惩罚权重 (λ)",
            "Non-negative multiplier on mean per-tick TopN pseudo-allocation turnover between adjacent as_of groups. Uses the same L1/2 turnover formula as backtest metrics, but on score-derived TopN token weights (optimization proxy ≠ backtest USD allocations).",
            "相邻 as_of group 间逐 tick TopN 伪配置换手均值的非负乘子。换手公式与回测相同（L1/2），但权重是 score 推导的 TopN token 配置（优化代理 ≠ 回测 USD 配置）。",
        )
        .col_span(12),
        decimal(
            "research.training.lambda_l2",
            "L2 penalty weight (λ)",
            "L2 惩罚权重 (λ)",
            "Non-negative multiplier on Σ weightᵢ². Keeps the learned simplex weights from collapsing too aggressively onto one factor.",
            "Σ weightᵢ² 的非负乘子。用于避免学习出的 simplex 权重过度集中到单一因子。",
        )
        .col_span(12),
        integer(
            "research.training.ndcg_k",
            "NDCG@k (diagnostic)",
            "NDCG@k（诊断）",
            "Truncation k for diagnostic NDCG@k reported in training metrics. Not part of the training loss. Must be in 1..=reports.max_top_n; default 20 aligns with typical TopN reports.",
            "训练指标中诊断用 NDCG@k 的截断 k。不进入训练损失。必须在 1..=reports.max_top_n；默认 20 与典型 TopN 报告对齐。",
        )
        .col_span(12),
        integer(
            "research.training.pseudo_top_n",
            "Pseudo TopN size",
            "伪组合 TopN",
            "How many top-scored tokens enter the score-derived pseudo portfolio used by tail and turnover penalties. Optimization proxy only; authoritative capital/LP checks remain in backtest/report.",
            "进入尾部/换手惩罚所用 score 伪组合的最高分 token 数量。仅优化代理；权威资金/LP 检查仍在回测/报告路径。",
        )
        .col_span(12),
    ]
}

fn research_validation_fields() -> Vec<FieldUiEntry> {
    let mut fields = research_validation_purge_cpcv_fields();
    fields.extend(research_validation_trials_fields());
    fields.extend(research_validation_pbo_gate_fields());
    fields
}

fn research_validation_purge_cpcv_fields() -> Vec<FieldUiEntry> {
    vec![
        ratio(
            "research.validation.purge.embargo_pct",
            "CPCV embargo fraction",
            "CPCV 禁运比例",
            "Post-test embargo window as a fraction of the full timeline span ([0, 1]). Groups in this window are excluded from training after each test fold. Label-horizon purge is always on (not configurable).",
            "禁运窗口占完整时间线跨度的比例（[0,1]）。每个测试折之后该窗口内的分组从训练中剔除。标签 horizon purge 恒开启（不可配置）。",
        )
        .col_span(12),
        integer(
            "research.validation.cpcv.n_groups",
            "CPCV partition count (N)",
            "CPCV 分区数 (N)",
            "Number of contiguous timeline partitions for Combinatorial Purged Cross-Validation (4..=32). Together with k_test determines φ reconstructed paths.",
            "组合 Purged 交叉验证的连续时间分区数（4..=32）。与 k_test 共同决定 φ 重构路径数。",
        )
        .col_span(12),
        integer(
            "research.validation.cpcv.k_test",
            "CPCV test folds (k)",
            "CPCV 测试折数 (k)",
            "Number of partitions held out as the test set per combination (1..=n_groups).",
            "每个组合的测试集分区数（1..=n_groups）。",
        )
        .col_span(12),
    ]
}

fn research_validation_trials_fields() -> Vec<FieldUiEntry> {
    vec![
        f(
            "research.validation.trials.lambda_multipliers",
            "Trial lambda multipliers",
            "Trial λ 乘子",
            "Multipliers applied to the base training lambdas when expanding the governed trial grid for DSR/PBO.",
            "展开受治理 trial 网格时应用于基础训练 λ 的乘子（DSR/PBO 多重检验校正）。",
        )
        .widget(FieldWidget::StringList)
        .col_span(12),
        f(
            "research.validation.trials.rank_loss_kinds",
            "Trial rank-loss variants",
            "Trial 排序损失变体",
            "Rank-loss kinds crossed with each lambda multiplier in the trial grid.",
            "trial 网格中与每个 λ 乘子交叉的排序损失变体。",
        )
        .widget(FieldWidget::EnumSet)
        .col_span(12),
        f(
            "research.validation.trials.forest_n_trees_multipliers",
            "Classical forest n_trees multipliers",
            "Classical 森林树数乘子",
            "Multipliers applied to ForestParams.n_trees when expanding the classical trial grid.",
            "展开 classical trial 网格时应用于 ForestParams.n_trees 的乘子。",
        )
        .widget(FieldWidget::StringList)
        .col_span(12),
        f(
            "research.validation.trials.linear_alpha_multipliers",
            "Classical linear alpha multipliers",
            "Classical 线性 α 乘子",
            "Multipliers applied to LinearParams.alpha when expanding the classical trial grid.",
            "展开 classical trial 网格时应用于 LinearParams.alpha 的乘子。",
        )
        .widget(FieldWidget::StringList)
        .col_span(12),
        integer(
            "research.validation.trials.max_trials",
            "Trial grid cap",
            "Trial 网格上限",
            "Hard cap on governed hyperparameter trials expanded for DSR/PBO multiple-testing correction.",
            "DSR/PBO 多重检验校正所展开的超参 trial 硬上限。",
        )
        .col_span(12),
    ]
}

fn research_validation_pbo_gate_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "research.validation.pbo.block_count",
            "PBO CSCV block count",
            "PBO CSCV 块数",
            "Number of equal-length time blocks for Combinatorially Symmetric Cross-Validation (must be even, default 16).",
            "组合对称交叉验证的等长时间块数（必须为偶数，默认 16）。",
        )
        .col_span(12),
        decimal(
            "research.validation.gates.rank_ic_min",
            "Minimum CPCV median rank IC",
            "CPCV 中位 rank IC 下限",
            "Hard floor on the CPCV path-set median rank IC (replaces the deleted single-path soft threshold).",
            "CPCV 路径集中位 rank IC 的硬下限（取代已删除的单路径软阈值）。",
        )
        .col_span(12),
        ratio(
            "research.validation.gates.dsr_significance",
            "DSR significance (α)",
            "DSR 显著性 (α)",
            "Target significance level: deflated_sharpe must clear 1 − α (hard gate).",
            "目标显著性水平：deflated Sharpe 必须超过 1 − α（硬门禁）。",
        )
        .col_span(12),
        ratio(
            "research.validation.gates.max_pbo",
            "Maximum PBO",
            "PBO 上限",
            "Maximum tolerated Probability of Backtest Overfitting ([0, 1], hard gate).",
            "可容忍的回测过拟合概率上限（[0,1]，硬门禁）。",
        )
        .col_span(12),
        ratio(
            "research.validation.gates.max_turnover",
            "Maximum turnover",
            "换手上限",
            "Maximum tolerated single-path turnover (hard gate; risk/execution realism).",
            "可容忍的单路径换手上限（硬门禁；风险/执行现实性）。",
        )
        .col_span(12),
        decimal(
            "research.validation.gates.min_tail_loss_bps",
            "Minimum tail loss (bps)",
            "尾部损失下限 (bps)",
            "Minimum tolerated single-path tail loss in bps (hard gate; tail_loss is typically negative, so this is a floor).",
            "可容忍的单路径尾部损失下限（bps，硬门禁；tail_loss 通常为负，因此这是下限）。",
        )
        .col_span(12),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Training
// ---------------------------------------------------------------------------

fn training_fields() -> Vec<FieldUiEntry> {
    vec![
        millis(
            "training.max_book_staleness_ms",
            "Historical max book staleness",
            "历史 PIT 最大订单簿陈旧度",
            "Point-in-time lookback for historical book resolution during dataset build. Snapshots older than (as_of − this) are treated as missing. Wider than the live gate because history is sparser; must be > 0.",
            "数据集构建时历史订单簿 point-in-time 解析的回看窗口。早于 (as_of − 此值) 的快照视为缺失。因历史数据更稀疏，此值比实时门更宽；必须大于 0。",
        ),
        usd(
            "training.min_exit_depth_usd",
            "Minimum exit depth",
            "退出标签最低深度",
            "Minimum forward top-1 depth (USD) required for the liquidity_exit_possible label to be true. Sets how much exit liquidity a training example must show to count as exitable.",
            "liquidity_exit_possible 标签为真所需的前瞻 top-1 深度（USD）下限。决定训练样本需展示多少退出流动性才算『可退出』。",
        ),
        usd(
            "training.min_selection_depth_usd",
            "Minimum PIT selection depth",
            "PIT 选择最低深度",
            "Book-derived liquidity floor (combined visible USD depth) a market must show at an as_of to enter the offline point-in-time selection funnel. The offline plane replays the online selection with book depth as the liquidity proxy (no Gamma liquidity/volume history), so this is a book-depth quantity distinct from selection.min_liquidity_usd; frozen with the config and captured in dataset_hash.",
            "市场在某个 as_of 进入离线 point-in-time 选择漏斗所需的书本派生流动性下限（合计可见 USD 深度）。离线平面用书本深度作为流动性代理复现线上选择（无 Gamma 流动性/量能历史），因此这是与 selection.min_liquidity_usd 不同的书本深度量纲；随配置冻结并计入 dataset_hash。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Reports
// ---------------------------------------------------------------------------

fn report_fields() -> Vec<FieldUiEntry> {
    vec![
        f(
            "reports.schedules",
            "Schedules",
            "报告计划",
            "Recurring report schedules. Each row has a stable id, a cadence (fixed interval or cron with optional timezone), a TopN size (≤ maximum TopN), a source delay, and an enabled toggle. Disabled schedules are removed from the live scheduler on activation.",
            "周期性报告计划。每行包含稳定 id、触发节奏（固定间隔或带可选时区的 cron）、TopN 规模（≤ 最大 TopN）、数据源延迟以及启用开关。停用的计划会在激活时从实时调度器移除。",
        )
        .widget(FieldWidget::ScheduleList),
        integer(
            "reports.max_top_n",
            "Maximum TopN",
            "最大 TopN",
            "Hard upper bound on TopN for every schedule and ad-hoc run. Each schedule's own TopN must be within 1..=this; must be > 0.",
            "所有计划与临时运行的 TopN 硬上限。每个计划自身的 TopN 必须在 1..=此值 内；必须大于 0。",
        ),
        secs(
            "reports.fallback_horizon_secs",
            "Fallback horizon",
            "回退预测周期",
            "Prediction horizon used only when the model provides no per-candidate suggested horizon (classical / non-ML runs). Not a flat TTL — per-recommendation validity is otherwise data-driven; must be > 0.",
            "仅当模型未提供逐候选建议周期（经典/非 ML 运行）时使用的预测周期。并非统一 TTL——逐建议有效期通常由数据驱动；必须大于 0。",
        ),
        boolean(
            "reports.publish_empty_reports",
            "Publish empty reports",
            "发布空报告",
            "When on, a run with no qualifying recommendations still publishes an (empty) report with a reason summary. Off suppresses empty reports (less noise, less audit trail).",
            "开启时，即使没有合格建议，运行也会发布一份带原因摘要的（空）报告。关闭则不发布空报告（噪声更少，但审计痕迹更少）。",
        ),
        ratio_half_open(
            "reports.entry_window_ratio",
            "Entry window ratio",
            "进场窗口比例",
            "Fraction of the effective horizon during which entry stays valid, in (0, 1]. 0.5 enters only while at least half the signal's edge remains; the time-stop / exit still uses the full horizon.",
            "入场保持有效的时长占有效周期的比例，(0,1]。0.5 表示仅在信号 edge 至少剩一半时入场；时间止损/退出仍使用完整周期。",
        ),
        boolean(
            "reports.ad_hoc_report_enabled",
            "Ad-hoc reports",
            "临时报告",
            "Whether operators may trigger an on-demand report outside the schedules. Off restricts report generation to scheduled runs only.",
            "是否允许操作员在计划之外手动触发按需报告。关闭则报告生成仅限计划运行。",
        ),
        enum_select(
            "reports.delivery_policy",
            "Delivery policy",
            "投递策略",
            "What happens after a report is built: 'Store & notify' persists and notifies operators; 'Store only' persists silently (no notification).",
            "报告构建后的处理：『存储并通知』持久化并通知操作员；『仅存储』静默持久化（不通知）。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Portfolio
// ---------------------------------------------------------------------------

fn portfolio_fields() -> Vec<FieldUiEntry> {
    let mut fields = portfolio_budget_fields();
    fields.extend(portfolio_constraint_fields());
    fields.extend(portfolio_correlation_fields());
    fields.extend(portfolio_sizing_fields());
    fields.extend(portfolio_kelly_safety_fields());
    fields.extend(portfolio_optimizer_fields());
    fields
}

fn portfolio_budget_fields() -> Vec<FieldUiEntry> {
    vec![
        usd(
            "portfolio.budget.total_budget_usd",
            "Total budget",
            "总预算",
            "Governance cap on deployable capital across all modes: equity = min(real net-liquidation value, this). It never stands in for real equity — only caps it. Raising it authorizes more capital at risk.",
            "所有模式下可部署资金的治理上限：权益 = min(真实净清算价值, 此值)。它绝不代替真实权益，只对其封顶。调高即授权更多在险资金。",
        )
        .critical(),
        usd(
            "portfolio.budget.min_recommendation_usd",
            "Minimum recommendation size",
            "最小建议金额",
            "Smallest useful per-recommendation allocation (USD). Candidates that would size below this are dropped to avoid dust positions.",
            "单条建议的最小有效配置金额（USD）。定量后低于此值的候选会被丢弃，避免尘埃仓位。",
        )
        .col_span(12),
        usd(
            "portfolio.budget.max_single_recommendation_usd",
            "Maximum recommendation size",
            "单建议最大金额",
            "Hard cap on capital allocated to any single recommendation (USD). Bounds single-name concentration; raising it permits larger concentrated bets.",
            "单条建议可分配资金的硬上限（USD）。限制单标的集中度；调高允许更大的集中押注。",
        )
        .critical()
        .col_span(12),
    ]
}

fn portfolio_constraint_fields() -> Vec<FieldUiEntry> {
    vec![
        usd(
            "portfolio.constraints.max_market_exposure_usd",
            "Max market exposure",
            "单市场最大敞口",
            "Maximum total USD exposure per market (existing position + new intent). Enforced hard by execution admission; 0 disables. Lower diversifies away from single-market risk.",
            "单个市场的最大 USD 敞口（现有仓位+新意图）。由执行准入硬性强制；填 0 关闭。调低可分散单市场风险。",
        )
        .critical(),
        usd(
            "portfolio.constraints.max_event_exposure_usd",
            "Max event exposure",
            "单事件最大敞口",
            "Maximum total USD exposure across all markets of one event. Enforced hard by admission; 0 disables. Guards against correlated same-event bets.",
            "单个事件下所有市场的最大 USD 总敞口。由准入硬性强制；填 0 关闭。防止同一事件的相关押注堆叠。",
        )
        .critical(),
        usd(
            "portfolio.constraints.max_category_exposure_usd",
            "Max category exposure",
            "单分类最大敞口",
            "Maximum total USD exposure per market category. Enforced hard by admission; 0 disables. Caps thematic concentration.",
            "单个市场分类的最大 USD 总敞口。由准入硬性强制；填 0 关闭。限制主题集中度。",
        )
        .critical(),
        usd(
            "portfolio.constraints.max_correlated_exposure_usd",
            "Max correlated exposure",
            "最大相关敞口",
            "Maximum total USD exposure across a correlated cluster. Only binds when the correlation cap below is enabled; 0 disables. Limits hidden co-movement risk.",
            "单个相关性簇内的最大 USD 总敞口。仅当下方相关性约束启用时才生效；填 0 关闭。限制隐含的联动风险。",
        )
        .critical(),
        ratio(
            "portfolio.constraints.liquidity_usage_cap_pct",
            "Liquidity usage cap",
            "流动性使用上限",
            "Maximum fraction of a market's visible liquidity a single allocation may consume ([0, 1]). Lower reduces market impact and improves fillability.",
            "单笔配置可占用某市场可见流动性的最大比例（[0,1]）。调低可降低市场冲击、提升可成交性。",
        ),
    ]
}

fn portfolio_correlation_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "portfolio.constraints.correlation.enabled",
            "Correlation cap enabled",
            "启用相关性约束",
            "Whether the correlated-exposure cap is actually enforced via clustering. Off makes the correlated cap non-binding (snapshot-only). On performs historical co-movement clustering (more compute).",
            "是否通过聚类真正强制相关敞口上限。关闭时相关敞口上限不生效（仅快照）。开启则进行历史联动聚类（计算更重）。",
        ),
        integer(
            "portfolio.constraints.correlation.lookback_days",
            "Correlation lookback",
            "相关性回看天数",
            "Historical mid-price lookback window (days) for co-movement estimation. Longer windows are more stable but slower to react to regime change.",
            "联动估计的历史中间价回看窗口（天）。窗口越长越稳定，但对市场状态切换的反应越慢。",
        )
        .suffix("d")
        .visible_when(enabled("portfolio.constraints.correlation.enabled")),
        integer(
            "portfolio.constraints.correlation.min_observations",
            "Correlation min observations",
            "相关性最小观测数",
            "Minimum paired observations before historical correlation is trusted; below it the estimator falls back to event / category proxy clusters.",
            "信任历史相关性所需的最小配对观测数；低于此值估计器回退到事件/分类代理簇。",
        )
        .visible_when(enabled("portfolio.constraints.correlation.enabled")),
        ratio(
            "portfolio.constraints.correlation.cluster_threshold",
            "Correlation cluster threshold",
            "相关性聚类阈值",
            "Absolute Pearson correlation at or above which two markets join the same cluster ([0, 1]). Lower thresholds create larger clusters (tighter correlated cap).",
            "两个市场被归入同一簇的绝对皮尔逊相关阈值（[0,1]）。阈值越低簇越大（相关敞口上限越紧）。",
        )
        .visible_when(enabled("portfolio.constraints.correlation.enabled")),
    ]
}

fn portfolio_sizing_fields() -> Vec<FieldUiEntry> {
    vec![
        ratio_half_open(
            "portfolio.sizing.kelly_fraction",
            "Kelly fraction",
            "Kelly 分数",
            "Fraction of full Kelly applied, in (0, 1] (half-Kelly ≈ 0.5). Lower is more conservative growth; full Kelly maximizes growth but is most sensitive to edge mis-estimation.",
            "应用的完整 Kelly 比例，(0,1]（半 Kelly≈0.5）。调低更保守；完整 Kelly 增长最快但对 edge 估计误差最敏感。",
        ),
        ratio_half_open(
            "portfolio.sizing.max_position_pct",
            "Max position pct",
            "单仓上限占比",
            "Maximum single-position size as a fraction of equity, in (0, 1]. A hard per-name sizing cap layered on top of Kelly.",
            "单仓规模占权益的最大比例，(0,1]。叠加在 Kelly 之上的单标的定量硬上限。",
        ),
        decimal(
            "portfolio.sizing.target_reward_multiple",
            "Target reward multiple",
            "目标盈亏倍数",
            "Target reward-to-risk multiple R (> 0): target gain = R × downside for the exit plan's take-profit price, and for the legacy TP/SL Kelly fallback a cold-start Heuristic candidate uses (a Calibrated candidate's Kelly fraction never reads this — it uses the calibrated win probability directly).",
            "目标盈亏比 R（>0）：用于止盈价 = R × 下行，以及冷启动 Heuristic 候选的遗留 TP/SL Kelly 兜底公式（Calibrated 候选的 Kelly 分数直接使用校准胜率，不读取该字段）。",
        ),
        enum_select(
            "portfolio.sizing.confidence_weighting",
            "Confidence weighting",
            "置信度收缩曲线",
            "How confidence shrinks the Kelly fraction (estimation-uncertainty mitigation): 'Linear' scales smoothly; 'Step' applies a discrete cut below a threshold.",
            "置信度如何收缩 Kelly 分数（缓解估计不确定性）：『线性』平滑缩放；『阶梯』在阈值以下离散削减。",
        ),
        enum_select(
            "portfolio.sizing.drawdown_scaling",
            "Drawdown scaling",
            "回撤缩放策略",
            "How sizing responds to drawdown: 'Fixed' keeps sizing flat; 'Conservative' de-risks as drawdown grows.",
            "定量对回撤的响应：『固定』保持不变；『保守』随回撤加深自动降险。",
        ),
    ]
}

fn portfolio_kelly_safety_fields() -> Vec<FieldUiEntry> {
    vec![
        decimal_bounded(
            "portfolio.kelly_safety.edge_uncertainty_k",
            "Edge uncertainty k",
            "Edge 不确定性系数",
            "Shrink coefficient k in shrink = clamp(1 − k·edge_std, floor, 1) from reliability-bin Wilson half-width. Must be within [0, 10] — beyond 10 every calibrated candidate collapses to the floor regardless of its actual edge uncertainty.",
            "由可靠性分箱 Wilson 半宽计算的收缩系数 k：shrink = clamp(1 − k·edge_std, floor, 1)。必须在 [0, 10] 之间——超过 10 后所有已校准候选都会被收缩到下限，与其真实 edge 不确定性无关。",
            0.0,
            10.0,
            0.1,
        ),
        // (0, 1] — a zero floor would let edge-uncertainty shrink zero out
        // Kelly sizing entirely instead of merely shrinking it.
        ratio_half_open(
            "portfolio.kelly_safety.edge_uncertainty_floor",
            "Edge uncertainty floor",
            "Edge 不确定性下限",
            "Minimum edge-uncertainty shrink multiplier (never sizes above this floor when uncertainty binds). Must be > 0.",
            "Edge 不确定性收缩乘数下限（不确定性绑定时不会高于此值）。必须大于 0。",
        ),
        // (0, 1] — a zero cap would allocate nothing at all.
        ratio_half_open(
            "portfolio.kelly_safety.max_aggregate_exposure_pct",
            "Max aggregate exposure",
            "总敞口上限",
            "Hard cap on total simultaneous portfolio exposure as a fraction of the governed capital base (LP bucket constraint). Must be > 0.",
            "同时组合总敞口占治理后资金基准（capital base）的硬上限（LP bucket 约束）。必须大于 0。",
        )
        .critical(),
        ratio_half_open(
            "portfolio.kelly_safety.binding_materiality_threshold",
            "Kelly binding materiality",
            "Kelly 绑定显著性阈值",
            "Emit ConfidenceCap / DrawdownCap / CorrelationCap when the dominant Kelly-stage shrink falls below this threshold. Must be > 0 (a zero threshold can never bind).",
            "当主导 Kelly 阶段收缩低于此阈值时发出 ConfidenceCap / DrawdownCap / CorrelationCap 绑定。必须大于 0（阈值为 0 永远不会触发绑定）。",
        ),
    ]
}

fn portfolio_optimizer_fields() -> Vec<FieldUiEntry> {
    vec![
        enum_select(
            "portfolio.optimizer.solver",
            "LP solver backend",
            "LP 求解器后端",
            "Allocation LP/MILP backend: 'microlp' (pure Rust, ships everywhere) or 'HiGHS' (native, requires the lp-solver-highs build; otherwise transparently downgrades to microlp).",
            "组合 LP/MILP 求解后端：microlp（纯 Rust，随处可用）或 HiGHS（原生，需 lp-solver-highs 构建；否则透明降级到 microlp）。",
        ),
        boolean(
            "portfolio.optimizer.integer_inclusion",
            "Exact MILP inclusion",
            "精确 MILP 选择",
            "On solves the exact binary-inclusion MILP (production primary). Off solves the continuous LP relaxation with deterministic integer recovery (cheaper, fully deterministic — also the fallback / backtest mode).",
            "开启求解精确的二元选择 MILP（生产主路径）。关闭求解连续 LP 松弛并做确定性整数恢复（更省、完全确定——也是回退/回测模式）。",
        ),
        decimal(
            "portfolio.optimizer.objective_return_weight",
            "Expected-return weight (λ)",
            "预期收益权重 (λ)",
            "λ ≥ 0 weights normalized expected return in the per-dollar objective wᵢ = scoreᵢ·(1 + λ·ret_normᵢ). 0 = pure conviction weighting; higher tilts toward higher expected-return names.",
            "λ≥0，为逐美元目标 wᵢ = scoreᵢ·(1 + λ·ret_normᵢ) 中的归一化预期收益加权。0=纯确定性加权；调高更偏向高预期收益标的。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Execution
// ---------------------------------------------------------------------------

fn execution_fields() -> Vec<FieldUiEntry> {
    let mut fields = execution_semi_auto_fields();
    fields.extend(execution_auto_fields());
    fields.extend(execution_entry_policy_fields());
    fields.extend(execution_kill_switch_fields());
    fields.extend(execution_capital_fields());
    fields.extend(execution_exit_monitor_fields());
    fields.extend(execution_reconciliation_fields());
    fields.extend(execution_settlement_redeem_fields());
    fields.extend(execution_attribution_fields());
    fields.extend(execution_breaker_fields());
    fields
}

fn execution_semi_auto_fields() -> Vec<FieldUiEntry> {
    vec![
        secs(
            "execution.semi_auto.approval_ttl_secs",
            "Approval TTL",
            "审批有效期",
            "How long a pending semi-auto approval stays actionable before it expires. Shorter windows reduce stale-approval risk but demand faster operator response.",
            "半自动待审批意图在过期前保持可操作的时长。窗口越短陈旧审批风险越小，但要求操作员更快响应。",
        ),
        boolean(
            "execution.semi_auto.allow_size_reduction",
            "Allow size reduction",
            "允许减少下单规模",
            "Whether an approver may reduce (never increase) the order size at approval time. On gives operators a manual risk-down lever.",
            "审批人是否可在审批时下调（绝不上调）下单规模。开启为操作员提供一个手动降险的杠杆。",
        ),
    ]
}

fn execution_auto_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.auto_execution.enabled",
            "Auto execution enabled",
            "启用自动执行",
            "Master switch for hands-off order submission from reports. Turning it on lets the system sign and submit orders without per-order approval — the highest-impact execution control.",
            "从报告自动提交订单的总开关。开启后系统可无需逐单审批即签名并提交订单——影响最大的执行控制项。",
        )
        .critical(),
        integer(
            "execution.auto_execution.max_orders_per_report",
            "Max orders per report",
            "单报告最大订单数",
            "Upper bound on the number of orders auto-created from a single report. Bounds blast radius per run.",
            "单份报告自动创建订单数量的上限。限制每次运行的影响范围。",
        )
        .visible_when(enabled("execution.auto_execution.enabled")),
        usd(
            "execution.auto_execution.max_total_usd_per_report",
            "Max auto USD per report",
            "单报告最大自动执行金额",
            "Hard cap on total USD auto-executed from one report. The primary money guardrail for hands-off execution.",
            "单份报告自动执行的总金额硬上限（USD）。无人值守执行的首要资金护栏。",
        )
        .critical()
        .visible_when(enabled("execution.auto_execution.enabled")),
        ratio(
            "execution.auto_execution.min_score",
            "Minimum auto score",
            "自动执行最低分",
            "Only recommendations scoring at least this ([0, 1]) are eligible for auto-execution. Higher restricts hands-off trading to the strongest signals.",
            "只有得分不低于此值（[0,1]）的建议才有资格自动执行。调高会将无人值守交易限制在最强信号上。",
        )
        .visible_when(enabled("execution.auto_execution.enabled")),
        ratio(
            "execution.auto_execution.min_confidence",
            "Minimum auto confidence",
            "自动执行最低置信度",
            "Only recommendations with model confidence at least this ([0, 1]) are eligible for auto-execution. A second gate independent of score.",
            "只有模型置信度不低于此值（[0,1]）的建议才有资格自动执行。独立于分数的第二道门。",
        )
        .visible_when(enabled("execution.auto_execution.enabled")),
    ]
}

fn execution_entry_policy_fields() -> Vec<FieldUiEntry> {
    vec![
        bps(
            "execution.entry_order_policy.max_slippage_bps",
            "Entry max slippage",
            "入场最大滑点",
            "Maximum tolerated entry-order slippage (basis points), frozen onto each recommendation's entry plan and enforced by admission. Tighter caps reduce cost but increase unfilled entries.",
            "入场订单可容忍的最大滑点（基点），冻结到每条建议的入场计划并由准入强制。收紧可降低成本但会增加未成交入场。",
        ),
        boolean(
            "execution.entry_order_policy.allow_market_orders",
            "Allow market orders",
            "允许市价单",
            "Whether entry may use marketable (immediate) order types. On fills faster at the risk of more slippage; off uses limit entries only.",
            "入场是否可使用可立即成交（市价）订单类型。开启成交更快但滑点风险更高；关闭仅用限价入场。",
        ),
        usd(
            "execution.entry_order_policy.min_entry_book_depth_usd",
            "Minimum entry book depth",
            "入场最低订单簿深度",
            "Minimum visible book depth (USD) required at entry, frozen onto each recommendation's entry plan and enforced by admission's liquidity-depth check (an intent is deferred when fillable ask notional up to the limit price is below this). 0 disables the floor.",
            "入场所需的最低可见订单簿深度（USD），冻结到每条建议的入场计划，并由准入的流动性深度检查强制（当到限价为止的可成交卖单名义额低于此值时，意图会被延后）。填 0 关闭下限。",
        ),
    ]
}

fn execution_kill_switch_fields() -> Vec<FieldUiEntry> {
    vec![
        enum_select(
            "execution.kill_switch.emergency_exit.kind",
            "Emergency exit action",
            "紧急退出策略",
            "What the kill-switch does on escalation to emergency: 'Liquidate all' submits reduce-only exits within the slippage cap; 'Manual only' routes exits to operators (no automated liquidation).",
            "熔断升级到紧急状态时的动作：『全部平仓』在滑点上限内提交只减仓退出；『仅人工处理』将退出交给操作员（不自动平仓）。",
        ),
        bps(
            "execution.kill_switch.emergency_exit.max_slippage_bps",
            "Emergency exit max slippage",
            "紧急退出最大滑点",
            "Slippage cap (basis points) for automated emergency liquidation. Applies only when the emergency action is 'Liquidate all'; must be > 0.",
            "自动紧急平仓的滑点上限（基点）。仅当紧急动作为『全部平仓』时生效；必须大于 0。",
        ),
    ]
}

fn execution_capital_fields() -> Vec<FieldUiEntry> {
    vec![
        usd(
            "execution.capital.max_reserved_usd",
            "Max reserved capital",
            "最大预留金额",
            "Maximum USD reserved across all open execution intents. Enforced hard by admission; 0 disables. Bounds total capital committed in-flight at once.",
            "所有打开的执行意图可预留的最大 USD。由准入硬性强制；填 0 关闭。限制同一时刻在途占用的总资金。",
        )
        .critical(),
        integer(
            "execution.capital.max_open_intents",
            "Max open intents",
            "最大打开意图数",
            "Maximum number of concurrently open execution intents. Enforced hard by admission; 0 disables. Bounds operational fan-out.",
            "并发打开的执行意图数量上限。由准入硬性强制；填 0 关闭。限制运营层的并发扩散。",
        ),
    ]
}

fn execution_exit_monitor_fields() -> Vec<FieldUiEntry> {
    let mut fields = vec![
        boolean(
            "execution.exit_monitor.enabled",
            "Exit monitor enabled",
            "启用退出监控",
            "Master switch for the exit-monitor worker that evaluates the exit ladder (TP / SL / trailing / time / signal / partial / emergency) on open lots. Off leaves open positions unmonitored for programmatic exit.",
            "退出监控 worker 的总开关，对持仓评估退出阶梯（止盈/止损/跟踪/时间/信号/分批/紧急）。关闭则持仓不再被程序化退出监控。",
        ),
        secs(
            "execution.exit_monitor.monitor_secs",
            "Exit monitor interval",
            "退出监控间隔",
            "How often (seconds) the exit monitor scans open lots for the price / time / trailing ladder. Read every tick, so changes take effect without restart. Lower reacts faster at higher load.",
            "退出监控扫描持仓以评估价格/时间/跟踪阶梯的间隔（秒）。每个 tick 重读，改动无需重启即生效。调低反应更快但负载更高。",
        )
        .visible_when(enabled("execution.exit_monitor.enabled")),
        secs(
            "execution.exit_monitor.signal_recheck_secs",
            "Signal re-check interval",
            "信号重算间隔",
            "Minimum seconds between heavier model re-inference checks for one lot. Larger values reduce inference cost but slow thesis-invalidation detection.",
            "对单个持仓两次较重的模型再推理检查之间的最小间隔（秒）。调大降低推理成本，但会减慢逻辑失效的发现。",
        )
        .visible_when(enabled("execution.exit_monitor.enabled")),
        ratio_half_open(
            "execution.exit_monitor.signal_invalidation_ratio",
            "Signal invalidation ratio",
            "信号失效比率",
            "Fresh composite score below entry_score × this ratio invalidates the thesis, in (0, 1]. Lower is more tolerant of drift; near 1 forces exits on tiny degradation.",
            "当最新综合分低于 入场分 × 该比率 时判定逻辑失效，(0,1]。调低对漂移更宽容；接近 1 会在轻微退化时就强制退出。",
        )
        .visible_when(enabled("execution.exit_monitor.enabled")),
    ];
    fields.extend(execution_reinference_fields());
    fields.extend(execution_opportunistic_sell_fields());
    fields
}

fn execution_reinference_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.exit_monitor.signal_reinference.enabled",
            "Signal re-inference enabled",
            "启用信号再推理",
            "Whether model-backed thesis-invalidation re-inference runs. On re-scores each lot's market via the intent-frozen model to detect signal decay; off relies on price/time/trailing exits only.",
            "是否运行由模型驱动的逻辑失效再推理。开启会用意图冻结的模型重新给持仓所在市场打分以发现信号衰减；关闭则仅依赖价格/时间/跟踪退出。",
        )
        .visible_when(enabled("execution.exit_monitor.enabled")),
        boolean(
            "execution.exit_monitor.signal_reinference.shadow_mode",
            "Signal re-inference shadow mode",
            "信号再推理影子模式",
            "When on, re-inference runs and is audited but thesis-invalidation exits are suppressed (fail-safe hold; SL / time / trailing still apply). Use to observe before letting it act.",
            "开启时再推理运行并审计，但抑制逻辑失效退出（安全持有；止损/时间/跟踪仍生效）。用于在放行前先观察。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.signal_reinference.enabled",
        )),
    ]
}

fn execution_opportunistic_sell_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.exit_monitor.opportunistic_sell.enabled",
            "Opportunistic sell enabled",
            "启用机会性卖出",
            "Whether the advisory model-driven scale-out runs (thesis still holds but the Sell scorer ranks exiting now above holding). Requires re-inference enabled; off disables opportunistic scale-outs.",
            "是否运行建议性的、由模型驱动的分批减仓（逻辑仍成立但卖出评分模型认为现在退出优于持有）。需先启用再推理；关闭则不做机会性减仓。",
        )
        .visible_when(enabled("execution.exit_monitor.enabled")),
        boolean(
            "execution.exit_monitor.opportunistic_sell.shadow_mode",
            "Opportunistic sell shadow mode",
            "机会性卖出影子模式",
            "When on, the Sell scorer runs and writes audit rows but never submits an opportunistic exit. Use to validate the scorer before it trades.",
            "开启时卖出评分模型运行并写审计行，但绝不提交机会性退出。用于在其真正交易前验证评分器。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.opportunistic_sell.enabled",
        )),
        ratio(
            "execution.exit_monitor.opportunistic_sell.min_confidence",
            "Opportunistic sell min confidence",
            "机会性卖出最低置信度",
            "Minimum Sell-scorer confidence to act ([0, 1]); below it the lot is held. Higher demands stronger conviction before scaling out.",
            "触发动作所需的卖出评分模型最低置信度（[0,1]）；低于则持有。调高要求更强确定性才减仓。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.opportunistic_sell.enabled",
        )),
        bps(
            "execution.exit_monitor.opportunistic_sell.min_expected_alpha_bps",
            "Opportunistic sell min expected alpha",
            "机会性卖出最低预期超额",
            "Minimum expected exit alpha (basis points over holding) to act; below it the lot is held. Sets the edge threshold for scaling out early.",
            "触发动作所需的最低预期退出超额收益（相对持有的基点）；低于则持有。设定提前减仓的 edge 门槛。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.opportunistic_sell.enabled",
        )),
        ratio(
            "execution.exit_monitor.opportunistic_sell.min_p_exit_better",
            "Opportunistic sell min P(exit better)",
            "机会性卖出最低退出更优概率",
            "Minimum probability that exiting now beats holding ([0, 1]) to act; below it the lot is held.",
            "触发动作所需的『现在退出优于持有』最低概率（[0,1]）；低于则持有。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.opportunistic_sell.enabled",
        )),
        ratio_half_open(
            "execution.exit_monitor.opportunistic_sell.max_sell_pct",
            "Opportunistic sell max cumulative fraction",
            "机会性卖出最大累计比例",
            "Upper bound on the target cumulative exit fraction the model may request, in (0, 1]. Caps how much of a lot opportunistic exits can drain.",
            "模型可请求的目标累计退出比例上限，(0,1]。限制机会性退出最多能减掉一个持仓的多少。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.opportunistic_sell.enabled",
        )),
        ratio_half_open(
            "execution.exit_monitor.opportunistic_sell.min_opportunistic_clip_pct",
            "Opportunistic sell min clip fraction",
            "机会性卖出最小增量比例",
            "Minimum incremental fraction (of entry-filled shares) worth submitting, in (0, 1]. Deltas below this are held to avoid dust exits and fee churn.",
            "值得提交的最小增量比例（占入场成交股数），(0,1]。低于此的增量会被持有，避免尘埃退出与手续费空耗。",
        )
        .visible_when(enabled2(
            "execution.exit_monitor.enabled",
            "execution.exit_monitor.opportunistic_sell.enabled",
        )),
    ]
}

fn execution_reconciliation_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.reconciliation.enabled",
            "Reconciliation enabled",
            "启用对账",
            "Whether the reconciliation worker resolves in-flight orders against venue truth. Runs in all modes when on — in-flight money must be reconciled regardless of mode.",
            "对账 worker 是否根据场馆真相解决在途订单。开启后在所有模式下运行——在途资金无论何种模式都必须对账。",
        ),
        secs(
            "execution.reconciliation.interval_secs",
            "Reconciliation interval",
            "对账间隔",
            "How often (seconds) the reconciliation sweep runs. Read every tick (no restart needed). Lower reconciles faster at higher venue-read load; must be > 0 when enabled.",
            "对账扫描运行的间隔（秒）。每个 tick 重读（无需重启）。调低对账更快但场馆读取负载更高；启用时必须大于 0。",
        )
        .visible_when(enabled("execution.reconciliation.enabled")),
        secs(
            "execution.reconciliation.stale_open_secs",
            "Reconciliation stale-open deadline",
            "对账强制终态期限",
            "How long an order may stay unreconciled before the worker forces a terminal resolution (cancel a stale resting order, escalate an unreadable one). Bounds how long capital stays in-flight.",
            "订单在被 worker 强制终态（撤销陈旧挂单、升级不可读订单）前可保持未对账的时长。限制资金在途的时长。",
        )
        .visible_when(enabled("execution.reconciliation.enabled")),
    ]
}

fn execution_settlement_redeem_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.settlement_redeem.enabled",
            "Settlement redeem enabled",
            "启用结算赎回",
            "Whether the worker may submit standard CTF redeem transactions for resolved lots. Off leaves settled payouts unredeemed until re-enabled.",
            "worker 是否可为已结算持仓提交标准 CTF 赎回交易。关闭则已结算收益在重新启用前不会被赎回。",
        ),
        secs(
            "execution.settlement_redeem.interval_secs",
            "Settlement redeem interval",
            "结算赎回间隔",
            "How often (seconds) the redeem sweep runs. Read every tick (no restart). Must be > 0 when enabled.",
            "赎回扫描运行的间隔（秒）。每个 tick 重读（无需重启）。启用时必须大于 0。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        integer(
            "execution.settlement_redeem.batch_size",
            "Settlement redeem batch size",
            "结算赎回批量大小",
            "Maximum condition-level redeem batches processed per sweep. Higher clears the backlog faster at more per-sweep on-chain work; must be > 0 when enabled.",
            "每次扫描处理的最大 condition 级赎回批数。调高清理积压更快但每次扫描的链上工作更多；启用时必须大于 0。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        integer(
            "execution.settlement_redeem.max_attempts",
            "Settlement redeem max attempts",
            "结算赎回最大尝试次数",
            "Maximum failed submit / confirm attempts before a redeem is escalated to manual handling. Must be > 0 when enabled.",
            "赎回被升级到人工处理前允许的最大失败（提交/确认）次数。启用时必须大于 0。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        secs(
            "execution.settlement_redeem.retry_backoff_secs",
            "Settlement redeem retry backoff",
            "结算赎回重试退避",
            "Base backoff (seconds) between redeem retries after a failure. Larger backoffs reduce chain pressure but slow recovery.",
            "失败后两次赎回重试之间的基础退避时长（秒）。退避越大链上压力越小，但恢复越慢。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        integer(
            "execution.settlement_redeem.confirmation_blocks",
            "Settlement redeem confirmations",
            "结算赎回确认块数",
            "Polygon block confirmations required before a redeem is treated as final and the lots closed. Higher is safer against reorgs but slower; must be ≥ 1 when enabled.",
            "赎回被视为最终并关闭持仓前所需的 Polygon 区块确认数。调高对重组更安全但更慢；启用时必须≥1。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        boolean(
            "execution.settlement_redeem.allow_during_emergency",
            "Allow redeem during emergency",
            "允许紧急状态赎回",
            "Whether automatic redeem may sign new transactions while the system is in emergency halt. Off is safest (no new signing under halt).",
            "系统处于紧急停机时，自动赎回是否可签名新交易。关闭最安全（停机下不签新交易）。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        boolean(
            "execution.settlement_redeem.hold_to_resolution_enabled",
            "Hold near-resolution lots",
            "临近结算持有到期",
            "Whether the composer may elect to hold near-resolution lots to settlement (redeeming the full payout) instead of forcing an on-book exit that pays spread / slippage moments before settlement.",
            "报告 composer 是否可选择将临近结算的持仓持有到结算（赎回全额收益），而非在结算前一刻强制盘口退出、白付价差/滑点。",
        )
        .visible_when(enabled("execution.settlement_redeem.enabled")),
        secs(
            "execution.settlement_redeem.hold_to_resolution_within_secs",
            "Hold-to-resolution window",
            "持有到期判定窗口",
            "A lot whose market resolves within this many seconds (from as_of) is held to resolution rather than exited on the book. Must be > 0 when hold-to-resolution is enabled.",
            "市场在此秒数内（从 as_of 起）结算的持仓将被持有到结算而非盘口退出。启用『持有到期』时必须大于 0。",
        )
        .visible_when(enabled2(
            "execution.settlement_redeem.enabled",
            "execution.settlement_redeem.hold_to_resolution_enabled",
        )),
    ]
}

fn execution_attribution_fields() -> Vec<FieldUiEntry> {
    vec![
        boolean(
            "execution.attribution.enabled",
            "Attribution worker enabled",
            "启用归因 worker",
            "Whether the final recommendation-attribution worker runs (matches terminal recommendations / intents to realized outcomes for performance accounting).",
            "最终建议归因 worker 是否运行（将终态建议/意图匹配到已实现结果，用于绩效核算）。",
        ),
        secs(
            "execution.attribution.sweep_secs",
            "Attribution sweep interval",
            "归因扫描间隔",
            "How often (seconds) the attribution sweep runs. Read every tick (no restart).",
            "归因扫描运行的间隔（秒）。每个 tick 重读（无需重启）。",
        )
        .visible_when(enabled("execution.attribution.enabled")),
        integer(
            "execution.attribution.batch_size",
            "Attribution batch size",
            "归因批量大小",
            "Maximum terminal recommendation / intent candidates processed per sweep. Higher clears backlog faster at more per-sweep DB work.",
            "每次扫描处理的最大终态建议/意图候选数。调高清理积压更快但每次扫描的数据库工作更多。",
        )
        .visible_when(enabled("execution.attribution.enabled")),
    ]
}

fn execution_breaker_fields() -> Vec<FieldUiEntry> {
    vec![
        integer(
            "execution.breaker.venue_consecutive_failures_to_degrade",
            "Breaker degrade threshold",
            "熔断器降级阈值",
            "Consecutive venue failures that move the breaker to Degraded (admission defers, retries). Lower trips protection sooner on venue trouble.",
            "使熔断器进入 Degraded（准入延后、重试）的连续场馆失败次数。调低会在场馆异常时更早触发保护。",
        ),
        integer(
            "execution.breaker.venue_consecutive_failures_to_halt",
            "Breaker halt threshold",
            "熔断器熔断阈值",
            "Consecutive venue failures that move the breaker to Halted and latch the kill-switch (operator ack required to clear). Should exceed the degrade threshold.",
            "使熔断器进入 Halted 并锁存熔断开关（需操作员确认才能解除）的连续场馆失败次数。应大于降级阈值。",
        ),
        bps(
            "execution.breaker.venue_error_rate_bps_to_halt",
            "Breaker halt error rate",
            "熔断器熔断错误率",
            "Rolling-window venue error rate (basis points) that trips Halted. Evaluated only once the minimum window samples are met.",
            "触发 Halted 的滚动窗口场馆错误率（基点）。仅在达到最小窗口样本数后才评估。",
        ),
        integer(
            "execution.breaker.venue_min_window_samples",
            "Breaker min window samples",
            "熔断器窗口最小样本数",
            "Minimum observations in the window before the error-rate gate is evaluated (avoids small-N false trips).",
            "评估错误率门之前窗口内所需的最小观测数（避免小样本误触发）。",
        ),
        secs(
            "execution.breaker.venue_window_secs",
            "Breaker window seconds",
            "熔断器滚动窗口",
            "Length (seconds) of the rolling observation window for venue error-rate evaluation.",
            "评估场馆错误率的滚动观测窗口长度（秒）。",
        ),
        secs(
            "execution.breaker.cooldown_secs",
            "Breaker cooldown seconds",
            "熔断器冷却时间",
            "Seconds of failure-free operation before a Degraded breaker self-recovers to Healthy.",
            "Degraded 熔断器自恢复到 Healthy 前所需的无失败运行时长（秒）。",
        ),
        usd(
            "execution.breaker.daily_realized_loss_cap_usd",
            "Breaker daily realized-loss cap",
            "熔断器日内已实现亏损上限",
            "Cumulative same-day (UTC) realized loss cap. At ≥ 80% of the cap venue health degrades (admission defers); at ≥ the cap the kill-switch latches (execution halted). 0 disables this dimension.",
            "当日（UTC）累计已实现亏损上限。达到上限的 ≥80% 时场馆健康度降级（准入延后）；达到 ≥ 上限时锁存熔断开关（执行停机）。填 0 关闭该维度。",
        )
        .critical(),
    ]
}

// ---------------------------------------------------------------------------
// Field dictionary — Notification
// ---------------------------------------------------------------------------

fn notification_fields() -> Vec<FieldUiEntry> {
    vec![
        f(
            "notification.telegram.bot_token",
            "Telegram bot token",
            "Telegram Bot Token",
            "Bot API token used to deliver operator alerts and report notifications via Telegram. Stored masked; leave blank on edit to keep the existing secret. Hot-swapped on activation.",
            "通过 Telegram 投递操作员告警与报告通知所用的 Bot API Token。以掩码存储；编辑时留空即保留现有密钥。激活时热替换。",
        )
        .credential(),
        plain(
            "notification.telegram.chat_id",
            "Telegram chat id",
            "Telegram Chat ID",
            "Destination chat / channel id that receives Telegram notifications. Required for Telegram delivery to work.",
            "接收 Telegram 通知的目标会话/频道 id。Telegram 投递生效所必需。",
        ),
        f(
            "notification.webhook.url",
            "Webhook URL",
            "Webhook URL",
            "HTTPS endpoint that receives alert / report webhooks. Stored masked; leave blank on edit to keep the existing value. Hot-swapped on activation.",
            "接收告警/报告 webhook 的 HTTPS 端点。以掩码存储；编辑时留空即保留现有值。激活时热替换。",
        )
        .credential(),
        boolean(
            "notification.policies.report_published",
            "Notify on report published",
            "报告发布通知",
            "Whether operators are notified when a recommendation report is published (subject to the report delivery policy).",
            "建议报告发布时是否通知操作员（受报告投递策略约束）。",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Layout tree
// ---------------------------------------------------------------------------

fn build_tree() -> Vec<SchemaNode> {
    vec![
        selection_section(),
        data_quality_section(),
        features_section(),
        domain_section(),
        factors_section(),
        model_section(),
        quality_gate_section(),
        research_section(),
        training_section(),
        reports_section(),
        portfolio_section(),
        execution_section(),
        notification_section(),
    ]
}

fn selection_section() -> SchemaNode {
    section(
        section_spec(
            "selection",
            10,
            "lucide:layers",
            ls("Selection", "市场池"),
            ls(
                "Which markets are eligible for quant reports.",
                "哪些市场有资格进入 quant 报告。",
            ),
        ),
        fields_in_order(&[
            "selection.enabled_categories",
            "selection.min_liquidity_usd",
            "selection.min_volume_24h_usd",
            "selection.max_spread_bps",
            "selection.allow_near_resolution",
            "selection.min_time_to_resolution_secs",
            "selection.max_time_to_resolution_secs",
            "selection.max_selection_size",
        ]),
    )
}

fn data_quality_section() -> SchemaNode {
    section(
        section_spec(
            "data_quality",
            20,
            "lucide:shield-check",
            ls("Data quality", "数据质量"),
            ls(
                "Freshness and structural-validity gates for the live data plane.",
                "实时数据平面的新鲜度与结构有效性门禁。",
            ),
        ),
        fields_in_order(&[
            "data_quality.max_book_age_ms",
            "data_quality.max_ingest_lag_ms",
            "data_quality.max_feature_bucket_age_secs",
            "data_quality.max_trade_tape_age_secs",
            "data_quality.max_domain_observation_age_secs",
            "data_quality.reject_crossed_books",
            "data_quality.reject_empty_books",
            "data_quality.feature_staleness_policy",
            "data_quality.max_stale_book_ratio_bps",
        ]),
    )
}

fn features_section() -> SchemaNode {
    section(
        section_spec(
            "features",
            30,
            "lucide:sparkles",
            ls("Features", "特征"),
            ls(
                "Feature families, windows, and schema identity.",
                "特征族、窗口与 schema 身份。",
            ),
        ),
        fields_in_order(&[
            "features.feature_schema_version",
            "features.enabled_feature_families",
            "features.required_features",
            "features.bar_windows_secs",
            "features.momentum.roc_windows_secs",
            "features.momentum.roc_lag_secs",
            "features.momentum.ema_fast_secs",
            "features.momentum.ema_slow_secs",
            "features.momentum.slope_windows_secs",
            "features.volatility_windows_secs",
            "features.depth_levels",
            "features.structural.shock_window_secs",
            "features.structural.book_churn_window_secs",
            "features.structural.trade_tape_window_secs",
            "features.structural.trade_tape_min_unique_participants",
            "features.structural.trade_tape_min_notional_usd",
            "features.structural.trade_tape_min_coverage_ratio",
            "features.max_concurrent_market_resolves",
        ]),
    )
}

fn domain_section() -> SchemaNode {
    section(
        section_spec(
            "domain",
            35,
            "lucide:globe",
            ls("Domain", "外部域"),
            ls(
                "External vertical data plane (crypto underlying prices today).",
                "外部垂直数据平面（当前为加密标的价）。",
            ),
        ),
        fields_in_order(&[
            "domain.enabled_by_family",
            "domain.crypto.source_delay_secs",
            "domain.crypto.backfill_days",
            "domain.crypto.momentum_window_secs",
            "domain.crypto.volatility_window_secs",
            "domain.crypto.cross_check.max_basis_bps",
            "domain.crypto.cross_check.alert_cooldown_secs",
            "domain.crypto.cross_check.max_oracle_staleness_secs",
        ]),
    )
}

fn factors_section() -> SchemaNode {
    section(
        section_spec(
            "factors",
            40,
            "lucide:line-chart",
            ls("Factors", "因子"),
            ls(
                "Factor families and weighted-scorer parameters.",
                "因子族与加权打分参数。",
            ),
        ),
        fields_in_order(&[
            "factors.enabled_factor_families",
            "factors.factor_weights",
            "factors.min_factor_confidence",
            "factors.missing_factor_policy",
            "factors.normalization.default_winsor_p",
            "factors.normalization.default_clamp_sigma",
            "factors.normalization.per_factor",
            "factors.cross_section.min_size",
            "factors.cross_section.small_cross_section_policy",
            "factors.cross_section.historical_lookback_secs",
            "factors.orthogonalize.max_correlation",
            "factors.orthogonalize.neutralize_by",
            "factors.structural.reversal_after_shock.shock_k",
            "factors.structural.reversal_after_shock.shock_cap",
            "factors.structural.negrisk.min_legs",
            "factors.structural.participant_concentration.gini_weight",
            "factors.structural.participant_concentration.cr1_share_weight",
            "factors.structural.participant_concentration.hhi_weight",
            "factors.structural.favorite_longshot.bias_table_ref",
            "factors.structural.favorite_longshot.bins",
            "factors.structural.favorite_longshot.ttr_bucket_bounds_secs",
            "factors.structural.favorite_longshot.min_bin_samples",
            "factors.structural.favorite_longshot.min_curve_samples",
            "factors.structural.favorite_longshot.ci_confidence",
            "factors.structural.favorite_longshot.ic_significance_min",
            "factors.structural.favorite_longshot.fit_sample_stride_secs",
            "factors.structural.per_category_ic_gate",
        ]),
    )
}

fn model_section() -> SchemaNode {
    let mut children = fields_in_order(&[
        "model.active_model_version_id",
        "model.shadow_model_version_id",
        "model.active_exit_model_version_id",
        "model.category_model_pointers.crypto",
        "model.category_model_pointers.sports",
        "model.category_model_pointers.politics",
        "model.category_model_pointers.finance",
        "model.category_model_pointers.tech",
        "model.category_model_pointers.culture",
        "model.category_model_pointers.weather",
        "model.category_model_pointers.economics",
        "model.category_model_pointers.geopolitics",
        "model.category_model_pointers.other",
        "model.min_model_confidence",
        "model.min_quality_gate_age_secs",
        "model.candidate_score_floor",
        "model.shadow_diff_threshold",
    ]);
    children.push(subsection(
        section_spec(
            "model.calibration",
            20,
            "lucide:chart-spline",
            ls("Calibration", "校准"),
            ls(
                "Model-score calibrator fit policy (Phase 11.3).",
                "模型分数校准器拟合策略（Phase 11.3）。",
            ),
        ),
        fields_in_order(&[
            "model.calibration.method",
            "model.calibration.min_samples_isotonic",
            "model.calibration.embargo_secs",
            "model.calibration.require_for_publish",
            "model.calibration.ci_confidence",
        ]),
    ));
    section_node(
        section_spec(
            "model",
            50,
            "lucide:brain",
            ls("Model", "模型"),
            ls(
                "Active / shadow / exit model pointers and online scoring gates.",
                "活动/影子/退出模型指针与在线打分门禁。",
            ),
        ),
        children,
    )
}

fn training_section() -> SchemaNode {
    section(
        section_spec(
            "training",
            55,
            "lucide:book-open",
            ls("Training", "训练"),
            ls(
                "Offline training-dataset build parameters.",
                "离线训练数据集构建参数。",
            ),
        ),
        fields_in_order(&[
            "training.max_book_staleness_ms",
            "training.min_exit_depth_usd",
            "training.min_selection_depth_usd",
        ]),
    )
}

fn research_section() -> SchemaNode {
    section_node(
        section_spec(
            "research",
            54,
            "lucide:flask-conical",
            ls("Research", "研究"),
            ls(
                "Governed research-plane training objective and validation policy.",
                "研究平面的受治理训练目标与验证策略。",
            ),
        ),
        vec![
            subsection(
                section_spec(
                    "research.training",
                    10,
                    "lucide:chart-no-axes-combined",
                    ls("Learning-to-rank objective", "Learning-to-rank 目标"),
                    ls(
                        "Cross-sectional LTR loss, lower-tail penalty, turnover penalty, and L2 regularization frozen into trained weighted-model artifacts.",
                        "冻结进加权模型训练产物的横截面 LTR 损失、尾部惩罚、换手惩罚与 L2 正则。",
                    ),
                ),
                fields_in_order(&[
                    "research.training.rank_loss",
                    "research.training.optimizer",
                    "research.training.lambda_tail",
                    "research.training.tail_fraction",
                    "research.training.lambda_turnover",
                    "research.training.lambda_l2",
                    "research.training.ndcg_k",
                    "research.training.pseudo_top_n",
                ]),
            ),
            subsection(
                section_spec(
                    "research.validation",
                    20,
                    "lucide:shield-check",
                    ls("Leakage-aware validation", "泄漏感知验证"),
                    ls(
                        "CPCV purge/embargo, trial grid, DSR/PBO overfitting control, and alpha-significance publish gates.",
                        "CPCV purge/embargo、trial 网格、DSR/PBO 过拟合控制与 alpha 显著性发布门禁。",
                    ),
                ),
                fields_in_order(&[
                    "research.validation.purge.embargo_pct",
                    "research.validation.cpcv.n_groups",
                    "research.validation.cpcv.k_test",
                    "research.validation.trials.lambda_multipliers",
                    "research.validation.trials.rank_loss_kinds",
                    "research.validation.trials.forest_n_trees_multipliers",
                    "research.validation.trials.linear_alpha_multipliers",
                    "research.validation.trials.max_trials",
                    "research.validation.pbo.block_count",
                    "research.validation.gates.rank_ic_min",
                    "research.validation.gates.dsr_significance",
                    "research.validation.gates.max_pbo",
                    "research.validation.gates.max_turnover",
                    "research.validation.gates.min_tail_loss_bps",
                ]),
            ),
        ],
    )
}

fn reports_section() -> SchemaNode {
    section(
        section_spec(
            "reports",
            60,
            "lucide:file-text",
            ls("Reports", "报告"),
            ls(
                "Report schedules and publication policy.",
                "报告计划与发布策略。",
            ),
        ),
        fields_in_order(&[
            "reports.schedules",
            "reports.max_top_n",
            "reports.fallback_horizon_secs",
            "reports.publish_empty_reports",
            "reports.entry_window_ratio",
            "reports.ad_hoc_report_enabled",
            "reports.delivery_policy",
        ]),
    )
}

fn notification_section() -> SchemaNode {
    section(
        section_spec(
            "notification",
            90,
            "lucide:bell",
            ls("Notification", "通知"),
            ls(
                "Operator notification channels and policies.",
                "操作员通知渠道与策略。",
            ),
        ),
        fields_in_order(&[
            "notification.telegram.bot_token",
            "notification.telegram.chat_id",
            "notification.webhook.url",
            "notification.policies.report_published",
        ]),
    )
}

fn quality_gate_section() -> SchemaNode {
    let mut children = fields_in_order(&[
        "quality_gate.min_sample_count",
        "quality_gate.min_label_coverage",
        "quality_gate.min_critical_feature_coverage",
        "quality_gate.max_drawdown",
        "quality_gate.min_liquidity_exit_feasibility",
        "quality_gate.min_shadow_overlap_stability",
        "quality_gate.max_category_concentration",
        "quality_gate.required_shadow_window_secs",
    ]);
    children.push(subsection(
        section_spec(
            "quality_gate.sell",
            100,
            "lucide:trending-down",
            ls("Sell-side gate", "卖出侧门禁"),
            ls(
                "Hold-vs-exit Sell scorer publish thresholds.",
                "持有 vs 退出 卖出评分模型的发布阈值。",
            ),
        ),
        fields_in_order(&[
            "quality_gate.sell.min_sample_count",
            "quality_gate.sell.min_label_coverage",
            "quality_gate.sell.min_exit_alpha_rank_ic",
            "quality_gate.sell.min_l2_book_fidelity_ratio",
            "quality_gate.sell.max_fallback_ratio",
        ]),
    ));
    section_node(
        section_spec(
            "quality_gate",
            52,
            "lucide:badge-check",
            ls("Quality gate", "质量门"),
            ls(
                "Model publish / dataset promotion gates.",
                "模型发布 / 数据集晋升门禁。",
            ),
        ),
        children,
    )
}

fn portfolio_constraints_subsection() -> SchemaNode {
    let mut constraints = fields_in_order(&[
        "portfolio.constraints.max_market_exposure_usd",
        "portfolio.constraints.max_event_exposure_usd",
        "portfolio.constraints.max_category_exposure_usd",
        "portfolio.constraints.max_correlated_exposure_usd",
        "portfolio.constraints.liquidity_usage_cap_pct",
    ]);
    constraints.push(subsection(
        section_spec(
            "portfolio.constraints.correlation",
            100,
            "lucide:share-2",
            ls("Correlation cap", "相关性约束"),
            ls(
                "Correlation-cluster estimation gating the correlated-exposure cap.",
                "相关性聚类估计，用于约束相关敞口上限。",
            ),
        ),
        fields_in_order(&[
            "portfolio.constraints.correlation.enabled",
            "portfolio.constraints.correlation.lookback_days",
            "portfolio.constraints.correlation.min_observations",
            "portfolio.constraints.correlation.cluster_threshold",
        ]),
    ));
    subsection(
        section_spec(
            "portfolio.constraints",
            20,
            "lucide:scale",
            ls("Constraints", "约束"),
            ls("Exposure and liquidity constraints.", "敞口与流动性约束。"),
        ),
        constraints,
    )
}

fn portfolio_section_children() -> Vec<SchemaNode> {
    vec![
        subsection(
            section_spec(
                "portfolio.budget",
                10,
                "lucide:banknote",
                ls("Budget", "预算"),
                ls("Capital budget governance caps.", "资金预算治理上限。"),
            ),
            fields_in_order(&[
                "portfolio.budget.total_budget_usd",
                "portfolio.budget.min_recommendation_usd",
                "portfolio.budget.max_single_recommendation_usd",
            ]),
        ),
        portfolio_constraints_subsection(),
        subsection(
            section_spec(
                "portfolio.sizing",
                30,
                "lucide:percent",
                ls("Sizing", "定量"),
                ls("Fractional-Kelly position sizing.", "分数 Kelly 头寸定量。"),
            ),
            fields_in_order(&[
                "portfolio.sizing.kelly_fraction",
                "portfolio.sizing.max_position_pct",
                "portfolio.sizing.target_reward_multiple",
                "portfolio.sizing.confidence_weighting",
                "portfolio.sizing.drawdown_scaling",
            ]),
        ),
        subsection(
            section_spec(
                "portfolio.kelly_safety",
                35,
                "lucide:shield",
                ls("Kelly safety", "Kelly 安全层"),
                ls(
                    "Edge-uncertainty shrink, correlation shrink, and aggregate exposure cap (Phase 11.3).",
                    "Edge 不确定性收缩、相关性收缩与总敞口硬上限（Phase 11.3）。",
                ),
            ),
            fields_in_order(&[
                "portfolio.kelly_safety.edge_uncertainty_k",
                "portfolio.kelly_safety.edge_uncertainty_floor",
                "portfolio.kelly_safety.max_aggregate_exposure_pct",
                "portfolio.kelly_safety.binding_materiality_threshold",
            ]),
        ),
        subsection(
            section_spec(
                "portfolio.optimizer",
                40,
                "lucide:sliders-horizontal",
                ls("Optimizer", "优化器"),
                ls(
                    "LP / MILP allocation optimizer policy.",
                    "LP / MILP 配置优化器策略。",
                ),
            ),
            fields_in_order(&[
                "portfolio.optimizer.solver",
                "portfolio.optimizer.integer_inclusion",
                "portfolio.optimizer.objective_return_weight",
            ]),
        ),
    ]
}

fn portfolio_section() -> SchemaNode {
    section_node(
        section_spec(
            "portfolio",
            70,
            "lucide:briefcase",
            ls("Portfolio", "组合"),
            ls(
                "Budget, exposure constraints, sizing, and allocation optimizer.",
                "预算、敞口约束、定量与配置优化器。",
            ),
        ),
        portfolio_section_children(),
    )
}

fn execution_children_head() -> Vec<SchemaNode> {
    vec![
        subsection(
            section_spec(
                "execution.semi_auto",
                10,
                "lucide:user-check",
                ls("Semi-auto approval", "半自动审批"),
                ls(
                    "Operator approval policy for semi-auto execution.",
                    "半自动执行的操作员审批策略。",
                ),
            ),
            fields_in_order(&[
                "execution.semi_auto.approval_ttl_secs",
                "execution.semi_auto.allow_size_reduction",
            ]),
        ),
        subsection(
            section_spec(
                "execution.auto_execution",
                20,
                "lucide:bot",
                ls("Auto execution", "自动执行"),
                ls(
                    "Hands-off order submission policy and money guardrails.",
                    "无人值守下单策略与资金护栏。",
                ),
            ),
            fields_in_order(&[
                "execution.auto_execution.enabled",
                "execution.auto_execution.max_orders_per_report",
                "execution.auto_execution.max_total_usd_per_report",
                "execution.auto_execution.min_score",
                "execution.auto_execution.min_confidence",
            ]),
        ),
        subsection(
            section_spec(
                "execution.entry_order_policy",
                30,
                "lucide:door-open",
                ls("Entry order policy", "入场订单策略"),
                ls(
                    "Entry order type, slippage cap, and depth floor.",
                    "入场订单类型、滑点上限与深度下限。",
                ),
            ),
            fields_in_order(&[
                "execution.entry_order_policy.max_slippage_bps",
                "execution.entry_order_policy.allow_market_orders",
                "execution.entry_order_policy.min_entry_book_depth_usd",
            ]),
        ),
        kill_switch_subsection(),
        subsection(
            section_spec(
                "execution.capital",
                50,
                "lucide:landmark",
                ls("Capital limits", "资金限制"),
                ls(
                    "Reserved-capital and open-intent admission caps.",
                    "预留资金与打开意图的准入上限。",
                ),
            ),
            fields_in_order(&[
                "execution.capital.max_reserved_usd",
                "execution.capital.max_open_intents",
            ]),
        ),
    ]
}

fn execution_children_tail() -> Vec<SchemaNode> {
    vec![
        exit_monitor_subsection(),
        subsection(
            section_spec(
                "execution.reconciliation",
                70,
                "lucide:git-compare-arrows",
                ls("Reconciliation", "对账"),
                ls(
                    "In-flight order reconciliation against venue truth.",
                    "根据场馆真相对在途订单进行对账。",
                ),
            ),
            fields_in_order(&[
                "execution.reconciliation.enabled",
                "execution.reconciliation.interval_secs",
                "execution.reconciliation.stale_open_secs",
            ]),
        ),
        subsection(
            section_spec(
                "execution.settlement_redeem",
                80,
                "lucide:coins",
                ls("Settlement redeem", "结算赎回"),
                ls(
                    "On-chain CTF redemption worker policy.",
                    "链上 CTF 赎回 worker 策略。",
                ),
            ),
            fields_in_order(&[
                "execution.settlement_redeem.enabled",
                "execution.settlement_redeem.interval_secs",
                "execution.settlement_redeem.batch_size",
                "execution.settlement_redeem.max_attempts",
                "execution.settlement_redeem.retry_backoff_secs",
                "execution.settlement_redeem.confirmation_blocks",
                "execution.settlement_redeem.allow_during_emergency",
                "execution.settlement_redeem.hold_to_resolution_enabled",
                "execution.settlement_redeem.hold_to_resolution_within_secs",
            ]),
        ),
        subsection(
            section_spec(
                "execution.attribution",
                90,
                "lucide:link-2",
                ls("Attribution", "归因"),
                ls(
                    "Recommendation-attribution worker policy.",
                    "建议归因 worker 策略。",
                ),
            ),
            fields_in_order(&[
                "execution.attribution.enabled",
                "execution.attribution.sweep_secs",
                "execution.attribution.batch_size",
            ]),
        ),
        subsection(
            section_spec(
                "execution.breaker",
                100,
                "lucide:unplug",
                ls("Execution breaker", "执行熔断器"),
                ls(
                    "Venue-health and daily-loss circuit breaker thresholds.",
                    "场馆健康度与日内亏损的熔断阈值。",
                ),
            ),
            fields_in_order(&[
                "execution.breaker.venue_consecutive_failures_to_degrade",
                "execution.breaker.venue_consecutive_failures_to_halt",
                "execution.breaker.venue_error_rate_bps_to_halt",
                "execution.breaker.venue_min_window_samples",
                "execution.breaker.venue_window_secs",
                "execution.breaker.cooldown_secs",
                "execution.breaker.daily_realized_loss_cap_usd",
            ]),
        ),
    ]
}

fn execution_section() -> SchemaNode {
    let mut children = execution_children_head();
    children.extend(execution_children_tail());
    section_node(
        section_spec(
            "execution",
            80,
            "lucide:zap",
            ls("Execution", "执行"),
            ls("Order-intent and execution policy.", "订单意图与执行策略。"),
        ),
        children,
    )
}

/// Emergency-exit modeled as a real discriminated union: the slippage cap only
/// applies when the action is `liquidate_all`.
fn kill_switch_subsection() -> SchemaNode {
    let union = SchemaNode::Union(SchemaUnion {
        order: 20,
        discriminator: "execution.kill_switch.emergency_exit.kind".to_owned(),
        label: Some(UiText::localized("Liquidation parameters", "平仓参数")),
        cases: vec![
            SchemaUnionCase {
                case_value: Value::String("liquidate_all".to_owned()),
                children: fields_in_order(&[
                    "execution.kill_switch.emergency_exit.max_slippage_bps",
                ]),
            },
            SchemaUnionCase {
                case_value: Value::String("manual_only".to_owned()),
                children: Vec::new(),
            },
        ],
    });
    let children = vec![
        field_node("execution.kill_switch.emergency_exit.kind", 10),
        union,
    ];
    section_node(
        section_spec(
            "execution.kill_switch",
            40,
            "lucide:octagon-alert",
            ls("Kill-switch emergency exit", "熔断紧急退出"),
            ls(
                "Emergency-exit action applied when the kill-switch escalates.",
                "熔断升级时应用的紧急退出动作。",
            ),
        ),
        children,
    )
}

fn exit_monitor_subsection() -> SchemaNode {
    let mut children = fields_in_order(&[
        "execution.exit_monitor.enabled",
        "execution.exit_monitor.monitor_secs",
        "execution.exit_monitor.signal_recheck_secs",
        "execution.exit_monitor.signal_invalidation_ratio",
    ]);
    children.push(subsection(
        section_spec(
            "execution.exit_monitor.signal_reinference",
            100,
            "lucide:refresh-cw",
            ls("Signal re-inference", "信号再推理"),
            ls(
                "Model-backed thesis-invalidation re-inference.",
                "由模型驱动的逻辑失效再推理。",
            ),
        ),
        fields_in_order(&[
            "execution.exit_monitor.signal_reinference.enabled",
            "execution.exit_monitor.signal_reinference.shadow_mode",
        ]),
    ));
    children.push(subsection(
        section_spec(
            "execution.exit_monitor.opportunistic_sell",
            110,
            "lucide:chart-candlestick",
            ls("Opportunistic sell", "机会性卖出"),
            ls(
                "Advisory model-driven scale-out policy.",
                "建议性、由模型驱动的分批减仓策略。",
            ),
        ),
        fields_in_order(&[
            "execution.exit_monitor.opportunistic_sell.enabled",
            "execution.exit_monitor.opportunistic_sell.shadow_mode",
            "execution.exit_monitor.opportunistic_sell.min_confidence",
            "execution.exit_monitor.opportunistic_sell.min_expected_alpha_bps",
            "execution.exit_monitor.opportunistic_sell.min_p_exit_better",
            "execution.exit_monitor.opportunistic_sell.max_sell_pct",
            "execution.exit_monitor.opportunistic_sell.min_opportunistic_clip_pct",
        ]),
    ));
    section_node(
        section_spec(
            "execution.exit_monitor",
            60,
            "lucide:radar",
            ls("Exit monitor", "退出监控"),
            ls(
                "Exit ladder cadence and signal-degradation policy.",
                "退出阶梯节奏与信号退化策略。",
            ),
        ),
        children,
    )
}

// ---------------------------------------------------------------------------
// Tree helpers
// ---------------------------------------------------------------------------

/// Compile-time EN/ZH copy pair for section labels and descriptions.
struct LocalizedStr {
    en: &'static str,
    zh: &'static str,
}

const fn ls(en: &'static str, zh: &'static str) -> LocalizedStr {
    LocalizedStr { en, zh }
}

/// Metadata shared by top-level sections and nested subsections.
struct SectionSpec {
    id: &'static str,
    order: u16,
    icon: &'static str,
    label: LocalizedStr,
    description: LocalizedStr,
}

const fn section_spec(
    id: &'static str,
    order: u16,
    icon: &'static str,
    label: LocalizedStr,
    description: LocalizedStr,
) -> SectionSpec {
    SectionSpec {
        id,
        order,
        icon,
        label,
        description,
    }
}

fn fields_in_order(paths: &[&'static str]) -> Vec<SchemaNode> {
    paths
        .iter()
        .enumerate()
        .map(|(index, path)| field_node(path, u16::try_from((index + 1) * 10).unwrap_or(u16::MAX)))
        .collect()
}

fn field_node(path: &str, order: u16) -> SchemaNode {
    SchemaNode::Field(SchemaFieldRef {
        path: path.to_owned(),
        order,
    })
}

fn section_node(spec: SectionSpec, children: Vec<SchemaNode>) -> SchemaNode {
    let SectionSpec {
        id,
        order,
        icon,
        label,
        description,
    } = spec;
    SchemaNode::Section(SchemaSection {
        id: id.to_owned(),
        label: UiText::localized(label.en, label.zh),
        description: Some(UiText::localized(description.en, description.zh)),
        icon: Some(icon.to_owned()),
        collapsible: true,
        order,
        children,
    })
}

fn section(spec: SectionSpec, children: Vec<SchemaNode>) -> SchemaNode {
    section_node(spec, children)
}

fn subsection(spec: SectionSpec, children: Vec<SchemaNode>) -> SchemaNode {
    section_node(spec, children)
}
