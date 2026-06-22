//! Field UI catalog for preferences rendering.
//! Hand-maintained: add or update `FieldUiEntry` rows here when runtime-config fields change.

use crate::ui_text;

use super::ui_registry::{FieldUiEntry, field_catalog_lock};
use super::ui_widget::{FieldSemantics, FieldWidget};

/// Lazily-built field UI catalog for preferences rendering.
#[must_use]
pub fn fields() -> &'static [FieldUiEntry] {
    field_catalog_lock().get_or_init(build_fields)
}

fn build_fields() -> Vec<FieldUiEntry> {
    let mut out = Vec::with_capacity(114);
    out.extend(build_fields_market_data());
    out.extend(build_fields_detection());
    out.extend(build_fields_execution());
    out.extend(build_fields_risk());
    out.extend(build_fields_settlement());
    out.extend(build_fields_notification());
    out
}

/// `market_data` preferences section.
fn build_fields_market_data() -> Vec<FieldUiEntry> {
    let mut out = Vec::new();
    out.extend(build_fields_market_data_enabled());
    out.extend(build_fields_market_data_staleness());
    out
}

/// `detection` preferences section.
fn build_fields_detection() -> Vec<FieldUiEntry> {
    let mut out = Vec::new();
    out.extend(build_fields_detection_calibration_from_bootstrap_alpha());
    out.extend(build_fields_detection_calibration_from_min_sample_size());
    out.extend(build_fields_detection_endgame_convergence_tracker());
    out.extend(build_fields_detection_endgame_emission_cooldown());
    out.extend(build_fields_detection_endgame_fill_probability());
    out.extend(build_fields_detection_endgame());
    out.extend(build_fields_detection_endgame_scorer());
    out.extend(build_fields_detection_min());
    out
}

/// `execution` preferences section.
fn build_fields_execution() -> Vec<FieldUiEntry> {
    let mut out = Vec::new();
    out.extend(build_fields_execution_coalescer());
    out.extend(build_fields_execution_endgame_latency());
    out.extend(build_fields_execution_funnel());
    out.extend(build_fields_execution_reconciliation());
    out.extend(build_fields_execution_timeout());
    out
}

/// `risk` preferences section.
fn build_fields_risk() -> Vec<FieldUiEntry> {
    let mut out = Vec::new();
    out.extend(build_fields_risk_api());
    out.extend(build_fields_risk_bankroll());
    out.extend(build_fields_risk_base());
    out.extend(build_fields_risk_circuit_breaker_from_half_open_probes());
    out.extend(build_fields_risk_circuit_breaker_from_max_cooldown_secs());
    out.extend(build_fields_risk_cooldown());
    out.extend(build_fields_risk_daily());
    out.extend(build_fields_risk_drawdown());
    out.extend(build_fields_risk_heartbeat());
    out.extend(build_fields_risk_kelly_from_max_kelly());
    out.extend(build_fields_risk_kelly_from_kelly_fraction());
    out.extend(build_fields_risk_market());
    out.extend(build_fields_risk_max_concurrent());
    out.extend(build_fields_risk_max_consecutive());
    out.extend(build_fields_risk_max_cooldown());
    out.extend(build_fields_risk_max_daily());
    out.extend(build_fields_risk_max_depth());
    out.extend(build_fields_risk_max_hourly());
    out.extend(build_fields_risk_max_metrics());
    out.extend(build_fields_risk_max_open());
    out.extend(build_fields_risk_max_single());
    out.extend(build_fields_risk_max_total());
    out.extend(build_fields_risk_max_weekly());
    out.extend(build_fields_risk_metrics());
    out.extend(build_fields_risk_min());
    out.extend(build_fields_risk_permanent());
    out.extend(build_fields_risk_potential());
    out.extend(build_fields_risk_reconciliation());
    out.extend(build_fields_risk_reservation());
    out.extend(build_fields_risk_reserve());
    out.extend(build_fields_risk_ws());
    out
}

/// `settlement` preferences section.
fn build_fields_settlement() -> Vec<FieldUiEntry> {
    let mut out = Vec::new();
    out.extend(build_fields_settlement_lifecycle());
    out.extend(build_fields_settlement_oracle_from_all_sources_down_strategy());
    out.extend(build_fields_settlement_oracle_from_voting_quorum());
    out.extend(build_fields_settlement_redeem());
    out.extend(build_fields_settlement_redeem_neg_risk());
    out.extend(build_fields_settlement_redeem_standard());
    out
}

/// `notification` preferences section.
fn build_fields_notification() -> Vec<FieldUiEntry> {
    let mut out = Vec::new();
    out.extend(build_fields_notification_alert());
    out.extend(build_fields_notification_telegram());
    out.extend(build_fields_notification_webhook());
    out
}

/// Fields: `detection.calibration.bootstrap_alpha … detection.calibration.fusion_prior_strength`.
fn build_fields_detection_calibration_from_bootstrap_alpha() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.calibration.bootstrap_alpha",
            label: ui_text!(en = "Bootstrap alpha prior", zh = "Bootstrap Alpha 先验"),
            help: ui_text!(
                en =
                    "Bootstrap alpha prior (before `MoM` estimation is available). Default: `2.0`.",
                zh = "Bootstrap Alpha 先验。默认：2.0。"
            ),
            order: 10,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.calibration.bootstrap_beta",
            label: ui_text!(en = "Bootstrap beta prior", zh = "Bootstrap Beta 先验"),
            help: ui_text!(
                en = "Bootstrap beta prior. Default: `0.2`.",
                zh = "Bootstrap Beta 先验。默认：0.2。"
            ),
            order: 20,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.calibration.fused_p_ceiling",
            label: ui_text!(
                en = "Ceiling for the fused probability output",
                zh = "融合概率上限"
            ),
            help: ui_text!(
                en = "Ceiling for the fused probability output. Default: `0.995`.",
                zh = "融合概率上限。默认：0.995。"
            ),
            order: 30,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.calibration.fused_p_floor",
            label: ui_text!(
                en = "Floor for the fused probability output",
                zh = "融合概率下限"
            ),
            help: ui_text!(
                en = "Floor for the fused probability output. Default: `0.80`.",
                zh = "融合概率下限。默认：0.80。"
            ),
            order: 40,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.calibration.fusion_prior_strength",
            label: ui_text!(
                en = "Prior strength n₀ for the dynamic fusion weight w",
                zh = "融合先验强度 n₀"
            ),
            help: ui_text!(
                en = "Prior strength `n₀` for the dynamic fusion weight `w(n) = n / (n + n₀)`. Higher values give more weight to the calibrator (slower adaptation to real-time signals). Default: `20`.",
                zh = "融合先验强度 n₀。默认：20。"
            ),
            order: 50,
            widget: None,
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.calibration.min_sample_size … detection.calibration.refresh_interval_secs`.
fn build_fields_detection_calibration_from_min_sample_size() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.calibration.min_sample_size",
            label: ui_text!(
                en = "Minimum sample size before a bucket's resolution rate is trusted",
                zh = "桶最小样本量"
            ),
            help: ui_text!(
                en = "Minimum sample size before a bucket's resolution rate is trusted. Below this threshold the fallback chain is activated. Default: `30`.",
                zh = "桶最小样本量。默认：30。"
            ),
            order: 60,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.calibration.refresh_interval_secs",
            label: ui_text!(en = "How often", zh = "校准刷新间隔 (秒)"),
            help: ui_text!(
                en = "How often (seconds) the background updater reconciles calibration data from the DB and oracles. Default: `3600`.",
                zh = "校准刷新间隔 (秒)。默认：3600。"
            ),
            order: 70,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.endgame.convergence_tracker.max_capacity … detection.endgame.convergence_tracker.max_idle_secs`.
fn build_fields_detection_endgame_convergence_tracker() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.endgame.convergence_tracker.max_capacity",
            label: ui_text!(
                en = "Maximum number of tracked markets",
                zh = "收敛跟踪最大容量"
            ),
            help: ui_text!(
                en = "Maximum number of tracked markets. Caution: capacity changes only apply to detectors constructed after activation (the live tracker keeps its capacity to preserve accumulated convergence durations). Default: `10000`.",
                zh = "收敛跟踪最大容量。默认：10000。注意：运行中修改可能重建内部状态或清空在途缓存，请在低流量窗口操作。"
            ),
            order: 80,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.convergence_tracker.max_idle_secs",
            label: ui_text!(
                en = "Max idle time before a market's convergence state is evicted",
                zh = "收敛空闲淘汰 (秒)"
            ),
            help: ui_text!(
                en = "Max idle time before a market's convergence state is evicted (seconds). Default: `7200`.",
                zh = "收敛空闲淘汰 (秒)。默认：7200。"
            ),
            order: 90,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.endgame.emission_cooldown.base_cooldown_secs … detection.endgame.emission_cooldown.max_multiplier`.
fn build_fields_detection_endgame_emission_cooldown() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.endgame.emission_cooldown.base_cooldown_secs",
            label: ui_text!(
                en = "Base cooldown duration in seconds",
                zh = "发射基础冷却 (秒)"
            ),
            help: ui_text!(
                en = "Base cooldown duration in seconds. Default: `30`.",
                zh = "发射基础冷却 (秒)。默认：30。"
            ),
            order: 100,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.emission_cooldown.max_capacity",
            label: ui_text!(en = "Maximum cache capacity", zh = "发射冷却缓存容量"),
            help: ui_text!(
                en = "Maximum cache capacity (number of tracked markets). Caution: changing this at runtime rebuilds the cache, clearing all in-flight cooldown state. Default: `4096`.",
                zh = "发射冷却缓存容量。默认：4096。注意：运行中修改可能重建内部状态或清空在途缓存，请在低流量窗口操作。"
            ),
            order: 110,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.emission_cooldown.max_multiplier",
            label: ui_text!(
                en = "Maximum exponential backoff multiplier for consecutive emissions",
                zh = "发射指数退避上限"
            ),
            help: ui_text!(
                en = "Maximum exponential backoff multiplier for consecutive emissions. Default: `16.0`.",
                zh = "发射指数退避上限。默认：16.0。"
            ),
            order: 120,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.endgame.fill_probability.base_fill_prob … detection.endgame.fill_probability.staleness_penalty_per_level`.
fn build_fields_detection_endgame_fill_probability() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.endgame.fill_probability.base_fill_prob",
            label: ui_text!(
                en = "Base fill probability for a single FOK order with fresh data",
                zh = "FOK 基础成交概率"
            ),
            help: ui_text!(
                en = "Base fill probability for a single FOK order with fresh data. Default: `0.90`.",
                zh = "FOK 基础成交概率。默认：0.90。"
            ),
            order: 130,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.fill_probability.depth_penalty_per_pct",
            label: ui_text!(
                en = "Per-percentage-point penalty above the threshold",
                zh = "深度惩罚/百分点"
            ),
            help: ui_text!(
                en = "Per-percentage-point penalty above the threshold. Default: `0.02`.",
                zh = "深度惩罚/百分点。默认：0.02。"
            ),
            order: 140,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.fill_probability.depth_penalty_threshold_pct",
            label: ui_text!(en = "Depth usage", zh = "深度惩罚阈值 (%)"),
            help: ui_text!(
                en = "Depth usage (%) above which fill probability drops. Default: `20`.",
                zh = "深度惩罚阈值 (%)。默认：20。"
            ),
            order: 150,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.fill_probability.resolution_proximity_bonus",
            label: ui_text!(
                en = "Bonus for near-resolution markets",
                zh = "临近结算奖励"
            ),
            help: ui_text!(
                en = "Bonus for near-resolution markets (within 6 hours). Default: `0.05`.",
                zh = "临近结算奖励。默认：0.05。"
            ),
            order: 160,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.fill_probability.staleness_penalty_per_level",
            label: ui_text!(
                en = "Per-StalenessLevel-step penalty",
                zh = "陈旧度逐级惩罚"
            ),
            help: ui_text!(
                en = "Per-`StalenessLevel`-step penalty. Default: `0.05`.",
                zh = "陈旧度逐级惩罚。默认：0.05。"
            ),
            order: 170,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.endgame.high_threshold … detection.endgame.settlement_window_hours`.
fn build_fields_detection_endgame() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.endgame.high_threshold",
            label: ui_text!(
                en = "Best-ask price at or above this value marks a market as converged",
                zh = "收敛判定价格阈值"
            ),
            help: ui_text!(
                en = "Best-ask price at or above this value marks a market as converged (YES or NO side). Money-critical: lowering it admits less-certain markets into the endgame funnel. Default: `0.95`.",
                zh = "收敛判定价格阈值。资金关键：该参数会直接影响可交易范围、敞口或检测准入，变更前请仔细评估。默认：0.95。"
            ),
            order: 180,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.max_investment_usd",
            label: ui_text!(
                en = "Maximum USD walked into the order book per opportunity",
                zh = "单次最大 Walk 金额 (USD)"
            ),
            help: ui_text!(
                en = "Maximum USD walked into the order book per opportunity. Caps single-shot sizing before risk sizing applies. Default: `500`.",
                zh = "单次最大 Walk 金额 (USD)。默认：500。"
            ),
            order: 190,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.min_convergence_duration_secs",
            label: ui_text!(
                en = "A market must hold convergence for at least this long before an opportunity may be emitted",
                zh = "最小收敛持续时间 (秒)"
            ),
            help: ui_text!(
                en = "A market must hold convergence for at least this long before an opportunity may be emitted. Guards against transient spikes. Default: `300` (5 minutes).",
                zh = "最小收敛持续时间 (秒)。默认：300。"
            ),
            order: 200,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.min_profit_per_share",
            label: ui_text!(en = "Minimum profit per share", zh = "最低每股利润"),
            help: ui_text!(
                en = "Minimum profit per share (`1 - entry VWAP`) to act. Below this the edge cannot cover fees + slippage. Default: `0.005`.",
                zh = "最低每股利润。默认：0.005。"
            ),
            order: 210,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.settlement_window_hours",
            label: ui_text!(
                en = "Only markets settling within this many hours are scanned",
                zh = "结算扫描窗口 (小时)"
            ),
            help: ui_text!(
                en = "Only markets settling within this many hours are scanned. Larger windows admit slower-converging markets but tie up capital longer. Default: `24`.",
                zh = "结算扫描窗口 (小时)。默认：24。"
            ),
            order: 250,
            widget: None,
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.endgame.scorer.category_weights … detection.endgame.scorer.min_score`.
fn build_fields_detection_endgame_scorer() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "detection.endgame.scorer.category_weights",
            label: ui_text!(
                en = "Per-category weight multipliers for scoring",
                zh = "分类评分权重"
            ),
            help: ui_text!(
                en = "Per-category weight multipliers for scoring (lower fee categories are weighted higher). Categories absent from the map default to `1.0` at conversion time.",
                zh = "分类评分权重。"
            ),
            order: 220,
            widget: Some(FieldWidget::EnumDecimalMap),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.scorer.max_depth_usage_pct",
            label: ui_text!(en = "Maximum depth usage", zh = "检测器最大深度占用 (%)"),
            help: ui_text!(
                en = "Maximum depth usage (%) the detector may accept. Default: `50`.",
                zh = "检测器最大深度占用 (%)。默认：50。"
            ),
            order: 230,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "detection.endgame.scorer.min_score",
            label: ui_text!(en = "Minimum composite score", zh = "最低综合分数"),
            help: ui_text!(
                en = "Minimum composite score (0..1) to emit an opportunity. Default: `0.10`.",
                zh = "最低综合分数。默认：0.10。"
            ),
            order: 240,
            widget: None,
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `detection.min_profit_threshold_usd … detection.min_profit_threshold_usd`.
fn build_fields_detection_min() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "detection.min_profit_threshold_usd",
        label: ui_text!(
            en = "Authoritative minimum net profit",
            zh = "最低净利润阈值 (USD)"
        ),
        help: ui_text!(
            en = "Authoritative minimum net profit (USD) for detection, validation, and risk (single source per ADR-001 — never duplicated under `execution` or `risk`). Opportunities below this expected net profit are dropped. Default: `0.50`.",
            zh = "最低净利润阈值 (USD)。默认：0.50。"
        ),
        order: 260,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `execution.coalescer.coalesce_window_ms … execution.coalescer.coalesce_window_ms`.
fn build_fields_execution_coalescer() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "execution.coalescer.coalesce_window_ms",
        label: ui_text!(en = "Max wait", zh = "双 token 合并窗口 (ms)"),
        help: ui_text!(
            en = "Max wait (ms) for the second token leg before flushing a market scan. Lower = lower latency, more duplicate scans. Default: `40`.",
            zh = "双 token 合并窗口 (ms)。默认：40。"
        ),
        order: 270,
        widget: Some(FieldWidget::DurationMs),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `execution.endgame_latency.dispatch_immediate_threshold … execution.endgame_latency.max_book_to_order_ms`.
fn build_fields_execution_endgame_latency() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "execution.endgame_latency.dispatch_immediate_threshold",
            label: ui_text!(
                en = "Scores at or above this bypass the funnel sweep delay",
                zh = "即时分发分数阈值"
            ),
            help: ui_text!(
                en = "Scores at or above this bypass the funnel sweep delay (immediate shard dispatch). Default: `0.5`.",
                zh = "即时分发分数阈值。默认：0.5。"
            ),
            order: 280,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.endgame_latency.max_book_to_order_ms",
            label: ui_text!(
                en = "Max ms from last book apply to order emit",
                zh = "订单簿到下单 SLO (ms)"
            ),
            help: ui_text!(
                en = "Max ms from last book apply to order emit (SLO-2); older books fail validation. Default: `5`.",
                zh = "订单簿到下单 SLO (ms)。默认：5。"
            ),
            order: 290,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `execution.funnel.max_queue_size … execution.funnel.min_dispatch_interval_ms`.
fn build_fields_execution_funnel() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "execution.funnel.max_queue_size",
            label: ui_text!(
                en = "Bounded priority-queue capacity; overflow evicts the lowest score",
                zh = "漏斗队列容量"
            ),
            help: ui_text!(
                en = "Bounded priority-queue capacity; overflow evicts the lowest score. Default: `50`.",
                zh = "漏斗队列容量。默认：50。"
            ),
            order: 300,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.funnel.min_dispatch_interval_ms",
            label: ui_text!(en = "Sweep interval", zh = "低优先级分发间隔 (ms)"),
            help: ui_text!(
                en = "Sweep interval (ms) between low-priority dispatches (high-score opportunities bypass via the fast lane). Default: `75`.",
                zh = "低优先级分发间隔 (ms)。默认：75。"
            ),
            order: 310,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `execution.timeout.dispatcher_timeout_ms … execution.timeout.trade_confirm_timeout_secs`.
fn build_fields_execution_timeout() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "execution.timeout.dispatcher_timeout_ms",
            label: ui_text!(en = "Hard-kill timeout", zh = "执行派发超时 (ms)"),
            help: ui_text!(
                en = "Hard-kill timeout (ms) for execution dispatch (FOK order round trip). Default: `30000`.",
                zh = "执行派发超时 (ms)。默认：30000。"
            ),
            order: 320,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.timeout.max_validation_slippage_bps",
            label: ui_text!(
                en = "Max price slippage between detection and validation",
                zh = "校验最大滑点 (bps)"
            ),
            help: ui_text!(
                en = "Max price slippage between detection and validation (bps). Exceeding this rejects the trade. Default: `50`.",
                zh = "校验最大滑点 (bps)。默认：50。"
            ),
            order: 330,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.timeout.trade_confirm_poll_interval_secs",
            label: ui_text!(en = "Interval", zh = "成交确认轮询间隔 (秒)"),
            help: ui_text!(
                en = "Interval (s) between confirmation polls. Read per relay poll — takes effect on the next cycle. Default: `2`.",
                zh = "成交确认轮询间隔 (秒)。默认：2。"
            ),
            order: 340,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.timeout.trade_confirm_timeout_secs",
            label: ui_text!(en = "Total time budget", zh = "成交确认总超时 (秒)"),
            help: ui_text!(
                en = "Total time budget (s) to confirm a trade reached a terminal state. Read per relay poll — takes effect on the next cycle. Default: `60`.",
                zh = "成交确认总超时 (秒)。默认：60。"
            ),
            order: 350,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `execution.reconciliation.backoff_base_secs … execution.reconciliation.trade_lookback_secs`.
fn build_fields_execution_reconciliation() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "execution.reconciliation.backoff_base_secs",
            label: ui_text!(
                en = "Reconciliation backoff base (seconds)",
                zh = "对账退避基数 (秒)"
            ),
            help: ui_text!(
                en = "Base delay (seconds) for exponential backoff when reconciliation evidence is insufficient. Default: `5`.",
                zh = "证据不足时对账退避的基数 (秒)。默认：5。"
            ),
            order: 320,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.reconciliation.backoff_max_secs",
            label: ui_text!(
                en = "Reconciliation backoff maximum (seconds)",
                zh = "对账退避上限 (秒)"
            ),
            help: ui_text!(
                en = "Maximum defer delay (seconds) between reconciliation scans. Default: `300`.",
                zh = "对账扫描之间的最大延迟 (秒)。默认：300。"
            ),
            order: 325,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.reconciliation.min_miss_age_secs",
            label: ui_text!(
                en = "Minimum miss evidence age (seconds)",
                zh = "Miss 证据最小年龄 (秒)"
            ),
            help: ui_text!(
                en = "Minimum age (seconds) after submit before a proven-negative Miss is allowed. Default: `120`.",
                zh = "允许判定 Miss 的最小提交后等待时间 (秒)。默认：120。"
            ),
            order: 330,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.reconciliation.min_fill_ratio",
            label: ui_text!(en = "Minimum CTF fill ratio", zh = "CTF 最小成交比例"),
            help: ui_text!(
                en = "Minimum fill ratio (0..=1) for CTF balance-delta evidence to count as filled. Default: `1`.",
                zh = "CTF 余额增量视为成交所需的最小比例 (0..=1)。默认：1。"
            ),
            order: 335,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "execution.reconciliation.trade_lookback_secs",
            label: ui_text!(
                en = "CLOB trade lookback (seconds)",
                zh = "CLOB 成交回溯 (秒)"
            ),
            help: ui_text!(
                en = "CLOB trade lookback (seconds) before `submitted_at` for L2 matching. Default: `5`.",
                zh = "L2 匹配时在 submitted_at 之前回溯 CLOB 成交的秒数。默认：5。"
            ),
            order: 340,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `market_data.enabled_categories … market_data.enabled_categories`.
fn build_fields_market_data_enabled() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "market_data.enabled_categories",
        label: ui_text!(
            en = "Categories admitted into the tradeable universe",
            zh = "启用交易品类"
        ),
        help: ui_text!(
            en = "Categories admitted into the tradeable universe (WS subscriptions + scanner sweep). An event matches when any of its tag-derived categories is enabled. Empty list = every category. The full catalog is always ingested and persisted regardless of this filter — it only bounds the hot trading set, so narrowing it never loses settlement or evidence data. Default: empty (all categories).",
            zh = "启用交易品类。空列表表示全部品类可交易；仅收窄热交易集合，不影响全量入库与结算证据。默认：空。"
        ),
        order: 360,
        widget: Some(FieldWidget::EnumSet),
        semantics: Some(FieldSemantics::EmptyMeansAll),
        visible: true,
    }]
}

/// Fields: `market_data.staleness_acceptable_ms … market_data.staleness_stale_ms`.
fn build_fields_market_data_staleness() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "market_data.staleness_acceptable_ms",
            label: ui_text!(en = "Book age", zh = "可接受陈旧度 (ms)"),
            help: ui_text!(
                en = "Book age (ms) at or below which data is `Acceptable` (still tradeable). Default: `5000`.",
                zh = "可接受陈旧度 (ms)。默认：5000。"
            ),
            order: 370,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "market_data.staleness_expired_ms",
            label: ui_text!(en = "Book age", zh = "过期陈旧度上界 (ms)"),
            help: ui_text!(
                en = "Book age (ms) above `staleness_stale_ms` is `Expired` and ignored. This field documents the ladder's outer bound. Default: `30000`.",
                zh = "过期陈旧度上界 (ms)。默认：30000。"
            ),
            order: 380,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "market_data.staleness_fresh_ms",
            label: ui_text!(en = "Book age", zh = "新鲜度阈值 (ms)"),
            help: ui_text!(
                en = "Book age (ms) at or below which data is `Fresh`. Default: `2000`.",
                zh = "新鲜度阈值 (ms)。默认：2000。"
            ),
            order: 390,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "market_data.staleness_stale_ms",
            label: ui_text!(en = "Book age", zh = "陈旧阈值 (ms)"),
            help: ui_text!(
                en = "Book age (ms) at or below which data is `Stale` (scored with discount, never traded). Default: `15000`.",
                zh = "陈旧阈值 (ms)。默认：15000。"
            ),
            order: 400,
            widget: Some(FieldWidget::DurationMs),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `notification.alert_cooldown_secs … notification.alert_cooldown_secs`.
fn build_fields_notification_alert() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "notification.alert_cooldown_secs",
        label: ui_text!(en = "Minimum interval", zh = "告警冷却 (秒)"),
        help: ui_text!(
            en = "Minimum interval (seconds) between alerts with the same severity+title (anti-flood; applies to all channels). Default: `60`.",
            zh = "告警冷却 (秒)。默认：60。"
        ),
        order: 410,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `notification.telegram.bot_token … notification.telegram.enabled`.
fn build_fields_notification_telegram() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "notification.telegram.bot_token",
            label: ui_text!(en = "Bot token", zh = "Telegram Bot Token"),
            help: ui_text!(
                en = "Bot token (sensitive — masked in read APIs). Default: empty.",
                zh = "Telegram Bot Token。敏感字段：读取接口会掩码，未修改时不会回传。默认：空。"
            ),
            order: 420,
            widget: Some(FieldWidget::SecretString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "notification.telegram.chat_id",
            label: ui_text!(en = "Destination chat ID", zh = "Telegram Chat ID"),
            help: ui_text!(
                en = "Destination chat ID (numeric, as a string). Default: empty.",
                zh = "Telegram Chat ID。默认：空。"
            ),
            order: 430,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "notification.telegram.enabled",
            label: ui_text!(
                en = "Whether Telegram alerts are dispatched",
                zh = "启用 Telegram"
            ),
            help: ui_text!(
                en = "Whether Telegram alerts are dispatched. Live-mode validation requires a non-empty `bot_token` and `chat_id` when enabled. Default: `false`.",
                zh = "启用 Telegram。默认：false。"
            ),
            order: 440,
            widget: None,
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `notification.webhook.enabled … notification.webhook.url`.
fn build_fields_notification_webhook() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "notification.webhook.enabled",
            label: ui_text!(
                en = "Whether webhook alerts are dispatched",
                zh = "启用 Webhook"
            ),
            help: ui_text!(
                en = "Whether webhook alerts are dispatched. Live-mode validation requires a non-empty `url` when enabled. Default: `false`.",
                zh = "启用 Webhook。默认：false。"
            ),
            order: 450,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "notification.webhook.url",
            label: ui_text!(en = "POST target URL", zh = "Webhook URL"),
            help: ui_text!(
                en = "POST target URL (sensitive — masked in read APIs). Default: empty.",
                zh = "Webhook URL。敏感字段：读取接口会掩码，未修改时不会回传。默认：空。"
            ),
            order: 460,
            widget: Some(FieldWidget::SecretString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.api_error_rate_threshold … risk.api_error_rate_threshold`.
fn build_fields_risk_api() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.api_error_rate_threshold",
        label: ui_text!(en = "API error rate threshold", zh = "API 错误率阈值"),
        help: ui_text!(
            en = "API error rate threshold (0..1). Exceeding trips the L2 Session breaker. Default: `0.10`.",
            zh = "API 错误率阈值。默认：0.10。"
        ),
        order: 470,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.bankroll_usd … risk.bankroll_usd`.
fn build_fields_risk_bankroll() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.bankroll_usd",
        label: ui_text!(
            en = "Total bankroll available for Kelly computation",
            zh = "Kelly 总资金 (USD)"
        ),
        help: ui_text!(
            en = "Total bankroll available for Kelly computation (USD). Also seeds the simulated balance in `DryRun`/`Paper`. Default: `1000`.",
            zh = "Kelly 总资金 (USD)。默认：1000。"
        ),
        order: 480,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.base_cooldown_secs … risk.base_cooldown_secs`.
fn build_fields_risk_base() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.base_cooldown_secs",
        label: ui_text!(
            en = "Base adaptive cooldown after repeated misses",
            zh = "基础冷却 (秒)"
        ),
        help: ui_text!(
            en = "Base adaptive cooldown after repeated misses (seconds). Default: `900`.",
            zh = "基础冷却 (秒)。默认：900。"
        ),
        order: 490,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.circuit_breaker.half_open_probes … risk.circuit_breaker.l4_cooldown_secs`.
fn build_fields_risk_circuit_breaker_from_half_open_probes() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.circuit_breaker.half_open_probes",
            label: ui_text!(
                en = "Successful probe trades required in HalfOpen before Recovered",
                zh = "半开成功探测次数"
            ),
            help: ui_text!(
                en = "Successful probe trades required in `HalfOpen` before Recovered. Default: `2`.",
                zh = "半开成功探测次数。默认：2。"
            ),
            order: 500,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.circuit_breaker.l1_cooldown_secs",
            label: ui_text!(en = "L1", zh = "L1 冷却 (秒)"),
            help: ui_text!(
                en = "L1 (Trade): per-opportunity static filter failure cooldown (seconds). Default: `60`.",
                zh = "L1 冷却 (秒)。默认：60。"
            ),
            order: 510,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.circuit_breaker.l2_cooldown_secs",
            label: ui_text!(en = "L2", zh = "L2 冷却 (秒)"),
            help: ui_text!(
                en = "L2 (Session): rolling window breach cooldown (seconds). Default: `900`.",
                zh = "L2 冷却 (秒)。默认：900。"
            ),
            order: 520,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.circuit_breaker.l3_cooldown_secs",
            label: ui_text!(en = "L3", zh = "L3 冷却 (秒)"),
            help: ui_text!(
                en = "L3 (Daily): daily/weekly cap breach cooldown (seconds). Default: `3600`.",
                zh = "L3 冷却 (秒)。默认：3600。"
            ),
            order: 530,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.circuit_breaker.l4_cooldown_secs",
            label: ui_text!(en = "L4", zh = "L4 冷却 (秒)"),
            help: ui_text!(
                en = "L4 (System): connectivity/balance emergency cooldown (seconds). Default: `7200`.",
                zh = "L4 冷却 (秒)。默认：7200。"
            ),
            order: 540,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.circuit_breaker.max_cooldown_secs … risk.circuit_breaker.recovery_observation_secs`.
fn build_fields_risk_circuit_breaker_from_max_cooldown_secs() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.circuit_breaker.max_cooldown_secs",
            label: ui_text!(en = "Maximum cooldown duration", zh = "L2 最大冷却 (秒)"),
            help: ui_text!(
                en = "Maximum cooldown duration (seconds) for L2 exponential back-off. Default: `14400`.",
                zh = "L2 最大冷却 (秒)。默认：14400。"
            ),
            order: 550,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.circuit_breaker.recovery_observation_secs",
            label: ui_text!(en = "Observation period", zh = "恢复观察期 (秒)"),
            help: ui_text!(
                en = "Observation period (seconds) in Recovered before returning to Closed. Default: `300`.",
                zh = "恢复观察期 (秒)。默认：300。"
            ),
            order: 560,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.cooldown_multiplier … risk.cooldown_multiplier`.
fn build_fields_risk_cooldown() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.cooldown_multiplier",
        label: ui_text!(
            en = "Exponential multiplier applied per consecutive cooldown",
            zh = "冷却指数倍数"
        ),
        help: ui_text!(
            en = "Exponential multiplier applied per consecutive cooldown. Default: `2.0`.",
            zh = "冷却指数倍数。默认：2.0。"
        ),
        order: 570,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.daily_budget_usd … risk.daily_directional_budget`.
fn build_fields_risk_daily() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.daily_budget_usd",
            label: ui_text!(en = "Independent daily spend budget", zh = "日预算 (USD)"),
            help: ui_text!(
                en = "Independent daily spend budget (USD). Execution stops when exhausted. Default: `50`.",
                zh = "日预算 (USD)。默认：50。"
            ),
            order: 580,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.daily_directional_budget",
            label: ui_text!(
                en = "Daily budget of directional trades per side",
                zh = "方向日交易预算"
            ),
            help: ui_text!(
                en = "Daily budget of directional trades per side. Default: `10`.",
                zh = "方向日交易预算。默认：10。"
            ),
            order: 590,
            widget: None,
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.drawdown.drawdown_reduction_factor … risk.drawdown.max_drawdown_pct`.
fn build_fields_risk_drawdown() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.drawdown.drawdown_reduction_factor",
            label: ui_text!(
                en = "Size reduction factor applied when the drawdown limit is hit",
                zh = "回撤减仓因子"
            ),
            help: ui_text!(
                en =
                    "Size reduction factor applied when the drawdown limit is hit. Default: `0.5`.",
                zh = "回撤减仓因子。默认：0.5。"
            ),
            order: 600,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.drawdown.max_drawdown_pct",
            label: ui_text!(en = "Maximum drawdown", zh = "最大回撤 (%)"),
            help: ui_text!(
                en = "Maximum drawdown (%) before position sizes are reduced. Default: `10`.",
                zh = "最大回撤 (%)。默认：10。"
            ),
            order: 610,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.heartbeat_max_failures … risk.heartbeat_max_failures`.
fn build_fields_risk_heartbeat() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.heartbeat_max_failures",
        label: ui_text!(
            en = "Consecutive heartbeat failures before an L4 System halt",
            zh = "心跳失败阈值"
        ),
        help: ui_text!(
            en = "Consecutive heartbeat failures before an L4 System halt. Default: `3`.",
            zh = "心跳失败阈值。默认：3。"
        ),
        order: 620,
        widget: None,
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.kelly.max_kelly … risk.kelly.min_probability_confidence`.
fn build_fields_risk_kelly_from_max_kelly() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.kelly.max_kelly",
            label: ui_text!(
                en = "Maximum Kelly fraction before capping",
                zh = "Kelly 上限"
            ),
            help: ui_text!(
                en = "Maximum Kelly fraction before capping. Default: `0.25`.",
                zh = "Kelly 上限。默认：0.25。"
            ),
            order: 630,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.kelly.max_probability_staleness_secs",
            label: ui_text!(en = "Maximum staleness", zh = "Kelly 校准最大陈旧度 (秒)"),
            help: ui_text!(
                en = "Maximum staleness (seconds) of the calibration model before Kelly returns zero. Default: `7200`.",
                zh = "Kelly 校准最大陈旧度 (秒)。默认：7200。"
            ),
            order: 640,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.kelly.min_calibration_samples",
            label: ui_text!(
                en = "Minimum historical sample count for calibration to be trusted",
                zh = "Kelly 最小样本数"
            ),
            help: ui_text!(
                en =
                    "Minimum historical sample count for calibration to be trusted. Default: `10`.",
                zh = "Kelly 最小样本数。默认：10。"
            ),
            order: 650,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.kelly.min_edge_bps",
            label: ui_text!(en = "Minimum edge", zh = "Kelly 最小边际 (bps)"),
            help: ui_text!(
                en = "Minimum edge (bps) below which Kelly returns zero. Default: `200`.",
                zh = "Kelly 最小边际 (bps)。默认：200。"
            ),
            order: 660,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.kelly.min_probability_confidence",
            label: ui_text!(
                en = "Minimum calibration confidence",
                zh = "Kelly 最小置信度"
            ),
            help: ui_text!(
                en = "Minimum calibration confidence (0..1) below which Kelly returns zero. Default: `0.3`.",
                zh = "Kelly 最小置信度。默认：0.3。"
            ),
            order: 670,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.kelly_fraction … risk.kelly_fraction`.
fn build_fields_risk_kelly_from_kelly_fraction() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.kelly_fraction",
        label: ui_text!(
            en = "Quarter-Kelly fraction multiplier",
            zh = "四分之一 Kelly 倍数"
        ),
        help: ui_text!(
            en = "Quarter-Kelly fraction multiplier (`f*/4`). Default: `0.25`.",
            zh = "四分之一 Kelly 倍数。默认：0.25。"
        ),
        order: 680,
        widget: None,
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.market_miss_blacklist_count … risk.market_miss_blacklist_duration_secs`.
fn build_fields_risk_market() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.market_miss_blacklist_count",
            label: ui_text!(
                en = "Consecutive misses on one market before auto-blacklisting",
                zh = "未命中拉黑阈值"
            ),
            help: ui_text!(
                en = "Consecutive misses on one market before auto-blacklisting. Default: `3`.",
                zh = "未命中拉黑阈值。默认：3。"
            ),
            order: 690,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.market_miss_blacklist_duration_secs",
            label: ui_text!(en = "Auto-blacklist TTL", zh = "自动拉黑 TTL (秒)"),
            help: ui_text!(
                en = "Auto-blacklist TTL (seconds). Default: `3600`.",
                zh = "自动拉黑 TTL (秒)。默认：3600。"
            ),
            order: 700,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.max_concurrent_directional … risk.max_concurrent_directional`.
fn build_fields_risk_max_concurrent() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_concurrent_directional",
        label: ui_text!(
            en = "Max concurrent positions on the same directional side",
            zh = "同方向最大并发"
        ),
        help: ui_text!(
            en = "Max concurrent positions on the same directional side. Default: `3`.",
            zh = "同方向最大并发。默认：3。"
        ),
        order: 710,
        widget: None,
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.max_consecutive_misses … risk.max_consecutive_misses`.
fn build_fields_risk_max_consecutive() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_consecutive_misses",
        label: ui_text!(
            en = "Consecutive misses before the session breaker trips",
            zh = "会话连续未命中阈值"
        ),
        help: ui_text!(
            en = "Consecutive misses before the session breaker trips. Default: `3`.",
            zh = "会话连续未命中阈值。默认：3。"
        ),
        order: 720,
        widget: None,
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.max_cooldown_secs … risk.max_cooldown_secs`.
fn build_fields_risk_max_cooldown() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_cooldown_secs",
        label: ui_text!(
            en = "Hard ceiling for the adaptive cooldown",
            zh = "自适应冷却上限 (秒)"
        ),
        help: ui_text!(
            en = "Hard ceiling for the adaptive cooldown (seconds). Default: `7200`.",
            zh = "自适应冷却上限 (秒)。默认：7200。"
        ),
        order: 730,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.max_daily_fee_spend_usd … risk.max_daily_loss_usd`.
fn build_fields_risk_max_daily() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.max_daily_fee_spend_usd",
            label: ui_text!(en = "Daily fee-spend cap", zh = "日手续费上限 (USD)"),
            help: ui_text!(
                en = "Daily fee-spend cap (USD); breach halts at L3. Default: `25`.",
                zh = "日手续费上限 (USD)。默认：25。"
            ),
            order: 740,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.max_daily_loss_usd",
            label: ui_text!(en = "Daily realized-loss cap", zh = "日亏损上限 (USD)"),
            help: ui_text!(
                en = "Daily realized-loss cap (USD); breach halts at L3. Default: `75`.",
                zh = "日亏损上限 (USD)。默认：75。"
            ),
            order: 750,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.max_depth_usage_pct … risk.max_depth_usage_pct`.
fn build_fields_risk_max_depth() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_depth_usage_pct",
        label: ui_text!(
            en = "Maximum fraction of visible book depth a single order may consume",
            zh = "最大深度占用 (%)"
        ),
        help: ui_text!(
            en = "Maximum fraction of visible book depth a single order may consume (%). Default: `30`.",
            zh = "最大深度占用 (%)。默认：30。"
        ),
        order: 760,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.max_hourly_fee_spend_usd … risk.max_hourly_loss_usd`.
fn build_fields_risk_max_hourly() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.max_hourly_fee_spend_usd",
            label: ui_text!(
                en = "Rolling hourly fee-spend cap",
                zh = "小时手续费上限 (USD)"
            ),
            help: ui_text!(
                en = "Rolling hourly fee-spend cap (USD); breach trips the L2 breaker. Default: `10`.",
                zh = "小时手续费上限 (USD)。默认：10。"
            ),
            order: 770,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.max_hourly_loss_usd",
            label: ui_text!(en = "Rolling hourly loss cap", zh = "小时亏损上限 (USD)"),
            help: ui_text!(
                en = "Rolling hourly loss cap (USD); breach trips the L2 breaker. Default: `30`.",
                zh = "小时亏损上限 (USD)。默认：30。"
            ),
            order: 780,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.max_metrics_staleness_secs … risk.max_metrics_staleness_secs`.
fn build_fields_risk_max_metrics() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_metrics_staleness_secs",
        label: ui_text!(en = "Maximum age", zh = "Live 指标最大陈旧度 (秒)"),
        help: ui_text!(
            en = "Maximum age (seconds) of the risk metrics snapshot allowed on the Live hot path. Must be >= `metrics_refresh_interval_secs`. Default: `15`.",
            zh = "Live 指标最大陈旧度 (秒)。默认：15。"
        ),
        order: 790,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.max_open_positions … risk.max_open_positions`.
fn build_fields_risk_max_open() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_open_positions",
        label: ui_text!(
            en = "Maximum concurrently open positions",
            zh = "最大持仓数"
        ),
        help: ui_text!(
            en = "Maximum concurrently open positions. Default: `3`.",
            zh = "最大持仓数。默认：3。"
        ),
        order: 800,
        widget: None,
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.max_single_bet_usd … risk.max_single_market_exposure_usd`.
fn build_fields_risk_max_single() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.max_single_bet_usd",
            label: ui_text!(
                en = "Maximum USD for a single bet",
                zh = "单笔最大下注 (USD)"
            ),
            help: ui_text!(
                en = "Maximum USD for a single bet. Default: `25`.",
                zh = "单笔最大下注 (USD)。默认：25。"
            ),
            order: 810,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.max_single_loss_usd",
            label: ui_text!(en = "Single-trade loss cap", zh = "单笔亏损上限 (USD)"),
            help: ui_text!(
                en = "Single-trade loss cap (USD); breach halts at L3. Default: `30`.",
                zh = "单笔亏损上限 (USD)。默认：30。"
            ),
            order: 820,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.max_single_market_exposure_usd",
            label: ui_text!(
                en = "Maximum exposure per market",
                zh = "单市场敞口上限 (USD)"
            ),
            help: ui_text!(
                en = "Maximum exposure per market (USD). Preflight rejects activation when set below any in-flight market exposure. Default: `500`.",
                zh = "单市场敞口上限 (USD)。默认：500。"
            ),
            order: 830,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.max_total_exposure_pct … risk.max_total_exposure_usd`.
fn build_fields_risk_max_total() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.max_total_exposure_pct",
            label: ui_text!(
                en = "Maximum portfolio exposure as a percentage of available balance",
                zh = "组合敞口占比上限 (%)"
            ),
            help: ui_text!(
                en = "Maximum portfolio exposure as a percentage of available balance. Default: `80`.",
                zh = "组合敞口占比上限 (%)。默认：80。"
            ),
            order: 840,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.max_total_exposure_usd",
            label: ui_text!(
                en = "Maximum total exposure across all reservations",
                zh = "总敞口上限 (USD)"
            ),
            help: ui_text!(
                en = "Maximum total exposure across all reservations (USD). Preflight rejects activation when set below the currently reserved total. Default: `5000`.",
                zh = "总敞口上限 (USD)。默认：5000。"
            ),
            order: 850,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.max_weekly_loss_usd … risk.max_weekly_loss_usd`.
fn build_fields_risk_max_weekly() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.max_weekly_loss_usd",
        label: ui_text!(en = "Weekly realized-loss cap", zh = "周亏损上限 (USD)"),
        help: ui_text!(
            en = "Weekly realized-loss cap (USD); breach halts at L4. Default: `120`.",
            zh = "周亏损上限 (USD)。默认：120。"
        ),
        order: 860,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.metrics_refresh_interval_secs … risk.metrics_refresh_interval_secs`.
fn build_fields_risk_metrics() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.metrics_refresh_interval_secs",
        label: ui_text!(en = "Interval", zh = "指标刷新间隔 (秒)"),
        help: ui_text!(
            en = "Interval (seconds) between CLOB balance + open-position metrics refreshes. Default: `5`.",
            zh = "指标刷新间隔 (秒)。默认：5。"
        ),
        order: 870,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.min_balance_usd … risk.min_trade_usd`.
fn build_fields_risk_min() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.min_balance_usd",
            label: ui_text!(
                en = "Minimum CLOB collateral balance",
                zh = "最低余额 (USD)"
            ),
            help: ui_text!(
                en = "Minimum CLOB collateral balance (USD); below this trading is gated. Default: `50`.",
                zh = "最低余额 (USD)。默认：50。"
            ),
            order: 880,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.min_depth_usd",
            label: ui_text!(en = "Minimum order-book depth", zh = "最低深度 (USD)"),
            help: ui_text!(
                en = "Minimum order-book depth (USD) required before execution. Default: `200`.",
                zh = "最低深度 (USD)。默认：200。"
            ),
            order: 890,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.min_trade_usd",
            label: ui_text!(en = "Minimum trade size", zh = "最小交易规模 (USD)"),
            help: ui_text!(
                en = "Minimum trade size (USD); sized below this the opportunity is skipped. Default: `1`.",
                zh = "最小交易规模 (USD)。默认：1。"
            ),
            order: 900,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.permanent_blacklist_markets … risk.permanent_blacklist_tokens`.
fn build_fields_risk_permanent() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.permanent_blacklist_markets",
            label: ui_text!(
                en = "Permanently blacklisted market condition IDs",
                zh = "永久黑名单市场"
            ),
            help: ui_text!(
                en = "Permanently blacklisted market condition IDs. Reload merges with — and never removes — entries added at runtime via the blacklist API. Default: empty.",
                zh = "永久黑名单市场。默认：空。"
            ),
            order: 910,
            widget: Some(FieldWidget::StringList),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.permanent_blacklist_tokens",
            label: ui_text!(
                en = "Permanently blacklisted CLOB token IDs",
                zh = "永久黑名单 Token"
            ),
            help: ui_text!(
                en = "Permanently blacklisted CLOB token IDs. Same merge semantics as `permanent_blacklist_markets`. Default: empty.",
                zh = "永久黑名单 Token。默认：空。"
            ),
            order: 920,
            widget: Some(FieldWidget::StringList),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.potential_loss_escalation_secs … risk.potential_loss_escalation_secs`.
fn build_fields_risk_potential() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.potential_loss_escalation_secs",
        label: ui_text!(en = "Maximum age", zh = "潜在亏损升级超时 (秒)"),
        help: ui_text!(
            en = "Maximum age (seconds) of an active potential-loss entry before escalation triggers an L4 System halt. Default: `3600`.",
            zh = "潜在亏损升级超时 (秒)。默认：3600。"
        ),
        order: 930,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.reconciliation_interval_secs … risk.reconciliation_tolerance_usd`.
fn build_fields_risk_reconciliation() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.reconciliation_interval_secs",
            label: ui_text!(en = "Interval", zh = "对账间隔 (秒)"),
            help: ui_text!(
                en = "Interval (seconds) between ledger reconciliation runs. Default: `300`.",
                zh = "对账间隔 (秒)。默认：300。"
            ),
            order: 940,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.reconciliation_tolerance_usd",
            label: ui_text!(
                en = "Maximum acceptable balance drift",
                zh = "对账容差 (USD)"
            ),
            help: ui_text!(
                en = "Maximum acceptable balance drift (USD) before alerting. Default: `1.0`.",
                zh = "对账容差 (USD)。默认：1.0。"
            ),
            order: 950,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.reservation_gc_interval_secs … risk.reservation_ttl_secs`.
fn build_fields_risk_reservation() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "risk.reservation_gc_interval_secs",
            label: ui_text!(en = "Interval", zh = "预占 GC 间隔 (秒)"),
            help: ui_text!(
                en = "Interval (seconds) for cleaning expired in-flight reservations. Default: `30`.",
                zh = "预占 GC 间隔 (秒)。默认：30。"
            ),
            order: 960,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "risk.reservation_ttl_secs",
            label: ui_text!(en = "Default TTL", zh = "预占 TTL (秒)"),
            help: ui_text!(
                en = "Default TTL (seconds) for in-flight capital reservations. Default: `300`.",
                zh = "预占 TTL (秒)。默认：300。"
            ),
            order: 970,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `risk.reserve_balance_usd … risk.reserve_balance_usd`.
fn build_fields_risk_reserve() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.reserve_balance_usd",
        label: ui_text!(en = "Balance reserve", zh = "Kelly 预留余额 (USD)"),
        help: ui_text!(
            en = "Balance reserve (USD) excluded from the Kelly bankroll. Default: `100`.",
            zh = "Kelly 预留余额 (USD)。默认：100。"
        ),
        order: 980,
        widget: Some(FieldWidget::DecimalString),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `risk.ws_disconnect_threshold_secs … risk.ws_disconnect_threshold_secs`.
fn build_fields_risk_ws() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "risk.ws_disconnect_threshold_secs",
        label: ui_text!(en = "WS disconnect duration", zh = "WS 断连阈值 (秒)"),
        help: ui_text!(
            en = "WS disconnect duration (seconds) before trading is gated. Default: `30`.",
            zh = "WS 断连阈值 (秒)。默认：30。"
        ),
        order: 990,
        widget: Some(FieldWidget::Integer),
        semantics: None,
        visible: true,
    }]
}

/// Fields: `settlement.lifecycle.dedup_window_secs … settlement.lifecycle.retry_interval_secs`.
fn build_fields_settlement_lifecycle() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "settlement.lifecycle.dedup_window_secs",
            label: ui_text!(en = "Window", zh = "结算去重窗口 (秒)"),
            help: ui_text!(
                en = "Window (seconds) for deduplicating settlement triggers for the same market. Caution: shrinking it mid-flight admits duplicate triggers for markets settled within the old window. Default: `30`.",
                zh = "结算去重窗口 (秒)。默认：30。注意：运行中修改可能重建内部状态或清空在途缓存，请在低流量窗口操作。"
            ),
            order: 1000,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.lifecycle.max_redeem_attempts",
            label: ui_text!(
                en = "Maximum redeem attempts per position before terminal failure",
                zh = "最大赎回尝试次数"
            ),
            help: ui_text!(
                en = "Maximum redeem attempts per position before terminal failure (operator alert + manual intervention). Default: `5`.",
                zh = "最大赎回尝试次数。默认：5。"
            ),
            order: 1010,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.lifecycle.retry_interval_secs",
            label: ui_text!(en = "Interval", zh = "结算重试间隔 (秒)"),
            help: ui_text!(
                en = "Interval (seconds) between retry sweeps over failed settlements. Default: `60`.",
                zh = "结算重试间隔 (秒)。默认：60。"
            ),
            order: 1020,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `settlement.oracle.all_sources_down_strategy … settlement.oracle.uma_timeout_secs`.
fn build_fields_settlement_oracle_from_all_sources_down_strategy() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "settlement.oracle.all_sources_down_strategy",
            label: ui_text!(
                en = "Behaviour when every oracle source is unavailable",
                zh = "Oracle 全源不可用策略"
            ),
            help: ui_text!(
                en = "Behaviour when every oracle source is unavailable. Default: `conservative_reject` (fail-closed; never settle blind).",
                zh = "Oracle 全源不可用策略。失败关闭：异常情况下拒绝操作，不会盲目放行。默认：conservative_reject。"
            ),
            order: 1030,
            widget: Some(FieldWidget::EnumSelect),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.oracle.cross_check_delay_secs",
            label: ui_text!(en = "Delay", zh = "交叉校验延迟 (秒)"),
            help: ui_text!(
                en = "Delay (seconds) before the post-settlement cross-check re-queries sources. Default: `120`.",
                zh = "交叉校验延迟 (秒)。默认：120。"
            ),
            order: 1040,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.oracle.enabled",
            label: ui_text!(
                en = "Whether the post-settlement oracle cross-check audit runs",
                zh = "启用 Oracle 交叉校验"
            ),
            help: ui_text!(
                en = "Whether the post-settlement oracle cross-check audit runs. Disabling skips the audit only — settlement itself still requires a resolution verdict. Default: `true`.",
                zh = "启用 Oracle 交叉校验。默认：true。"
            ),
            order: 1050,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.oracle.uma_endpoint",
            label: ui_text!(en = "UMA optimistic-oracle API endpoint", zh = "UMA 端点"),
            help: ui_text!(
                en = "UMA optimistic-oracle API endpoint. Default: `https://api.uma.xyz`.",
                zh = "UMA 端点。默认：https://api.uma.xyz。"
            ),
            order: 1060,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.oracle.uma_timeout_secs",
            label: ui_text!(en = "UMA request timeout", zh = "UMA 超时 (秒)"),
            help: ui_text!(
                en = "UMA request timeout (seconds). Default: `10`.",
                zh = "UMA 超时 (秒)。默认：10。"
            ),
            order: 1070,
            widget: Some(FieldWidget::Integer),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `settlement.oracle.voting_quorum … settlement.oracle.voting_quorum`.
fn build_fields_settlement_oracle_from_voting_quorum() -> Vec<FieldUiEntry> {
    vec![FieldUiEntry {
        path: "settlement.oracle.voting_quorum",
        label: ui_text!(
            en = "Sources that must agree before a resolution verdict is accepted",
            zh = "投票法定人数"
        ),
        help: ui_text!(
            en = "Sources that must agree before a resolution verdict is accepted. Default: `2` (of Gamma / CTF / UMA).",
            zh = "投票法定人数。默认：2。"
        ),
        order: 1080,
        widget: None,
        semantics: None,
        visible: true,
    }]
}

/// Fields: `settlement.redeem.gas_limit … settlement.redeem.overrides`.
fn build_fields_settlement_redeem() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "settlement.redeem.gas_limit",
            label: ui_text!(
                en = "Gas limit for redeem transactions",
                zh = "赎回 Gas 上限"
            ),
            help: ui_text!(
                en = "Gas limit for redeem transactions. Default: `500000`.",
                zh = "赎回 Gas 上限。默认：500000。"
            ),
            order: 1090,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.redeem.matic_usd_price",
            label: ui_text!(en = "MATIC/USD price", zh = "MATIC/USD 价格"),
            help: ui_text!(
                en = "MATIC/USD price for converting on-chain redeem gas to USD in Live settlement PnL. Default: `0.5`.",
                zh = "Live 结算中将链上赎回 Gas 换算为 USD 的 MATIC/USD 价格。默认：0.5。"
            ),
            order: 1100,
            widget: Some(FieldWidget::DecimalString),
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.redeem.overrides",
            label: ui_text!(en = "Per-market redeem overrides", zh = "按市场赎回覆盖"),
            help: ui_text!(
                en = "Optional condition_id keyed overrides. Each override must match the market's standard or neg-risk class.",
                zh = "可选的 condition_id 覆盖配置；每条覆盖必须匹配市场的 standard 或 neg-risk 类别。"
            ),
            order: 1120,
            widget: None,
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `settlement.redeem.neg_risk.holder_address … settlement.redeem.neg_risk.route`.
fn build_fields_settlement_redeem_neg_risk() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "settlement.redeem.neg_risk.holder_address",
            label: ui_text!(
                en = "Neg-risk market token holder",
                zh = "Neg-risk Holder 地址"
            ),
            help: ui_text!(
                en = "Token holder for neg-risk markets when it differs from the signer. `None` uses the signer address.",
                zh = "Neg-risk 市场 token holder 与 signer 不同时填写；None 表示使用 signer 地址。"
            ),
            order: 1100,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.redeem.neg_risk.route",
            label: ui_text!(
                en = "Neg-risk market redeem route",
                zh = "Neg-risk 赎回路由"
            ),
            help: ui_text!(
                en = "Route used for neg-risk markets. Default: `neg_risk_legacy_adapter`.",
                zh = "Neg-risk 市场使用的赎回路由。默认：neg_risk_legacy_adapter。"
            ),
            order: 1110,
            widget: Some(FieldWidget::EnumSelect),
            semantics: None,
            visible: true,
        },
    ]
}

/// Fields: `settlement.redeem.standard.holder_address … settlement.redeem.standard.route`.
fn build_fields_settlement_redeem_standard() -> Vec<FieldUiEntry> {
    vec![
        FieldUiEntry {
            path: "settlement.redeem.standard.holder_address",
            label: ui_text!(
                en = "Standard market token holder",
                zh = "普通市场 Holder 地址"
            ),
            help: ui_text!(
                en = "Token holder for standard markets when it differs from the signer. `None` uses the signer address.",
                zh = "普通市场 token holder 与 signer 不同时填写；None 表示使用 signer 地址。"
            ),
            order: 1130,
            widget: None,
            semantics: None,
            visible: true,
        },
        FieldUiEntry {
            path: "settlement.redeem.standard.route",
            label: ui_text!(en = "Standard market redeem route", zh = "普通市场赎回路由"),
            help: ui_text!(
                en = "Route used for standard (non-neg-risk) markets. Default: `standard_ctf`.",
                zh = "普通（非 neg-risk）市场使用的赎回路由。默认：standard_ctf。"
            ),
            order: 1140,
            widget: Some(FieldWidget::EnumSelect),
            semantics: None,
            visible: true,
        },
    ]
}
