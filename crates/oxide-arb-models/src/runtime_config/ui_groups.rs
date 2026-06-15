//! Preferences group metadata for runtime-config sections.

use std::sync::OnceLock;

use crate::ui_text;

use super::ui_registry::GroupUiEntry;

/// Registered preferences groups.
#[must_use]
pub fn groups() -> &'static [GroupUiEntry] {
    static GROUPS: OnceLock<Vec<GroupUiEntry>> = OnceLock::new();
    GROUPS.get_or_init(|| {
        vec![
            GroupUiEntry {
                id: "market_data",
                order: 10,
                label: ui_text!(en = "Market data", zh = "市场数据"),
                description: ui_text!(
                    en = "Book staleness ladder and tradeable universe filters.",
                    zh = "订单簿新鲜度阶梯与可交易集合过滤。"
                ),
            },
            GroupUiEntry {
                id: "detection",
                order: 20,
                label: ui_text!(en = "Detection", zh = "检测"),
                description: ui_text!(
                    en = "Endgame detection, calibration, and scoring tunables.",
                    zh = "终局检测、校准与评分参数。"
                ),
            },
            GroupUiEntry {
                id: "execution",
                order: 30,
                label: ui_text!(en = "Execution", zh = "执行"),
                description: ui_text!(
                    en = "Operational execution timeouts, funnel, and latency controls.",
                    zh = "执行超时、漏斗与延迟控制。"
                ),
            },
            GroupUiEntry {
                id: "risk",
                order: 40,
                label: ui_text!(en = "Risk", zh = "风控"),
                description: ui_text!(
                    en = "Capital limits, circuit breaker, sizing, and exposure controls.",
                    zh = "资金限额、熔断、仓位与敞口控制。"
                ),
            },
            GroupUiEntry {
                id: "settlement",
                order: 50,
                label: ui_text!(en = "Settlement", zh = "结算"),
                description: ui_text!(
                    en = "Oracle policy, lifecycle retries, and on-chain redeem route.",
                    zh = "预言机策略、生命周期重试与链上赎回路径。"
                ),
            },
            GroupUiEntry {
                id: "notification",
                order: 60,
                label: ui_text!(en = "Notification", zh = "通知"),
                description: ui_text!(
                    en = "Operator alert channels (Telegram and webhook).",
                    zh = "运营告警通道（Telegram 与 Webhook）。"
                ),
            },
            GroupUiEntry {
                id: "schema_version",
                order: 0,
                label: ui_text!(en = "Schema", zh = "Schema"),
                description: ui_text!(
                    en = "Internal document version (not editable in preferences).",
                    zh = "内部文档版本（偏好设置中不可编辑）。"
                ),
            },
        ]
    })
}
