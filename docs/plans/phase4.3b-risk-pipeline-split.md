# Phase 4.3b — Risk Pipeline Auto-Split (`requires_metrics`)

> **状态**: 待实施
>
> **前置**: 无（可独立执行）
>
> **影响 crate**: `oxide-arb-risk`

---

## 问题

`oxide-arb-risk/src/engine.rs` L61 硬编码 `const PHASE1_GATE_COUNT: usize = 4`。如果 pipeline 的 check 顺序变化，这个常量会 silently break — phase-1（pre-metrics）和 phase-2（需要 metrics）的分界线错位，可能在 breaker open 时仍去加载 metrics，或者反过来在不该短路时短路。

---

## 方案

### Step 1: 扩展 `RiskCheck` trait

**文件**: `oxide-arb-risk/src/pipeline/mod.rs`

```rust
pub trait RiskCheck: Send + Sync {
    fn id(&self) -> RiskCheckId;
    fn kind(&self) -> RiskCheckKind;
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult;

    /// Whether this check requires live `RiskMetricsSnapshot` data.
    /// Checks that only read from the `RiskSnapshot` (ArcSwap) return `false`.
    /// Default: `true` (most checks need metrics).
    fn requires_metrics(&self) -> bool { true }
}
```

### Step 2: Override 不需要 metrics 的 checks

**文件**: `oxide-arb-risk/src/pipeline/checks.rs`

以下 4 个 check 只读 `RiskSnapshot`（ArcSwap 发布的不可变快照），不需要 live metrics：

| Check | 数据源 | `requires_metrics()` |
|-------|--------|---------------------|
| `ManualHaltCheck` | `snap.circuit_breaker.manual_halt` | `false` |
| `CircuitBreakerCheck` | `snap.circuit_breaker.circuit_breaker` | `false` |
| `BlacklistCheck` | `snap.blacklist` (bloom) | `false` |
| `TokenBlacklistCheck` | `snap.blacklist` (bloom) | `false` |

每个 impl 添加：

```rust
fn requires_metrics(&self) -> bool { false }
```

其余 20 个 check 使用默认 `true` — 不需要改动。

### Step 3: `StaticRiskPipeline` 计算 split index

**文件**: `oxide-arb-risk/src/pipeline/mod.rs`

在 `StaticRiskPipeline` 中增加缓存字段和方法：

```rust
pub struct StaticRiskPipeline {
    // ... existing 24 check fields ...
    metrics_split: usize,  // NEW: 缓存的分割点
}

impl StaticRiskPipeline {
    /// Index of the first check that requires live metrics.
    #[must_use]
    pub const fn metrics_split_index(&self) -> usize {
        self.metrics_split
    }
}
```

在 `build_default_pipeline` 中计算：

```rust
pub fn build_default_pipeline(config: &RiskConfig) -> StaticRiskPipeline {
    let pipeline = StaticRiskPipeline {
        manual_halt: ManualHaltCheck,
        // ... all checks ...
        metrics_split: 0,  // placeholder
    };

    // 计算 split index: 遍历 check_order，找第一个 requires_metrics == true
    let split = pipeline.check_order().iter().position(|id| {
        match id {
            RiskCheckId::ManualHalt => ManualHaltCheck.requires_metrics(),
            RiskCheckId::CircuitBreaker => CircuitBreakerCheck.requires_metrics(),
            RiskCheckId::BlacklistTradingPath => BlacklistCheck.requires_metrics(),
            RiskCheckId::TokenBlacklist => TokenBlacklistCheck.requires_metrics(),
            _ => true, // all others require metrics
        }
    }).unwrap_or(pipeline.len());

    StaticRiskPipeline { metrics_split: split, ..pipeline }
}
```

注意：由于 `build_default_pipeline` 目前是 `const fn`，加了 split 计算后需要移除 `const` 限定符（`position` 不是 const）。

### Step 4: 替换 `PHASE1_GATE_COUNT`

**文件**: `oxide-arb-risk/src/engine.rs`

**删除**: L61 `const PHASE1_GATE_COUNT: usize = 4;`

**替换** L123, L136 的两处引用：

```rust
// Before:
.evaluate_range(&phase1_ctx, mode, 0, PHASE1_GATE_COUNT);
// After:
.evaluate_range(&phase1_ctx, mode, 0, self.pipeline.metrics_split_index());

// Before:
PHASE1_GATE_COUNT, self.pipeline.len(),
// After:
self.pipeline.metrics_split_index(), self.pipeline.len(),
```

---

## 测试

`oxide-arb-risk/tests/pipeline_tests.rs` 增加 golden test：

```rust
#[test]
fn metrics_split_index_matches_check_order() {
    let config = RiskConfig::default();
    let pipeline = build_default_pipeline(&config);

    // Split 应该在第 4 个 check (0-indexed = 4)
    assert_eq!(pipeline.metrics_split_index(), 4);

    // 前 4 个不需要 metrics
    let order = pipeline.check_order();
    assert!(!ManualHaltCheck.requires_metrics());
    assert!(!CircuitBreakerCheck.requires_metrics());
    assert!(!BlacklistCheck.requires_metrics());
    assert!(!TokenBlacklistCheck.requires_metrics());

    // 第 5 个开始需要 metrics
    // (MinDepthCheck reads opportunity.depth_used_pct — from context, not metrics,
    //  but it also reads metrics for available depth inference, so requires_metrics = true)
}
```
