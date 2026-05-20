# Phase 1 关键架构决策

> 请在每个问题下方的 **【你的选择】** 处填写你的选项字母（如 A/B/C/D），可附加说明。
> 填完后通知我即可。

---

## Q1: SDK 选择

发现有两个 Polymarket Rust SDK 可用：

| 选项 | 包名 | 版本 | 说明 |
|------|------|------|------|
| A | `polymarket_client_sdk_v2` | v0.5.1 (stable) | 官方 Polymarket/rs-clob-client-v2 仓库，2026-04-24 发布 |
| B | `polymarket_client_sdk_v2` | v0.6.0-canary.1 | 最新 canary，2026-05-01 发布 |
| C | `polymarket-client-sdk` | v0.4.4 | 旧版（v1），维护较少 |
| D | 不用第三方 SDK | — | 全部自己实现（最大控制权但工作量巨大） |

v2 SDK 已内置：CLOB REST、WebSocket streaming、Gamma API、CTF 合约交互、EIP-712 签名、heartbeats。模块化 feature flags。

**【你的选择】**：
A

---

## Q2: WebSocket 架构

v2 SDK 已内置 WebSocket 客户端。我们的 oxide-arb-api 需要在其上层构建分片管理（sharding）、自动重连、统一事件总线。

| 选项 | 方案 | 说明 |
|------|------|------|
| A | 包装 SDK 的 WebSocket | 在上层加分片/重连/事件归一化逻辑 |
| B | 绕过 SDK WebSocket | 直接用 tokio-tungstenite 从头实现（更多控制权，但需要自己处理认证帧） |
| C | 混合 | 用 SDK 处理认证和消息类型解析，但自己管理连接生命周期（推荐） |

**【你的选择】**：
A

---

## Q3: Fee 和 Oracle 实现策略

原 plan 中 FeeService 和 OracleSource 都用了 trait。ADR-001 说：
- FeeService 应为**具体 struct**（只有 Polymarket 一个实现，trait 是多余间接层）
- OracleSource **保留 trait**（多数据源投票需要多态，且用于 mock 测试）

| 选项 | 方案 |
|------|------|
| A | 同意 ADR-001：FeeService 是具体 struct，OracleSource 保持 trait（2-of-3 投票） |
| B | 全部具体 struct，连 OracleSource 也不用 trait（减少间接层） |
| C | 全部保持 trait（即使只有一个实现，为了可测试性/mock 注入） |

**【你的选择】**：
A

---

## Q4: 错误处理策略

oxide-arb-error 已有 `OxideError`。oxide-arb-api 内部应该如何处理 SDK 抛出的错误？

| 选项 | 方案 | 说明 |
|------|------|------|
| A | 薄包装 ApiError | `ApiError` 枚举包装 SDK 错误，`impl From<ApiError> for OxideError`（plan 中的方案） |
| B | 直接用 OxideError | 不引入 ApiError 中间层，所有错误直接转为 OxideError |
| C | 丰富的 ApiError（推荐） | 含重试信息、rate-limit 元数据（retry_after_ms、retryable 标记），仅在 crate 边界转换为 OxideError |

选项 C 的优势：crate 内部可以基于 `ApiError::is_retryable()` 和 `ApiError::retry_after_ms()` 做智能重试，上游只看到简化的 `OxideError`。

**【你的选择】**：
你深度评估后来拍板最佳实践。但是我希望oxide-arb-error的OxideError是一个全局的大Error，所有的错误统一在oxide-arb-error进行管理和定义，并且OxideError目前大部分都是String，我认为应该拆成强类型的、语义明确切准确的Error，用'子error.rs'这种来拆，用上From/Into的代码范式，你可以深度阅读一下类似/Users/eason/code/personal/ng-gateway/ng-gateway-error

---

## Q5: alloy（以太坊库）版本策略

v2 SDK 依赖 `alloy ^1.6.3`。这会是我们 workspace 的首次引入 alloy。alloy 不仅用于 keystore/签名，还用于 CTF 合约调用（oracle on-chain 查询 getPayouts）。

| 选项 | 方案 | 说明 |
|------|------|------|
| A | 使用 SDK re-export 的 alloy 类型 | 不在 workspace 单独声明 alloy 依赖，通过 SDK 的 re-export 使用 |
| B | 在 workspace 显式声明 alloy 依赖（推荐） | 与 SDK 版本一致（^1.6），用于 oracle 等需要直接 alloy 的地方 |

选项 B 的优势：oracle 的 CTF 合约调用需要直接使用 alloy 的 Provider、Contract、sol! 宏等，仅通过 SDK re-export 可能不够用。

**【你的选择】**：
B

---

## Q6: 额外问题（可选回答）

1. **是否需要在 Phase 1 就实现 UMA Oracle Source？** 还是先用 Gamma + CTF 两个源做 2-of-2，UMA 留到后续 Phase？
先用 Gamma + CTF 两个源做 2-of-2

2. **对 `reqwest` 版本有偏好吗？** SDK 用 `reqwest ^0.13.2`，这要求我们也用 0.13+（比较新）。
没偏好，直接用最新的就好

3. **是否需要我在实现时同步创建 `config/oxide-arb.toml` 示例配置文件？** Phase 0 计划了但还没创建。
需要

**【你的回答】**（自由回答即可）：
你顺便看看Phase 0是否有什么遗漏的点，如果有一并补充在plan中


