# Phase 10 — Operator Console 当前实现索引

> 本目录描述已经落地的 canonical UI，不是兼容迁移计划。首次部署采用 fresh boot；旧 IA、旧 URL、旧截图和旧 resource 页面没有运行期入口。

## 1. 文档所有权

| 文档 | 唯一职责 |
|---|---|
| [10.0](10.0-contract-and-deletion-inventory.md) | fresh-boot、路由、权限、删除与防回流契约 |
| [10.1](10.1-frontend-domain-foundation.md) | TypeScript domain/API/store/WS 基础层历史实施细节 |
| [10.2](10.2-navigation-dashboard-markets-account.md) | 5 域导航、Dashboard、Activity、Market Intelligence |
| [10.3](10.3-report-plane.md) | Recommendations workspace 与 report/recommendation 证据 |
| [10.4](10.4-execution-plane.md) | Intent 到 Settlement 的执行闭环 |
| [10.5](10.5-research-and-governance.md) | Research Lab、Learning & Policy、Data Reliability、System |
| [10.6](10.6-hardening.md) | 通用工程硬化历史与底层测试约束 |
| [10.7](10.7-deploy-config-and-preferences.md) | Config schema/activation/deployment 的后端治理细节 |

父级产品架构见 [Operator Console](../10-frontend-refactor.md)。新事实必须写入拥有该语义的文档，不复制到多个阶段形成第二真理。

## 2. Canonical 页面树

```text
Command
├── /dashboard
└── /runtime/activity
Trading
├── /trading/market-intelligence
└── /trading/recommendations
Execution
├── /execution/orders
├── /execution/portfolio
└── /execution/post-trade
Research
├── /research/lab
├── /research/learning-policy
└── /research/data-reliability
System
├── /system/config
└── /system/audit
```

页面内部状态使用 query contract：

- `module`：选择一个 workspace module；未知值被替换为首个 canonical module。
- `entity` + `id`：打开可深链对象；切换 module 时清除对象 query。
- `resource`：仅 System Config 用于六类强类型 Config resource。
- 领域过滤器保留领域名，例如 Activity Center 的 `domain`/`status`。

## 3. 不变量

1. 后端 menu seed、role-menu、Casbin permission 与 workspace component 必须原子一致。
2. 旧路径没有 redirect/alias；旧组件名、死 API、dead locale 与 compatibility export 的 inventory 必须为 0。
3. REST 是权威快照，WS 是失效提示；断线重连从持久 revision cursor 恢复。
4. 所有 mutation 经 governed action；真实资金与执行永远 fail closed。
5. 枚举展示从 Rust schema 生成，未知值不可静默降灰。
6. 视觉交付必须同时覆盖桌面明暗、移动暗色、可访问性、overflow 与 deterministic evidence。

## 4. 质量门

```bash
cd ui
pnpm check:config-api
pnpm check:research-model-api
pnpm check:enum-catalog
pnpm check:e2e-types
pnpm -F @vben/web-antdv-next test
pnpm -F @vben/web-antdv-next typecheck
pnpm lint && pnpm check
pnpm build:antdv-next
pnpm test:e2e:ui-release-closure

cd ..
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```
