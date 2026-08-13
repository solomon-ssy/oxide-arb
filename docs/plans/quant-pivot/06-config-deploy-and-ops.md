# 06 — Config、Deploy 与 Lifecycle 治理

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目从未生产运行；唯一 bootstrap snapshot 直接定义终态 schema，不迁移、
>   归档或转换任何旧 runtime/report 数据。
> - `schema_data_version_impact`: 六类 Runtime Config 只接受 clean-install schema `1`；该 discriminator
>   仅校验持久化完整性，不提供多版本分派。Deploy Config 为 required single-file contract。
> - `pre_deployment_behavior`: 六资源 bundle 不完整时仅控制面可启动，report scheduler 与 execution
>   admission fail closed。
> - `post_deployment_behavior`: 不接受旧 resource kind、旧 policy payload、默认补值、overlay 或环境变量
>   override。
> - `rollback_and_data_verification`: 本次实施只验证空数据库 fresh boot；不创建 upgrade/downgrade migration，
>   也不自动重置任何真实数据库。

> 状态：Runtime/Deploy Config 唯一权威目标设计。
>
> 详细实施与字段审计：
> [`quant-pivot-global-portfolio-runtime-deploy-config-ui-ux-closure-plan.md`](../../codex-plans/quant-pivot-global-portfolio-runtime-deploy-config-ui-ux-closure-plan.md)、
> [`quant-pivot-current-config-field-inventory.md`](../../audit/quant-pivot-current-config-field-inventory.md)。

## 0. 所有权与单一来源

配置只允许四种 owner：

1. Runtime Policy：会改变后续业务决策、需要热更新、审批、审计与原子生效。
2. Immutable Artifact / Job Spec：影响研究、训练、校准、场景或 replay 的方法定义。
3. Deploy Config：进程构造、外部绑定、host capacity、credential；仅重启生效。
4. Code Constant：外部协议事实、数学不变量和不可放大的安全上限。

每个字段只能有一个 owner。Runtime descriptor 与 Deploy descriptor 分别是 schema、metadata、validation、
UI/TOML rendering、consumer inventory 和 audit 的单一来源。禁止人工维护不受 CI 约束的影子 schema。

<a id="runtime-config-contract"></a>

## 1. 六类 Runtime Policy

唯一 resource kind：

| Resource kind | Rust document | Consumer | Atomic apply boundary |
|---|---|---|---|
| `recommendation_policy` | `RecommendationPolicy` | selector、data-quality gate、report coordinator | 新 claim report run |
| `execution_risk_policy` | `ExecutionRiskPolicy` | global planner、intent builder、admission、execution workers | 新 plan/intent/admission |
| `model_routing` | `ModelRouting` | represented-route readiness、serving generation | 新 report/evaluation run |
| `report_schedule` | `ReportSchedule` | durable scheduler | 尚未 claim 的 future run |
| `operations_policy` | `OperationsPolicy` | runtime admission、notification、worker supervisors | 下一次受控 admission |
| `execution_automation_policy` | `ExecutionAutomationPolicy` | runtime-mode gate、auto execution preflight | preflight 后的下一次 admission |

删除 `operational_control`、`execution_authorization` 和全部别名/parser/DTO/UI mapping。HTTP namespace 可保留
现有版本，但 wire contract 只接受 target shape。

每个 resource document 使用唯一 `schema_version = 1`，revision immutable。该字段是 content-integrity
invariant，不是向前/向后兼容开关；任何其他值直接拒绝且不存在 converter。validate、preflight、approval、CAS
activation、snapshot、audit/outbox 与 ArcSwap publication 保持一个事务/一个 committed bundle identity。
同一操作者可在具有不同权限时分别执行 create/approve/activate，但三个动作必须独立请求和留痕。

### 1.1 RecommendationPolicy

- `selection.enabled_categories = []` 在所有 consumer 中表示全部受支持分类。
- selection 只定义市场 eligibility，不决定 report partition 或单 Route。
- data-quality、TopN、knowledge lag、delivery/expiry 由新 report run 冻结。
- 删除 fallback horizon；每个 Route serving contract 必须提供兼容 horizon/time buckets。

### 1.2 ExecutionRiskPolicy

`portfolio` 只包含：

- `budget`：total budget、cash reserve、maximum open capital。
- `exposure_limits`：single recommendation、market、event、category、Route、open recommendation count。
- `tail_risk`：CVaR confidence/cap、maximum scenario loss、drawdown、capital time buckets/bucket caps。
- `admission`：minimum nominal/robust net USD、profit probability lower bound、maximum probability interval
  width、liquidity buffer。

entry/exit/reconciliation/breaker 可继续作为同一 risk document 的命名 section。删除 sizing model、Kelly、
confidence curve、drawdown sizing multiplier、correlation estimator 和 optimizer/backend/weight config。
所有 risk value required；零值具有普通数值语义，禁止 `0 = unlimited`。无上限必须使用显式 enum/newtype，
但资金/损失 hard cap 在 production 不允许 unlimited。

### 1.3 ModelRouting

每个 Buy Route 绑定 Champion、optional Shadow 和完整 serving source。`ModelRouting` 只绑定长期的
`PortfolioScenarioModelArtifactBinding`：ordered represented Route set、per-route serving/calibration/Trade
Policy contract、scenario-generation schema、capital time buckets 和 artifact hash。该 artifact 封存 PIT
residual/dependence、calibration uncertainty、stress catalog、bootstrap/ambiguity-set 与 discount contract，
但绝不枚举未来具体 market/token。promotion 改变兼容 digest 时，champion 与新的 scenario-model binding
必须在同一治理事务中原子切换；rollback 也必须同时恢复两者。每次 report 的 concrete
`PortfolioScenarioArtifact` 由冻结市场/L2/candidate 输入现场生成并归属 report，不进入 Runtime Config。

### 1.4 Operations / automation

`OperationsPolicy` 持有 report pause、execution halt、notification routing、worker admission、entry-condition
与 reconciliation operational controls。`ExecutionAutomationPolicy` 持有 SemiAuto approval TTL 和
AutoExecution 的经济/订单硬上限，不使用 raw score/confidence threshold；自动执行只能消费已发布报告的
`RecommendationEconomics` 与 frozen risk lineage。

## 2. Runtime field descriptor 与 validation contract

每个 leaf descriptor 必须提供：

```text
RFC 6901 pointer / title / description / unit / format / required /
example / bounds / enum / UI control / group / order / risk /
apply effect / readonly / writeonly / visible-when / docs link / consumer
```

schema 生成器、API contract 和 generated TypeScript 都从该 descriptor/Rust document 产生。validation issue：

```rust
pub struct PolicyValidationIssue {
    pub pointer: JsonPointer,
    pub code: PolicyValidationCode,
    pub severity: PolicyValidationSeverity,
    pub message_parameters: ValidationMessageParameters,
    pub remediation: Option<PolicyRemediation>,
}
```

禁止点分路径、自由文本-only error 或前端二次猜测字段。每个 editable pointer 在 domain editor 中恰好有
一个 `data-config-pointer`；readonly lineage 以 definition list/card 展示，不使用 disabled form control。

## 3. Clean bootstrap 与 capability gate

- 唯一 bootstrap snapshot 直接创建终态表、enum、constraint 和 index；仓库不存在旧 policy payload、archive
  table、upgrade migration、converter、dual read/write 或 resource-kind alias。
- fresh boot 不伪造 active policy。六类资源必须由操作者按终态 contract 显式 create、validate、approve、activate。
- risk/scenario 字段没有安全推导值，必须由操作者显式填写。
- 六个 active revision 与完整 scenario-model binding 未就绪时，Config/auth/health 控制面可启动；report scheduler、
  research promotion 的 production publication 和 execution admission fail closed。

## 4. Deploy Config descriptor

`DeployConfigDescriptor` 与 Rust serde tree 同源，覆盖全部静态 leaf、dynamic map element contract 和 tagged
enum variant。每个 descriptor 必须记录：

```text
path / Rust owner / type / required / profile value or placeholder / unit /
constraints / sensitivity / consumer / restart impact / safe projection /
cross-field rules / Purpose comment
```

所有字段均 `deny_unknown_fields` 且 parse-required；禁止 struct-level `serde(default)` 补齐文件缺失值。
Rust `Default` 仅可服务显式测试 builder，不能参与 production deserialization。

## 5. TOML generation

以下文件从 descriptor 确定性生成：

- `config/quant-pivot.toml`：完整 development profile/template。
- `config/quant-pivot.production.example.toml`：完整 production example。

```bash
cargo xtask config render
cargo xtask config render --check
cargo xtask config audit
```

每个静态 leaf 在两份文件中各出现一次。optional field 使用 commented assignment；tagged union 每个 variant
给出完整互斥示例；dynamic map 每个受支持 binding 给出 canonical example。

每个 field comment 必须同时包含 Purpose、Required/Optional、Type/Unit、Constraints、Recommended value、
Operational impact、Restart、Sensitivity 和 cross-field dependency。comment 缺项、path 重复/遗漏、render
diff、consumer/projection 缺失均令 CI 失败。

## 6. Single-file secure loader

删除：

- `--config-dir` 与 `QUANT_PIVOT_CONFIG_DIR`；
- optional `quant-pivot.toml`；
- `quant-pivot.local.toml` overlay；
- compiled-default merge；
- 任意环境变量或 CLI leaf override。

binary 与所有 schema/reset/smoke xtask 使用必填 absolute `--config-file` 和必填 expected environment。
`DeployConfig::load(DeployConfigLoadRequest)`：

1. race-safe no-follow open；
2. 从同一 fd 验证 absolute path、regular file、owner/mode；
3. 读取并 strict deserialize required tree；
4. 验证 file environment 与 expected environment；
5. 执行 semantic、placeholder、secret 和 cross-field validation；
6. 只返回 fully validated `DeployConfig`。

production 文件由 effective user 拥有且 mode 为 0400/0600，拒绝 group/world bits、symlink、placeholder、
empty secret 和 environment mismatch。tracked development template 可以是 0644，但不能含真实 secret，也不
能作为需要 credential capability 的启动文件；真实 local config 必须 gitignored + 0600。

secret 继续使用 plaintext TOML + `SecretText`：zeroizing storage、固定 redacted Debug、无 Serialize/
Display、日志/错误不得包含原值。程序不自动读取、复制或生成真实 credential。

## 7. Deployment safe projection

Deployment API 从 descriptor 穷尽投影，返回每个 path 的 apply boundary、configured/validation state 和安全
value representation：

- public：可返回 normalized value；
- sensitive endpoint/identity：返回类型、configured/health，不返回 literal host/URL/user/address；
- secret：只返回 configured/missing/invalid，不返回长度、prefix、hash 或 fingerprint。

禁止直接 Serialize/Debug `DeployConfig`，禁止手写遗漏字段的 projection switch；descriptor coverage test
保证全部 target path 恰好投影一次。

## 8. Config UI/UX

六个 domain-specific typed editor 分别拥有 Recommendation、Execution Risk、Model Routing、Report
Schedule、Operations、Execution Automation。共享 Money/Probability/Duration/Artifact controls 可以复用，
递归 generic editor 不拥有 production edit path。

工作区必须提供 Current/Draft 分栏、unit/risk/apply effect、inline + Error Summary、pointer focus、semantic
diff、preflight、approve、activate、rollback、stale CAS。live runtime controls 与 revisioned policy 视觉隔离。
Deployment 页面消费 safe projection；不展示 raw JSON/config。

## 9. SeaORM、fresh boot 与 rollback

- resource kind 使用 target `ActiveEnum`；document 使用封闭 typed JSONB，不用 `serde_json::Value` 逃避建模。
- revision、approval、activation、snapshot、audit/outbox 事务与 global bundle generation 保持原子。
- schema migration、fresh boot snapshot、entities、repository 和 API contract 同步更新。
- 应用启动只验证 schema，不自动 DDL；实际 reset/destructive operation 仍需单独授权。
- clean-break rollback 只恢复数据库备份与对应 build，不提供 application compatibility bridge。

## 10. 验收

- Runtime descriptor、API schema、generated client、six editors、consumer 和 apply boundary 双向相等。
- Deploy descriptor、两份 TOML、strict parser、consumer 和 safe projection 双向相等。
- missing/unknown/default/overlay/env/symlink/owner/mode/placeholder/secret-redaction tests 全部通过。
- governance validate/preflight/approval/CAS activation/rollback/restart reconciliation 只使用唯一终态 kind 和 shape。
- Playwright 覆盖六资源、deployment、live controls、validation/CAS/recovery、四 viewport、light/dark、zh-CN、
  reduced motion、axe、keyboard 和 visual assertions；连续两个 fresh-stack run 通过。
