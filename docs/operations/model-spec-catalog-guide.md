# ModelSpec、ModelVersion 与 Serving Route 运维指南

> 本文描述当前生产合同。历史上的
> `ModelVersion Candidate → Shadow → Published → Retired` lifecycle、直接 model publish/bind
> API，以及通过通用 Config mutation 改 serving pointer 的流程均已删除，不得继续操作。
>
> Fresh boot、schema 和首份报告的完整步骤见 [runbook §7.5–§8](./runbook.md)；自动反馈闭环见
> [Phase 11.9](../plans/quant-pivot/phase-11/11.9-attribution-feedback-and-auto-retraining.md)。

## 1. 权威对象与唯一真相

```text
ModelSpec（append-only 研究定义）
  └── TrainingDataset（PIT、sealed、purpose/window/profile 固定）
        └── ModelVersion（immutable artifact + serving contract）
              ├── calibration derivation（如需要，生成新的 ModelVersion）
              ├── CPCV path set / backtest report
              ├── feature-parity proof
              ├── quality / explanation evidence
              └── route generation 中的角色：Inactive / Shadow / Champion
```

| 对象 | 回答的问题 | 是否可变 |
|---|---|---|
| `ModelSpec` | 训练什么 family、horizon、input/label contract 与研究假设 | 否；语义变化创建新 spec |
| `TrainingDataset` | 哪个 PIT cohort、窗口、profile、policy snapshot 与 artifact bytes | 否 |
| `ModelVersion` | 哪些模型 bytes、dataset、transform、calibration、feature contract | 否 |
| `ModelCandidateManifest` | feedback candidate 的完整 validation/promotion preimage | 否 |
| `ModelBootstrapManifest` | 空 route 的首个 Champion 所使用的完整 server-derived preimage | 否 |
| `ModelRouting` route generation | 当前具体 route 的 Champion/Shadow 是谁 | 只允许治理事务或 rollback 改变 |

模型没有全局 mutable publication status。`Inactive`、`Shadow`、`Champion` 必须结合具体
`BuyModelRoute` 和 policy bundle generation 派生；同一个 immutable artifact 不能凭一列全局状态宣称
“已发布”。

因子定义仍有自己独立的 Draft/Published 治理状态。不要把因子 lifecycle 与已删除的模型 publication
lifecycle 混为一谈。

## 2. Route 拓扑

| Route | 持久化 pointer | 合法模型 scope | 缺失时行为 |
|---|---|---|---|
| `Pooled Buy` | `active_model_version_id` | `category_scope = null` | 需要 pooled report 时 fail closed |
| `Crypto Buy` | `category_model_pointers.crypto` | `category_scope = crypto` | Crypto report fail closed |
| `Weather Buy` | `category_model_pointers.weather` | `category_scope = weather` | Weather report fail closed |
| `Sell exit` | `active_exit_model_version_id` | `HoldVsExitWeighted` Sell contract | opportunistic Sell 不可用 |

`Crypto` 和 `Weather` 永不 fallback 到 `Pooled Buy`。三条 Buy route 的 profile、domain source、
factor plane、horizon 和 serving contract 不同；缺失 specialist pointer 不能通过 generic model
静默继续。

`shadow_model_version_id` 是 feedback comparison 使用的全局 challenger pointer，但 route promotion
必须同时验证其 exact category/profile/contract，不能把另一个 route 的 artifact 当 challenger。

## 3. 何时创建新 ModelSpec

以下变化必须创建新 spec：

- model family 改变；
- prediction horizon、label horizon 或 target label 改变；
- input contract、feature schema 或 label schema 改变；
- 研究假设的业务含义改变；
- Buy 与 Sell 目标之间切换。

以下变化通常保留同一 spec，并创建新的 dataset/model version：

- 训练窗口推进；
- 在同一输入合同下重估权重或树；
- recipe/trial 参数变化；
- calibration 使用新的独立 cohort；
- 同一假设下的 retraining。

不要为每次训练复制 spec；也不要把语义不同的 Pooled/Crypto/Weather 或 Buy/Sell 模型塞进一个
含糊 spec。

## 4. ModelVersion 不可变合同

一个可进入治理评估的 Buy `ModelVersion` 至少要精确绑定：

- `model_spec_id` 与 definition hash；
- training dataset ID、dataset manifest/artifact hash；
- model artifact bytes hash；
- feature schema、input contract 与 transform hash；
- decision-policy snapshot/profile；
- family、horizon 与 `category_scope`；
- production runtime 可加载的 payload；
- 需要 calibration 的 family 对应的 immutable calibration binding。

任何 binding 改变都必须创建新的 `ModelVersion`。不得回填 artifact、重绑 calibration、重绑 CPCV
path set，或修改旧 version 使其“看起来通过”。

当前首个 Champion bootstrap 只接受 canonical runtime 可执行的
`WeightedFactor` 或 `ClassicalGradientBoostedTrees` Buy artifact，并要求 calibration。Sell
`HoldVsExitWeighted` 不属于 Buy bootstrap。

## 5. Fresh boot：建立首个 Champion

Fresh PostgreSQL 的 canonical policy bundle 没有 serving pointer。首个 route 不能靠旧 publish API，
也不能靠 runtime 自动挑一个模型。

### 5.1 前置证据

按顺序完成：

1. 创建 ModelSpec，并冻结匹配的 training dataset。
2. 训练 immutable model artifact。
3. 若 family 需要 calibration，在独立 purged/embargoed cohort 上 fit；派生并保存新的 calibrated
   `ModelVersion`。
4. 对该 calibrated version 运行 CPCV；path set 与对应 `ModelRun` 原子提交。
5. 对同一当前 decision-policy snapshot 运行 frozen backtest。
6. 完成 subject-bound full feature parity，且全局 parity latch 为 `Clear`。
7. `GET /api/research/models/{id}/quality-gate` 必须返回通过的 Candidate gate。
8. serving contract 必须通过 explanation efficiency 与 runtime load 校验。

DSR/PBO、rank IC、coverage、calibration、feature parity、explainability 等结果由唯一
`ModelQualityGate` 计算。客户端不能自行拼一个“通过”结论。

### 5.2 Bootstrap mutation

唯一入口：

```text
POST /api/research/model-route-bootstraps
```

请求只表达操作者意图：

```json
{
  "model_version_id": "<candidate>",
  "expected_policy_generation": 1,
  "expected_runtime_control_revision": 0,
  "idempotency_key": "<uuid-or-governed-key>",
  "reason_code": "first_champion_bootstrap",
  "note": "Establish the first governed Champion for this empty Buy route"
}
```

服务端从模型推导 route：

- `category_scope = null` → `Pooled Buy`
- `category_scope = crypto` → `Crypto Buy`
- `category_scope = weather` → `Weather Buy`

请求不能提交 route、profile、model family、path set、backtest、gate hash 或 parity hash。服务端使用
PostgreSQL clock 和当前 durable policy/runtime state 重算全部证据。

Bootstrap 只在 `ReportOnly`、目标 route 为空、candidate 未被其他 route 引用时成立。事务原子写入：

- 新 ModelRouting revision；
- approval 与完整 policy snapshot；
- policy activation；
- typed model-governance audit；
- activation outbox；
- content-addressed bootstrap transaction record。

任一写入失败整笔回滚。相同 idempotency key + 相同意图返回 exact replay；同 key 的任何语义漂移
返回 conflict。

成功 receipt 必须展示 route、before/after generation、model、activation/audit/outbox IDs、
transaction hash、server timestamp 和 actor lineage。该动作不会改变 execution mode、capital、
signing authority 或 funder authority。

## 6. 后续 retraining 与 Champion 变更

目标 route 已有 Champion 后，bootstrap 必须拒绝。后续变更只能走 feedback DAG：

```text
Trigger → TruthFreeze → Coverage → AttributionPlan → Drift → DatasetSeal
→ Training → Calibration → CPCV → Validation → Comparison → Shadow → Decision
```

Scheduler 或 manual trigger 最多自动运行到 `CandidateReady`；不会自动签 permit 或切 route。

治理动作是两个独立请求：

```text
POST /api/research/model-route-activation-permits
POST /api/research/model-route-activations
```

Permit request 只包含：

```json
{
  "feedback_cycle_id": "<cycle>",
  "ttl_secs": 1800,
  "idempotency_key": "<key>",
  "reason_code": "candidate_approved",
  "note": "Approve the sealed CandidateReady evidence"
}
```

TTL 范围为 5–60 分钟，默认 30 分钟，按 PostgreSQL clock。candidate、route、runtime mode、base
revision、manifest 和 aggregate gate hash 全由服务端推导。

Activation request 只包含 permit/cycle/CAS 和操作者意图：

```json
{
  "promotion_permit_id": "<permit>",
  "feedback_cycle_id": "<cycle>",
  "expected_policy_generation": 7,
  "expected_runtime_control_revision": 3,
  "idempotency_key": "<key>",
  "reason_code": "activate_candidate",
  "note": "Activate the exact permit-bound candidate"
}
```

事务必须重新验证 permit 未过期/撤销、CandidateReady evidence、manifest/gate hash、route/runtime
revision、protected execution/capital authority 与 current serving generation。任何漂移原子拒绝。

Permit 可通过以下入口撤销：

```text
POST /api/research/model-route-activation-permits/{permit_id}/revoke
```

## 7. Rollback

Rollback 复用现有 Config revision rollback，目标是一个已经存在、经过审计的旧 ModelRouting
revision。没有第二套 model rollback API。

操作前确认：

- 目标 revision 的 artifact bytes 和 serving contract 仍可加载；
- route scope 与当前 selection/profile 匹配；
- protected execution/capital authority 不会改变；
- approval、preflight token、expected active revision 和 idempotency key 都是当前值。

Rollback 成功后以返回的 durable generation 为 authority；runtime apply 失败时不要再次创建一笔
“补偿 publish”，应修复 apply 问题并从已提交 generation 收敛。

## 8. Config UI 边界

Model Routing 页面允许查看当前 pointer、generation、revision diff 与 rollback 历史。Serving pointer
字段不能通过通用 Draft/Approve/Activate 编辑：

- 空 route：从模型详情执行 first-Champion bootstrap；
- 已占用 Crypto/Weather route：从 Feedback workbench 执行 permit → activation；
- 历史恢复：从 Model Routing 执行 revision rollback。

ModelRouting 中与 serving pointer 无关的阈值仍按正常 Config governance 管理。

## 9. Sell exit 当前边界

`active_exit_model_version_id` 是独立 Sell route，不得被 Buy bootstrap 或 Buy feedback promotion
修改。当前 `HoldVsExitWeighted` trainer 明确返回 `OofPredictionsRequired`，Buy-style CPCV service
也会以 `SellOofEstimatorRequired` fail closed。

因此在完整、可审计的 Sell OOF estimator、CPCV、quality gate 和 dedicated route-governance contract
落地前：

- 不得手工填 `active_exit_model_version_id`；
- 不得借 generic Config mutation 绕过；
- opportunistic Sell 保持不可激活；
- ReportOnly 的 Buy 报告不因此获得任何执行、资金或签名权限。

## 10. 已删除接口

以下 endpoint 必须保持不存在；不要添加 alias、wrapper 或 compatibility route：

```text
POST /api/research/models/{id}/publish
POST /api/research/models/{id}/bind-calibration
POST /api/research/models/{id}/bind-publish-path-set
```

Runner 首次 inference 后 best-effort 把模型改为 Shadow 的行为也已删除。

## 11. 常见故障

| 现象 | 含义 | 处理 |
|---|---|---|
| target route already has a champion | 对已占用 route 调用了 bootstrap | 使用 Feedback permit/activation |
| candidate has no immutable training dataset | artifact lineage 不完整 | 从 sealed dataset 重新训练 |
| candidate has no CPCV path set/backtest | current policy 下 validation 不完整 | 对 exact model/current snapshot 重跑 |
| Candidate gate failed | 至少一个 hard gate 未通过 | 查看 quality scorecard；不得 override |
| parity latch is open/uninitialized | serving evidence 不可信 | 完成 newer full replay 与 governed acknowledge |
| stale policy/runtime revision | 操作者基于旧页面提交 | authoritative refresh 后重新确认 |
| permit expired/revoked | PostgreSQL clock 下 authority 已失效 | 重新签发；不得延长旧 permit |
| transaction replay drift | idempotency key 被不同意图复用 | 保留原 key 查 receipt；新意图使用新 key |
| route runtime apply failed after commit | durable generation 已提交，内存未收敛 | 修复 runtime load/apply，并从 committed generation 恢复 |

## 12. 最低验收

在声明 serving 治理可用前至少验证：

- 旧 publish/bind routes 不存在；
- Pooled/Crypto/Weather empty route 都只能由 bootstrap 填充；
- 第二次 bootstrap 被拒绝；
- stale runtime/policy revision 被拒绝；
- outbox 或 audit 中途失败时 revision/snapshot/activation 全回滚；
- exact replay 不新增行，同 key drift 被拒绝；
- CandidateReady→permit→activation 覆盖 expiry/revoke/stale/conflict；
- bootstrap、promotion、rollback 最终都由同一 durable policy generation 决定 serving truth；
- UI 显示 receipt、actor lineage、route diff 与 rollback 深链；
- execution mode、capital、signing/funder authority 在 route change 前后保持不变。
