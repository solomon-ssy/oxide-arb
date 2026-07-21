# SeaORM 与强类型持久化规范

本文档是 quant-pivot PostgreSQL/SeaORM 持久化的规范性事实源。所有新表、字段、repository、migration 和 phase 计划都必须遵守本文；现有实现与本文冲突时，应在当前 clean-break 阶段直接修正，不保留兼容 parser、dual-write、compatibility view 或 compatibility re-export。作为正式模块 API 的 canonical public barrel 不属于兼容层，可以保留。

## 1. 核心原则

1. 先确定数据的关系语义，再选择数据库类型。不得因为 Rust 类型可序列化就默认存 JSONB，也不得为了“去 JSON”把天然原子文档拆成无意义的小表。
2. 数据库必须表达可由数据库判定的业务不变量。需要 SQL 过滤、排序、索引、唯一约束、FK、聚合或独立生命周期的数据，必须使用具名列或关系表。
3. 只有整体读取、整体校验、整体替换的闭合值对象或 immutable artifact document 才使用 typed JSONB。
4. domain-owned JSONB 必须是具体 Rust struct/enum，并 derive `Serialize`、`Deserialize`、`FromJsonQueryResult`；禁止在 entity、persistence DTO 和 repository 正常路径暴露裸 `Json`/`serde_json::Value`。
5. compute、persistence 和 API 共享同一业务结构时，canonical 类型下沉到 `quant-pivot-models`。禁止复制同构结构后通过 `serde_json::to_value`/`from_value` 转换。
6. 只有未经本系统解释、必须无损保存的外部原始载荷可以使用 `ExternalJsonDocument(serde_json::Value)`。
7. 删除优先于伪强类型：若 JSON 只是复制其他权威表或制品的数据，且无独立消费者或生命周期，应删除字段，而不是再包装一层 typed document。

## 2. PostgreSQL 类型决策顺序

对每个候选 JSON 字段按以下顺序判断；前一个问题命中后不再继续。

| 判断 | 存储方式 | 典型场景 |
|---|---|---|
| 子项有独立 identity、生命周期、FK 或一对多关系？ | 独立表 + typed FK | profile artifact、evidence、outbox item |
| 字段需要过滤、排序、聚合、索引、唯一约束、CAS 或局部更新？ | 具名 typed column；重复子项使用子表 | status、kind、generation、hash、revision |
| key 集合可演进，但 key/value 受项目 registry 约束？ | 固定 envelope + typed key/value；高频查询时改长表 | feature vector 的 feature-name map |
| 字段集合固定，作为一个原子值整体读写，内部字段不承担独立关系语义？ | typed JSONB (`FromJsonQueryResult`) | factor explanation、quality-gate report |
| 是未经解释的外部原始 payload，必须保真以供审计/重放？ | `ExternalJsonDocument` JSONB | catalog/CLOB upstream raw payload |
| 只是可选文本备注，且没有结构化业务语义？ | nullable `TEXT` + 长度/CHECK | operator comment |

“将来可能增加字段”本身不是使用裸 JSON 的理由。闭合 struct 可通过显式 schema-version 和破坏式 migration 演进；可选字段只能表示真实业务可选性，不能作为兼容旧 payload 的后门。

## 3. Typed JSONB 范式

```rust
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FactorExplanation {
    pub headline: String,
    pub drivers: Vec<FactorDriver>,
}
```

Entity 直接使用该类型：

```rust
#[sea_orm(column_type = "JsonBinary")]
pub explanation: FactorExplanation,
```

要求：

- 固定对象使用 `#[serde(deny_unknown_fields)]`，损坏、旧 tag、拼错 key 必须读取失败，不能静默丢弃。
- 多形文档使用内部 tagged enum；tag 必须是封闭 enum variant，禁止自由字符串分派。
- 若表中另有 `kind`/`action`/`status` discriminator，数据库必须有 CHECK 保证 JSON tag 与关系列一致。
- `Option<T>` 只表示列可空；“尚未产生结果”使用 `NULL` 或显式 enum variant，不使用 `{}`。
- JSON 字段名不使用 `_json` 后缀。Rust 类型已经表达它是文档；字段名描述业务语义，例如 `payload`、`detail`、`metrics`。
- repository 不得重复 `serde_json::from_value`。SeaORM row decoding 是唯一反序列化边界，失败映射为 typed persistence error。
- 为每种文档测试 DB round-trip、未知字段、错误 tag、kind/tag mismatch、损坏 JSON 和内容 hash mismatch。

`FromJsonQueryResult` 只提供 SeaORM 的 JSON value conversion。它不会替代关系建模、字段级业务校验、PostgreSQL CHECK、schema version 或 content hash。

### 3.1 固定 struct 不等于必须拆成多列

Rust struct 与 PostgreSQL 列布局是两个不同层次的决策：

- 内部字段需要 SQL 查询、索引、约束、局部更新，或拥有各自生命周期时，struct 只是 domain DTO，数据库仍应拆成具名列/子表；此时不需要 `FromJsonQueryResult`。
- 内部 key 固定、整体写入/读取/校验/哈希、没有单字段查询与局部更新语义时，它是 atomic value object，使用 typed JSONB；此时 entity 字段必须直接使用 derive `FromJsonQueryResult` 的 struct/enum。
- key 是否“未来可能扩展”不能决定 JSONB。项目自有 fixed document 的演进必须通过显式 format version + clean-break migration，不能退化成 `Value` 或随意 optional key。

`ModelSpecThesis` 属于第二类：`summary`、`hypothesis`、`limitations` 是一个不可分割的研究论题，随整个 ModelSpec WORM、共同参与 `definition_hash`，系统不按单字段过滤或局部 patch。因此使用 `ModelSpecThesis` typed JSONB + `FromJsonQueryResult`；可执行字段仍分别使用关系列或独立 typed contract。若未来出现“按 limitation 检索/审批单条 limitation”的真实需求，应迁移到关系模型，而不是对 JSONB 做临时字符串查询。

`quant_model_version.training_objective` 同样是原子 provenance document，但其形态不是一个“可选字段大全”。canonical 类型是 format v1 的 `ModelTrainingObjective`，内部使用 `learning_to_rank`、`classical_pointwise`、`hand_authored` 判别联合；LTR 的固定参数由 `TrainingObjectiveSpec` 表达。SeaORM entity 直接持有该类型，API/TypeScript 使用同构 union，数据库 CHECK 约束 format/tag。不得恢复 `_json` 字段名、`Record<String, Value>` 或前端 key 探测。

`quant_model_version.quality_gate_report` 使用 `Option<QualityGateReport>`：`NULL` 精确表达“尚未评估”，完整 typed document 表达“已评估”。禁止用 `{}` 伪造未评估状态。DB CHECK 必须验证 format v1、数组/boolean/hash 形态，并保证 document subject ID 等于行的 `model_version_id`。

`quant_model_version.metrics` 使用 format v1 的 `ModelVersionMetrics`，其闭合 tag 只有 `learning_to_rank`、`classical_pointwise`、`not_measured`。训练/验证指标作为一个模型版本的不可变结果整体产生、读取和校验，因此适合 typed JSONB；训练目标、metrics tag 必须由 DB CHECK 保持一致。`artifact_lineage` 仅复制跨边界核验所需的内容 hash、serialization format 和 factor input names，不得复制 artifact 中完整 input contract、fitted transform、URI 或任意 diagnostics map。完整模型结构的唯一事实源仍是内容寻址 artifact；UI/API 不提供 `Record<String, Value>` fallback。

`quant_model_run.metrics_json` 不应存在：training 指标的权威来源是 immutable `quant_model_version.metrics`，backtest/CPCV/shadow 均已有 normalized report/comparison，live inference 指标属于 observability/ClickHouse。该字段没有独立事实语义，typed wrapper 只会固化重复数据，因此 clean-break schema 直接删除。

`operation_log.detail` 是一个有意保留的开放文档边界：它是跨领域、非权威、只整体展示的脱敏取证摘要，内部 key 不参与 SQL 查询、业务决策或状态重建。禁止为它制造覆盖全仓 action 的巨大 tagged enum；写入边界必须保证 object shape、大小/深度上限和敏感 key 拒绝规则。各 action 的权威状态仍由 typed domain table / governance audit 保存。

`quant_feature_vector` 的四份文档必须分别建模：`decision_boundary` 是统一 PIT 时间边界，`payload` 是固定 envelope + registry 驱动的 feature-name map，`source_refs` 是 `Vec<EvidenceSourceRef>`，`decision_capture` 是内容哈希承诺的 `DecisionCaptureEvidence`。四者都由系统解释，因此使用 canonical typed JSONB；动态 feature key 不等于允许裸 `Value`。这些类型由 compute/persistence 共享，repository 不做二次 JSON 解码。

### 3.2 当前 JSONB 逐域决策

下表记录当前 runtime entity 的 JSONB 字段族和业务访问模式，不作为需要逐字段同步的机器清单。`cargo xtask architecture check` 直接发现所有 `JsonBinary` entity 字段，拒绝裸 `Json`/`Value`、缺少 `FromJsonQueryResult` 的类型以及 fail-open 的顶层 serde shape；Rust 编译继续证明 entity/DTO 转换，fresh-boot schema verification 和 repository system tests 证明数据库约束与解码行为。这里的“保留”不是因为已经 derive `FromJsonQueryResult`，而是因为生产者、消费者、查询、更新、约束和生命周期共同证明其为原子文档。任何调用方开始按内部字段过滤、局部 patch 或建立 FK 时，必须重新打开该决策。

| 字段族 | 上下文与访问模式 | 决策与取舍 |
|---|---|---|
| `catalog_event_object.payload`、`catalog_market_object.payload`、`catalog_sync_rejection.raw_payload`、`clob_market_info_version.raw_payload` | 外部 API 原始响应；系统不拥有 schema；只用于审计、重放和 parser 修复 | 保留 `ExternalJsonDocument`。这是唯一开放 JSON allow-list；业务判断只能消费 normalized typed 字段。 |
| `clob_market_info_version.tokens_json`、`fee_details_json` | CLOB metadata 的闭合、嵌套、版本化快照；随 market-info version 整体写入，不按子字段查询 | 保留 `ClobTokenSet` / `ClobFeeDetails` typed JSONB。拆 token 子表会把上游单一版本快照变成多行一致性问题，却没有独立生命周期收益。 |
| `decision_policy_snapshot.snapshot`、`policy_revision.document`、`validation_evidence`、`policy_approval.validation_subject`、`policy_profile_artifact.document` | Config candidate/snapshot 与 profile artifact 是 WORM、内容寻址、整体审批/激活/校验的 canonical document；approval 必须冻结审批当时完整 subject | 保留 tagged typed JSONB；`PolicyValidationSubject` 的固定 key 作为原子值整体比较。approval subject 不引用 revision 当前可变 validation 状态，activation 必须逐字段证明其等于当前 validated subject；generation、revision、kind、hash、actor、lifecycle 留在关系列并受 CAS/CHECK/unique 约束。 |
| `research_profile_artifact.spec`、`quant_trade_policy_artifact.payload_json`、`quant_research_readiness_evidence.payload_json` | 不可变研究/交易策略/evidence artifact；完整文档参与内容 hash，variant 由 kind 约束 | 保留 typed tagged JSONB。所有跨 artifact 引用使用 typed FK + hash，禁止复制 profile document。 |
| `quant_model_spec.thesis`、`input_contract`、`training_contract` | immutable model research definition；三者与具名 scalar columns 共同参与 `definition_hash`；训练和推理实际消费 input/training contract | 保留三个职责分离的 typed document。`thesis` 不是含糊 `description`：它保存可证伪 hypothesis 和 limitations；input/target/validation 是可执行契约。当前没有按 thesis 子字段 SQL 检索或单字段审批，拆列无收益。author user/label/role、reason 是需要独立约束与展示的关系属性，必须作为 WORM 行上的具名 typed columns 与 spec 原子提交，不能塞进 JSON 或只依赖 HTTP operation log。 |
| `quant_model_version.score_multiplier_calibration_report`、`metrics`、`training_objective`、`quality_gate_report` | 一次训练版本的 immutable provenance/result；不同模型族形态不同，整体生成和验签 | 保留 tagged typed JSONB；model/dataset/artifact/profile lineage 均为独立 typed FK/column。`NULL` 表示尚未产生，禁止 `{}`。 |
| `quant_calibration_artifact.payload` | 内容寻址 calibration artifact；variant 由 calibration kind 决定 | 保留 typed tagged JSONB + kind/tag CHECK。publication identity/lifecycle 在独立表，不嵌入 payload。 |
| `quant_source_slice.profile_ref`、`manifest_json`、`quant_training_dataset.manifest_json`、`horizons_secs`、`sample_sources`、`coverage_json` | frozen PIT/dataset provenance；manifest 是 artifact 的 exact wire image；coverage 是整体验收报告 | 暂保留 typed JSONB。`profile_ref` 仅作为 hash-bound artifact provenance，不得替代 persisted `research_profile_artifact_id` lineage；若需要反向 profile 查询，必须加 FK projection 并删除重复权威值。纯标量集合会在 native-array 审计中单独判断，不能因“是 Vec”自动使用 JSONB。 |
| `quant_research_job.params_json` | job kind 对应不同 replayable request；job worker 整体读取并 dispatch | 保留 tagged typed JSONB + relational kind/tag CHECK。拆成所有 job kind 的 nullable 列会制造无效组合。 |
| `quant_research_job.progress_json`、`error_json`、`coverage_json` | 当前进度/终态错误/coverage 随 job 行整体 CAS 更新或完成；SQL 只按 job status/kind/lease 查询，从不按内部字段过滤 | 保留 typed JSONB。progress/error 虽固定，但拆列没有查询收益，并会增加 nullable 多列一致性约束；如未来需要按 error code 聚合告警，应把 `error_code` 提升为 ActiveEnum column，而不是在 JSONB 上写 ad-hoc 查询。 |
| `quant_backtest_path_set.sharpe_distribution`、`paths`、`quant_backtest_report.expected_vs_realized`、`category_breakdown`、`report_pnl_simulation`、`quant_model_comparison_report.category_breakdown_diff`、`quant_shadow_comparison.*` | immutable research result/evidence，包含有序路径、分布或按 category 的闭合映射；整体展示/验收 | 保留 typed JSONB；可跨报告聚合的 scalar headline metrics 已是具名列。禁止把结果 maps 退化为 `Map<String, Value>`。 |
| `quant_factor_definition.definition`、`quant_factor_value.explanation` | definition 是内容寻址 evaluator contract；explanation 是一次 factor value 的可读证据 | 保留 typed JSONB。definition hash/family/status/version 为列；用于筛选/排序的 raw/normalized value、confidence、time 已为列。 |
| `quant_feature_vector.decision_boundary`、`payload`、`source_refs`、`decision_capture` | immutable PIT feature evidence；payload key 由 feature registry 控制，其他文档共同形成 capture hash | 保留 typed JSONB。按 feature name 的分析事实进入 ClickHouse/训练 artifact；PostgreSQL ledger 不做 JSON key 查询。 |
| `quant_market_linkage.outcome`、`quant_market_selection.exclusion_summary` | linkage 是 resolved/unresolved tagged result；selection summary 是一次选择批次的聚合解释 | 保留 typed JSONB；linkage status/family/tier/confidence/version/hash 与 market relation 均为列。 |
| `quant_account_snapshot.positions_json`、`exposures_json` | report generation 时冻结的 real-account truth；作为一个 point-in-time aggregate 被 report/hash 引用 | 保留 typed JSONB。它不是当前 position ledger，子项不独立更新；拆表会增加跨多行 snapshot 原子性成本。 |
| `quant_portfolio_plan.risk_budget_json`、`constraints_json`、`rejected_summary`、`optimizer_meta_json` | 一次 optimizer run 的 frozen input/result explanation；整体构建，整体读取 | 保留 typed JSONB；allocated amount/status 等需要 lifecycle/query 的 scalar 已提升为列。 |
| `quant_recommendation.identity`、`market_context`、`trade_plan`、`factor_breakdown`、`evidence_refs`、`execution_eligibility`，以及 `quant_recommendation_report.summary_json` | 发布时冻结的可解释 recommendation/report artifact；后续不得局部改写 | 保留 typed JSONB。market/event/token/profile/report 等可引用 identity 已是 typed FK；文档是发布时快照而不是重复 authority。 |
| `quant_recommendation_attribution.entry_outcome_json`、`exit_outcome_json`、`attribution_json`、`quant_report_data_quality_snapshot.tokens_json` | append-only closed outcome / quality snapshot；整体计算和读取 | 保留 typed JSONB；可聚合的 PnL/return/terminal state 已使用 typed scalar columns。 |
| `quant_order_intent.entry_order_json`、`exit_policy_json`、`latest_reinference_json`、`scale_out_state` | executable projection 与退出状态必须在 row lock/CAS 下整体替换；内部字段组合具有跨字段不变量 | 保留 typed JSONB。拆成 nullable 列会允许半个 order/policy；需要 SQL 调度的 status/time/price 已为列。任何 JSON patch 均禁止。 |
| `quant_execution_order.prepared_order_json`、`quant_reconciliation.evidence_json` | venue submission exact image 与 append-only reconciliation evidence chain | 保留 typed JSONB；venue/order identity、state、amount 和时间为列。exact replay 要求文档原子不变。 |
| `quant_entry_condition_artifact.payload_json`、`quant_entry_condition_instance.truth_json`、`fold_state_json`、`quant_entry_condition_audit.truth_json` | artifact 是 hash-bound tree；truth 是携带 typed unavailable reason 的和类型；fold state 是按实例 row lock 更新的原子状态 | 保留 tagged typed JSONB。`ConditionTruth` 不能无损压成单一 ActiveEnum；拆成多组 nullable reason 列会制造非法组合。lifecycle state/revision/lease/time 为列。 |
| `quant_domain_source_cursor.checkpoint_json` | source family 决定不同 checkpoint shape；cursor CAS 整体前移 | 保留 tagged typed JSONB + source/checkpoint kind binding。event/freshness time 通过 typed methods读取，不写 JSON path SQL。 |
| `quant_domain_source_expectation.affected_market_ids/profile_ids` | 无独立 lifecycle 的同质 typed ID 集合；expectation 可先于 catalog/profile artifact 出现，因此不能强加 FK | 已从 JSONB 改为 `AffectedMarketIds` / `AffectedProfileIds` native PostgreSQL `text[]`，并由 `StrId` macro 生成 array conversion。保持 canonical sort/dedup；不建立会阻断 pre-cursor expectation 的关系表。 |
| `quant_domain_event_outbox.envelope_json`、`quant_entry_condition_evaluation_outbox.event_json` | commit 时冻结、必须 exact replay 到 ClickHouse 的 typed wire image | 保留 typed JSONB。outbox claim/retry/publish state 全部为列；outbox 不从当前业务表重建 payload。 |
| `quant_model_governance_audit.detail`、`operation_log.detail` | 前者是 action-tagged authority audit，后者是跨域非权威、脱敏、受大小/深度/敏感 key 限制的 forensic summary | 分别保留 closed tagged document 与 controlled-open `OperationDetailDocument`。任何业务状态重建都不得依赖 operation detail。 |
| `system_production_baseline.evidence` | irreversible seal 的完整、hash-bound evidence references；只在 seal 时写一次 | 保留 `ProductionSealEvidence` typed JSONB；PG/CH/build/bundle 的可比较 fingerprint 同时在现场验证，不能只信任 document。 |

本账本的直接结论是：固定 key 只排除了裸 `Value`，没有自动决定“列”或“JSONB”。native array、关系表、具名列与 typed JSONB 都可能是正确答案；决定因素是数据库需要承担的查询和不变量。

## 4. 外部原始 JSON 边界

`ExternalJsonDocument` 只允许用于系统不拥有 schema、需要原样保留的 upstream payload。当前允许清单：

- catalog event payload；
- catalog market payload；
- catalog sync rejection payload；
- CLOB market raw payload。

外部 payload 中一旦有字段进入业务判断，就必须在 ingest boundary 解析到 typed normalized columns/document；业务代码不得在 `Value` 上用字符串 key 取值。新增允许项必须更新显式 inventory 和测试，不能使用名称启发式绕过 lint。

## 5. Scalar newtype 与 enum

- 有业务语义的 UUID、String、integer 和 decimal 使用 newtype；ID、hash、correlation id、worker id、artifact id 均不得退化为裸类型。
- SeaORM 支持的 scalar wrapper 优先使用 `DeriveValueType`。newtype 的 `FromStr` 是读取时的强校验边界；格式错误必须失败。
- 数据库有限集合使用 `DeriveActiveEnum`，PostgreSQL native enum 由同一 ActiveEnum spec 生成。仅客户端内部、无需 DB enum 的 string-like enum 才考虑 `DeriveValueType`。
- PostgreSQL native enum 必须使用 SeaORM `rs_type = "Enum"`（本仓由 `pg_enum!` 统一生成），不能使用 `rs_type = "String"`。前者会在 `Value` 中保留 enum type identity，使 prepared scalar/array 参数由 SeaQuery 写出显式 `qp_*` / `qp_*[]` cast；后者的 `Vec<T>` 会退化成 `text[]`，无法写入 native enum array。
- money、price、shares、probability 使用 `rust_decimal` typed newtype，禁止 `f64`。
- DB 列类型与 Rust 类型语义一致：IP 使用 `inet`，时间使用 timestamptz，内容 hash 使用受验证 text newtype，数组只用于无独立 identity 的同质 scalar 集合。

当前项目的边界决策：

- `UserId` 只表示已认证用户主体；system automation 使用 `Option<UserId>::None`，同时保留非空 actor label 快照。不得以 username `String` 替代主体 identity。
- `RoleCode` 是可由 RBAC 数据扩展的稳定 code，因此使用 validated newtype，而不是把当前角色列表冻结成 Rust/PG enum。
- `research_job.result_ref` 是跨多个结果实体 namespace 的多态引用。持久化使用受 lifecycle CHECK 约束的 `result_kind + result_ref` 成对列，domain/API/UI 使用 `{ kind, id }` tagged reference；禁止把 UUID 包装成任一具体结果 ID 来制造虚假单一 identity，也禁止恢复无 discriminator 的 response 字段。
- `SHOW`、catalog、ClickHouse identifier 等 SQL identifier 必须来自封闭 enum/manifest。即使 SQL 是静态管理语句，也不得让 `&str` 从调用方流入格式化位置。

## 6. Entity First 与 boot migration

- runtime dense entity 是常规 table、column、relation、FK 和基础 index 的源。
- 唯一 clean-boot migration 保存 immutable v1 entity time capsule，并使用 `SchemaBuilder::apply`。migration ledger 已保证 migration 只执行一次；初始化路径禁止使用 runtime `SchemaRegistry::sync`。
- ActiveEnum、array-only enum、复杂 CHECK、partial/expression index、WORM trigger 等 Entity First 尚不能表达的对象，通过集中 typed schema spec/SeaQuery 补齐。
- 禁止在 repository 或 runtime startup 中手写 DDL。
- PostgreSQL empty check 覆盖 table、view、materialized view、sequence、type、function 和 trigger；ClickHouse 对象必须与 manifest 精确相等。
- migration snapshot、runtime entity、normalized manifest 三者必须由 CI 做 regenerate-and-diff；禁止手工维护互相漂移的多份 schema owner。

参考 SeaORM 官方的 [Entity First workflow](https://www.sea-ql.org/SeaORM/docs/generate-entity/entity-first/)：`apply` 用于 migration 初始化，`sync` 会查询 live schema，更适合开发期同步而非生产 boot migration。

## 7. Query 与 relation loading

- CRUD、join、aggregate、upsert、subquery 和可表达 DDL 默认使用 SeaORM/SeaQuery。
- 1:1/N:1 projection 使用 `DerivePartialModel` + join；避免先查主表再逐行查关联。
- Nested result 的 SQL aliases 必须与 `#[sea_orm(nested(prefix = "..."))]` 明确一致；无 prefix 的 nested 只允许查询确实返回内层原始列名时使用。禁止为了“以后可能 join”给 DTO 空挂 `FromQueryResult`，实际 join projection 必须有集成测试完成一次 decode。
- 1:N/M:N 使用 `LoaderTrait` 或 SeaORM 2 Entity Loader。它们会批量收集 ID，阻止 N+1；不要用循环内 query 伪装为 repository abstraction。
- 多来源 feed/activity 在数据库中 `UNION ALL + global ORDER BY/LIMIT`，禁止分别查询后内存合并导致分页语义错误。
- 一个 API response 的一致性边界由同一 transaction/snapshot 或单条 statement 保证；禁止混合 DB row、ArcSwap cache 和多个时点的独立查询。
- 关键 repository 必须有 statement-count test/tracing budget。查询次数是契约，而不仅是性能建议。

循环 I/O 必须在评审中归入下列唯一类别：

| 类别 | 允许条件 | 确定性预算 |
|---|---|---|
| `TrueNPlusOne` | 不允许 | 改为 join/Loader/`IN`/batch write 后为固定 statement 数 |
| `BindLimitedBatch` | 仅数据库 bind/packet 上限要求分块 | PostgreSQL 写入为 `ceil(rows / floor(65535 / entity_column_count))`；读取为 `ceil(ids / 65535)` |
| `PerAggregateTransaction` | 每项必须独立持锁、校验所有权、释放资本或留下独立审计 | 调用方必须有 hard batch cap、timeout 和 metrics；不能用该分类掩盖普通 CRUD N+1 |

当前闭环边界：

- Config resources、activity、snapshot options 和 model picker 均由单条 DB-authoritative projection 返回，并有 statement-count 回归测试。
- Casbin policy/role 同步、weather projection/outbox 和 basis-alert cooldown 已从逐行查询/写入改为集合查询与 bind-safe batch write。
- 所有普通多行 insert/upsert/returning 进入 `postgres::write`；唯一直接 `insert_many` 例外是 Casbin `add_policies`，因为其“任一重复则整批回滚”语义依赖单语句 affected-row count，并在调用边界硬性拒绝超过 bind budget 的输入。
- report recommendation 的无副作用状态迁移使用集合 update；intent 终止仍逐 aggregate 执行，因为每个 intent 必须在同一事务中独立更新 condition、capital 和 audit。`reports.max_top_n` 另受绝对 1,000 行 cascade ceiling 约束。
- domain-event 外部投递后的 publish/failure 回写保留逐 event 所有权校验；worker batch 固定为 500。这里的单项失败隔离属于 `PerAggregateTransaction`，不是加载关联的 N+1。

CI 只用 deterministic statement/row/byte budget 判定。p50/p95/p99 通过 tracing 与数据库 query log 留作运行验收证据，不作为易波动的静态失败门槛。

参考官方 [Nested PartialModel](https://www.sea-ql.org/SeaORM/docs/relation/nested-selects/)、[Model Loader](https://www.sea-ql.org/SeaORM/docs/relation/model-loader/) 与 [Entity Loader](https://www.sea-ql.org/SeaORM/docs/relation/entity-loader/)。

## 8. Transaction、CAS 与 idempotency

- 多表业务写入使用显式 SeaORM transaction；commit 前不得发布 ArcSwap、发送非 durable event 或返回成功。
- closure transaction 是默认；需要显式 isolation/access mode、row lock 或复杂 lifetime 时使用 `begin_with_config`，所有退出路径必须 commit 或自动 rollback。
- CAS 同时验证完整业务前置条件：global generation、resource revision、candidate hash、approval/token、idempotency digest。approval 必须持久化审批当时的完整 `PolicyValidationSubject`；只冻结 resource revision hash 会让旧审批在 revision revalidation 后被错误复用。
- idempotency 表保存完整 request digest 和 committed result。同 key/同 digest 返回原结果；同 key/不同 digest 返回 typed conflict。
- audit/outbox 与状态变更同事务写入。进程内 publish 只消费 committed bundle；commit 后崩溃由 durable outbox/reconciler 恢复。
- repository 以 typed outcome enum 区分 inserted、exact replay、stale conflict 等结果，不能依赖字符串匹配 DB error。

参考官方 [Transaction](https://www.sea-ql.org/SeaORM/docs/advanced-query/transaction/)。

## 9. Raw SQL 例外

允许的 native SQL 只存在于 owning crate 的集中 typed dialect module：

- PostgreSQL catalog/admin/advisory lock；
- SeaORM/SeaQuery 无法表达的 ordered-set percentile；
- ClickHouse typed schema/query renderer；
- 明确标注的 test-only corruption case。

`PostgreSQL` raw result 由固定 query shape 和 typed decode 约束；其 catalog、lifecycle 与 migration SQL 必须与 owning function 放在同一模块。`ClickHouse` native reads 通过 `ClickHouseQueryLimits` 设置稳定 `log_comment`，并以 `max_result_rows`、`max_result_bytes`、`result_overflow_mode=throw` 在服务端拒绝越界结果。identifier 必须来自封闭 enum/manifest，不得格式化用户输入字符串。

动态 renderer 的 identifier 来源同样是契约：runtime research-readiness 在插值前要求整个 `ResearchSourceBinding` 与 canonical built-in registry 精确相等；PG/CH lifecycle 只接受通过长度/字符校验并被 dialect quote 的配置标识，或 compiled migration manifest 中的封闭对象名。仅“做了转义”但来源仍开放的字符串不满足该边界。

普通 CRUD、join、aggregate、upsert 和 SeaQuery 可表达的 DDL 不得以“更方便”为由改写成 native SQL。review 必须确认 raw SQL 位于基础设施或 repository owner，参数通过 bind/typed `Statement` 传入，动态 identifier 已按 dialect 校验并 quote；test-only corruption SQL 只允许留在隔离的测试模块。

## 10. Review checklist

- 这个字段为何是列、子表或 JSONB？是否记录了查询/约束/更新语义？
- JSONB 是否为 canonical fixed struct/tagged enum，并 derive `FromJsonQueryResult`？
- 是否存在 `serde_json::Value`、`json!`、`.get("...")` 或 index-by-string 进入持久化业务路径？
- discriminator 是否为 ActiveEnum，且与 document tag 有 DB CHECK？
- ID/hash/method/status 是否为 newtype/enum？
- 能否一次 join/load 完成，是否有 statement-count 断言？
- transaction 是否覆盖状态、audit、outbox、idempotency result？
- migration/entity/manifest/API/TS contract 是否 regenerate-and-diff 一致？
- corruption、unknown field/tag、hash mismatch、CAS race、rollback 是否有测试？

## 11. 官方资料基线

本规范基于当前 workspace 的 SeaORM 2.x 能力和以下官方文档：

- [Column types / custom JSON struct](https://www.sea-ql.org/SeaORM/docs/generate-entity/column-types/)
- [Newtype / DeriveValueType / FromJsonQueryResult](https://www.sea-ql.org/SeaORM/docs/generate-entity/newtype/)
- [ActiveEnum](https://www.sea-ql.org/SeaORM/docs/generate-entity/enumeration/)
- [Entity First](https://www.sea-ql.org/SeaORM/docs/generate-entity/entity-first/)
- [Select and partial models](https://www.sea-ql.org/SeaORM/docs/basic-crud/select/)
- [Nested selects](https://www.sea-ql.org/SeaORM/docs/relation/nested-selects/)
- [Entity Loader](https://www.sea-ql.org/SeaORM/docs/relation/entity-loader/)
- [Transactions](https://www.sea-ql.org/SeaORM/docs/advanced-query/transaction/)
- [MLflow Model Signatures](https://mlflow.org/docs/latest/ml/model/signatures/)
- [MLflow Model Registry](https://mlflow.org/docs/latest/ml/model-registry/)
