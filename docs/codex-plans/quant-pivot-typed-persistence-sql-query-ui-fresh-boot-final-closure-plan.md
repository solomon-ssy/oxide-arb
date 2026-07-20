# Quant Pivot 强类型持久化、SQL/查询治理、Config UI 与 Fresh Boot 最终闭环计划

## 1. 文档定位与执行授权

本文档是以下两份计划在 2026-07-20 补审后的最终整改与验收计划：

- `quant-pivot-boot-config-governance-ui-ux-refactor-plan.md`；
- `quant-pivot-boot-config-governance-seaorm-typed-closure-remediation-plan.md`。

原计划中的业务目标继续有效；与本文冲突的完成状态、技术基线和验收结论以本文为准。特别是，原整改计划中将全仓强类型、raw SQL、查询预算、Config UI 状态矩阵和文档规则标为 `Verified` 的结论过于乐观，必须回退为 `Partial`，重新以机器证据验收。

用户已经明确授权删除并重建以下 preproduction 目标：

- PostgreSQL database：`quant_pivot`；
- ClickHouse database：`quant_pivot`；
- Redis DB0 中解析并验证为 `qp:` 的 namespace。

该授权不扩展到其他 PostgreSQL database/cluster role、其他 ClickHouse database/user、Redis DB0 的非 `qp:*` key、对象存储、宿主机目录、无 ownership 证明的容器或进程。不可逆 production seal 仍只在 disposable environment 自动化验证；本地验收环境保持 `pre_production_resettable`。

Fresh Boot 的唯一人工 stop gate 是凭证轮换完成确认。实现和无真实账户依赖的 deterministic/disposable 验证可以先执行；在用户确认实际启用凭证已轮换并正确安装前，不执行本地 destructive `apply` 和 live-account `ReportOnly` smoke。

## 2. 目标、非目标与不可妥协原则

### 2.1 最终目标

1. 全仓每个持久化字段、有限状态、业务 ID、协议值和自由文本都有上下文驱动的类型决策，不为 JSON 而 JSON，也不为 typed 而 typed。
2. 项目拥有的闭合 JSONB 使用 canonical struct/tagged enum；需要关系约束、SQL 查询或独立生命周期的数据使用具名列/关系表；真实外部原始载荷保留受控开放 JSON。
3. SeaORM/SeaQuery 成为 PostgreSQL 普通 CRUD、join、aggregate、upsert 和可表达 DDL 的默认路径；raw SQL 只存在于显式登记、类型约束、可测试的 dialect boundary。
4. Config 和关键业务读取具有明确的一致性边界和 statement budget；消除真实 N+1，对不可避免的 bind-limit chunk 和逐聚合事务给出上限与理由。
5. Config route、Rust request/response、JSON Schema、generated TypeScript、前端调用和错误契约来自同一 endpoint registry，无法静默漂移。
6. Config Playwright 使用真实后端和数据库完成治理主链，并以可执行状态 registry 覆盖全部 24 个状态、viewport/theme/motion/accessibility/keyboard 证据。
7. 在轮换凭证后，安全删除限定范围内的 PG/CH/Redis 状态，从零 boot、启动、重启、完成 Config 全流程、单实例内存状态恢复、研究/模型冷启动和 live-account `ReportOnly` smoke。
8. 所有静态、Rust、Docker、network、contract、UI、Playwright、fresh-boot 和 evidence gates 全部通过后，才允许把本阶段标记为 closed。

### 2.2 非目标

- 不保留旧数据库数据、旧 Runtime Config parser、dual-write、兼容 view、兼容 DTO、旧路径 alias 或迁移/删除后的 compatibility re-export。
- 不为了减少 diff、工作量或侵入性保留语义错误的结构。
- 不禁止正常模块 API 使用 canonical public barrel；只禁止为已删除/迁移路径提供偷懒转发。
- 不把所有 `String` 无脑改成 enum，不把所有固定 struct 无脑拆列，也不把所有 fixed key 无脑存 JSONB。
- 不在本地验收环境执行不可逆 `production_frozen`。
- 不删除对象存储；若后续证明对象存储中的 preproduction namespace 会破坏 clean boot，必须先形成独立清理计划并再次取得明确范围授权。

### 2.3 “系统没有任何问题”的可验证口径

绝对证明软件不存在任何缺陷不可实现。本计划将其收敛为可审计的 Definition of Done：所有已知 P0/P1、本文 requirement、关键失败注入、完整质量门和现场验收均通过；无未解释 warning、failed request、schema drift、unknown object、secret exposure 或残留 partial state。任何失败、跳过、未覆盖或证据缺失都阻止 close/seal，不以人工口头判断替代。

### 2.4 已锁定部署拓扑

- 正式部署是 AWS 上由 systemd 管理的单个 `quant-pivot` 应用实例、单个应用进程；不设计 active-active、standby、rolling deployment 或水平扩容。
- 不引入 leader election、distributed consensus、跨实例 Config 广播、实例 membership、租约选主或多副本 ArcSwap 收敛协议。
- Config activation 只需保证同一进程内的并发 HTTP/worker 请求不会 lost update；PostgreSQL 是持久化 authority，成功 commit 后发布到本进程唯一 RuntimeConfigStore/ArcSwap。
- durable outbox/reconciler 只负责同一实例在 commit 后 publish 前崩溃、systemd restart、事件发布重试和启动恢复，不承担多实例同步。
- migration、reset、ClickHouse schema mutation 和 seal 虽会作为独立管理命令/进程运行，仍必须与应用管理操作互斥；第 14.2 节的 lifecycle lease 是部署管理锁，不是多实例运行时协调协议。
- 验收使用并发请求和 systemd stop/start/restart，不启动第二个应用实例，也不要求 rolling-upgrade 行为。

## 3. 补审后的事实基线

| ID | 审计域 | 当前事实 | 状态 |
|---|---|---|---|
| BASE-01 | Config activation/seal/boot 主体 | 全局 bundle、事务 activation、outbox/reconciler、single boot migration 等主体已实现并通过既有测试 | Implemented，需随最终 schema 回归 |
| BASE-02 | 字符串/UUID 强类型 | 已有大量 newtype/ActiveEnum，但 `role.code`、source-slice track、trade-tape cursor、acting role、diagnostic、trigger/result refs 等仍有裸语义 | Partial |
| BASE-03 | constant inventory | 当前只扫描具名 `const/static`，并用文件/符号正则推断 disposition，不能证明字符串字面量和 entity 字段 | Not closed |
| BASE-04 | raw SQL | PostgreSQL repository 普通查询基本收口，但 storage/migration/xtask 和 Core ClickHouse 查询未进入统一 exception registry | Partial |
| BASE-05 | 查询预算 | Config resources/activity 已有单 statement 测试，snapshot options/model picker 已收敛；全仓循环内 I/O 和关键 API budget 未覆盖 | Partial |
| BASE-06 | UI E2E | Config Playwright 主链确实穿透真实 Rust/PG/CH/Redis；异常态部分定向注入；此前当前树 7/7 通过 | Implemented but not gated |
| BASE-07 | UI matrix/a11y | 24-state manifest 只是名称数组；axe 只覆盖 overview；focus trap/order 和完整 viewport/theme 矩阵未证明 | Not closed |
| BASE-08 | API contract | Rust DTO → schemars → JSON Schema → TS regenerate-and-diff 已通过，但 schema root 手工枚举且不绑定 route/method/auth/error | Partial |
| BASE-09 | persistence 文档 | canonical SeaORM/typed 文档、Cursor rule、AGENTS 引用已存在，JSONB 场景决策方向正确 | Implemented，机器约束不足 |
| BASE-10 | SeaORM 版本 | workspace 仍精确锁定 `2.0.0-rc.43`；crates.io 和当前官方文档已经提供 `2.0.0` stable | Stale baseline |
| BASE-11 | Fresh Boot 工具 | 已有 `preproduction-reset plan|apply|verify`、nonce、target fingerprint、PG/CH exact DB 和 Redis `SCAN+UNLINK` | Implemented，需安全/恢复补强 |
| BASE-12 | 现场 Fresh Boot | 因凭证轮换前置条件未满足，尚未执行本地 destructive reset、真实启动/重启和 live smoke | Blocked by credential rotation |

## 4. Requirement 追踪矩阵

实现开始时将本表扩展为 `requirement → code → test → evidence hash → status`。只有实际证据存在时才允许 `Verified`。

| ID | Requirement | 必须交付的证据 | 初始状态 |
|---|---|---|---|
| TYPE-01 | 全仓 primitive semantic field 显式决策 | 无启发式的 field inventory、双向 lint、逐字段 migration/test | Pending |
| TYPE-02 | 高置信度 String/Uuid 迁移 | typed entity/DTO/API/UI、DB round-trip、非法值拒绝 | Pending |
| JSON-01 | 每个 JSONB 的上下文决策 | producer/consumer/query/update/hash/evolution ledger | Partial |
| JSON-02 | typed JSONB/normalized/external boundary 精确落地 | DB CHECK、corruption/tag/hash tests、无裸 Value 泄漏 | Partial |
| ORM-01 | SeaORM stable 2.0.0 基线 | dependency/API/SQL/schema diff 和全套 gates | Pending |
| SQL-01 | raw SQL typed exception registry | 全仓 inventory、唯一 exception ID、typed input、专项测试 | Pending |
| SQL-02 | Core SQL 下沉 | Core/Web 不直接持有 PG/CH statement；通过 repository/port 调用 | Pending |
| QUERY-01 | Config 查询一致性与 budget | resources/activity/snapshot options 单一致性边界和 statement count | Partial |
| QUERY-02 | 全仓 N+1/重复查询审计 | query-classification ledger、budget tests、tracing report | Pending |
| API-01 | endpoint/DTO/schema/TS 单一契约 | endpoint registry regenerate-and-diff、route coverage test | Pending |
| UI-01 | 24 状态可执行 E2E registry | 每个 state 都有 setup/assert/evidence，无 orphan name | Pending |
| UI-02 | responsive/theme/motion/a11y/keyboard | 固定 CI snapshots、axe、focus trap/order、keyboard-only | Pending |
| DOC-01 | 范式与规则机器闭环 | docs/rule/registry/lint 一致性检查 | Partial |
| RESET-01 | 凭证轮换与 secret boundary | 用户确认、credential preflight、无 secret evidence | Blocked |
| RESET-02 | 可恢复的限定范围 destructive reset | plan/apply/failure-journal/verify tests 和现场记录 | Pending |
| BOOT-01 | 真空环境 PG/CH/Redis boot | PG=1、CH=1、Redis target empty、无 unknown objects | Pending |
| ACCEPT-01 | 单实例启动/重启与 Config 恢复 | readiness、workflow、commit/publish crash recovery、无 duplicate seed | Pending |
| ACCEPT-02 | 完整研究到报告闭环 | spec→dataset→train→validate→publish→report lineage | Pending |
| ACCEPT-03 | live-account ReportOnly 安全 | account truth/report 成功，零 signing/order/intent | Blocked |
| SEAL-01 | disposable seal/frozen denial | live evidence、mutation denial、restore test | Pending |

## 5. Workstream A：字符串魔法值与全仓强类型

### 5.1 建立非启发式语义 inventory

新增 typed xtask 审计器，以 Rust AST/SeaORM entity metadata 枚举所有 runtime entity 和 persistence DTO 中的：

- `String` / `Option<String>`；
- `Uuid` / `Option<Uuid>`；
- 有限集合的 integer/string discriminator；
- address/hash/method/version/source/status/kind/code/key/ref 等高风险命名字段；
- 生产代码中参与比较、分派、format key、SQL predicate 和 API path 的字符串字面量。

审计器读取 checked-in、schema-validated 的显式 decision registry。每个候选必须使用封闭 disposition enum：

| Disposition | 适用语义 | 要求 |
|---|---|---|
| `ActiveEnum` | 数据库有限集合、需要 CHECK/filter/order | Rust/PG/TS 同源、非法值 DB round-trip 失败 |
| `ValidatedNewtype` | 可扩展但有格式/长度/namespace 的 code/hash/address/key | `DeriveValueType` 或完整 SeaORM conversion、构造校验 |
| `TypedId` | 单一实体 identity/FK | 禁止裸 UUID，关系和 API 使用同一 ID |
| `TaggedReference` | 跨多个 entity namespace 的多态引用 | `kind + typed identity`，能加 FK 的 variant 必须加 FK |
| `FreeText` | 人类 reason/name/description/comment | 长度、控制字符、敏感内容规则；不得参与状态分派 |
| `ExternalProtocolValue` | 项目不拥有的协议标识 | validated adapter type，记录外部 owner，不冻结为错误 enum |
| `OpaqueSnapshotLabel` | append-only actor/display snapshot | 明确非 authority，不替代主体 ID/role code |

CI 必须双向证明：每个候选字段恰有一个 decision；decision 指向的字段仍存在且类型与 disposition 相符。禁止继续根据文件路径或变量名正则自动给出业务结论。

### 5.2 第一批高置信度整改对象

以下字段优先处理，但最终类型仍由调用方、查询和 DB 约束审计确认：

- `role.code: String` → `RoleCode`；
- `quant_source_slice.evaluation_track: String` → canonical `ResearchEvaluationTrack` persistence type；
- `quant_trade_tape_block_cursor.source/status: String` → `TradeTapeSourceKind` / `TradeTapeBlockCursorStatus`；
- feature-parity/research-job/system-bootstrap 的 `acting_role` → 区分 `RoleCode`、system actor kind 与 display snapshot；
- `failure_code` / `diagnostic_kind` → `DiagnosticCode` 或对应 closed diagnostic enum；
- `quant_research_job.result_ref: Option<Uuid>` → `result_kind + tagged result reference`，不包装成虚假单一 ID；
- `trigger_key`、`candidate_id`、`artifact_version`、`attestation_key_id`、EVM address/tx hash → 各自 validated newtype；
- `reader_contract_version` 等项目自有 version → typed version value，基线为 1；外部协议版本不重置。

`reason`、`description`、展示名称和外部原文不因本计划自动 enum 化。每项迁移必须同步修改 entity time capsule、boot schema、repository、domain DTO、API schema、generated TS、UI formatter/filter 和 tests。

### 5.3 system actor 与 RBAC 语义拆分

禁止继续使用字符串 `"system"` 同时表达 user、role、worker、actor kind 和展示名称。建立明确结构：

- `PolicyActorKind`/`SystemActorKind` 表达主体类别；
- `Option<UserId>` 表达已认证用户；
- `Option<RoleCode>` 表达授权角色；
- `WorkerId`/service component enum 表达机器执行者；
- non-empty label 只保存不可变审计快照。

DB CHECK 保证不同 actor kind 下合法字段组合，API/UI 不再根据字符串猜测身份。

## 6. Workstream B：JSON/JSONB 场景化建模

### 6.1 决策顺序

每个 JSON/JSONB 字段必须逐项回答：

1. 谁生产、谁消费，系统是否拥有 schema？
2. 子项是否有 identity、FK、独立生命周期或一对多关系？
3. 是否按内部字段过滤、排序、聚合、唯一约束、CAS 或局部更新？
4. 是否整体生成、整体校验、整体哈希、整体替换？
5. 是否需要 exact replay/WORM wire image？
6. key 是 closed、registry-controlled map，还是外部不可控？
7. schema/format 如何演进，错误 tag/unknown field 如何 fail closed？

结论只能是以下之一：具名列、关系表、native array、typed JSONB、controlled-open audit document、`ExternalJsonDocument`，或删除重复字段。

### 6.2 `FromJsonQueryResult` 的精确使用

`FromJsonQueryResult` 只作为 SeaORM 对“已判定为原子 JSONB value object”的 DB conversion。它不替代：

- 关系建模和 FK；
- ActiveEnum/newtype；
- `deny_unknown_fields`/tag validation；
- DB CHECK；
- schema/format version；
- content hash 和 kind/hash binding；
- persistence error mapping。

固定 key 不自动意味着拆列，也不自动意味着 JSONB。需要 SQL 语义的固定字段拆成列；没有内部查询/局部更新语义、随聚合根 WORM 的固定对象使用 typed JSONB。

### 6.3 已锁定的关键场景

- `ModelSpecThesis` 保留 typed JSONB：summary/hypothesis/limitations 是不可分割研究论题并参与 definition hash；可执行 input/training contract 独立建模并被训练/推理消费，不能退化为 description 或空对象。
- `ModelTrainingObjective`、model metrics、quality-gate report 使用 closed tagged document；`NULL` 表示尚未产生，禁止 `{}`。
- operation log detail 保留受限开放文档，因为它跨域、非权威、只整体展示；必须限制 object shape、字节数、深度、敏感 key，任何业务恢复不得依赖它。
- feature vector 使用 fixed envelope + registry-controlled typed feature key/value；动态 feature 名不等于允许裸 `Value`。
- 四个真正外部原始边界继续使用 `ExternalJsonDocument`：catalog event、catalog market、catalog rejection、CLOB market raw payload。
- profile、artifact、evidence、outbox、lineage 中能独立引用的数据使用 typed ID/hash/FK，禁止复制完整权威文档。

### 6.4 JSON 验收

每种 persisted document 至少覆盖：合法 DB round-trip、unknown field、错误 tag、非法 enum/newtype、kind/tag mismatch、损坏 JSON、subject ID mismatch、schema version mismatch、content hash mismatch。测试必须经过真实 PostgreSQL decode；只做 serde unit test 不算持久化闭环。

## 7. Workstream C：SeaORM stable 与 Entity First

### 7.1 版本基线

将 SeaORM/sea-orm-migration 从精确锁定的 `2.0.0-rc.43` 收敛到官方 stable `2.0.0`，不同时支持 RC 和 stable。升级前生成以下 before evidence：

- resolved dependency tree/features；
- PG boot SQL normalized manifest/fingerprint；
- ActiveEnum scalar/array bind SQL；
- `DeriveValueType` ID round-trip；
- typed JSONB decode；
- PartialModel/nested alias SQL；
- transaction isolation/row-lock SQL；
- MockDatabase statement logs。

升级后逐项 diff。任何 SQL cast、enum name、column type、FK/index、migration checksum 或 decode 行为改变都必须显式判断并更新唯一 v1 time capsule；不能只因编译通过就接受。

### 7.2 Entity First 规则

- 唯一 PG boot migration 使用 v1 entity time capsule + `SchemaBuilder::apply`。
- runtime entity、migration snapshot、normalized manifest 三方 regenerate-and-diff。
- ActiveEnum、array-only enum、复杂 CHECK、partial/expression index、WORM trigger 等 Entity First 缺口使用集中 typed SeaQuery/schema spec。
- runtime startup 只 verify，不执行 schema sync/DDL。
- PG empty/object verifier覆盖 table、partitioned table、view、materialized view、sequence、foreign table、enum/domain/range type、function 和 non-internal trigger。
- CH manifest 精确匹配全部受管 object，未知 object fail closed。

## 8. Workstream D：raw SQL typed exception boundary

### 8.1 显式 exception registry

新增 canonical Rust registry。每条 raw statement 必须有唯一 `RawSqlExceptionId`，并登记：

- dialect：PostgreSQL / ClickHouse；
- purpose：catalog inspection、admin/reset、advisory/lifecycle lock、ordered-set percentile、typed CH fact query、test corruption；
- owning module 和调用方；
- 完整输入类型和 result row 类型；
- identifier 来源：sealed enum、compile-time manifest 或 validated newtype；
- bind policy；
- statement-count budget；
- unit/integration/failure test；
- 保留 raw SQL 而非 SeaORM/SeaQuery 的具体原因。

普通 CRUD、join、aggregate、upsert、可表达 DDL 不得登记为例外。registry 是代码事实源，文档 inventory 由其生成，不手工维护第二份。

### 8.2 模块边界

- PostgreSQL repository 只在集中 dialect primitive 中保留 SeaQuery 无法表达的 ordered-set percentile。
- PostgreSQL catalog/admin/reset/lease SQL进入 migration/storage typed dialect module，不在 xtask 业务流程散落。
- ClickHouse query renderer 和 typed row/bind contract 位于 storage/repository ClickHouse boundary。
- Core/Web 通过 port/repository method 调用，不直接持有 `SELECT/INSERT/DDL` 字符串。
- test-only corruption 放在 `cfg(test)` 的独立模块，以 exception ID 标记，禁止进入 production binary。

### 8.3 全仓 lint

用 typed xtask/source parser 扫描所有 production crates 的 SeaORM raw API、sqlx API、ClickHouse `.query`、SQL macro/string/变量传递。静态 regex 可以作为快速提示，但不能是唯一证明。任何未登记 SQL、动态 identifier、`format!` 拼入非封闭标识符或 Core/Web SQL 都直接失败。

## 9. Workstream E：重复查询、N+1 与一致性预算

### 9.1 分类而非无脑“一次查询”

每个循环内 I/O 分成三类：

1. `TrueNPlusOne`：先查列表再逐行读取关联，必须 join/PartialModel/Loader/IN query。
2. `BindLimitedBatch`：因 PostgreSQL bind/packet 上限分 chunk，允许多 statement，但预算为 `ceil(n/chunk_size)`，需要测试。
3. `PerAggregateTransaction`：每个业务聚合必须独立锁、审计、释放资金或容忍单项冲突，可以逐项事务；必须证明语义需要并设置 bounded batch、timeout、metrics。

### 9.2 优先审计路径

- Config resources/activity/snapshot options：同一 DB-authoritative generation boundary；分别保持明确的单 statement/单 snapshot 契约。
- profile applicator：四类 artifact 一次 `IN` 加载并校验 kind/hash。
- model picker/report projection：1:1/N:1 使用 `DerivePartialModel` join；1:N/M:N 使用 Loader/Entity Loader。
- intent cascade invalidation：评估是否用一次 locked select + batch state update + batch audit/outbox；若逐 intent capital/condition transition 必须保留，给出每 intent 固定 statement budget。
- report expiry/roll-up：避免每个 recommendation 重复读取同一 report；按 report group 批量计算，仍保持单 recommendation 冲突隔离。
- bias-table fitting：禁止 market × time-grid 逐点远程/PIT query；扩展 batched boundary API，一次/分块加载 market-token-window 数据后内存求样本。
- catalog ingest/write helpers：保留 bind-safe chunk，但建立输入规模与 statement 数断言。

### 9.3 证明机制

- MockDatabase 只用于 SQL shape/statement count 快测。
- 关键 repository 使用真实 PG integration test 验证 decode、lock 和 transaction。
- API/service 层加入 query tracing collector，以 request/job 为 scope 断言预算。
- 输出 p50/p95/p99 statement count 和 wall time；超过预算 fail CI，而不是只打 warning。
- 禁止通过预加载整表换取“一次查询”；同时记录 row/byte budget，防止 over-fetch。

## 10. Workstream F：Config API 契约单一来源

### 10.1 Endpoint registry

建立 `config_endpoint_contract!` 或等价 canonical registry。每个 endpoint 一次声明：

- endpoint ID、method、path template、API version；
- permission/auth requirement；
- path/query/body request 类型；
- success response 类型；
- typed error variants/status code；
- idempotency/CAS header/body requirement。

同一 registry 生成或编译绑定：Actix route registration、schemars root、JSON contract、TypeScript endpoint descriptor 和 contract tests。删除只为 codegen 手工维护且不被 handler 使用的平行 DTO root。

### 10.2 Config 激活公共契约

Activation request/response 必须保留并验证：

- `expected_bundle_generation`；
- expected active resource revision；
- candidate/request hash；
- approval ID、preflight token、idempotency key；
- committed generation、snapshot ID/hash、完整 revision vector；
- exact replay/new commit 的 typed outcome。

resources/current/activity/snapshot-options/lifecycle 均返回 DB-authoritative consistency metadata；UI 不混合 ArcSwap 与不同时点的独立查询。

### 10.3 Drift gates

- route registry 中每个 endpoint 必须被 Actix 注册且只注册一次；
- 每个 UI Config API 调用只能引用 generated endpoint ID/path/method；
- regenerate-and-diff 同时比较 route manifest、JSON Schema 和 TS；
- request/response serialization golden tests；
- permission、401/403、validation 400、CAS/idempotency 409、service 503 的 error envelope tests。

## 11. Workstream G：Config UI 与真实 E2E

### 11.1 可执行状态 registry

把当前字符串数组替换为 typed Playwright registry。每个 state entry 必须包含：

- state ID；
- route/resource；
- fixture/setup 类型；
- 业务 assertion；
- permission/backend mode；
- required viewport/theme/motion combinations；
- axe scope；
- keyboard/focus assertion；
- visual snapshot 名称和 volatile mask；
- teardown/recovery。

测试 runner 从 registry 生成用例，并反向断言 requirement state 无遗漏、snapshot 无 orphan、entry 至少包含一个真实业务 assertion。禁止只增加名称让 coverage 数量通过。

### 11.2 24 个最低状态

保留并逐项证明：overview healthy、pending approval/restart required、recommendation default、draft dirty、inline validation error、review diff、approval pending、activation preflight、activation success、stale generation conflict、rollback review/result、model routing picker、report schedule preview、operational control halted、deployment redacted、lifecycle preproduction、seal confirmation、production frozen、read-only、backend recovery、execution authorization、1024 overflow、reduced motion。

### 11.3 真实与注入边界

- login、Draft→validate→approve→activate→rollback 使用真实 Rust API 和真实 PG。
- stale generation 优先使用两个 browser context/并发 candidate 真实触发 CAS；不得只伪造 409。
- read-only 使用真实 seeded principal/permission，不只改写 `/auth/me`。
- production frozen 因本地不可逆，使用 typed disposable fixture/route injection，但必须另有后端 frozen integration test。
- transient backend recovery 可使用定向 fault injection；恢复后必须重新命中真实 API。
- fixture body 使用 generated TS `satisfies` 校验，不手写漂移结构。

### 11.4 viewport、主题、动效与可访问性

- 核心状态：1440×900 light/dark；
- Overview、editor、review、lifecycle、error/conflict：390×844 light/dark；
- 所有主页面：1024px overflow；
- 高密度页面：1280×800；
- reduced motion：无 position/scale，必要 opacity ≤100ms；正常动效 ≤350ms，无 infinite animation；
- axe：所有核心页面和 dialog 无 critical/serious violation；
- keyboard-only 完成完整治理流程；验证 visible focus、focus order、dialog initial focus、Tab/Shift+Tab trap、Escape policy、关闭后 focus restoration、ARIA live/status/error summary 跳转。

视觉基线固定在 CI Linux image，不伪装为 Darwin suffix；动态值只 mask 必要区域，`maxDiffPixelRatio <= 0.001`。每次 baseline 更新必须人工审阅 diff artifact。

### 11.5 CI 接线

canonical protected E2E 同时执行：

- `phase-11-7-protected-flow.spec.ts`；
- `config-governance.spec.ts`。

CI 上传 HTML report、trace、failure screenshot、visual diff 和 state coverage manifest。Config spec 未运行、skip、缺 browser baseline 或只运行字符串 manifest test 均视为失败。

## 12. Workstream H：文档、规则与机器契约

### 12.1 Canonical 文档

继续以 `docs/persistence/seaorm-and-typed-persistence.md` 为 persistence 规范，并补充：

- semantic-field decision registry 格式；
- raw SQL exception registry；
- query classification/budget；
- endpoint contract registry；
- Fresh Boot 证据和失败恢复；
- canonical public barrel 与 compatibility re-export 的边界。

`.cursor/rules/quant-pivot-persistence.mdc` 和 `AGENTS.md` 只摘要强制规则并链接 canonical 文档，避免三份规则漂移。

### 12.2 文档 lint

CI 验证：

- canonical 文档、Cursor rule、AGENTS 引用存在；
- 文档声称的 registry/gate 在代码和 CI 中真实存在；
- requirement matrix 的 `Verified` 必须有可读取 evidence；
- 无 Runtime v17/v18、schema v3、DryRun/Paper/Live、旧 Runtime Config endpoint/parser；
- 无 secret env 示例、明文 TOML secret、旧 UI JSON editor 指引；
- runbook 不再使用 `QUANT_PIVOT__*` 注入 funder/RPC/secret，与当前 typed TOML/credential-file 来源一致；
- SeaORM 版本说明与 workspace lock 一致。

## 13. 凭证轮换前置条件

### 13.1 零复用原则

- 不读取、打印、复制、hash、备份或复用旧配置中已暴露的 secret value。
- 不把 secret 放入 Git、TOML、environment value、CLI argument、Docker `.env`、日志、截图、trace、reset plan 或 evidence manifest。
- secret hash 也不作为轮换证明，避免对低熵 secret 提供离线验证材料。

### 13.2 必须轮换的实际启用凭证

| 类别 | 轮换与验证 |
|---|---|
| Wallet/private key | 生成或安装新 key；只在内存派生 public signer address；用户确认资金/权限迁移与旧 key 撤销策略 |
| Funder/wallet topology | EOA 必须与 signer 一致；proxy/safe 必须现场证明 signer 的控制/owner 关系和 relayer 路径 |
| Polygon/RPC/provider | 轮换 JWT/API token/URL credential；只记录 provider/key ID 和健康结果 |
| Relayer | 轮换 API key/secret，验证 address、wallet kind 和最小权限；ReportOnly 不调用提交接口 |
| PostgreSQL | 分别轮换 runtime/migrator role credential；先在服务端生效，再安装 credential file；reset 不擅自删除 cluster role |
| ClickHouse | 分别轮换 runtime/migration user credential；Docker 使用 secrets/file mount，删除 `.env` 明文依赖 |
| Redis | 轮换 runtime credential/ACL，确认只访问配置 DB 和 `qp:` namespace |
| JWT signing | 使用符合长度/编码的新 key；确认旧 session/token 失效 |
| Evidence signing | 与 JWT/wallet 分离；轮换 current key，previous key 只保留真实验证需求且使用独立 credential reference |
| Notification/domain provider | 只轮换实际启用项；禁用项不得用假 secret 绕过 validation |

### 13.3 Credential file 验收

- 通过 systemd `CREDENTIALS_DIRECTORY` 或等价只读 file mount 注入；TOML 只保存 typed credential name。
- 文件必须是 regular file、非 symlink、非 hard-link escape、非空、大小受限，mode 0400/0600，owner 为预期 service user，group/world 无读取权限。
- credential directory 不可被非授权用户写入；启动前检查 owner/mode/mount source。
- runtime config 不含 DDL credential；migration/reset command 才加载 migrator credential。
- credential preflight 输出只包含字段名、configured/loaded/validated、public identity 和脱敏 endpoint fingerprint。

### 13.4 人工完成声明

用户需确认：所有实际启用凭证已轮换、新 wallet 与 funder 关系正确、credential files 已安装。确认后 destructive reset 无需再次扩大授权，但仍必须通过工具自身 plan fingerprint、短时 nonce 和 lifecycle lease。

## 14. 破坏式 Fresh Boot 安全模型

### 14.1 精确目标

| 系统 | 允许目标 | 明确禁止 |
|---|---|---|
| PostgreSQL | 当前配置 endpoint 上 database `quant_pivot`、schema `public` | 其他 database、template、cluster role、tablespace |
| ClickHouse | 当前 deployment ID/endpoint 上 database `quant_pivot` | `default`、`system`、其他 database/user |
| Redis | DB0 且 exact non-empty prefix `qp:`，仅 `qp:*` | `FLUSHDB`/`FLUSHALL`、其他 DB、非 `qp:*` key |
| Process/container | 当前 repo systemd unit、PID/application name、compose/project ownership label 可证明的实例 | 名称相似但 ownership 不明的进程/容器 |
| Artifact store | 只读核验，不删除 | 任意 object/bucket/prefix deletion |

### 14.2 生命周期协调锁

PG migration、CH migration、reset 和 seal 必须通过同一个 `LifecycleLeaseProvider`，连接同一个固定 coordination database/lock namespace。不能只复用相同整数 key，却在不同 PostgreSQL database connection 上假设必然互斥。

实现要求：

- coordination endpoint/DB 是 typed deploy contract；
- least-privileged lifecycle credential 只能连接协调库并取得指定 lock；
- 所有四类 mutation 先取 lease，再现场重读 baseline、target fingerprint、migration ledger 和 active bundle；
- reset 在删除 target DB 期间 lease 仍存活；
- lease 丢失/connection 断开立即停止后续 stage；
- 集成测试真实并发 reset、PG migration、CH migration、seal，四者任意两项都只能一个成功进入 mutation。

### 14.3 Reset operation journal

跨 PG/CH/Redis 不存在原子事务，因此 reset 必须使用 durable、0600、无 secret 的 stage journal：

```text
Planned -> Armed -> Quiesced -> PgReset -> ChReset -> RedisReset
        -> PgBoot -> ChBoot -> Seeded -> Verified -> Completed
                                 \-> Failed(stage, evidence_hash)
```

- plan 包含 format version、nonce、created/expires、Git/build identity、lifecycle state、脱敏 endpoint fingerprint、exact target、对象/连接/key 计数、plan hash。
- apply 消费一次性 nonce，获取 lease 后重新采集并逐字段比较 inventory。
- 每个 stage 完成后 fsync/atomic rename journal；进程崩溃不能被误报 Completed。
- 失败后保持 `Failed`，输出无 secret 的恢复说明。新的 plan 必须基于当前 partial inventory 生成；下一次 apply 从重新清空三个允许目标开始，不在未知中间状态上“猜测续跑”。
- `verify` 只接受 Completed operation ID，并把现场 fingerprint 与 journal/evidence 对齐。

### 14.4 进程静默与连接处理

1. 列出 repo-owned systemd unit、PID、PG application_name/role、CH query/user、Redis client name。
2. 优雅停止并等待 worker/outbox checkpoint；记录非敏感 shutdown evidence。
3. 只终止能证明属于当前 repo/runtime role/target DB 的残留连接。
4. 发现未知 PG session、CH writer 或持续创建 `qp:*` key 的 client 时 fail closed，不强杀。
5. PG 禁止仅依赖 `DROP DATABASE ... WITH FORCE` 掩盖竞态；先禁止新连接、验证 session ownership/zero unknown session，再删除 exact DB。

## 15. Fresh Boot 执行步骤

### 15.1 Reset dress rehearsal

先在 disposable testcontainers 环境完整运行两次：

1. 正常 plan/apply/verify；
2. 在 PG reset 后注入 CH failure，证明 journal 为 Failed、不会报 Completed；
3. 从 partial state 创建新 plan，重新清空并成功 boot；
4. 注入 expired/tampered nonce、target drift、lease conflict、unknown session、Redis concurrent writer、seed failure；
5. 证明每种情况 fail closed，其他 DB/key/object 不变。

### 15.2 本地 plan

凭证确认后：

1. 记录当前 clean/dirty build identity，但不要求提交用户现有改动；evidence 明确 dirty 状态和 diff hash。
2. 运行 credential preflight、lifecycle/baseline preflight、target ownership/inventory。
3. 生成短时 plan 和一次性 nonce，只输出脱敏 endpoint fingerprint、target 名称、对象/连接/key 数和 expiry。
4. 人工核对输出恰为 PG `quant_pivot`、CH `quant_pivot`、Redis DB0 `qp:`。

### 15.3 本地 apply

在共享 lifecycle lease 和 stage journal 下：

1. quiesce 当前 repo services/writers；
2. 删除并重建 PG `quant_pivot`，owner 为 canonical migrator role；不删除/重建其他 role；
3. 删除并重建 CH `quant_pivot`；
4. Redis `SCAN MATCH qp:*` + bounded `UNLINK`，禁止 `FLUSHDB`；
5. 证明 PG/CH target 空、Redis `qp:* = 0`；
6. 应用唯一 PG boot migration，期望 migration count=1、version=1；
7. 应用唯一 CH boot migration，期望 version=1、object manifest 精确相等；
8. finalize runtime grants，runtime identity 无 DDL privilege；
9. seed bootstrap admin、immutable research profiles、policy profile artifacts 和六类 Config resource boot bundle；
10. 校验 generation=1、snapshot/hash/revision vector 完整、无 pending approval/outbox；
11. verify PG/CH schema fingerprint、ledger/audit checksum、Redis target empty；
12. journal 标记 Completed 并生成脱敏 evidence manifest。

### 15.4 Reset 后启动与重启

1. migration identity 完成 apply/verify 后退出；
2. runtime 只用 runtime credentials 启动，必须只读 verify schema，禁止 startup DDL；
3. 等待 PG/CH/Redis/web/WS/ingest/readiness；任何 component degraded 必须有预期原因和恢复动作；
4. 检查结构化日志无 secret、panic、unknown schema、retry storm、failed migration；
5. 记录初次 seed/worker/outbox 数量；
6. 优雅重启；migration 不重复、seed 不重复、generation/hash 不漂移、outbox 不重复发布；
7. 启动后 Redis 可以产生 `qp:*` runtime key，但必须全部属于 registry 中的 typed namespace；非 `qp:*` key 数和值不因 reset/boot 改变。

## 16. Reset 后业务与 UI 全闭环验收

### 16.1 Config governance

使用真实 API/DB 完成六类资源至少一次读取和以下写链：

1. create immutable Draft；
2. inline/server validation；
3. dependency preflight；
4. approve exact candidate/base generation/revision vector；
5. activate with CAS/idempotency；
6. exact replay 返回原 committed result；
7. same key/different digest 冲突；
8. stale generation/resource CAS 拒绝；
9. explicit rollback 经重新 validate/approve/activate；
10. audit、activation、snapshot、outbox 原子落账。

在同一个应用实例上并发提交不同资源 activation：两次变更必须都保留，generation 连续，本进程 RuntimeConfigStore/ArcSwap 与 DB generation/hash/revision vector 一致。注入 commit 后 publish 前进程崩溃，再由 systemd 启动同一实例；startup recovery/reconciler 必须从 DB/outbox 恢复 committed bundle，且不得重复 activation、audit 或业务事件。

### 16.2 UI

- 运行完整 Config state registry 和 Phase 11.7 protected flow；
- 主治理链命中 fresh-boot 真实后端；
- 无 console error、unhandled rejection、unexpected failed request；
- 24 状态、axe、keyboard、focus、responsive、theme、motion、visual 全部通过；
- 人工复核关键 screenshot diff 和 390/1024 overflow。

### 16.3 从空库到可发布模型

Fresh Boot 后没有已发布模型时，报告 fail closed 是正确状态，不能用假 seed 掩盖。完整验收必须走 canonical 冷启动：

1. 临时关闭 default report schedule，避免 readiness 未完成时错误风暴；
2. 创建包含 `ModelSpecThesis`、input contract、training contract 的 immutable model spec，禁止空 `{}`；
3. 注册并发布启用 factor definitions；
4. ingest 足够 PIT/catalog/CLOB/domain 数据；
5. plan/build immutable training dataset；
6. train model，生成 typed objective/metrics/quality evidence；
7. backtest、calibration、CPCV/path-set、training-serving/full parity；
8. governed latch acknowledge 和 model publish；
9. 通过 model routing picker 激活 published model artifact；
10. ad-hoc canary 成功后再恢复 schedule。

每步验证 research profile、decision policy snapshot、dataset、model spec/version/artifact、factor schema 和 evidence hash lineage；不得通过 DBA 写表或兼容旧 artifact 跳步。

### 16.4 Live-account `ReportOnly` smoke

只在凭证轮换、模型/数据 readiness 完成后执行：

- runtime mode 精确为 `ReportOnly`；
- 从真实 CLOB/Data API 读取 collateral、positions 和 account truth；
- signer/funder/wallet topology 验证通过；
- 生成并发布 RecommendationReport，所有 recommendation 绑定 fresh-boot decision-policy/model/profile/account/data-quality lineage；
- 前后比较 `quant_order_intent`、execution order/submission/reconciliation 表，新增数均为 0；
- outbound audit 证明没有 order submit、signature、relayer transaction 或 on-chain mutation；
- 缺任一 account/provider evidence 时 fail closed，不回退模拟预算。

私钥存在用于账户/CLOB credential 派生不等于允许签单；ReportOnly execution ports 必须在类型和运行时双重拒绝 mutation。

## 17. Production seal 与备份恢复验收

仅在 disposable environment：

1. 对 fresh-boot 数据执行实际 PG/CH backup；
2. 恢复到独立 disposable target，验证 manifest、row counts、bundle/evidence hash；
3. 运行 protected Config E2E 并记录 content-addressed evidence；
4. 使用 clean compiled Git SHA/build identity 执行 seal；
5. seal 现场复核 PG/CH ledger/fingerprint、DB active bundle、build、backup restore、E2E evidence；
6. baseline 写入后，PG migration、CH migration、reset 和再次 seal 全部拒绝；
7. 篡改/缺失 evidence、pending migration、dirty/mismatched build、generation race 均拒绝。

本地验收不执行 seal；只验证 lifecycle view 显示 preproduction 和 seal readiness。

## 18. 自动化质量门

### 18.1 Rust/static

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
bash scripts/lint-import-style.sh
bash scripts/lint-quant-pivot-boundary.sh
bash scripts/lint-quant-pivot-errors.sh
bash scripts/lint-dead-semantics.sh
bash scripts/lint-clickhouse-correctness.sh
bash scripts/lint-training-serving-parity.sh
bash scripts/lint-phase-lifecycle.sh
bash scripts/lint-config-inventory.sh
bash scripts/lint-seaorm-persistence.sh
bash scripts/lint-secret-boundaries.sh
cargo test --workspace
```

并新增：semantic field registry、raw SQL registry、query budget、route contract、documentation claim 和 Fresh Boot journal lint/test。

### 18.2 Feature/Docker/network

- 全 feature clippy/test/build/bench gate；
- canonical Docker registry 连续运行两次，证明无顺序/残留依赖；
- PG migration、policy governance、production lifecycle、Redis、ClickHouse、Core/Web integration 全通过；
- network-shaped wiremock tests 全通过；
- live-account smoke 独立标记，不与 deterministic CI 混淆。

### 18.3 UI/contract

```bash
cd ui
pnpm check:config-api
pnpm lint
pnpm check:circular
pnpm check:dep
pnpm check:type
pnpm test:unit
pnpm build:antdv-next
pnpm exec playwright test \
  apps/web-antdv-next/tests/e2e/phase-11-7-protected-flow.spec.ts \
  apps/web-antdv-next/tests/e2e/config-governance.spec.ts
```

CI 中不得 skip Config E2E；snapshot/trace/report 必须归档。

## 19. Evidence pack

每个 gate 生成 `.local/acceptance/<operation-id>/` 下的非提交 evidence，并生成可提交的脱敏 manifest/WORM evidence reference。内容至少包括：

- operation ID、时间、Git SHA、dirty diff hash、toolchain/dependency versions；
- command ID、exit status、duration、sanitized log content hash；
- PG/CH manifest/fingerprint/migration ledger；
- reset target fingerprint、before/after object/key counts、stage journal hash；
- policy bundle generation/snapshot/revision vector/hash；
- statement budget report；
- raw SQL/semantic-field registry coverage report；
- UI state coverage、axe、visual snapshot/trace hash；
- account truth/report lineage 和 no-order assertions；
- disposable backup/restore/seal evidence。

evidence 不包含 secret、完整 credential path、带认证 URL、private address 以外的敏感账户材料、raw provider payload 或未脱敏日志。

## 20. 实施顺序与 stop/go gates

| Phase | 内容 | Exit gate |
|---|---|---|
| P0 | 回退错误 Verified 状态，固化本文 traceability | 所有 requirement 初始状态真实 |
| P1 | SeaORM stable 2.0.0 before/after 审计与升级 | SQL/schema/decode diff 全解释 |
| P2 | semantic-field inventory 和高置信度强类型迁移 | 全候选显式 decision，双向 lint 通过 |
| P3 | JSONB 逐字段复核和 DB constraint/test | 无未登记裸 JSON/Value |
| P4 | raw SQL exception registry、Core SQL 下沉 | 全 production SQL 有唯一合法 owner |
| P5 | N+1/重复查询重构和 budget | 关键 API/job statement/row budget 通过 |
| P6 | endpoint registry 和 generated contract | route/schema/TS/UI 无漂移 |
| P7 | Config executable E2E registry和 a11y matrix | 两套 protected E2E 进入 CI并通过 |
| P8 | 文档/rule/runbook/CI 收口 | 文档声明与机器 gate 一致 |
| P9 | 全静态、feature、Docker、network 回归 | 无失败、skip 或未解释 warning |
| P10 | disposable reset/failure-recovery rehearsal | 正常与失败注入全部通过 |
| P11 | 用户确认凭证轮换 | credential preflight 全绿 |
| P12 | 本地 destructive Fresh Boot | Completed journal + clean verify |
| P13 | 单实例启动/重启/Config 恢复/完整模型闭环 | 所有业务 lineage、内存/DB 一致性通过 |
| P14 | live-account ReportOnly smoke | report 成功且零 mutation |
| P15 | disposable backup/restore/seal | frozen mutation denial 全通过 |
| P16 | 最终证据审计 | requirement 全部 Verified，零 open P0/P1 |

P1–P10 可以在凭证轮换前进行。P11 未通过时禁止进入 P12/P14。任一 phase 失败必须保留证据、修复根因并从受影响的最早 gate 重跑；不得删除测试、放宽 snapshot/statement budget、加 lint suppression 或把失败改成 warning。

## 21. 最终完成标准

仅当以下条件同时满足，才能宣布本阶段完整闭环：

1. 所有 requirement 均有 code/test/evidence hash，状态为 Verified。
2. 无未决 P0/P1、无未分类 primitive semantic field、无未登记 raw SQL、无超预算查询。
3. SeaORM stable、entity time capsule、PG/CH manifest、API/TS contract完全同步。
4. Config 24 状态、全部核心 viewport/theme/motion/a11y/keyboard/visual gates 在 canonical CI 通过。
5. 凭证已轮换，secret boundary 审计无泄漏。
6. 本地限定范围 Fresh Boot 完成，其他 DB/role/key/object 未改变；启动和重启无漂移。
7. 六类 Config 治理、同实例并发 activation、commit/publish crash recovery 和 systemd restart 恢复全通过。
8. 从空库完成 model spec→dataset→train→validate→publish→RecommendationReport 的真实 lineage。
9. live-account ReportOnly 成功，且零签名、零订单提交、零 OrderIntent。
10. disposable backup/restore/seal/frozen denial 通过；本地仍为 preproduction。
11. 最终 evidence pack 完整、脱敏、可复核，原计划中不再存在与事实冲突的 Verified 声明。

在上述条件完成前，不进行正式 freeze/seal，也不把“测试曾经通过”解释为生产级闭环。

## 22. 官方技术资料基线

实现和 review 以当前官方资料为准，不以仓库旧注释或二手文章替代：

- SeaORM [Entity First workflow](https://www.sea-ql.org/SeaORM/docs/generate-entity/entity-first/)：migration 初始化使用 `SchemaBuilder::apply`，并保存 initial entity time capsule；
- SeaORM [ActiveEnum](https://www.sea-ql.org/SeaORM/docs/generate-entity/enumeration/)；
- SeaORM [newtype / `DeriveValueType` / `FromJsonQueryResult`](https://www.sea-ql.org/SeaORM/docs/generate-entity/newtype/)；
- SeaORM [Nested PartialModel](https://www.sea-ql.org/SeaORM/docs/relation/nested-selects/) 和 [Entity Loader](https://www.sea-ql.org/SeaORM/docs/relation/entity-loader/)；
- SeaORM [transactions](https://www.sea-ql.org/SeaORM/docs/advanced-query/transaction/)；
- crates.io [SeaORM 2.0.0 stable](https://crates.io/crates/sea-orm/2.0.0)；
- PostgreSQL [`pg_locks`](https://www.postgresql.org/docs/current/view-pg-locks.html)：advisory lock 带 database identity，因此共享 lifecycle lease 必须证明所有参与者使用同一协调数据库/命名空间；
- Redis [`SCAN`](https://redis.io/docs/latest/commands/scan/)：完整迭代允许重复返回且并发变化只有有限保证，因此 namespace cleanup 必须幂等、阻止 writer 并循环验证到零；
- Redis [`UNLINK`](https://redis.io/docs/latest/commands/unlink/)：只异步释放明确列出的 key，不使用 `FLUSHDB`；
- ClickHouse [`DROP`](https://clickhouse.com/docs/sql-reference/statements/drop) 与 [`CREATE DATABASE`](https://clickhouse.com/docs/sql-reference/statements/create/database)：reset 只渲染封闭 manifest 中的 exact database identifier。

依赖升级或官方文档发生变化时，先更新本节、canonical persistence 文档和 before/after evidence，再修改代码；不能让“当前官方最佳实践”成为无版本、不可复核的口头结论。
