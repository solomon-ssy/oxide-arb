# Quant Pivot Boot / Config Governance / SeaORM 强类型闭环整改计划

## 1. 目标与完成口径

本计划以 clean-break 方式完成 Boot、Config Governance、SeaORM persistence、API/UI contract、production seal 与 fresh-boot 验收闭环。旧数据库、旧 runtime parser、兼容 API、dual-write、compatibility re-export 均不保留。

只有以下条件全部满足，本阶段才完成：

1. Config 不同资源并发激活无 lost update，DB、durable ledger 与所有进程内快照收敛。
2. Production seal 现场复核 PG、CH、active bundle、compiled build 和不可变 evidence，并与 migration/reset 共用冻结锁。
3. 唯一 PG/CH boot migration 可从真正空环境启动、重复验证，并拒绝未知对象。
4. 业务持久化边界使用 typed ID、ActiveEnum、typed JSON document 与 FK；裸 JSON 仅允许明确登记的外部原始载荷。
5. SeaORM/SeaQuery 覆盖 CRUD、join、aggregate、upsert 和可表达的 DDL；raw SQL 仅存在于受审计 dialect boundary。
6. Rust、architecture、contract、UI、Playwright 和 fresh-boot quality gates 全部通过。
7. 轮换凭证后完成真实账户 `ReportOnly` smoke，且没有签名、提交订单或生成 `OrderIntent`。

## 2. Requirement 追踪矩阵

| ID | Requirement | Implementation evidence | Verification evidence | Status |
|---|---|---|---|---|
| CFG-01 | Global typed bundle generation and revision vector | `types/config_governance.rs`, repository activation guard and DB-authoritative bundle projection | `pg_policy_governance` concurrent cross-resource activation and rollback cases | Verified in the complete Docker registry |
| CFG-02 | Transactional activation, audit/outbox and exact idempotency | `postgres/governance/runtime_config.rs`, typed audit/outbox entities | repository replay, single-use approval and test-only outbox-write corruption prove activation/snapshot/guard/audit/outbox/approval-consumption atomicity | Verified in the complete Docker registry |
| CFG-03 | Durable multi-process reconciliation | atomic durable activation outbox plus DB-authoritative polling reconciler in the governance bundle/store | restart bootstrap, commit-before-publish recovery, delayed-instance convergence, idempotent replay and same-generation fork rejection tests | Verified by Rust and Docker suites |
| SEAL-01 | Live PG/CH/bundle/build/evidence verification | typed schema verification port, WORM production evidence and live seal route | `pg_production_lifecycle` creates exact backup/config-E2E evidence before sealing | Verified in a disposable environment |
| SEAL-02 | Shared lifecycle lease and frozen mutation denial | lifecycle lease shared by migration/reset/seal | lifecycle, PG migration-lock and CH schema-lock denial tests | Verified in disposable tests; no local irreversible freeze |
| BOOT-01 | One PG and one CH boot migration, version 1 | single PG bootstrap migration, CH manifest, regenerated `schema/postgres/migrations.json` | PG manifest/empty boot and CH first-deployment tests | Verified in the complete Docker registry |
| BOOT-02 | Complete managed-object inventory | PG/CH verifier and exact object manifests | unknown PG schema and unmanaged CH object rejection tests | Verified in the complete Docker registry |
| ORM-01 | Entity-first `SchemaBuilder::apply` | PG boot Entity First application path and v1 time capsule | 14 PG migration tests plus exact regenerated fingerprint | Verified in the complete Docker registry |
| ORM-02 | Immutable policy/research profile artifacts | WORM entities, snapshots, repository loaders | policy bootstrap/governance and research lineage/hash tests | Verified by Rust and Docker suites |
| ORM-03 | Typed JSON/ID/enum persistence | typed runtime documents; immutable ModelSpec executable lineage; typed operation/audit boundaries; native array for expectation ID sets | typed document unit tests, DB corruption/round-trip tests and migration manifest | Verified by Rust and Docker suites |
| ORM-04 | Audited raw-SQL boundary and query budgets | typed PostgreSQL dialect primitives, typed migration catalog boundary, joined model/spec projection, single-statement Config projections | `lint-seaorm-persistence.sh`, `config_activity` / `config_resources` statement-count tests | Verified by static and Rust gates |
| API-01 | Handler/schema/generated-client contract equality | Config schema generator and generated client | `pnpm check:config-api` regenerate-and-diff gate | Verified |
| API-02 | Consistent resource/activity/snapshot projections | DB-authoritative resource bundle and dedicated snapshot options | query budget and global activity ordering tests | Verified by Rust and Docker suites |
| UI-01 | Config state/theme/viewport/motion matrix | 24-state manifest-driven Config UI/E2E suite | Playwright axe, keyboard, visual, theme, 1440/1024/390 and reduced-motion evidence | Implemented; final current-tree rerun recorded in section 11 |
| DOC-01 | Canonical SeaORM/typed persistence standard | `docs/persistence/seaorm-and-typed-persistence.md`, cursor rule and AGENTS reference | documentation/rule lint | Implemented |
| CLEAN-01 | Remove hidden helper, stale semantics and compatibility re-exports | helper removal and active version/secret semantics cleanup | dead-semantic/compatibility-re-export lint | Verified; canonical public barrels remain supported |
| RESET-01 | Guarded PG/CH/Redis preproduction reset | `quant-pivot-xtask preproduction-reset plan|apply|verify` | plan/apply/verify evidence | Implemented; execution blocked by credential rotation |
| ACCEPT-01 | Clean start/restart and Config governance E2E | disposable PG/CH boot and protected Config E2E | complete Docker registry and Config Playwright evidence | Disposable acceptance verified; scoped local reset/start/restart awaits credential rotation |
| ACCEPT-02 | Rotated-credential live `ReportOnly` smoke | Blocked by credential rotation | Account/report/no-order evidence | Blocked |

## 3. Config 激活与一致性

- 引入 `ActivePolicyBundle`，携带 `PolicyBundleGeneration`、snapshot ID/hash、完整 revision vector 和解析后的 `DecisionPolicySnapshot`；boot generation 从 1 开始。
- validate/preflight/approve 绑定 candidate hash、base generation 和完整 revision vector。
- activation request 携带 `expected_bundle_generation`；事务锁定 singleton guard 后校验 generation、resource CAS、candidate、approval、token 和 idempotency digest。
- 同一事务写入 snapshot、activation、global generation、typed audit 与 durable outbox；只发布 repository 返回的 committed bundle。
- exact replay 返回原 committed result；同 key 不同 digest 返回 typed conflict。
- ArcSwap 只接受单调 generation；同 generation 不同 hash 是一致性错误。durable reconciler 负责多进程、重启和 commit 后崩溃恢复。

## 4. Lifecycle 与 Production Seal

- seal、PG/CH schema mutation 和 reset 共用 lifecycle lease。
- seal 现场复核 PG/CH catalog 和 migration ledger、active DB bundle、compiled clean build identity、backup-restore 与 protected-E2E WORM evidence。
- 删除调用方提供 build commit、配置中直接信任 evidence hash 的接口。
- baseline/frozen 存在时 schema/reset mutation fail closed。
- seal API 统一返回 `LifecycleView`，Rust handler、schemars contract 与 generated TypeScript 必须一致。

## 5. Boot、SeaORM 与强类型持久化

- 保持唯一 PG/CH boot migration，system-owned 版本统一为 1；清除活跃代码/文档中的 Runtime v17/v18/schema v3 项目语义。
- PG 使用 Entity First `SchemaBuilder::apply`；array-only enum 由 typed schema spec 补充。
- PG 空库检查覆盖 table/view/materialized view/sequence/type/function/trigger；CH 拒绝 manifest 外任意对象。
- 建立 WORM `policy_profile_artifact`，四类 policy profile 以 typed ID/hash 被 snapshot 引用，并一次加载、校验、物化。
- 建立 immutable `research_profile_artifact`，消除 profile ID 与 profile JSON 的重复持久化。
- runtime entity 中每个 JSON/JSONB 字段先完成 ownership、shape、查询、约束、更新原子性、hash 和演进审计，再在关系列/子表、native array、typed JSONB 或受控开放文档中选择语义最精确的表示。`FromJsonQueryResult` 只用于已判定为原子 JSONB document 的 SeaORM decode adapter，不作为建模目标。四个外部原始载荷边界使用 `ExternalJsonDocument`；跨域非权威 operation detail 保留受大小、深度和敏感 key 约束的 controlled-open document。
- 业务 String/Uuid 替换为 newtype/ActiveEnum，包括 correlation、role、operation action、hash、worker、schedule、policy/research profile、diagnostic、cursor、HTTP method 与 artifact/evidence IDs。
- 集中 registry/macro 生成 ID/value conversion、Config resource mapping、profile dispatch、FSM 约束和 system contract versions。
- 删除迁移/删除后的 compatibility `pub use` shim；允许作为正式模块 API 的 canonical public barrel，lint 对旧路径和旧语义转发 fail closed。
- 删除 `.schema-helper`，合法能力迁入 `quant-pivot-xtask`。

## 6. 查询、SQL 与契约

- CRUD、join、aggregate、upsert 和可表达 DDL 只使用 SeaORM/SeaQuery。
- raw SQL 仅允许 catalog/admin/advisory-lock、ordered-set percentile、ClickHouse typed renderer 和 test-only corruption；统一进入 typed dialect module 和 exception registry。
- Config resources 使用一次 DB-authoritative projection；activity 使用 `UNION ALL + global ORDER/LIMIT`；snapshot options 使用独立单查询 endpoint。
- 1:1 使用 `DerivePartialModel`/join，1:N 使用 `LoaderTrait`，关键路径有 statement-count budget。
- schemars → contract JSON → generated TypeScript 加入 regenerate-and-diff gate。
- inventory 记录 immutable before baseline、canonical metadata、source/consumer/test 和迁移/删除 tombstone，并双向校验。

## 7. UI、文档与规则

- 修复 lifecycle markup 和 seal response 行为。
- Config Playwright 状态矩阵覆盖原计划 20 个状态、validation/CAS/rollback/frozen/read-only/recovery/ExecutionAuthorization/1024 overflow，并覆盖 1440/1280/1024/390、light/dark、reduced motion、axe、keyboard/focus 与视觉 snapshot。
- `docs/persistence/seaorm-and-typed-persistence.md` 是 canonical persistence 范式。
- `.cursor/rules/quant-pivot-persistence.mdc` 和项目 `AGENTS.md` 强制引用该范式。
- 删除或修订旧 runtime、secret env、明文 TOML、旧 editor 和旧 entity-generation 文档。

## 8. Fresh Boot 与验收

- 实现 `preproduction-reset plan|apply|verify`，验证 preproduction、baseline absent 并获取 lifecycle lease。
- reset 仅作用于 PostgreSQL `quant_pivot`、ClickHouse `quant_pivot` 和 resolved `qp:` Redis namespace；其他 DB、role、container 和 key 保留。
- Redis 使用 `SCAN + UNLINK`，禁止 `FLUSHDB`；命令要求一次性确认 nonce，输出仅包含脱敏 fingerprint 和对象计数。
- 凭证先轮换：不读取、复制或复用旧 secret；新凭证使用 0600 credential files，ClickHouse Docker 使用 secrets mount。
- 完成空环境 migration/seed、启动/重启、Config governance E2E 和真实账户 `ReportOnly` smoke。
- production seal 只在 disposable environment 自动化验证；本地验收环境不执行不可逆 freeze。

## 9. 技术基线

- SeaORM 固定在当前官方最新 `2.0.0-rc.43`。
- 使用 Entity First、ActiveEnum、`DeriveValueType`、`FromJsonQueryResult`、Nested PartialModel、Entity Loader 和显式事务。
- 金额、价格和份额继续使用 `rust_decimal` newtypes，禁止 `f64`。
- 所有安全、资金、credential、schema mutation 和 runtime governance 路径默认 fail closed。

## 10. JSONB 语义建模决策（强制）

JSONB 不是默认存储类型，也不能通过 `payload: serde_json::Value` 的通用 envelope 冒充强类型。每个字段必须先根据生产者、消费者、hash、查询和演进语义分类：

| 分类 | 落地范式 | 当前字段 |
|---|---|---|
| 固定且封闭、整体读写 | 直接使用具名结构体；字段使用 Decimal/newtype/ActiveEnum/typed ID；仅在确认是原子文档后 derive `FromJsonQueryResult` | backtest expected-vs-realized/category/PnL、CPCV distribution/path、factor explanation、feature payload/source refs/decision capture、comparison/shadow metrics |
| 由已有 discriminator 决定且集合封闭 | `#[serde(tag = "kind")]` enum；每个 variant 使用独立结构体；DB CHECK 保证关系列 discriminator 与 JSON tag 一致 | calibration payload、research-job params、model metrics/training objective、governance/audit detail |
| 需要过滤、排序、FK、唯一性、独立生命周期或局部更新 | 规范化为列/子表，不保留 JSONB 镜像 | research/policy profile references、可查询 lineage/identifier、lifecycle/CAS 字段 |
| 无独立 identity 的同质 scalar 集合 | PostgreSQL native array + typed element/newtype；需要反向关系或 FK 时升级为关系表 | domain-source expectation 的 affected market/profile IDs |
| 项目无法控制的外部原始输入 | 仅允许 `ExternalJsonDocument`；禁止业务逻辑依赖其内部 shape | catalog event/market/rejection、CLOB raw payload |

逐字段规则：

1. 固定 key 的内部权威数据零容忍用 `serde_json::Value`、`HashMap<String, Value>` 或通用 payload envelope 逃避建模；固定 key 本身不自动决定“拆列”或“JSONB”，仍由查询、约束、原子性和生命周期决定。明确登记的 controlled-open 非权威审计摘要不适用 closed-document 规则，但不得参与业务状态重建。
2. JSONB 内的金额/概率/ID/状态继续使用业务类型，而不是 string/number 魔法值。
3. typed document 必须覆盖 DB round-trip、unknown field、非法 enum、错误 kind、损坏 JSON 和 hash mismatch。
4. 能拆成关系模型并被查询/约束的数据优先拆表；JSONB 仅用于一个聚合根内原子读取的固定值对象。
5. compute 类型与 persistence 类型只能有一个 canonical 定义；共享类型下沉到 `quant-pivot-models`，禁止跨 crate 复制同构结构后用 serde 转换。

## 11. 当前验收证据与剩余现场门禁

2026-07-20 在当前工作树完成以下无凭证或 disposable 验收：

- `cargo fmt --all --` 通过。
- `cargo clippy --workspace --all-targets -- -D warnings` 通过；Config activity 稳定排序查询通过拆分 typed SeaQuery builder 降低复杂度，没有 lint suppression。
- `cargo test --workspace` 通过；唯一 PG migration manifest 断言通过。
- `cargo run -p quant-pivot-xtask -- test-docker` 以单次完整 registry 运行通过，覆盖 PG migration/catalog/repository、Redis、ClickHouse、Core E2E 和 73 个 Web Docker tests。
- Config publication 状态机覆盖 generation 单调性、exact idempotent publish、旧 generation 忽略和同 generation 不同 identity/hash fail closed；restart、commit 后 publish 前崩溃与延迟实例通过 DB-authoritative bundle 恢复收敛。
- Config activation Docker 测试通过 test-only corruption 强制 outbox 最终写入失败，并证明 activation、snapshot、guard、audit、outbox 和 approval consumption 整体回滚；正常路径显式断言 audit/outbox 与 activation 同事务落账。
- architecture、import style、boundary、typed error、dead semantics、ClickHouse correctness、training-serving parity、phase lifecycle、Config inventory、SeaORM persistence、secret boundary 和 UI semantic-color lint 全部通过。
- UI lint、45-package typecheck、Config contract regenerate-and-diff、69 files / 459 unit tests、production build 通过。
- Config Playwright suite 使用 24-state manifest，覆盖 20 个原始状态以及 validation/CAS/rollback/frozen/read-only/recovery/ExecutionAuthorization/1024 overflow，并执行 axe、focus/keyboard、light/dark、reduced motion 与视觉 snapshot。
- `git diff --check` 在主仓库和 UI 子模块均通过；工作树审计未发现新增 plaintext secret、`.env`、credential 或 keystore 文件。

以下门禁明确未完成，因此本阶段仍不能 production freeze/seal：

1. 用户尚未确认 wallet/private key、relayer、JWT/RPC、PG/CH 等实际启用凭证已全部轮换并以 0600 credential files 安装；实现过程未读取、复制或复用旧 secret。
2. 因上述前置条件未满足，尚未对本地目标执行 `preproduction-reset apply`，也未执行 reset 后真实启动/重启与 live-account `ReportOnly` smoke。
3. 最终现场 smoke 必须证明 account truth、positions/collateral 和 RecommendationReport 正确，并证明没有签名、订单提交或新增 `OrderIntent`；完成前 `ACCEPT-02` 保持 blocked。
