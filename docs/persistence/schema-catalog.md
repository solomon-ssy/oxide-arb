# Persistence Schema Catalog

本文档定义 `quant-pivot` 当前 PostgreSQL/ClickHouse schema 的唯一 fresh-bootstrap 维护规则。项目从未
生产运行；当前只维护一个终态 PostgreSQL v1 bootstrap snapshot 和一个无版本链的 ClickHouse
bootstrap。不存在旧数据导入、升级/降级链、`alter_*` lane、dual read/write、compatibility view、
历史 payload converter 或 data migration。

## 1. Canonical owners

| Contract | Canonical owner |
|---|---|
| PostgreSQL runtime shape | `crates/quant-pivot-models/src/entities/` dense SeaORM entities |
| PostgreSQL fresh-bootstrap time capsule | `crates/quant-pivot-migration/src/snapshots/v1/` |
| Entity First 无法表达的 enum/CHECK/index/trigger | `crates/quant-pivot-migration/src/migrations/support/` typed specs |
| PostgreSQL normalized evidence | `schema/postgres/manifest.json` and `schema/postgres/migrations.json` |
| ClickHouse fresh bootstrap | `crates/quant-pivot-storage/src/clickhouse/` canonical schema owner |
| ClickHouse normalized evidence | `schema/clickhouse/manifest.json` |

`quant-pivot-migration`、SeaORM `MigrationTrait` 和 manifest 中保留的 migration 命名只是唯一 bootstrap
的现有实现/API 名称，不代表存在可执行的版本升级链。应用 runtime 只验证 schema，禁止启动时 DDL。

## 2. Core persistence rules

- 内部 UUID-backed ID 使用原生 PostgreSQL `uuid` 与项目 `Copy` newtype；不得存为字符串或
  `Arc<Uuid>`。
- 外部 venue identifier 使用经过边界校验的字符串 newtype。`MarketId`/`TokenId` 是 opaque venue
  identity；数据库不复制 source-specific 格式 regex，格式校验留在解析/类型边界。
- money、price、shares、bps 和 probability 使用项目 Decimal newtype 与原生 `NUMERIC`；禁止
  `f64` 或文本金额。
- 需要 SQL 查询、排序、约束、FK、CAS 或独立生命周期的事实使用具名列/关系表。
- domain-owned JSONB 只能使用 canonical typed struct/tagged enum、`deny_unknown_fields` 与
  `FromJsonQueryResult`；禁止裸 `Json`/`serde_json::Value` 穿越 persistence/domain 边界。
- WORM、idempotency、FK、unique、lifecycle 与 hash invariants 必须由数据库和 repository
  round-trip/corruption tests 共同证明。
- DDL 只存在于 deploy-only bootstrap owner；repository、handler 和 runtime startup 禁止手写 DDL。

完整 SeaORM/typed persistence 规则见
[`seaorm-and-typed-persistence.md`](seaorm-and-typed-persistence.md)。

## 3. Changing the schema before first production deployment

任何表、列、enum、index、constraint、trigger 或 seed 变化都直接修改唯一终态：

1. 修改 runtime entity/domain persistence DTO/repository contract。
2. 同一变更窗口更新 v1 bootstrap snapshot 与无法由 Entity First 表达的 typed support specs。
3. 从全新 owned disposable PostgreSQL 16 / ClickHouse 环境生成 normalized manifests。
4. regenerate-and-diff 必须达到 fixed point；禁止手工修补 manifest 来掩盖 owner 漂移。
5. 运行 repository/system behavior tests，证明约束、事务、WORM、idempotency 与 typed decode。

禁止为了保留本地开发/测试旧库而新增第二 bootstrap、`ALTER`/data/down step、nullable compatibility
column、旧 reader、dual writer 或版本分派。需要继续验证时，创建新的 disposable 空基础设施并从唯一
bootstrap 重建。任何真实数据库销毁仍需操作者对精确目标另行授权。

## 4. Indexes, triggers, and seeds

- index、partial predicate、expression、WORM trigger 和 lifecycle function 必须由封闭 typed spec
  拥有，不能散落在 repository/native SQL 中。
- 当前只有 fresh bootstrap，因此 index 直接以终态形状创建；不存在“已上线热表在线加索引”实施路径。
- static seed 必须确定、可验证并由唯一 bootstrap 创建；governed policy 只播种首个 safe boot bundle。
- boot 后的 policy 变化只走 Draft → Validate/Preflight → Approve → Activate，不由 TOML 或 schema
  bootstrap 覆盖。
- seed dependency、checksum、idempotency 与 audit 必须由 compiled bootstrap contract 和 system tests
  证明；不存在 seed down 或数据搬运。

## 5. Verification

Canonical local gates:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo xtask postgres-schema manifest-clean
cargo xtask clickhouse-schema manifest
cargo test --workspace
```

使用真实配置执行 `postgres-schema apply|verify|manifest` 或 `clickhouse-schema bootstrap|verify` 时，必须
同时提供 absolute `--config-file` 与 exact `--expected-environment`。这些命令只允许在空或与唯一
bootstrap 精确一致的目标上运行；发现未知 history、未知对象或 fingerprint/checksum 漂移即 fail closed。

## 6. Review checklist

- 是否只有一个 PostgreSQL v1 snapshot owner 和一个无版本链的 ClickHouse bootstrap owner？
- entity、bootstrap snapshot、typed support spec 与 normalized manifest 是否 fixed point？
- 是否引入了任何 upgrade/downgrade/data migration、兼容列、旧 reader、dual write 或版本分派？
- money/ID/document 是否使用 canonical newtype/typed persistence contract？
- 数据库约束与 repository transaction 是否表达完整业务不变量？
- corruption、unknown field/tag、hash mismatch、CAS race、WORM update/delete 是否有真实测试？
