# Persistence Schema Catalog

本文档是 `quant-pivot` 后续所有 Postgres 表结构、索引、trigger、seed 变更的统一范式。

schema catalog 是唯一事实源。storage migrations 只消费 catalog metadata，禁止在 migration 里手写业务表字段、业务索引、trigger 表名列表或 seed 排序。

## 核心规则

- 每个 iden enum 必须使用 `#[quant_schema]`，禁止裸写 `#[derive(DeriveIden)]`。
- 非 core 表必须显式声明 lifecycle，例如 `#[quant_schema(lifecycle = "control")]` 或 `#[quant_schema(lifecycle = "audit")]`。未声明时只允许作为 core schema。
- 表 DDL、索引、依赖、trigger、seed specs 都放在该表的 `idens/<table>.rs` schema module 中。
- 不为 schema API 添加兼容 re-export。调用方必须使用明确模块路径。
- 如果表需要 `UpdatedAt`，只在 enum 中声明 `UpdatedAt` variant，并在 `table()` 中使用 `timestamp_with_write_default(UpdatedAt)`。trigger metadata 会自动生成。
- seed 依赖必须声明在 `SeedSpec` 中，禁止把 graph dependency 藏在 loader 函数体里。
- 依赖上游 seed 输出时，loader 必须使用 `ctx.require<T>()?`，禁止 `unwrap()`。

## 列类型规范（强制）

所有列类型必须通过 `crate::schema::column` 的 builder 声明，禁止在 iden 中手写 `.text()` / `.decimal()` 作为标识列或金额列。

### 标识符三分法

```text
是 UUID（系统内部生成）？ ── 是 ──> 原生 uuid 列；Rust = #[derive(UuidId)] (Uuid, 16-byte Copy)
        │
        否
        │
是外部定义的语义字符串？ ── 是 ──> text / varchar 列；Rust = #[derive(StrId)] (Arc<str>)
        │
        否
        │
仅为高频插入的代理键？ ──────────> bigint / integer 自增；无独立 Rust ID 类型
```

| 家族 | 例子 | Postgres 列类型 | builder |
|---|---|---|---|
| 内部 UUID | `TradeId` `UserId` `OpportunityId` `ControlFactorId` `ResolutionEventId` | `uuid` | `column::uuid_pk` / `uuid_fk` / `uuid_null` |
| 外部字符串 | `MarketId`（`condition_id`） | `varchar(66)` | `column::market_id_pk` / `market_id` |
| 外部字符串 | `TokenId`（CLOB 十进制） | `text` | `column::token_id` / `token_id_null` |
| 外部字符串 | `EventId` `OrderId` `ReportId` | `text` | 直接 `.text()`（无定长语义） |
| 代理键 | `casbin_rule.id`、审计/报告行、`risk_engine_state.id` | `bigint` / `integer` | 行内声明 |

- **内部 ID 一律用原生 `uuid`**（16 字节定长），不得用 `text` 存 UUID，不得加 `prefix_` 前缀。命名空间安全由 Rust newtype 提供，可读性由结构化日志（`trade_id=%`）提供。
- **联结表用复合主键**（如 `user_role` 的 `(user_id, role_id)`），禁止为联结行引入无业务意义的代理 UUID/BIGINT 主键。
- UUID 由应用层生成：时间有序行用 `XxxId::from_v7()`（保持 B-tree 紧凑），纯随机行用 `XxxId::from_v4()`。DDL 不设 UUID 默认值，禁止空字符串 sentinel（用 `Option<XxxId>` 表达"未赋值"）。

### 金额列

金额一律用原生 `NUMERIC(precision, scale)`，禁止用 `TEXT` 存金额。`NUMERIC` 值域完全覆盖 `rust_decimal::Decimal`，round-trip 无损（workspace 已启用 `sea-orm/with-rust_decimal`）。

| newtype | NUMERIC | builder |
|---|---|---|
| `Usd` | `(28, 8)` | `column::usd` / `usd_null` / `usd_default_zero` |
| `Price` | `(20, 18)` | `column::price` |
| `Shares` | `(38, 18)` | `column::shares` |
| `Bps` | `(10, 4)` | `column::bps_null` |
| `Probability` | `(20, 18)` | `column::probability` / `probability_null` / `probability_default_one` |

精度由每个 newtype 的 `PRECISION` 常量统一定义；新增金额列必须复用上述 builder，不得自写 `default_zero_*` 私有 helper。

### 定长字符串与 CHECK

- `char(n)` **禁止**：Postgres 中 `char(n)`/`varchar(n)`/`text` 索引性能一致，`char(n)` 仅因 padding 浪费空间。
- `varchar(n)` 仅用于**语义长度约束**（如 `market_id` 固定 66 字符，`username` ≤ 64，`role.code` ≤ 32），非性能优化。
- `market_id` / `token_id` **不加格式正则 CHECK**：dry-run / paper 模式会持久化合成 id（非 `0x…` 格式），DB 级正则会误拒合法的非 live 行。格式校验留在类型层（`TokenId::debug_validate`）。

## 新增一张表

1. 新增 `crates/quant-pivot-models/src/idens/<table>.rs`。
2. 在 `crates/quant-pivot-models/src/idens/mod.rs` 添加 module。
3. 在 iden enum 上使用 `#[quant_schema]`。
   - control registry 表使用 `#[quant_schema(lifecycle = "control")]`。
   - append-only audit 表使用 `#[quant_schema(lifecycle = "audit")]`。
4. 实现 `table() -> TableCreateStatement`。
5. 实现 `indexes() -> Vec<IndexSpec>`。
6. 实现 `dependencies() -> Vec<TableDependency>`。
7. 实现 `seed_units() -> Vec<SeedSpec>`。
8. 新增 SeaORM entity 和 domain DTO。
9. 如果业务代码会访问该表，新增 repository 方法。
10. 跑 schema graph tests 和 Postgres migration tests。

## 修改表结构

首次正式部署前：直接修改 schema module，并让 initial schema lane 重新消费 catalog。

正式部署后：

- 禁止修改已经发布的 initial migrations。
- 新增 `alter_*` migration lane。
- 大表变更必须拆成 expand / data / contract 阶段。
- 新列优先 nullable 或提供安全 DB default。
- 昂贵约束必须分阶段添加和校验。

## 新增或修改索引

- 在该表 schema module 的 `indexes()` 中添加 index metadata。
- greenfield 初始 schema 或小表使用 `IndexBuildMode::Transactional`。
- 已上线热表新增索引必须使用 `IndexBuildMode::Concurrent`。
- raw partial index 必须写清 predicate SQL 和用途说明。
- 禁止在 storage migration 文件里直接手写业务索引。

## Seed 数据

seed metadata 必须通过相关表的 schema module 暴露。

静态 seed：

- 定义稳定的 `SeedSpec`，包含 `id`、`version`、`checksum`、`conflict_policy`。
- operator-owned 数据，例如 governed policy revision，必须使用 no-clobber 策略。
- loader SQL 必须确定、幂等。

六类 policy revision 的持久化类型是
`quant_pivot_models::runtime_config::PolicyDocument`：闭集 enum 承载六个强类型 struct，
每个资源固定 `schema_version = 1` 并拒绝 unknown fields。runtime entity 的 `JsonBinary`
字段由 `cargo xtask architecture check` 自动发现，并要求 canonical typed document、
`FromJsonQueryResult` 与闭合 serde shape；数据库表示与约束由 fresh-boot schema verification
和 repository system tests 验证。`JSONB` 只作为不可变聚合的
物理存储，不以 `serde_json::Value` 穿越 repository/domain 边界，且任何可查询字段都
必须拆为原生列或 PostgreSQL enum。bootstrap 只播种首个 boot revision bundle；之后
所有变更只通过 Draft → Validate/Preflight → Approve → Activate 写入，TOML 永不覆盖
policy 表。激活时 `PolicySnapshotApplicator` 先 prepare 全部强类型 consumer snapshot，
数据库 CAS 成功后再原子 publish 到 `DecisionPolicyStore`；任一 prepare/CAS 失败均保持
旧 snapshot，不做隐式自动回滚。

依赖型 graph seed：

- 在 `depends_on` 中声明所有上游 artifact。
- 在 `produces` 中声明本 seed 产出的 artifact。
- 用 `SeedContext::put` 保存上游输出。
- 用 `SeedContext::require` 读取下游输入。
- 返回结构化错误，禁止 `unwrap()`。

依赖形态示例：

```text
RoleSeed
UserSeed   -> RelationSeed
MenuSeed
```

## 需要 `updated_at`

1. 在 iden enum 中添加 `UpdatedAt`。
2. 在 `table()` 中添加 `.col(crate::schema::timestamp_with_write_default(Table::UpdatedAt))`。
3. 不写 trigger 注册代码。
4. 不在 storage migration 里添加表名。
5. 不在应用层 update 路径手动设置 `updated_at`。

## Down 和测试

- down 顺序由 catalog 的 reverse topological order 生成。
- seed down 默认 no-op。
- data migration down 默认禁止，除非该 data migration 明确可逆。
- 每个 schema 变更必须通过 macro tests、schema graph tests、seed graph tests 和 Postgres migration tests。

## 完整 Schema Module 示例

```rust
use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement,
    },
};

use crate::{
    enums::{common::TickSize, market::MarketStatus},
    idens::event::Event,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[quant_schema]
pub enum Market {
    Table,
    MarketId,
    EventId,
    Question,
    Slug,
    Categories,
    Status,
    Outcome,
    YesTokenId,
    NoTokenId,
    TickSize,
    NegRisk,
    EndDate,
    ResolvedAt,
    FeesEnabled,
    FeeRate,
    FeeExponent,
    FeeTakerOnly,
    FeeRebateRate,
    FeeSource,
    FeeObservedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Market::Table)
        .if_not_exists()
        .col(column::market_id_pk(Market::MarketId))
        .col(ColumnDef::new(Market::EventId).text().not_null())
        .col(ColumnDef::new(Market::Question).text().not_null())
        .col(ColumnDef::new(Market::Slug).text().not_null())
        .col(
            ColumnDef::new(Market::Categories)
                .array(ColumnType::Text)
                .not_null()
                .default(Expr::cust("'{}'::text[]")),
        )
        .col(
            ColumnDef::new(Market::Status)
                .text()
                .not_null()
                .default(MarketStatus::Active),
        )
        .col(ColumnDef::new(Market::Outcome).text().null())
        .col(column::token_id(Market::YesTokenId))
        .col(column::token_id(Market::NoTokenId))
        .col(
            ColumnDef::new(Market::TickSize)
                .text()
                .not_null()
                .default(TickSize::Hundredth),
        )
        .col(ColumnDef::new(Market::NegRisk).boolean().not_null().default(false))
        .col(ColumnDef::new(Market::EndDate).timestamp_with_time_zone().null())
        .col(ColumnDef::new(Market::ResolvedAt).timestamp_with_time_zone().null())
        .col(ColumnDef::new(Market::FeesEnabled).boolean().not_null().default(true))
        .col(ColumnDef::new(Market::FeeRate).decimal().null())
        .col(ColumnDef::new(Market::FeeExponent).decimal().null())
        .col(ColumnDef::new(Market::FeeTakerOnly).boolean().null())
        .col(ColumnDef::new(Market::FeeRebateRate).decimal().null())
        .col(ColumnDef::new(Market::FeeSource).text().null())
        .col(ColumnDef::new(Market::FeeObservedAt).timestamp_with_time_zone().null())
        .col(crate::schema::timestamp_with_write_default(Market::CreatedAt))
        .col(crate::schema::timestamp_with_write_default(Market::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_market_event")
                .from(Market::Table, Market::EventId)
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_markets_event_id",
            market_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_markets_event_id")
                .table(Market::Table)
                .col(Market::EventId)
                .to_owned(),
            "market lookup by event",
        ),
        IndexSpec::raw(
            "idx_markets_active_endgame",
            market_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_markets_active_endgame \
             ON market (end_date) \
             WHERE status = 'active' AND end_date IS NOT NULL",
            "scanner hot path for active endgame candidates",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(event_table_name)]
}

pub fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
```

说明：

- `#[quant_schema]` 注入 `DeriveIden` 并注册 `TableSpec`。
- `UpdatedAt` trigger 自动生成。
- `dependencies()` 是 create/drop 拓扑排序来源。
- `indexes()` 是定义该表业务索引的唯一位置。
- `seed_units()` 拥有该表相关 seed metadata。
