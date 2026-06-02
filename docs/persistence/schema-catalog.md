# Persistence Schema Catalog

本文档是 `oxide-arb` 后续所有 Postgres 表结构、索引、trigger、seed 变更的统一范式。

schema catalog 是唯一事实源。storage migrations 只消费 catalog metadata，禁止在 migration 里手写业务表字段、业务索引、trigger 表名列表或 seed 排序。

## 核心规则

- 每个 iden enum 必须使用 `#[oxide_schema]`，禁止裸写 `#[derive(DeriveIden)]`。
- 非 core 表必须显式声明 lifecycle，例如 `#[oxide_schema(lifecycle = "control")]` 或 `#[oxide_schema(lifecycle = "audit")]`。未声明时只允许作为 core schema。
- 表 DDL、索引、依赖、trigger、seed specs 都放在该表的 `idens/<table>.rs` schema module 中。
- 不为 schema API 添加兼容 re-export。调用方必须使用明确模块路径。
- 如果表需要 `UpdatedAt`，只在 enum 中声明 `UpdatedAt` variant，并在 `table()` 中使用 `timestamp_with_write_default(UpdatedAt)`。trigger metadata 会自动生成。
- seed 依赖必须声明在 `SeedSpec` 中，禁止把 graph dependency 藏在 loader 函数体里。
- 依赖上游 seed 输出时，loader 必须使用 `ctx.require<T>()?`，禁止 `unwrap()`。

## 新增一张表

1. 新增 `crates/oxide-arb-models/src/idens/<table>.rs`。
2. 在 `crates/oxide-arb-models/src/idens/mod.rs` 添加 module。
3. 在 iden enum 上使用 `#[oxide_schema]`。
   - control registry 表使用 `#[oxide_schema(lifecycle = "control")]`。
   - append-only audit 表使用 `#[oxide_schema(lifecycle = "audit")]`。
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
- operator-owned 数据，例如 `runtime_config`，必须使用 no-clobber 策略。
- loader SQL 必须确定、幂等。

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
use oxide_arb_macros::oxide_schema;
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
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[oxide_schema]
pub enum Market {
    Table,
    MarketId,
    EventId,
    Question,
    Slug,
    Category,
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
        .col(ColumnDef::new(Market::MarketId).text().not_null().primary_key())
        .col(ColumnDef::new(Market::EventId).text().not_null())
        .col(ColumnDef::new(Market::Question).text().not_null())
        .col(ColumnDef::new(Market::Slug).text().not_null())
        .col(ColumnDef::new(Market::Category).text().not_null())
        .col(
            ColumnDef::new(Market::Status)
                .text()
                .not_null()
                .default(MarketStatus::Active),
        )
        .col(ColumnDef::new(Market::Outcome).text().null())
        .col(ColumnDef::new(Market::YesTokenId).text().not_null())
        .col(ColumnDef::new(Market::NoTokenId).text().not_null())
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

- `#[oxide_schema]` 注入 `DeriveIden` 并注册 `TableSpec`。
- `UpdatedAt` trigger 自动生成。
- `dependencies()` 是 create/drop 拓扑排序来源。
- `indexes()` 是定义该表业务索引的唯一位置。
- `seed_units()` 拥有该表相关 seed metadata。
