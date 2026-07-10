# DTO 与 API 契约范式

本文档是 `quant-pivot` 后续所有「领域 DTO」与「HTTP API 契约类型」的统一范式，是这两层类型的唯一事实源。

它向上承接 [`docs/persistence/schema-catalog.md`](../persistence/schema-catalog.md)：schema-catalog 定义表结构 / 索引 / seed，本文档定义建立在表之上的读写 DTO 与对外契约类型。两份文档共同描述一条完整的纵向数据链路。

## 纵向数据链路

```text
idens/<table>.rs        表结构 / 索引 / seed          ← schema-catalog.md
entities/<table>.rs     SeaORM Entity / Model        （由表结构派生）
domain/<ctx>/<x>.rs     持久化 DTO：*Info / New* / *Patch
domain/api/<x>.rs       API 契约：*Request / *Query / *View / *Response
routes/<x>.rs (web)     handler：校验 → 翻译 → 持久化 → 投影
```

每个资源（user、role、menu …）都贯穿这五层。**同一个领域概念在不同层用不同类型表达，这是分层而非重复。**

## 三类 DTO 的职责边界（不可合并）

| 关注点 | `*Request` / `*Query`（入站契约） | `*Info` / `New*` / `*Patch`（持久化） | `*View` / `*Response`（出站契约） |
|---|---|---|---|
| 所在模块 | `domain/api/` | `domain/<ctx>/` | `domain/api/` |
| serde 方向 | `Deserialize` | 双向（read 模型） | `Serialize` only |
| 校验 | `#[derive(Validate)]` + `#[validate(..)]` | 无 | 无 |
| ORM 耦合 | 无 | `DeriveIntoActiveModel` / `DerivePartialModel` / `FromQueryResult` | 无 |
| null 语义 | serde 三态（`serde_with::rust::double_option` → `Option<Option<T>>`） | 类型化 `Patch<T>` / `NullablePatch<T>` | 不适用 |
| 敏感字段 | 携带明文凭证（如 `password`） | 携带哈希列（如 `password_hash`） | **必须剥离**（如不含 `password_hash`） |

> **入站契约统一收口**：一个资源对外暴露的**全部** HTTP 契约（`*Request` 入站 body、`*PageQuery` / `*WindowQuery` 入站 query string、`*View` / `*Response` 出站投影）都集中在 `domain/api/<resource>.rs`。`domain/<ctx>/` 只保留与表一一对应的纯持久化 DTO（`*Info` / `New*` / `*Patch`）。`*Query` 虽被 repository 的 `page()` 读方法消费，但它本质是"外部不可信的查询入参契约"，因此归 `domain/api/`，与其它入站契约同处一地，便于发现与后续 OpenAPI 生成。

合并会直接破坏三条安全 / 解耦不变量：

- **凭证边界**：`CreateUserRequest.password`（明文）在 handler 内经 `hash_password` 变成 `NewUser.password_hash`（哈希）。合并意味着明文入库或哈希出网。
- **校验归属**：`#[validate]` 是对「外部不可信输入」的契约，不属于内部已可信的持久化 DTO。
- **null 表示**：HTTP 层用 `Option<Option<T>>` 区分「缺省 / 置空 / 赋值」，持久层用 `Patch` / `NullablePatch` 直接映射 `ActiveValue`。`From<UpdateXRequest> for XPatch` 正是两种表示之间的翻译器，它的存在本身就证明这是「两层之间的转换」。

## 放置决策（已采纳）

API 契约类型（`*Request` / `*View` / `*Response`）**留在 `quant-pivot-models` 的 `domain::api` 模块**，与持久化 DTO 同一个 crate。

- 已知代价：`quant-pivot-models` 因此依赖 `validator`，且当前唯一消费者是 `quant-pivot-web`。这是被接受的取舍，目标是把一个资源的「请求 / 持久化 / 响应」契约集中在单一可发现位置，便于后续接入 OpenAPI 生成。
- 触发重新评估的条件：出现 `quant-pivot-web` 之外的第二个契约消费者（如独立 CLI client）。届时应把 `domain::api` 抽成独立的 `quant-pivot-contract` crate，而非散落进 web。
- 在重新评估之前，**不要**把 `*Request` / `*View` 移进 `quant-pivot-web`，保持本范式一致。

## 持久化 DTO 范式（`domain/<ctx>/`）

承接 schema-catalog 中已声明的表，为其建立读写 DTO。

### 读模型 `*Info`

- DB 行投影，由 Repository `find_*` / `page` 返回。
- 字段与表列对齐时优先 `#[derive(DerivePartialModel, FromQueryResult)]` + `#[sea_orm(entity = "...")]`；insert-return 路径另需 `From<Model>`，用 `info_from_model!` 宏生成。
- 可以携带敏感列（如 `password_hash`），因为 repository 内部路径需要；出网前必须经 `*View` 投影剥离。
- **允许不可变 N:1 身份字段**（例如 `ModelVersionInfo.model_family` 来自 owning `quant_model_spec` 的 JOIN 投影）。这类字段：
  - **可以**出现在读模型 `*Info` 上（repository 必须 JOIN-fill，不得依赖 handler 二次查询）；
  - **不得**出现在 `New*` / `*Patch` 上（写入仍只对主表列；身份列以父行 / FK 为唯一事实源）。

### 写 DTO `New*` / `*Patch`

- `New*`：插入载荷，`#[derive(DeriveIntoActiveModel)]` + `#[sea_orm(active_model = "...")]`。DB 托管的写时间戳（`created_at` / `updated_at`）一律省略，由 Postgres default / trigger 充当唯一事实源。
- `*Patch`：部分更新，非空列用 `Patch<T>`，可空列用 `NullablePatch<T>`，让「保持 / 设置 / 清空」三种写意图显式化。
- **不可变列与敏感转换不进 `*Patch`**：凭证、`status` 等敏感转换必须各自走专用的单一职责方法（如 `change_password` / `change_status`），避免被通用列更新静默绕过。被排除的字段要在 doc 注释里写明原因。

### 硬性约束

- 任何公开签名 / 字段**禁止**出现 `ActiveModel` / `ActiveValue`。
- **禁止** `to_active_model()` 方法——交给 `DeriveIntoActiveModel`。
- Repository 写动词只有五个：`create` / `create_batch` / `update` / `upsert` / `upsert_batch`；写方法只接收 `New*` / `Upsert*`，绝不接收 `*Info`。
- 命名固定：DB 读模型一律 `*Info`，运行时聚合 `*State`，审计冻结 `*Snapshot`。

## API 契约范式（`domain/api/`）

一个资源对外的全部 HTTP 契约都住在这里：入站 body（`*Request`）、入站 query（`*Query`）、出站投影（`*View` / `*Response`）。

### 入站查询 `*Query`

- 分页 + 过滤参数（如 `MarketPageQuery`），或时间窗口参数（如 `TimeWindowQuery`）。分页类复用共享的 `PageRequest`，用 `#[serde(flatten)]` 保持 query string 扁平（`?keyword=&status=&page=&size=`）。
- **三层分页职责**：
  1. **Wire** — `PageRequest` 承载不可信 `page`/`size`。
  2. **Contract** — `#[normalize_page]` + `#[derive(NormalizePageQuery)]`（sealed trait，`quant-pivot-macros`）提供 `normalized(self)`；需 server-side enrich 的 query 用具名 domain 方法（如 `MarketPageQuery::prepare()`，与 `resolve()` 同级）。**禁止手写**等价 impl。
  3. **SQL 边界** — `PageWindow::from_query` / `PageWindow::harden` 是 `paginate_mapped` 唯一接受的窗口类型；分页 clamp 逻辑唯一实现在 `PageRequest::normalized()`。
- 时间窗口类提供 `resolve(...)`，把 `from`/`to`/默认回看/最大跨度等校验收敛在契约类型内，返回**领域错误**（不依赖 `quant-pivot-web` 的 `WebError`），web 侧通过 `From<_> for WebError` 翻译。
- `*Query` 是 repository 读方法（`page` / 窗口查询）的入参契约；尽管被持久层消费，它仍是"外部不可信入参"，归 `domain/api/`。
- 跨资源复用的读过滤领域类型（`TimeWindow` / `MarketFilter`）住在 `domain/query.rs`，被 `domain/api` 的 `*Query::resolve()` 与 repository 读方法共用。

**Forbidden（分页）**：repository 内 `query.normalized()`；`paginate_mapped(..., &PageRequest)`；`Paginated::from_request`；业务代码 `query.page.normalized()` 绕过 trait；handler 内裸 mutate enrich（用 `prepare` / `resolve`）。

#### 跨表分页

列表需要 N:1 附属列（如 version → spec.`model_family`）时，在 repository SQL 边界完成 JOIN，再分页：

| 助手 | 用途 |
|---|---|
| `paginate_mapped` | 单实体行 → 映射为 `*Info` |
| `paginate_into_model` | 自定义 `FromQueryResult` 投影（含 N:1 `JOIN` + `column_as`） |

硬性规则：

- **仅允许 N:1** `INNER JOIN` 过滤 / 投影（一主行对应一 join 行，`COUNT` / page 不膨胀）。
- **禁止 1:N join 分页** — `total` 与页内容会重复膨胀。
- **禁止** handler 对分页结果做 per-row `find_*` 富化（N+1）；身份 / 展示列必须由 repository JOIN-fill 进 `*Info`，再 `From` 到 `*View`。
- `total` 始终来自主 paginator，不得用第二次 enrich 查询回填。

### 入站 `*Request`

- `#[derive(Debug, Deserialize, Validate)]`，所有外部约束以 `#[validate(length / email / range ...)]` 表达。
- 部分更新字段用 `#[serde(default, with = "double_option")]`（`serde_with::rust::double_option`）表达 null 三态，对应 `Option<Option<T>>`。
- 可省略字段用 `Option<T>` + `#[serde(default)]`；默认值（如 `status` 缺省为 `Active`）在 handler 内补齐，不在 `Request` 内写死。
- 与持久化 DTO 的差异通过 `From` / `TryFrom` 翻译：`From<UpdateXRequest> for XPatch` 用 `Patch::from_option` / `NullablePatch::from_nested_option` 桥接。

### 出站 `*View` / `*Response`

- `#[derive(Debug, Serialize)]`，single-direction。
- **必须剥离敏感列**：`UserView` 不含 `password_hash`。
- 由 `*Info` 投影而来，用 `From<XInfo> for XView`（或 `From<&XInfo>`，当聚合响应需要复用 borrow 时）。
- 聚合响应（如 `MeResponse`）组合多个 `*View` + 领域视图（如 `MenuTreeNode`）。

### serde 辅助

统一使用 `serde_with` 的官方适配器，**禁止手写等价 deserializer**：

- 三态 null：`serde_with::rust::double_option`，配 `#[serde(default, with = "double_option")]`，直接对接 `NullablePatch::from_nested_option`。
- query-string 数字：`#[serde(flatten)]` 会把字段缓冲进 serde 的 `Content` 树，丢失 query string 的 string→number 自描述强制转换；被 flatten 的数字字段（如 `PageRequest::page/size`）用 `#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]`（JSON 数字优先，query string 回退 `FromStr`，序列化恒为数字）。未经 flatten 的 query 字段由 `serde_urlencoded` 原生解析，**不要**画蛇添足地标注。
- 字符串语义枚举（wire 名与变体名不同，如 `MaterializationErrorCode` 的点分代码、`WsChannel` 的通道名）：实现 `Display` + `FromStr` 后派生 `SerializeDisplay` / `DeserializeFromStr`，不再手写 `impl Serialize / Deserialize`。

## 转换方向（`From` / `TryFrom`）

| 转换 | 方式 | 发生位置 |
|---|---|---|
| `*Request → New*` | handler 内手工构造（需补默认值 / 哈希凭证） | `routes/<x>.rs` |
| `*Request → *Patch` | `impl From<UpdateXRequest> for XPatch` | `domain/api/<x>.rs` |
| `*Info → *View` | `impl From<XInfo> for XView` | `domain/api/<x>.rs` 或 `auth.rs` |
| `Model → *Info` | `info_from_model!` 宏 | `domain/<ctx>/<x>.rs` |

- 完整、无歧义的映射用 `From`；形变 / 可能失败的映射用 `TryFrom`。
- 转换实现与「下游类型」同住一个模块，遵守孤儿规则。

## Web handler 范式（`quant-pivot-web/routes/`）

```text
ValidatedJson<CreateUserRequest>       // 提取 + 校验
  → hash_password(request.password)    // 凭证哈希（敏感转换在边界完成）
  → NewUser { id: UserId::from_v7(), password_hash, .. }
  → state.users.create(new)            // 持久化，返回 *Info
  → UserView::from(user)               // 投影剥离敏感列
  → WebResponse::ok(view)
```

- 入站一律用 `ValidatedJson<T>` 提取器，确保 `#[validate]` 在进入 handler 前执行。
- 分页响应用 `Paginated<XView>`，通过私有 `project_page` 把 `Paginated<XInfo>` 逐项投影。
- 凭证哈希、ID 生成（`from_v7` / `from_v4`）、`status` 默认值等「补齐」逻辑只在 handler 完成，不下沉到 DTO。
- 改动了 Casbin 策略表的写路径（删除用户、改角色绑定）必须在成功后 `state.casbin.reload()`。

## 新增一个资源的清单

1. 按 schema-catalog 新增表 / entity。
2. `domain/<ctx>/<x>.rs`：写 `XInfo`（+ `info_from_model!`）、`NewX`、`XPatch`。
3. `domain/api/<x>.rs`：写 `CreateXRequest` / `UpdateXRequest`（+ `From for XPatch`）/ 专用转换 Request；以及入站查询 `XPageQuery` / `XWindowQuery`（含 `normalized` / `resolve`）。
4. 出站视图写 `XView`（+ `From<XInfo>`）；放 `domain/api/<x>.rs` 或聚合所在的 `auth.rs`。
5. 在 `domain/api/mod.rs` 挂模块并 `pub use`。
6. repository 实现对应读写方法（只用五个写动词 + 专用敏感转换方法）。
7. `routes/<x>.rs` 按 handler 范式接线，敏感写后 `reload` enforcer。

## 反模式（禁止）

- ❌ 把 `*Request` 与 `New*` 合并成一个类型（破坏凭证 / 校验 / null 边界）。
- ❌ 在 `*View` / `*Response` 中保留 `password_hash` 等敏感列。
- ❌ 在持久化 DTO 上加 `#[validate]`，或在 `*Request` 上加 SeaORM 派生。
- ❌ 在 `*Patch` 中放入凭证或 `status` 等需专用方法的敏感转换字段。
- ❌ 在 handler 之外（如 DTO 的 `Default` / 构造器）写死业务默认值。
- ❌ 在没有第二个消费者之前把 `domain::api` 移出 `quant-pivot-models`。
- ❌ handler 对列表 / 详情做 N+1 enrich 循环（per-row `find_*` 补 N:1 身份列）；应在 repository JOIN-fill `*Info`。
