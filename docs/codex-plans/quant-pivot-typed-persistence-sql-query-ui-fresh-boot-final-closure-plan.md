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

2026-07-20 用户进一步收敛配置模型：生命周期互斥与安全 Fresh Boot 仍是必须完成的 W7 边界，但 runtime/migration 双身份以及 systemd credential source 属于当前部署的过度设计。数据库每个后端只使用一套身份，deploy secret 只接受明文 `SecretText`；生命周期参与者使用同一 PostgreSQL 身份连接固定的 `postgres` 协调库，不再增加第三套 credential 或 secret-bearing `lifecycle_url`。

## 2. 目标、非目标与不可妥协原则

### 2.1 最终目标

1. 全仓每个持久化字段、有限状态、业务 ID、协议值和自由文本都有上下文驱动的类型决策，不为 JSON 而 JSON，也不为 typed 而 typed。
2. 项目拥有的闭合 JSONB 使用 canonical struct/tagged enum；需要关系约束、SQL 查询或独立生命周期的数据使用具名列/关系表；真实外部原始载荷保留受控开放 JSON。
3. SeaORM/SeaQuery 成为 PostgreSQL 普通 CRUD、join、aggregate、upsert 和可表达 DDL 的默认路径；raw SQL 只存在于显式登记、类型约束、可测试的 dialect boundary。
4. Config 和关键业务读取具有明确的一致性边界和 statement budget；消除真实 N+1，对不可避免的 bind-limit chunk 和逐聚合事务给出上限与理由。
5. Config route/RBAC、Rust request/response、JSON Schema、generated TypeScript 和前端调用分别保持轻量事实源，并通过双向完整性、生成差异和行为测试阻止静默漂移；不建立 endpoint 元注册表。
6. Config Playwright 使用真实后端和数据库完成治理主链，并以 24 个直接可执行场景覆盖业务状态、viewport/theme/motion/accessibility/keyboard 证据。
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
| BASE-02 | 字符串/UUID 强类型 | W2 已迁移首批高置信度语义并以 entity/DTO/fresh-boot 双向 AST 审计闭合；W3 已补真实 PG corruption/decode 与约束拒绝证明 | Verified by W2/W3 |
| BASE-03 | constant inventory | 启发式 constant inventory 及生成器已删除；部署配置保留 `config-leaf-inventory.tsv`，持久化字段改由显式 decision registry 治理 | Verified by W2 |
| BASE-04 | raw SQL | migration/storage/repository/xtask 共 57 个 PostgreSQL/ClickHouse 原生 SQL 已进入唯一 typed contract registry；双向 AST 审计证明 registered/compiled/used 集合一致，Core/Web 不再持有原生 statement | Verified by W4 |
| BASE-05 | 查询预算 | Config 四类读取具备单 statement 断言；原生查询按 statement/row/byte budget fail closed；全仓循环 I/O 已按 TrueNPlusOne/BindLimitedBatch/PerAggregateTransaction 分类并回归 | Verified by W4 |
| BASE-06 | UI E2E | W6 已将 7 个聚合测试替换为 24 个直接可执行 Config 场景；主链穿透真实 Rust/PG/CH/Redis，stale CAS 使用双浏览器上下文，viewer 使用真实种子身份 | Verified by W6 |
| BASE-07 | UI matrix/a11y | W6 已按风险矩阵覆盖桌面明暗、关键移动端、1024 overflow、reduced motion、axe、键盘与焦点恢复；Linux Chromium 37 张审查基线、Darwin 基线为零 | Verified by W6 |
| BASE-08 | API contract | Rust DTO → schemars → JSON Schema → TS regenerate-and-diff 已通过；`RouteSpec` 继续只统一 Actix/RBAC，DTO schema root 只负责 wire type 可达性，两者职责分离是本轮锁定设计 | Adequate，维持现有 gate |
| BASE-09 | persistence 文档 | 83 个 runtime JSONB 字段已进入双向 machine registry；owned/external/controlled-open 边界、serde 闭合和 DB corruption 均有机器证明 | Verified by W3 |
| BASE-10 | SeaORM 版本 | workspace 已精确锁定单一 stable `2.0.0`；migration artifact checksum/length 未漂移，并有 `count(limit/offset)` SQL 语义回归 | Verified by W1 |
| BASE-11 | Fresh Boot 工具 | `preproduction-reset plan|apply|verify`、operation-bound v2 journal、canonical lifecycle lease、PG/CH exact DB 和 Redis duplicate-safe `SCAN+UNLINK` | Verified：W7 实现/分系统证明 `d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2`；W9 跨系统恢复/备份/seal `41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb` |
| BASE-12 | 现场 Fresh Boot | W10 已完成本机限定范围 credential rotation、plan/apply/verify、PG/CH manifest 与 fresh seed 审计；runtime 完成基础设施 verify 后因缺少 private key/funder fail closed，restart/Config/cold-start/live smoke 尚未执行 | Partial；blocked on live-account identity/provider credential (`10908d653e1c62c6f3f09964adf9ffd02647f814a28bdcddb13740e7c9d782e1`) |
| BASE-13 | deploy secret source | 配置字段直接使用零化、脱敏的 `SecretText`；TOML 只接受明文 string，不支持 credential file/source union | Verified (`d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2`) |

## 4. Requirement 追踪矩阵

实现开始时将本表扩展为 `requirement → code → test → evidence hash → status`。只有实际证据存在时才允许 `Verified`。

| ID | Requirement | 必须交付的证据 | 初始状态 |
|---|---|---|---|
| TYPE-01 | 全仓 primitive semantic field 显式决策 | 无启发式的 field inventory、双向 lint、逐字段 migration/test | Verified (`124078914d50909328e32ccea879810a77a3a22e9ed46df3c04c2cf2b45b3fc9`) |
| TYPE-02 | 高置信度 String/Uuid 迁移 | typed entity/DTO/API/UI、DB round-trip、非法值拒绝 | Verified (`124078914d50909328e32ccea879810a77a3a22e9ed46df3c04c2cf2b45b3fc9` + `e3d459d78c4728f2446735cbf94700a17df427a31c0baebca6edfb8e17d3cc10`) |
| JSON-01 | 每个 JSONB 的上下文决策 | producer/consumer/query/update/hash/evolution ledger | Verified (`e3d459d78c4728f2446735cbf94700a17df427a31c0baebca6edfb8e17d3cc10`) |
| JSON-02 | typed JSONB/normalized/external boundary 精确落地 | DB CHECK、corruption/tag/hash tests、无裸 Value 泄漏 | Verified (`e3d459d78c4728f2446735cbf94700a17df427a31c0baebca6edfb8e17d3cc10`) |
| ORM-01 | SeaORM stable 2.0.0 基线 | dependency/API/SQL/schema diff 和全套 gates | Verified (`defdf0ffe7ec23fcaad6513128ef3835eb48b7d166b8bc1079cc8588a13d05d5`) |
| SQL-01 | raw SQL typed exception registry | 全仓 inventory、唯一 exception ID、typed input、专项测试 | Verified (`f2a0a3f6d65efbffff41825c1f75ba72e95070a27165fbfe1e7f97f905a203f4`) |
| SQL-02 | Core SQL 下沉 | Core/Web 不直接持有 PG/CH statement；通过 repository/port 调用 | Verified (`f2a0a3f6d65efbffff41825c1f75ba72e95070a27165fbfe1e7f97f905a203f4`) |
| QUERY-01 | Config 查询一致性与 budget | resources/activity/snapshot options 单一致性边界和 statement count | Verified (`f2a0a3f6d65efbffff41825c1f75ba72e95070a27165fbfe1e7f97f905a203f4`) |
| QUERY-02 | 全仓 N+1/重复查询审计 | query-classification ledger、budget tests、tracing report | Verified (`f2a0a3f6d65efbffff41825c1f75ba72e95070a27165fbfe1e7f97f905a203f4`) |
| UI-01 | 24 状态可执行 E2E registry | 每个 state 都有 setup/assert/evidence，无 orphan name | Verified (`b7e5ca09d9ffab243988f39c4ec073bf8dc3b946e36963fbe18fd59b3a27c235`) |
| UI-02 | responsive/theme/motion/a11y/keyboard | 固定 CI snapshots、axe、focus trap/order、keyboard-only | Verified (`b7e5ca09d9ffab243988f39c4ec073bf8dc3b946e36963fbe18fd59b3a27c235`) |
| DOC-01 | 范式与规则机器闭环 | docs/rule/registry/lint 一致性检查 | Verified (`14bb76c67d6c38cd9b2d7a52e674bffa9c193719ebd33a663c3321149b553b1a`) |
| RESET-01 | 凭证轮换与 secret boundary | 用户确认、credential preflight、无 secret evidence | Partial：PG/CH 已轮换为独立随机 credential，旧双身份已删除，local TOML=0600；Redis 按 loopback local-dev 决策保持无密码；wallet/funder 缺失且已观察到的 authenticated RPC credential 必须由 provider owner 轮换后才能封板 |
| SECRET-01 | 单一明文 secret 契约 | 删除 `DeploySecret`/`SystemdCredentialRef`，所有 secret 字段直接使用 `SecretText`，覆盖零化、Debug 脱敏、无 Serialize、tracked-file 扫描和 adapter 使用测试 | Verified (`d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2`) |
| DBID-01 | 单一数据库身份 | PostgreSQL、ClickHouse 各保留一套 `user + password`；删除 migration identity/password、双身份校验与专用 credential 装载 | Verified (`d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2`) |
| RESET-02 | 可恢复的限定范围 destructive reset | plan/apply/failure-journal/verify tests 和现场记录 | Verified：W7 分系统边界 + W9 PG/CH/Redis 三阶段真实故障、新 operation 全清理恢复和 foreign namespace 保留（`d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2` + `41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb`） |
| BOOT-01 | 真空环境 PG/CH/Redis boot | PG=1、CH=1、Redis target empty、无 unknown objects | Verified：W9 在 disposable 三系统连续完成初始 cycle 与三次 recovery cycle，逐次验证 exact target 和 foreign Redis marker（`41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb`） |
| ACCEPT-01 | 单实例启动/重启与 Config 恢复 | readiness、workflow、commit/publish crash recovery、无 duplicate seed | Partial：W8 已闭合真实 Config/CAS/RBAC/recovery 自动化；W10 fresh local runtime 的 PG/CH schema verify 通过，但因 signing key 缺失在 web/worker 启动前 fail closed，优雅重启及同一实例 crash recovery 尚未执行 |
| ACCEPT-02 | 完整研究到报告闭环 | spec→dataset→train→validate→publish→report lineage | Partial：W9 的 Docker model train/backtest/calibration/CPCV、governance、report pipeline 全部通过；fresh local 数据上的单条端到端 lineage 与 live-account report 保留给 W10，不把分项集成测试冒领为现场单链证明 |
| ACCEPT-03 | live-account ReportOnly 安全 | account truth/report 成功，零 signing/order/intent | Blocked：缺少 private key、匹配 funder/wallet topology 与可封板的 rotated authenticated RPC；Fresh Boot 后已记录 order intent/execution order/report 三表零基线，但尚无 live account/report delta |
| SEAL-01 | disposable seal/frozen denial | live evidence、mutation denial、restore test | Verified：真实 PG/CH backup/restore、live fingerprint/policy/evidence 绑定、一次 seal、二次 seal 与 PG/CH/reset frozen denial（`41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb`） |

### 4.1 Execution Ledger

本表是本计划唯一执行账本；详细、可能包含环境信息的日志保存在 `.local/acceptance/<operation-id>/`，仓库只记录脱敏摘要与 BLAKE3。状态只允许 `Pending`、`In Progress`、`Verified`、`Blocked`，且任一时刻最多一个任务为 `In Progress`。

| Task ID | Requirement IDs | Status | Last verified commit | Changed surfaces | Targeted / full gates | Evidence / BLAKE3 | Blocker / resume instruction | Updated at (UTC) |
|---|---|---|---|---|---|---|---|---|
| W0 | BASE-01..12, ORM-01, ACCEPT-01 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | plan ledger、RC43 dependency/migration/query/contract baseline | baseline format/lints/migration/config-codegen passed；future-incompat owner graph recorded | `.local/acceptance/w0-baseline-20260720/manifest.md` / `1105074eb64774d32ac61fd501ac482f86e31e8bafbfaaf09ba0b4ddcca844cb` | owner dependency chain was removed and reverified in W7 | 2026-07-20T02:34:49Z |
| W1 | ORM-01, BOOT-01 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | stable Cargo graph、migration identity、count SQL regression | format；migration 5/5；repository 37/37；workspace clippy/lints/tests passed | `.local/acceptance/w1-seaorm-stable-20260720/manifest.md` / `defdf0ffe7ec23fcaad6513128ef3835eb48b7d166b8bc1079cc8588a13d05d5` | future-incompat dependency gate closed in W7 | 2026-07-20T03:09:48Z |
| W2 | TYPE-01, TYPE-02, SQL-01 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | decision registry/AST audit、semantic entity/DTO/schema/API/UI、relational checks | targeted semantic/migration/repository/UI；full fmt/clippy/lints/workspace tests/UI gates passed | `.local/acceptance/w2-semantic-fields-20260720/manifest.md` / `124078914d50909328e32ccea879810a77a3a22e9ed46df3c04c2cf2b45b3fc9` | — | 2026-07-20T05:07:51Z |
| W3 | JSON-01, JSON-02, TYPE-02 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | 83-field JSONB registry/AST audit、closed serde、PG corruption/decode、atomic artifact race fix | real PG 3/3；models/repository/migration/research/web targeted；full fmt/clippy/lints/workspace tests passed | `.local/acceptance/w3-jsonb-closure-20260720/manifest.md` / `e3d459d78c4728f2446735cbf94700a17df427a31c0baebca6edfb8e17d3cc10` | future-incompat dependency gate closed in W7 | 2026-07-20T05:51:11Z |
| W4 | SQL-01, SQL-02, QUERY-01, QUERY-02 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | 57-contract registry/AST audit、Core native reads 下沉、deterministic budgets、N+1 batch closure | targeted unit + real Docker PG；full fmt/clippy/lints/workspace tests passed | `.local/acceptance/w4-sql-contract-20260720/manifest.md` / `f2a0a3f6d65efbffff41825c1f75ba72e95070a27165fbfe1e7f97f905a203f4` | 记录并修复 PG ARE 256 上限、batch helper 二次除法与 CH immutable formatting drift；lifecycle 与 future-incompat 后续 gate 均已在 W7 闭合 | 2026-07-20T08:11:33Z |
| W6 | UI-01, UI-02, ACCEPT-02 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | 24 direct Playwright scenarios、real CAS/RBAC、a11y/focus、Linux visual CI、protected-suite isolation | Config 24/24 + protected 8/8；37 Linux/0 Darwin snapshots；UI lint/type/unit/build/bundle + Rust web targeted gates passed | `.local/acceptance/w6-ui-executable-20260720/manifest.md` / `b7e5ca09d9ffab243988f39c4ec073bf8dc3b946e36963fbe18fd59b3a27c235` | UI-01/UI-02 已闭合；ACCEPT-02 这里只提供 UI 侧证据，完整 lineage 仍由 W8/W9 验收；W5 endpoint 元注册表已确认过度设计并完整回退 | 2026-07-20T11:04:42Z |
| W7 | SECRET-01, DBID-01, RESET-01, RESET-02, BOOT-01, SEAL-01 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | direct `SecretText`、PG/CH single identity、canonical PG lease/cancellation、v2 atomic reset journal、PG/CH active-owner denial、Redis duplicate-safe cleanup、future-incompat dependency fix | config 75/75；migration unit 6/6；Docker PG lifecycle/reset 3/3、CH active-query/reset 1/1、Redis reset 2/2；xtask journal 2/2；clean PG16 manifest；57/57 SQL AST；full fmt/clippy/lints/workspace tests passed | `.local/acceptance/w7-lifecycle-fresh-boot-20260720/manifest.md` / `d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2` | 实现与分系统证明闭合；跨系统恢复/seal 已由 W9 补证；W10 destructive local apply 仍受人工门禁 | 2026-07-20T14:10:56Z |
| W8 | DOC-01, ACCEPT-01, ACCEPT-02 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | canonical docs、dead placeholder/admin UI、active-doc/CI lint、37 Linux visual baselines、schedule/trade-policy deterministic layout | full Rust/static/UI gates；fresh Config 24/24；independent protected 8/8；37 Linux/0 Darwin；final eslint/typecheck/build/diff checks passed | `.local/acceptance/w8-doc-ci-closeout-20260720/manifest.md` / `14bb76c67d6c38cd9b2d7a52e674bffa9c193719ebd33a663c3321149b553b1a` | UI/docs 实现与证明闭合；W9 已补 disposable runtime/model 分项证明，现场单实例/单链仍归 W10 | 2026-07-20T15:35:01Z |
| W9 | RESET-02, BOOT-01, ACCEPT-01, ACCEPT-02, SEAL-01 | Verified | `9d9701e5a87865abab6ed0065a251b08193bbafa` | single-owner disposable PG16/CH26.5/Redis7 harness、四次 full reset cycle、三阶段真实故障恢复、PG/CH backup/restore、dump-stable PG constraints、live evidence seal/frozen denial | Docker W9 1/1；model/cold-start 24/24；governance 12/12；migration 6/6；SQL 57/57；full fmt/clippy/static lints/workspace tests passed；无 Testcontainers leak | `.local/acceptance/w9-disposable-closeout-20260720/manifest.md` / `41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb` | disposable scope closed；未读取本机 secret 配置；ACCEPT-01 的 systemd restart、ACCEPT-02 的 fresh local 单链与 ACCEPT-03 仍由 W10 人工门禁后恢复 | 2026-07-20T16:58:01Z |
| W10 | RESET-01, RESET-02, BOOT-01, ACCEPT-01..03 | Blocked | `9d9701e5a87865abab6ed0065a251b08193bbafa` | local PG/CH credential rotation、legacy dual-identity deletion、exact destructive boot、manifest/seed/runtime preflight | plan/apply/verify passed；PG 1 migration/93 tables/274 indexes；CH v1/27 required objects；Redis 32→0；fresh policy 6 resources at intentional generation 2；runtime infra verify passed then signing fail-closed | `.local/acceptance/w10-local-preproduction-20260721/checkpoint-1.md` / `10908d653e1c62c6f3f09964adf9ffd02647f814a28bdcddb13740e7c9d782e1` | 安装 fresh private key、匹配 funder/wallet topology 与 rotated authenticated RPC 后恢复；不得把 secret 发到聊天/evidence；随后继续 restart、Config crash recovery、local single-lineage 与 ReportOnly no-mutation；本地禁止 seal | 2026-07-20T17:35:02Z |

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

## 10. 已排除设计：Config API endpoint 元注册表（非 Workstream）

W5 曾尝试把 endpoint ID、method/path、RBAC、path/query/body/success/error schema、CAS/idempotency 和 UI path 全部并入 `RouteSpec`。实现审查确认该方案会引入类型擦除、庞大生成描述符、重复的 success envelope/schema 视图和更深的间接调用，对当前 15 个稳定 Config endpoint 没有相称收益，因此按用户决策完整回退，不进入交付范围。

锁定的简洁边界如下：

- `RouteSpec` 只作为 Actix route 与服务端 RBAC 的唯一配对事实源；
- `ConfigApiContractSchema` 只让真实 Rust request/response DTO 可被 schemars 到达，继续执行 JSON Schema/TypeScript regenerate-and-diff；
- UI `ConfigApi` 集中维护少量 path builder，不生成 endpoint descriptor；
- CAS/idempotency/error 语义由真实 handler/repository 行为测试和 W6 protected E2E 证明，而不是复制到声明式 metadata；
- 不新增 endpoint registry、typed error status registry、route-to-schema 反射层或 UI path AST lint。

保留门禁为 `pnpm check:config-api`、服务端 protected-route/RBAC tests、Config governance E2E。若未来 endpoint 数量或多客户端规模显著增长，必须以实际 drift 事故和维护成本为证据另立计划，不能在本轮预置抽象。

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
- 既有 `RouteSpec`/RBAC 配对完整性与 DTO schema/TypeScript regenerate-and-diff 边界（不建立 endpoint 元注册表）；
- Fresh Boot 证据和失败恢复；
- canonical public barrel 与 compatibility re-export 的边界。

`.cursor/rules/quant-pivot-persistence.mdc` 和 `AGENTS.md` 只摘要强制规则并链接 canonical 文档，避免三份规则漂移。

### 12.2 文档 lint

CI 验证：

- canonical 文档、Cursor rule、AGENTS 引用存在；
- 文档声称的 registry/gate 在代码和 CI 中真实存在；
- requirement matrix 的 `Verified` 必须有可读取 evidence；
- 无 Runtime v17/v18、schema v3、DryRun/Paper/Live、旧 Runtime Config endpoint/parser；
- 无 secret env 示例、tracked TOML 真实 secret 或旧 UI JSON editor 指引；production example 只允许显式 `REPLACE_WITH_*` 占位符；
- runbook 不再使用 `QUANT_PIVOT__*` 注入 funder/RPC/secret，与当前 permission-restricted TOML 单一来源一致；
- SeaORM 版本说明与 workspace lock 一致。

## 13. 凭证轮换前置条件

### 13.1 零复用原则

- 不读取、打印、复制、hash、备份或复用旧配置中已暴露的 secret value。
- 不把 secret 放入 Git、tracked template、environment value、CLI argument、Docker `.env`、日志、截图、trace、reset journal 或 evidence manifest。部署实例使用未跟踪且 mode 0600 的 TOML；操作者本机使用已 gitignore 且 mode 0600 的 `quant-pivot.local.toml`。
- secret hash 也不作为轮换证明，避免对低熵 secret 提供离线验证材料。

### 13.2 必须轮换的实际启用凭证

| 类别 | 轮换与验证 |
|---|---|
| Wallet/private key | 生成或安装新 key；只在内存派生 public signer address；用户确认资金/权限迁移与旧 key 撤销策略 |
| Funder/wallet topology | EOA 必须与 signer 一致；proxy/safe 必须现场证明 signer 的控制/owner 关系和 relayer 路径 |
| Polygon/RPC/provider | 轮换 JWT/API token/URL credential；只记录 provider/key ID 和健康结果 |
| Relayer | 轮换 API key/secret，验证 address、wallet kind 和最小权限；ReportOnly 不调用提交接口 |
| PostgreSQL | 轮换唯一 `quant_pivot` credential；runtime、schema CLI 与 lifecycle coordination 使用同一身份；reset 不擅自删除 cluster role |
| ClickHouse | 轮换唯一 `quant_pivot` credential；runtime 与 schema CLI 使用同一身份，不保留 migration user |
| Redis | 轮换 runtime credential/ACL，确认只访问配置 DB 和 `qp:` namespace |
| JWT signing | 使用符合长度/编码的新 key；确认旧 session/token 失效 |
| Evidence signing | 与 JWT/wallet 分离；轮换 current key，previous key 只保留真实历史验证需求 |
| Notification/domain provider | 只轮换实际启用项；禁用项不得用假 secret 绕过 validation |

### 13.3 单一明文 SecretText 验收（W7 / `SECRET-01`）

W7 删除 `DeploySecret`、`SystemdCredentialRef` 及全部 credential resolution 生命周期。每个 secret 字段直接反序列化为 `SecretText`/`Option<SecretText>`，TOML wire contract 只接受 string：

```toml
# config/quant-pivot.local.toml；已 gitignore 且 mode 0600
password = "local-password"
```

- `SecretText(Zeroizing<String>)` 不实现 `Serialize`/`Display`；自定义 `Debug` 只能输出 `unset/redacted`，validation/error/trace 不得拼接值。
- 空值通过空 string 或省略 optional 字段表达；object `{ name = ... }`、unknown shape、secret env overlay 与旧 resolution API 均失败。
- tracked `quant-pivot.toml` 不得有非空 secret；production example 只能有显式 `REPLACE_WITH_*`，这些占位符在 quant-mode validation 中必须 fail closed。
- 实际 preproduction/production secret 写入未跟踪的部署 TOML，文件必须由部署层限制为 0600；代码不再假装能用类型系统保护磁盘明文。
- UI/API readiness 只能显示 `available/missing/invalid`，不能返回值；adapter 仅在拥有该 credential 的调用边界使用 `expose_secret()`。
- tests 覆盖 plaintext 反序列化、local overlay 到 adapter、reserved-character URL 编码、Debug 脱敏、无 Serialize/Display、tracked placeholder lint 和旧类型/旧 source shape 不可达。

### 13.4 人工完成声明

用户需确认：所有实际启用凭证已轮换、新 wallet 与 funder 关系正确、部署 TOML 已限制为 0600。确认后 destructive reset 无需再次扩大授权，但仍必须通过工具自身 target fingerprint、短时 nonce 和 lifecycle lease。

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

PG migration、CH migration、reset、verify 和 seal 必须通过同一个 `LifecycleLeaseProvider`，使用已配置的唯一 PostgreSQL 身份连接固定 `postgres` coordination database 和同一 lock key。不能只复用相同整数 key，却在不同 PostgreSQL database connection 上假设必然互斥。

实现要求：

- coordination database 固定为 canonical `postgres`，由相同 host/port/user/password 派生，不增加 `lifecycle_url` 或第三套 credential；
- 所有四类 mutation 先取 lease，再现场重读 baseline、target fingerprint、migration ledger 和 active bundle；
- reset 在删除 target DB 期间 lease 仍存活；
- lease 丢失/connection 断开立即停止后续 stage；
- Docker 集成测试终止持锁 session 后必须触发 cancellation；第二个任意 lifecycle participant 无法取得同一 session-scoped lease，因此 PG/CH/reset/verify/seal 的组合互斥由同一 provider 和 lock contract 证明。

### 14.3 Reset operation journal

跨 PG/CH/Redis 不存在原子事务，因此 reset 必须使用 durable、0600、无 secret 的 stage journal：

```text
Planned -> Applying -> PostgresReset -> ClickhouseReset -> RedisCleared
        -> SchemasApplied -> Verified -> Completed
        \-> Failed(failed_stage, failed_at, summary)
```

- journal 包含 format version、operation ID、nonce、stage、created/updated/expires/completed timestamps、脱敏 endpoint fingerprint、对象/连接/key 计数、failure 和 immutable journal hash。
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

执行状态：Verified。W7 分系统测试覆盖 nonce/target/lease/unknown-owner/concurrent-writer 等拒绝边界；W9 的 single-owner disposable harness 连续执行一次初始与三次恢复 `plan/apply/verify`，并分别在 PG、CH、Redis stage 注入真实失败。每次恢复均创建新 operation、重新清理全部授权目标并保留 foreign Redis namespace。证据：`d7eb81882c2f4633cc8aca5861db1885ea641615b4b4a0d08951e2510a35aab2`、`41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb`。

### 15.2 本地 plan

凭证确认后：

1. 记录当前 clean/dirty build identity，但不要求提交用户现有改动；evidence 明确 dirty 状态和 diff hash。
2. 运行 credential preflight、lifecycle/baseline preflight、target ownership/inventory。
3. 生成短时 plan 和一次性 nonce，只输出脱敏 endpoint fingerprint、target 名称、对象/连接/key 数和 expiry。
4. 人工核对输出恰为 PG `quant_pivot`、CH `quant_pivot`、Redis DB0 `qp:`。

### 15.3 本地 apply

在共享 lifecycle lease 和 stage journal 下：

1. quiesce 当前 repo services/writers；
2. 删除并重建 PG `quant_pivot`，owner 为唯一配置角色 `quant_pivot`；不删除/重建 cluster role；
3. 删除并重建 CH `quant_pivot`；
4. Redis `SCAN MATCH qp:*` + bounded `UNLINK`，禁止 `FLUSHDB`；
5. 证明 PG/CH target 空、Redis `qp:* = 0`；
6. 应用唯一 PG boot migration，期望 migration count=1、version=1；
7. 应用唯一 CH boot migration，期望 version=1、object manifest 精确相等；
8. finalize catalog seeds 与 PUBLIC privilege hardening；不再制造 runtime/migration 双角色 grants；
9. seed bootstrap admin、immutable research profiles、policy profile artifacts 和六类 Config resource boot bundle；
10. 校验空 CAS 基线为 generation 1、首次 committed 六资源 bundle 为 generation 2，snapshot/hash/revision vector 完整且 outbox facts 与六次 initial activation 一一对应；
11. verify PG/CH schema fingerprint、ledger/audit checksum、Redis target empty；
12. journal 标记 Completed 并生成脱敏 evidence manifest。

### 15.4 Reset 后启动与重启

1. schema CLI 使用配置中的唯一数据库身份完成 apply/verify 后退出；
2. runtime 使用同一数据库身份启动，但启动路径只读 verify schema，禁止 startup DDL；
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

Disposable 执行状态：Verified。W9 已完成 PG custom-format backup/restore、CH database backup/restore、live schema/policy/evidence 绑定、首次 seal、二次 seal 拒绝，以及 frozen 后 PG/CH schema mutation 与 reset 拒绝；现场本地环境未 seal。证据：`41b504fd0cf9a137d9af0bfbd46cb5024b6d05826626e6248d5a1497140d1beb`。

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

并新增：semantic field registry、raw SQL registry、query budget、既有 `RouteSpec`/RBAC completeness、DTO generation diff、documentation claim 和 Fresh Boot journal lint/test；不新增 endpoint 元注册表。

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
| P6 | 既有 Actix/RBAC route 边界与 DTO generated contract | route permission、Rust schema、TS/UI 无漂移；不引入 endpoint 元注册表 |
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
