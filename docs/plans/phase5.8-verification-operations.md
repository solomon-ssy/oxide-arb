# Phase 5.8 — Verification, Observability, Runbooks & Drift Control

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **前置依赖**: Phase 5.0-5.7  
> **覆盖原章节**: 18, 19  
> **目标**: 将每个子阶段的退出条件、测试矩阵、观测指标、运维手册和 PR 防漂移审查固化，防止 Phase 5 落地时跑偏。

---

## 0. 全阶段推进规则

1. 每个 phase 必须有明确退出条件。
2. 如果当前 phase 只有 stub，禁止进入下一 phase。
3. 任一 phase 的阻断条件未解除，后续 phase 只能做设计或 report-only，不得接 live。
4. 所有破坏式重构必须删除旧 alias/re-export/compat shim，不保留灰色兼容层。
5. 所有 materialization 输出必须可审计、可复现、可回滚。
6. 所有 live hot path 行为必须只读 `ArcSwap<ControlFactorSnapshot>`。

---

## 1. 子阶段退出矩阵

| 阶段 | 退出条件 | 阻止进入下一阶段的情况 |
|---|---|---|
| 5.0 Foundation | 架构边界、typed artifact、publication-first、破坏式原则冻结 | `runtime_config` 与 factor registry 边界不清；允许 re-export/alias |
| 5.1 Fact data plane | integration tests 中能写入 facts；hot path latency 不受影响；CH rows 包含必需 key | 缺 producer、缺 attribution、或 nullable 字段被当作 0 默认值 |
| 5.2 PIT/runner | resolver 可在任意 timestamp 重建 market/token/config/calibration/fee state | 任一 resolver 静默 fallback 到 current state |
| 5.3 Evidence engine | 每个 evidence stage 输出确定且包含 coverage metrics | stage output 依赖未排序查询结果 |
| 5.4 Builders/gates | all five builders reject insufficient evidence and write typed payloads | payload is stringly typed or gates are only warnings |
| 5.5 Registry/governance | publication, shadow, rollback, expiry, audit transactional | publication can leave two active Published versions |
| 5.6 Live consumption | hot path 只读 ArcSwap snapshot；fail closed tests 通过 | 任意 hot path code 查询 CH/PG |
| 5.7 Exit/token | report-only exit materialization proves executable path and token reconciliation | auto-exit enabled before token-level reconciliation |
| 5.8 Verification/ops | tests/metrics/runbooks/drift checks complete | PR review cannot prove no leakage/no re-export/no hot path DB |

---

## 2. 验收清单

### 2.1 Data

- [ ] L2 facts are written without blocking hot path.
- [ ] Book snapshots are available for replay bootstrap.
- [ ] Calibration snapshots are point-in-time queryable.
- [ ] Detection rows contain score and calibration components.
- [ ] Audit rows preserve terminal and settlement attribution.
- [ ] Balance and token snapshots are available.
- [ ] ERC1155 token balances are reconciled by `token_id` and shares.
- [ ] Sell-side bid depth is available for exit materialization.

### 2.2 Materialization

- [ ] Runs have immutable manifests.
- [ ] Runs support source delay windows.
- [ ] Stage reports include coverage, warnings, errors, and fingerprints.
- [ ] Partial runs never publish production factors.
- [ ] ReportOnly runs cannot write Candidate factors.

### 2.3 Factors

- [ ] All five factor types are strong typed.
- [ ] Every factor has evidence, TTL, owner, config hash, code sha.
- [ ] No factor automatically expands risk.
- [ ] Factor builders reject insufficient PIT data.

### 2.4 Governance

- [ ] Draft / Candidate / Shadow / Published / Superseded / Expired / RolledBack / Rejected are implemented.
- [ ] Every state transition writes audit event.
- [ ] Shadow deltas are recorded.
- [ ] Rollback restores known-good publication.
- [ ] Expiry behavior is type-specific.

### 2.5 Live

- [ ] Startup loads active publication into `ControlFactorSnapshot`.
- [ ] Periodic refresh and notify refresh work.
- [ ] Hot path reads no CH/PG.
- [ ] Safety factor load failure can fail closed.
- [ ] Applied factors are written to audit.
- [ ] No live auto-exit is enabled unless token-level reconciliation and exit accounting are complete.

---

## 3. Test Matrix

| 测试 | 必需场景 |
|---|---|
| PIT resolver | market metadata 变化、fee 变化、calibration 更新、runtime config activation |
| Book reconstruction | missing snapshot、crossed book、gap、out-of-order L2 events |
| Detector evidence | live match、missed live signal、extra materialized signal、bucket mismatch |
| Execution evidence | strict FOK fill、miss、latency shifted miss、depth stress |
| Portfolio evidence | risk reject、reservation pressure、drawdown、stale metrics |
| Settlement evidence | won、lost、delayed settlement、redeem failure |
| Reconciliation evidence | cash drift、token drift、stale balance、critical drift |
| Exit evidence | fixed stop、trailing stop、time stop、zone invalidation、bid-depth unavailable |
| Token reconciliation | PG 有 position 但链上无 token、链上有 token 但 PG 无 position、allowance missing、resolution 后 redeem |
| Factor builders | sufficient data、insufficient sample、insufficient coverage、non-conservative payload |
| Governance | Draft->Candidate、Candidate->Shadow、Shadow->Published、rollback、expiry |
| Live snapshot | startup success、startup fail closed、periodic refresh、notify refresh、schema mismatch |
| SELL plan | USD budget vs shares amount、partial fill accounting、allowance missing |
| Audit | hash chain verification、append-only、request id idempotency |

### 3.1 Mandatory end-to-end tests

1. Fact-to-snapshot path:

```text
live facts
  -> PIT resolver
  -> materialization run
  -> evidence stages
  -> factor builder
  -> Candidate
  -> Shadow
  -> Published
  -> ControlFactorSnapshot
  -> detector/scorer/risk/sizer audit trace
```

2. Fail-closed path:

```text
critical reconciliation factor expired
  -> snapshot refresh detects expiry
  -> Live risk gate rejects new entries
  -> audit/metrics/alert emitted
```

3. Leakage prevention path:

```text
historical detection at T
  -> calibration/config changed at T+1
  -> materialization at T+2
  -> resolver must use state visible at T, not T+1/T+2
```

4. Exit report-only path:

```text
filled position
  -> reconstruct bid books after entry
  -> simulate fixed/trailing/time/zone exit
  -> compare hold vs exit PnL
  -> no live SELL submitted
```

---

## 4. Observability

Required metrics：

- Materialization duration by stage.
- Materialization success/failure count by run kind.
- Latest successful run age by factor type.
- Coverage percentage by stage and factor type.
- Draft/Candidate/Rejected counts by factor type.
- Publication version and snapshot load age.
- Shadow would-reject / would-size deltas.
- Expired factor count by factor type.
- Fail closed events and startup assertion failures.
- Audit hash-chain verification failures.
- PIT resolver missing input count by domain.
- CH query row count and source delay lag.
- Snapshot refresh success/failure and last good publication id.
- Token balance drift count/value by severity.
- Exit report false-exit rate and executable-exit rate.

### 4.1 Alerts

| Alert | Condition |
|---|---|
| `materialization_missed` | latest successful run age > 2x cadence |
| `coverage_below_threshold` | factor-specific coverage gate fails repeatedly |
| `snapshot_load_failed` | active publication cannot be decoded/validated |
| `safety_factor_expired` | critical reconciliation/anomaly factor expired |
| `audit_chain_broken` | hash-chain verification fails |
| `pit_leakage_detected` | resolver uses state newer than event time |
| `token_drift_critical` | token drift severity critical |
| `shadow_delta_spike` | would-reject/size delta exceeds policy |

---

## 5. Operational Runbooks

- [ ] Backfill missing L2/book data and rerun materialization.
- [ ] Reject low-quality Candidate with reason.
- [ ] Promote Candidate to Shadow.
- [ ] Review shadow deltas.
- [ ] Publish conservative factor.
- [ ] Emergency publish market anomaly with short TTL.
- [ ] Roll back active publication.
- [ ] Recover from snapshot schema mismatch.
- [ ] Handle expired safety factor in Live mode.
- [ ] Verify audit event chain.
- [ ] Investigate exit report and decide whether to enable manual review / auto reduce.
- [ ] Resolve token-level drift before publishing reconciliation health recovery.

Each runbook must include:

- required role；
- required request id/idempotency key；
- pre-checks；
- exact API call or operator action；
- expected audit events；
- rollback path；
- verification query/metric。

---

## 6. Drift Review Checklist

Before merging any Phase 5 implementation PR, reviewers must check：

- Does this introduce any compatibility re-export or old alias? Reject.
- Does any hot path query CH/PG? Reject.
- Does any materialization query use current calibration/config/fee for historical time? Reject.
- Does any factor payload use untyped JSON in the decision path? Reject.
- Does any publication mutate active factors in place? Reject.
- Does any automatic factor expand risk? Reject unless manual approval path is explicit and audited.
- Does any CH/PG missing value become `0`, empty string, or default enum? Reject unless domain-correct.
- Does any stage lack coverage metrics? Reject.
- Does any API mutation lack actor, reason, request id, idempotency key? Reject.
- Does any exit logic submit SELL without token inventory reservation and ERC1155 allowance check? Reject.
- Does any reconciliation claim “complete” without token_id-level external balances? Reject.

---

## 7. Documentation Requirements

Every implementation PR must update the relevant subphase document if it changes:

- public model shape；
- schema/index；
- state transition；
- quality gate policy；
- fail-open/fail-closed behavior；
- scheduler cadence/source delay；
- live hot path application order；
- exit policy progression；
- runbook procedure。

Docs updates must not add compatibility language such as “keep old name for now”, “temporarily re-export”, “alias endpoint”, or “fallback to current state”。

---

## 8. Final Phase 5 Exit

Phase 5 is complete only when：

1. All data, materialization, factor, governance, live, exit/token, test, observability, and runbook checklists are complete.
2. Fact-to-snapshot E2E test passes.
3. PIT leakage tests pass.
4. Publication rollback tests pass.
5. Snapshot expiry/fail-closed tests pass.
6. Shadow decision delta tests pass.
7. Exit materialization tests pass, but live auto-exit remains disabled unless Phase 5.7 conditions are explicitly met.
8. No code path contains compatibility re-export/alias for old replay names.
9. No hot path code queries ClickHouse/Postgres.
10. Reviewers can trace every live factor decision to publication id, factor id, evidence run id, config hash, code sha, and audit event.
