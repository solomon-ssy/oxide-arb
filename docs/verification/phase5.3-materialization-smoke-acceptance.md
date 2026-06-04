# Phase 5.3 — Materialization Smoke Acceptance Record

> **Purpose**: Repeatable end-to-end evidence pipeline smoke using synthetic CH/PG facts (no live ClickHouse/Postgres).  
> **Scenario**: Single endgame market, one detection, FOK fill, settlement, PIT balance/token snapshots.  
> **Automated gate**: `cargo test -p oxide-arb-control materialization_smoke -- --nocapture`

---

## 1. How to run

```bash
cargo test -p oxide-arb-control materialization_smoke -- --nocapture
```

Expected: test `phase53_materialization_smoke_passes_acceptance_criteria` **PASS**, with a printed stage summary block.

Fixture code: `crates/oxide-arb-test-support/src/materialization/smoke.rs`  
Pipeline entry: `MaterializationRunner::execute_evidence_pipeline` (`crates/oxide-arb-control/src/materialization/runner.rs`)

---

## 2. Scenario constants

| Field | Value |
|-------|--------|
| Market | `0xsmoke_acceptance` |
| YES / NO tokens | `smoke_yes` / `smoke_no` |
| Opportunity | `opp_smoke_acceptance` |
| Holder (PIT balance) | `0xholder_smoke` |
| Window | `2026-06-03 10:00–12:00 UTC` (see `smoke_window()`) |
| Decision time | window start + 800 ms |

---

## 3. Acceptance criteria (P0 smoke)

| # | Criterion | Expected stage status | Verified by |
|---|-----------|----------------------|-------------|
| 1 | PIT `resolve_inputs` production-eligible | `Completed` / `CompletedWithWarnings` | manifest `production_eligible` |
| 2 | All PIT inputs have non-empty `query_fingerprint` | — | input manifest scan |
| 3 | Book reconstruction with bootstrap + L2 | `Completed` / `CompletedWithWarnings` | `BookReconstruction` |
| 4 | Detector materialized replay metrics available | `Completed` / `CompletedWithWarnings` | `DetectorEvidence` |
| 5 | Execution StrictFok replay + terminal audit | `Completed` / `CompletedWithWarnings` | `ExecutionEvidence` + metrics: `true_fill_count ≥ 1`, `simulated_vwap_p50_bps` available |
| 6 | Portfolio deterministic sequence (PG facts) | `Completed` / `CompletedWithWarnings` | `PortfolioRiskEvidence` |
| 7 | Settlement join + drift inputs | `Completed` / `CompletedWithWarnings` | `SettlementReconciliationEvidence` |
| 8 | Exit/token report-only | `EvidenceOnly` | `ExitTokenEvidence` |
| 9 | Training examples + dataset hash | `Completed` / `CompletedWithWarnings` | `TrainingExampleBuild` |
| 10 | Full stage graph (8 stage reports) | 8 rows | `stage_reports.len() == 8` |

---

## 4. What this smoke does **not** cover

- Live ClickHouse / Postgres connectivity (use ops backfill + manual run for that).
- Fee-schedule **policy** replay vs observed-fee divergence metric.
- Equity timeline / drawdown (PortfolioRisk v1 proxy still pending).
- Published control-factor promotion (Phase 5.4+).

---

## 5. Sign-off log

| Date | Operator | `cargo test` result | Notes |
|------|----------|---------------------|-------|
| 2026-06-04 | _automated_ | PASS (local) | Synthetic scenario: FOK depth 120@0.95, portfolio P0 gate, 8 stages |

---

## 6. Related docs

- Plan: [`docs/plans/phase5.3-evidence-engine.md`](../plans/phase5.3-evidence-engine.md) §10.1  
- Next phase: [`docs/plans/phase5.4-factor-builders-quality-gates.md`](../plans/phase5.4-factor-builders-quality-gates.md)
