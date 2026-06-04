//! Phase 5.3 end-to-end materialization smoke (synthetic facts, no CH/PG infra).
//!
//! Run: `cargo test -p oxide-arb-control materialization_smoke -- --nocapture`

use std::sync::Arc;

use oxide_arb_control::{
    evidence::engine::EvidenceEngine,
    materialization::{
        MaterializationRunReport, MaterializationRunner, MaterializationRunnerDeps,
        PointInTimeResolver, ResolverRepositories,
    },
};
use oxide_arb_models::{
    domain::control_factor::StageReportBody,
    enums::control_factor::{
        EvidenceStageStatus, FactorStatus, MaterializationOutputPolicy, MaterializationStageName,
    },
};
use oxide_arb_repository::traits::ControlFactorRepository;
use oxide_arb_test_support::materialization::{
    SMOKE_MARKET_ID, SMOKE_OPPORTUNITY_ID, SmokeRepositories, smoke_manifest,
};

#[tokio::test]
async fn phase53_materialization_smoke_passes_acceptance_criteria() {
    let repos = SmokeRepositories::build();
    let manifest = smoke_manifest();
    let runner = smoke_runner(&repos);
    let report = runner
        .execute_evidence_pipeline(&manifest)
        .await
        .expect("evidence pipeline should complete");

    print_acceptance_summary(manifest.run_id.as_ref(), &report);
    assert_pit_resolution(&report);
    assert_book_stage(&report);
    assert_detector_stage(&report);
    assert_execution_stage(&report);
    assert_portfolio_stage(&report);
    assert_settlement_stage(&report);
    assert_exit_and_training_stages(&report);
}

#[tokio::test]
async fn phase54_materialization_smoke_writes_factor_build_gate_and_draft_stages() {
    let repos = SmokeRepositories::build();
    let mut manifest = smoke_manifest();
    manifest.output_policy = MaterializationOutputPolicy::EmitDraftCandidates;
    let runner = smoke_runner(&repos);
    let report = runner
        .execute_evidence_pipeline(&manifest)
        .await
        .expect("phase 5.4 materialization should complete");

    assert!(stage_completed(
        stage(&report, MaterializationStageName::FactorBuild).status
    ));
    assert!(
        stage_completed(stage(&report, MaterializationStageName::QualityGateEvaluation).status)
            || stage(&report, MaterializationStageName::QualityGateEvaluation).status
                == EvidenceStageStatus::CompletedWithWarnings
    );
    assert!(stage_completed(
        stage(&report, MaterializationStageName::DraftWrite).status
    ));

    let factors = repos
        .control_factors
        .list_factors_by_run(&manifest.run_id)
        .await
        .expect("recorded factors");
    assert!(
        factors
            .iter()
            .any(|factor| factor.status == FactorStatus::Candidate),
        "phase 5.4 smoke expects at least one candidate factor"
    );
    assert!(
        factors
            .iter()
            .any(|factor| factor.status == FactorStatus::ReportOnly),
        "phase 5.4 smoke expects incomplete evidence to remain report-only"
    );
}

fn smoke_runner(repos: &SmokeRepositories) -> MaterializationRunner {
    let pit = PointInTimeResolver::new(ResolverRepositories {
        runtime_config: Some(repos.runtime_config.clone()),
        timeseries: Some(repos.timeseries.clone()),
        markets: Some(Arc::new(repos.markets.clone())),
        trades: Some(repos.trades.clone()),
        positions: Some(repos.positions.clone()),
        balances: Some(repos.balances.clone()),
        potential_loss: Some(repos.potential_loss.clone()),
        risk_audit: Some(repos.risk_audit.clone()),
        reconciliation: Some(repos.reconciliation.clone()),
        resolution_events: Some(repos.resolution_events.clone()),
        ..ResolverRepositories::default()
    });
    MaterializationRunner::new(MaterializationRunnerDeps {
        control_factors: repos.control_factors.clone(),
        pit_resolver: Arc::new(pit),
        evidence_engine: Arc::new(EvidenceEngine::new(repos.timeseries.clone())),
    })
}

fn assert_pit_resolution(report: &MaterializationRunReport) {
    assert!(
        report.input_resolution.manifest.is_production_eligible(),
        "PIT resolve_inputs must be production eligible"
    );
    assert!(
        !report
            .input_resolution
            .manifest
            .inputs
            .iter()
            .any(|input| input.query_fingerprint.0.is_empty()),
        "PIT inputs must carry canonical query fingerprints"
    );
}

fn assert_book_stage(report: &MaterializationRunReport) {
    let book = stage(report, MaterializationStageName::BookReconstruction);
    if !stage_completed(book.status) {
        println!("book metrics: {}", book.metrics);
        println!(
            "book insufficient: {:?}",
            book.coverage.insufficient_reasons
        );
    }
    assert!(
        stage_completed(book.status),
        "book reconstruction must complete: {:?}",
        book.status
    );
    assert!(
        !book.query_fingerprints.is_empty(),
        "book stage must record repository fingerprints"
    );
}

fn assert_detector_stage(report: &MaterializationRunReport) {
    let detector = stage(report, MaterializationStageName::DetectorEvidence);
    assert!(
        stage_completed(detector.status),
        "detector evidence must complete when replay inputs are pinned: {:?}",
        detector.status
    );
}

fn assert_execution_stage(report: &MaterializationRunReport) {
    let execution = stage(report, MaterializationStageName::ExecutionEvidence);
    // Metrics assert StrictFok replay matched live fill; status asserts P0 gate only
    // (book_age_fill_correlation is report-only until implemented — see metric_gate.rs).
    assert_execution_smoke_metrics(&execution.metrics);
    assert!(
        stage_completed(execution.status),
        "execution P0 replay must complete when true_fill and VWAP are available: {:?}",
        execution.status
    );
}

fn assert_portfolio_stage(report: &MaterializationRunReport) {
    let portfolio = stage(report, MaterializationStageName::PortfolioRiskEvidence);
    if !stage_completed(portfolio.status) {
        println!("portfolio metrics: {}", portfolio.metrics);
        println!(
            "portfolio insufficient: {:?}",
            portfolio.coverage.insufficient_reasons
        );
    }
    assert!(
        stage_completed(portfolio.status),
        "portfolio evidence must complete when PG facts are present: {:?}",
        portfolio.status
    );
}

fn assert_settlement_stage(report: &MaterializationRunReport) {
    let settlement = stage(
        report,
        MaterializationStageName::SettlementReconciliationEvidence,
    );
    assert!(
        stage_completed(settlement.status),
        "settlement evidence must complete: {:?}",
        settlement.status
    );
}

fn assert_exit_and_training_stages(report: &MaterializationRunReport) {
    let exit = stage(report, MaterializationStageName::ExitTokenEvidence);
    assert_eq!(
        exit.status,
        EvidenceStageStatus::EvidenceOnly,
        "exit/token stage must remain report-only in Phase 5.3"
    );

    let training = stage(report, MaterializationStageName::TrainingExampleBuild);
    assert!(
        stage_completed(training.status),
        "training example build must complete: {:?}",
        training.status
    );
    assert_eq!(
        report.stage_reports.len(),
        8,
        "expected resolve + 7 evidence stages"
    );
}

const fn stage_completed(status: EvidenceStageStatus) -> bool {
    matches!(
        status,
        EvidenceStageStatus::Completed | EvidenceStageStatus::CompletedWithWarnings
    )
}

fn stage(report: &MaterializationRunReport, name: MaterializationStageName) -> &StageReportBody {
    report
        .stage_reports
        .iter()
        .find(|stage| stage.stage_name == name)
        .unwrap_or_else(|| panic!("missing stage report for {name}"))
}

fn assert_execution_smoke_metrics(metrics: &serde_json::Value) {
    let true_fills = metrics
        .get("true_fill_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    assert!(
        true_fills >= 1,
        "execution smoke expects at least one true fill"
    );
    assert!(
        metric_is_available(metrics, "simulated_vwap_p50_bps"),
        "execution smoke expects available VWAP metric"
    );
}

fn metric_is_available(metrics: &serde_json::Value, key: &str) -> bool {
    metrics
        .get(key)
        .and_then(|value| value.get("status"))
        .and_then(serde_json::Value::as_str)
        == Some("available")
}

fn print_acceptance_summary(run_id: &str, report: &MaterializationRunReport) {
    println!("\n=== Phase 5.3 Materialization Smoke Acceptance ===");
    println!("run_id: {run_id}");
    println!("market: {SMOKE_MARKET_ID}");
    println!("opportunity: {SMOKE_OPPORTUNITY_ID}");
    println!(
        "pit_production_eligible: {}",
        report.input_resolution.manifest.is_production_eligible()
    );
    if !report.input_resolution.manifest.is_production_eligible() {
        for missing in &report.input_resolution.manifest.missing_inputs {
            println!(
                "  pit_missing: domain={:?} code={} detail={}",
                missing.domain, missing.code, missing.detail
            );
        }
        for code in &report.input_resolution.manifest.fatal_errors {
            println!("  pit_fatal: {code}");
        }
    }
    for stage in &report.stage_reports {
        println!(
            "  stage={} status={} fingerprints={} records_read={}",
            stage.stage_name,
            stage.status,
            stage.query_fingerprints.len(),
            stage.records_read
        );
    }
    println!("================================================\n");
}
