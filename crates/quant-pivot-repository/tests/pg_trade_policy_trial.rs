//! Append-only trade-policy trial-ledger integration tests.

use std::time::Duration;

use chrono::Utc;

use quant_pivot_models::{
    domain::{
        FitTradePolicyRequest, NewResearchJob, NewTradePolicyTrialAttempt, TradePolicyFitJobParams,
        TradePolicyFitSelection,
    },
    enums::quant::{
        ResearchJobKind, ResearchJobStatus, TradePolicyTrialScope, TradePolicyTrialStatus,
    },
    types::{
        ArtifactUri, ContentHash, ResearchEvaluationTrack, ResearchJobId, ResearchJobParams,
        RoleCode, TradePolicyCandidateId, TradePolicyTrialAttemptId, TradePolicyTrialMetrics,
        TrainingDatasetId,
    },
};
use quant_pivot_repository::{
    postgres::{PgResearchJobRepository, PgTradePolicyRepository},
    traits::{ResearchJobRepository, TradePolicyRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal::Decimal;
use sea_orm::ConnectionTrait;

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
}

fn job(job_id: ResearchJobId) -> NewResearchJob {
    NewResearchJob {
        job_id,
        kind: ResearchJobKind::TradePolicyFit,
        status: ResearchJobStatus::Queued,
        model_spec_id: None,
        decision_policy_snapshot_id: None,
        params_json: ResearchJobParams::TradePolicyFit(TradePolicyFitJobParams {
            training_dataset_id: TrainingDatasetId::from_v7(),
            request: FitTradePolicyRequest {
                selection: TradePolicyFitSelection {
                    profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
                    pit_cutoff: Utc::now(),
                },
                evaluation_track: ResearchEvaluationTrack::ResearchOnly,
                candidates: Vec::new(),
                reason: "trial-ledger-it".to_owned(),
                idempotency_key: "trial-ledger-it".to_owned(),
            },
        }),
        requested_by: None,
        acting_role: RoleCode::new("admin"),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    }
}

fn successful_attempt(
    fit_job_id: ResearchJobId,
    attempt_ordinal: i64,
    candidate_id: &str,
) -> NewTradePolicyTrialAttempt {
    let mut attempt = NewTradePolicyTrialAttempt {
        trial_attempt_id: TradePolicyTrialAttemptId::from_v7(),
        fit_job_id,
        attempt_ordinal,
        experiment_family_hash: hash('a'),
        research_program_hash: hash('b'),
        candidate_id: TradePolicyCandidateId::parse(candidate_id).expect("candidate id"),
        candidate_hash: hash('c'),
        scope: TradePolicyTrialScope::Fold,
        fold_index: Some(i32::try_from(attempt_ordinal).expect("ordinal fits i32")),
        path_index: None,
        status: TradePolicyTrialStatus::Succeeded,
        metrics_json: Some(TradePolicyTrialMetrics {
            sample_count: 100,
            effective_sample_size: Decimal::from(80),
            net_return_bps: Decimal::from(25),
            sharpe_ratio: Some(Decimal::ONE),
            executable_coverage: Decimal::ONE,
            full_l2_coverage: Decimal::ONE,
            fee_catalog_coverage: Decimal::ONE,
            ambiguous_touch_rate: Decimal::ZERO,
            depth_failure_rate: Decimal::ZERO,
            latency_stress_multiplier: Decimal::ONE,
        }),
        evidence_uri: Some(
            ArtifactUri::parse(format!("s3://trial/{candidate_id}/{attempt_ordinal}.json"))
                .expect("artifact URI"),
        ),
        evidence_hash: Some(hash('d')),
        evidence_row_count: Some(100),
        failure_detail: None,
        row_hash: hash('0'),
    };
    attempt.row_hash = attempt.expected_row_hash().expect("row hash");
    attempt
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trial_ledger_is_ordered_idempotent_cutoff_bound_and_append_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let jobs = PgResearchJobRepository::new(db.clone());
    let trials = PgTradePolicyRepository::new(db.clone());
    let fit_job_id = ResearchJobId::from_v7();
    jobs.enqueue(job(fit_job_id.clone()))
        .await
        .expect("enqueue fit job");

    let first_input = successful_attempt(fit_job_id.clone(), 0, "candidate-a");
    let first = trials
        .append_trial_attempt(first_input.clone())
        .await
        .expect("append first attempt");
    let duplicate = trials
        .append_trial_attempt(first_input)
        .await
        .expect("idempotent retry");
    assert_eq!(duplicate.trial_attempt_id, first.trial_attempt_id);

    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = trials
        .append_trial_attempt(successful_attempt(fit_job_id.clone(), 1, "candidate-b"))
        .await
        .expect("append second attempt");
    assert!(second.created_at > first.created_at);

    let prefix = trials
        .list_trial_attempts(&fit_job_id, Some(first.created_at))
        .await
        .expect("cutoff prefix");
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].attempt_ordinal, 0);
    let full = trials
        .list_trial_attempts(&fit_job_id, None)
        .await
        .expect("full ledger");
    assert_eq!(
        full.iter()
            .map(|attempt| attempt.attempt_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );

    let ordinal_conflict =
        successful_attempt(fit_job_id.clone(), 1, "different-candidate-same-ordinal");
    assert!(trials.append_trial_attempt(ordinal_conflict).await.is_err());

    let mutation = db
        .execute_unprepared("UPDATE quant_trade_policy_trial_attempt SET candidate_id = 'tampered'")
        .await;
    assert!(
        mutation.is_err(),
        "append-only trigger must reject mutation"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trial_ledger_rejects_row_hash_or_terminal_shape_tampering() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let jobs = PgResearchJobRepository::new(db.clone());
    let trials = PgTradePolicyRepository::new(db);
    let fit_job_id = ResearchJobId::from_v7();
    jobs.enqueue(job(fit_job_id.clone()))
        .await
        .expect("enqueue fit job");

    let mut wrong_hash = successful_attempt(fit_job_id.clone(), 0, "candidate-a");
    wrong_hash.row_hash = hash('f');
    assert!(trials.append_trial_attempt(wrong_hash).await.is_err());

    let mut wrong_terminal = successful_attempt(fit_job_id, 1, "candidate-b");
    wrong_terminal.failure_detail = Some("success cannot carry failure".to_owned());
    wrong_terminal.row_hash = wrong_terminal.expected_row_hash().expect("row hash");
    assert!(trials.append_trial_attempt(wrong_terminal).await.is_err());
}
