use std::mem::size_of;

use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackComparisonArtifactRef, FeedbackShadowContract, FeedbackShadowContractInput,
            FeedbackShadowExecutionPort, FeedbackShadowJobParams, FeedbackShadowSubject,
        },
        quant::{ResearchJobArtifactRef, ShadowObservationWindow},
    },
    types::{
        ArtifactUri, ContentHash, DecisionPolicySnapshotId, FeedbackComparisonArtifactId,
        FeedbackCycleId, FeedbackShadowArtifactId, ModelVersionId, PolicyBundleGeneration,
        Probability, ResearchJobId, builtin_research_profiles,
    },
};
use quant_pivot_research::feedback_shadow::{
    FeedbackShadowArtifact, FeedbackShadowArtifactInput, FeedbackShadowCodec,
    FeedbackShadowEvaluator, FeedbackShadowOutcome, FeedbackShadowUnstableReason,
};
use rust_decimal_macros::dec;
use serde_json::Value;

type ShadowExecutionPort = dyn FeedbackShadowExecutionPort;

const fn hash(seed: u8) -> ContentHash {
    ContentHash::from_bytes([seed; 32])
}

fn instant(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
}

fn contract(
    active_contract: ContentHash,
    candidate_contract: ContentHash,
) -> FeedbackShadowContract {
    let profile = builtin_research_profiles()
        .expect("built-in profiles")
        .remove(0);
    FeedbackShadowContract::try_seal(FeedbackShadowContractInput {
        profile_ref: profile.profile_ref,
        feedback_policy_hash: profile
            .spec
            .feedback_policy
            .content_hash()
            .expect("policy hash"),
        category_scope: profile.spec.category,
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        decision_policy_snapshot_hash: hash(3),
        policy_bundle_generation: PolicyBundleGeneration::FIRST,
        champion_model_version_id: ModelVersionId::from_v7(),
        champion_serving_contract_hash: active_contract,
        candidate_model_version_id: ModelVersionId::from_v7(),
        candidate_serving_contract_hash: candidate_contract,
        observation_window_start: instant(0),
        observation_window_end: instant(3_600),
        minimum_observations: profile.spec.feedback_policy.shadow_minimum_observations,
        required_window_secs: 600,
        minimum_topn_overlap: Probability::new(dec!(0.60)),
    })
    .expect("shadow contract")
}

fn params(contract: FeedbackShadowContract) -> FeedbackShadowJobParams {
    let cycle_hash = hash(10);
    let feedback_cycle_id = FeedbackCycleId::from_idempotency_hash(&cycle_hash);
    FeedbackShadowJobParams {
        feedback_cycle_id,
        cycle_idempotency_hash: cycle_hash,
        artifact_id: FeedbackShadowArtifactId::from_cycle_id(feedback_cycle_id),
        previous: FeedbackComparisonArtifactRef {
            feedback_cycle_id,
            job_id: ResearchJobId::from_v7(),
            artifact_id: FeedbackComparisonArtifactId::from_cycle_id(feedback_cycle_id),
            input_hash: hash(11),
            candidate_family_hash: hash(12),
            decision_policy_snapshot_id: contract.decision_policy_snapshot_id(),
            artifact: ResearchJobArtifactRef {
                uri: ArtifactUri::parse("memory://feedback-shadow/comparison.json")
                    .expect("artifact URI"),
                content_hash: hash(13),
            },
        },
        profile_ref: contract.profile_ref().clone(),
        feedback_policy_hash: contract.feedback_policy_hash(),
        subject: FeedbackShadowSubject::Candidate {
            candidate_recipe_hash: hash(14),
            contract: Box::new(contract),
        },
    }
}

#[test]
fn f10_surface_is_linked() {
    assert!(size_of::<FeedbackShadowContract>() > 0);
    assert!(size_of::<FeedbackShadowJobParams>() > 0);
    assert!(size_of::<FeedbackShadowOutcome>() > 0);
    assert!(size_of::<FeedbackShadowArtifact>() > 0);
    assert_ne!(
        FeedbackShadowCodec::schema_hash().expect("schema hash"),
        hash(0)
    );
    assert!(size_of::<&ShadowExecutionPort>() > 0);
}

#[test]
fn insufficient_omits_stability_metrics() {
    let contract = contract(hash(1), hash(2));
    let outcome = FeedbackShadowEvaluator::evaluate(
        &contract,
        &ShadowObservationWindow {
            sample_count: 999,
            first_decision_at: Some(instant(0)),
            last_decision_at: Some(instant(700)),
            mean_topn_overlap: Some(Probability::new(dec!(0.99))),
            any_hard_divergence: false,
        },
    )
    .expect("typed insufficient");
    let FeedbackShadowOutcome::InsufficientObservations {
        observed, required, ..
    } = &outcome
    else {
        panic!("count floor must fail");
    };
    assert_eq!((*observed, *required), (999, 1_000));
    let encoded = serde_json::to_string(&outcome).expect("serialize outcome");
    assert!(!encoded.contains("mean_topn_overlap"));
    assert!(!encoded.contains("any_hard_divergence"));
}

#[test]
fn stable_unstable_are_typed() {
    let contract = contract(hash(1), hash(2));
    let stable = FeedbackShadowEvaluator::evaluate(
        &contract,
        &ShadowObservationWindow {
            sample_count: 1_000,
            first_decision_at: Some(instant(0)),
            last_decision_at: Some(instant(700)),
            mean_topn_overlap: Some(Probability::new(dec!(0.80))),
            any_hard_divergence: false,
        },
    )
    .expect("stable outcome");
    assert!(matches!(stable, FeedbackShadowOutcome::Stable { .. }));

    let unstable = FeedbackShadowEvaluator::evaluate(
        &contract,
        &ShadowObservationWindow {
            sample_count: 1_000,
            first_decision_at: Some(instant(0)),
            last_decision_at: Some(instant(700)),
            mean_topn_overlap: Some(Probability::new(dec!(0.50))),
            any_hard_divergence: true,
        },
    )
    .expect("unstable outcome");
    let FeedbackShadowOutcome::Unstable { reasons, .. } = unstable else {
        panic!("divergence must be unstable");
    };
    assert_eq!(
        reasons,
        vec![
            FeedbackShadowUnstableReason::HardDivergence,
            FeedbackShadowUnstableReason::TopnOverlapBelowMinimum,
        ]
    );
}

#[test]
fn contracts_must_be_distinct() {
    let profile = builtin_research_profiles()
        .expect("built-in profiles")
        .remove(0);
    let model = ModelVersionId::from_v7();
    let result = FeedbackShadowContract::try_seal(FeedbackShadowContractInput {
        profile_ref: profile.profile_ref,
        feedback_policy_hash: profile
            .spec
            .feedback_policy
            .content_hash()
            .expect("policy hash"),
        category_scope: profile.spec.category,
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        decision_policy_snapshot_hash: hash(3),
        policy_bundle_generation: PolicyBundleGeneration::FIRST,
        champion_model_version_id: model,
        champion_serving_contract_hash: hash(4),
        candidate_model_version_id: model,
        candidate_serving_contract_hash: hash(4),
        observation_window_start: instant(0),
        observation_window_end: instant(3_600),
        minimum_observations: profile.spec.feedback_policy.shadow_minimum_observations,
        required_window_secs: 600,
        minimum_topn_overlap: Probability::new(dec!(0.60)),
    });
    assert!(result.is_err());
}

#[test]
fn artifact_restart_detects_tamper() {
    let params = params(contract(hash(1), hash(2)));
    let FeedbackShadowSubject::Candidate { contract, .. } = &params.subject else {
        panic!("candidate subject");
    };
    let outcome = FeedbackShadowEvaluator::evaluate(
        contract,
        &ShadowObservationWindow {
            sample_count: 1_000,
            first_decision_at: Some(instant(0)),
            last_decision_at: Some(instant(700)),
            mean_topn_overlap: Some(Probability::new(dec!(0.80))),
            any_hard_divergence: false,
        },
    )
    .expect("stable outcome");
    let artifact = FeedbackShadowArtifact::try_seal(FeedbackShadowArtifactInput {
        artifact_id: params.artifact_id,
        feedback_cycle_id: params.feedback_cycle_id,
        job_input_hash: params.input_hash().expect("input hash"),
        previous: params.previous.clone(),
        profile_ref: params.profile_ref.clone(),
        feedback_policy_hash: params.feedback_policy_hash,
        subject: params.subject.clone(),
        outcome,
    })
    .expect("artifact");
    artifact.validate_for(&params).expect("exact params");
    let bytes = FeedbackShadowCodec::encode(&artifact).expect("encode");
    let restored = FeedbackShadowCodec::decode(&bytes).expect("restart decode");
    assert_eq!(restored, artifact);

    let mut tampered: Value = serde_json::from_slice(&bytes).expect("JSON");
    tampered["outcome"]["evidence"]["mean_topn_overlap"] = serde_json::json!("0.20");
    let tampered = serde_json::to_vec(&tampered).expect("tampered bytes");
    assert!(FeedbackShadowCodec::decode(&tampered).is_err());

    let short = FeedbackShadowEvaluator::evaluate(
        contract,
        &ShadowObservationWindow {
            sample_count: 1_000,
            first_decision_at: Some(instant(0)),
            last_decision_at: Some(instant(0) + Duration::seconds(599)),
            mean_topn_overlap: Some(Probability::new(dec!(0.99))),
            any_hard_divergence: false,
        },
    )
    .expect("time insufficient");
    assert!(matches!(
        short,
        FeedbackShadowOutcome::InsufficientObservations { .. }
    ));
}
