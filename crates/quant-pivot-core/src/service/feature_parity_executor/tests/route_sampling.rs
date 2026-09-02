//! Native sampler/outcome counterexample; no external serving run is claimed.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, research::ResearchError};
use quant_pivot_models::{
    domain::{
        ports::{FeatureParityExecutionOutcome, FeatureParityExecutionPort},
        quant::NoopProgressSink,
    },
    enums::quant::{FeatureParityRunKind, FeatureParityRunStatus, FeatureParityStage},
    types::{
        ContentHash, FeatureParityDetailSource, FeatureVectorId, MarketId, ModelRunId,
        ModelVersionId, RecommendationReportId, SelectorHashEvidence, SelectorParityEvidence,
        stable_name::FeatureName,
    },
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    super::{
        FeatureParityCandidate, FeatureParityComparison, FeatureParityInputWitness,
        FeatureParityReplayAttempt, FeatureParitySubject, build_outcome, deterministic_sample,
    },
    FixedReplaySource, InMemoryParityRepository, RecordingFactWriter, RecordingIncidentPort,
    evidence, executor, lock, params, run,
};

struct GlobalSelectionFixture {
    crypto: ModelRunId,
    weather: ModelRunId,
    report: RecommendationReportId,
    candidates: Vec<FeatureParityCandidate>,
}

impl GlobalSelectionFixture {
    fn new(decision_at: DateTime<Utc>) -> Self {
        let crypto = ModelRunId::new(
            Uuid::parse_str("00000000-0000-7000-8000-00000000041f").expect("Crypto model id"),
        );
        let weather = ModelRunId::new(
            Uuid::parse_str("00000000-0000-7000-8000-fffffffffff1").expect("Weather model id"),
        );
        let report = RecommendationReportId::new(
            Uuid::parse_str("00000000-0000-7000-8000-fffffffffff2").expect("report id"),
        );
        let mut candidates = Vec::new();
        for subject in [
            FeatureParitySubject::ModelRun(crypto),
            FeatureParitySubject::ModelRun(weather),
            FeatureParitySubject::RecommendationReport(report),
        ] {
            let identity = match &subject {
                FeatureParitySubject::ModelRun(id) => id.to_string(),
                FeatureParitySubject::RecommendationReport(id) => id.to_string(),
            };
            for family in ["crypto", "weather"] {
                for index in 0_u128..5 {
                    let market_id = MarketId::new(format!("{family}-{index}"));
                    let input_witness = match &subject {
                        FeatureParitySubject::ModelRun(model_id)
                            if (*model_id == crypto && family == "crypto")
                                || (*model_id == weather && family == "weather") =>
                        {
                            FeatureParityInputWitness::VerifiedModelInput {
                                feature_vector_id: FeatureVectorId::new(Uuid::from_u128(
                                    if *model_id == crypto { 1000 } else { 2000 } + index,
                                )),
                            }
                        }
                        _ => FeatureParityInputWitness::SelectionOnly,
                    };
                    candidates.push(FeatureParityCandidate {
                        sampling_key: format!("{identity}/{market_id}"),
                        subject: subject.clone(),
                        market_id: Some(market_id),
                        decision_at,
                        input_witness,
                    });
                }
            }
        }
        Self {
            crypto,
            weather,
            report,
            candidates,
        }
    }

    fn selection(&self, candidate: &FeatureParityCandidate) -> FeatureParityComparison {
        let hash = ContentHash::from_bytes([7; 32]);
        let selector = SelectorHashEvidence {
            selector_hash: hash,
            contract_hash: hash,
            boundary_hash: hash,
            selection_policy_hash: hash,
            data_quality_policy_hash: hash,
            feature_schema_hash: hash,
            model_requirements_hash: hash,
            candidates_hash: hash,
            candidate_catalog_hash: hash,
            candidate_book_hash: hash,
            candidate_domain_hash: hash,
            candidate_decision_hash: hash,
            included_hash: hash,
            excluded_hash: hash,
            exclusion_summary_hash: hash,
        };
        let model_run_id = match candidate.subject {
            FeatureParitySubject::ModelRun(id) => Some(id),
            FeatureParitySubject::RecommendationReport(_) => None,
        };
        let exact = evidence(&hash.to_string(), candidate.decision_at);
        FeatureParityComparison {
            sampling_key: candidate.sampling_key.clone(),
            decision_at: candidate.decision_at,
            stage: FeatureParityStage::Selection,
            report_id: Some(self.report),
            model_run_id,
            model_version_id: model_run_id.map(|id| {
                ModelVersionId::new(Uuid::from_u128(if id == self.crypto { 101 } else { 102 }))
            }),
            training_dataset_id: None,
            market_id: candidate.market_id.clone(),
            feature_name: None,
            reason: None,
            online: exact.clone(),
            replay: exact,
            transform_hash: None,
            detail: FeatureParityDetailSource::Selection {
                online_count: 10,
                replay_count: 10,
                selector_evidence: Box::new(SelectorParityEvidence {
                    online: selector,
                    replay: selector,
                }),
                replay_excluded_count: 0,
            },
        }
    }

    fn replay(&self, selected: &[FeatureParityCandidate]) -> FeatureParityReplayAttempt {
        let mut comparisons = Vec::new();
        let mut globals = HashSet::new();
        for candidate in selected {
            let selection = self.selection(candidate);
            comparisons.push(selection.clone());
            let Some(model_id) = selection.model_run_id else {
                continue;
            };
            // This is the real select_comparisons shape: a global Selection
            // row for every candidate, a run-global Prediction once, and
            // market-scoped ModelInput only for the model's actual Route.
            if globals.insert(model_id) {
                let mut prediction = selection.clone();
                prediction.stage = FeatureParityStage::Prediction;
                prediction.market_id = None;
                prediction.detail = FeatureParityDetailSource::Prediction { candidate_count: 5 };
                comparisons.push(prediction);
            }
            if let FeatureParityInputWitness::VerifiedModelInput { feature_vector_id } =
                candidate.input_witness
            {
                let mut input = selection;
                input.stage = FeatureParityStage::ModelInput;
                input.feature_name = Some("encoded.book_spread".to_owned());
                input.transform_hash = Some(ContentHash::from_bytes([9; 32]));
                input.detail = FeatureParityDetailSource::ModelInput {
                    raw_input_name: FeatureName::new("book.spread_bps"),
                    feature_vector_id,
                };
                comparisons.push(input);
            }
        }
        FeatureParityReplayAttempt {
            comparisons,
            pending: Vec::new(),
        }
    }

    fn rejects_model_evidence(&self, attempt: FeatureParityReplayAttempt) {
        let now = self
            .candidates
            .first()
            .expect("global candidates")
            .decision_at;
        let run = run(now);
        let baseline = build_outcome(&run, &self.candidates, self.replay(&self.candidates))
            .expect("the complete two-model evidence baseline is valid");
        assert_eq!(baseline.completion.status, FeatureParityRunStatus::Passed);
        let result = build_outcome(&run, &self.candidates, attempt);
        let Err(QuantError::Research(ResearchError::Determinism { detail })) = result else {
            panic!(
                "one model's missing or foreign evidence must fail closed despite the other model's complete inputs"
            );
        };
        assert!(
            detail.contains(&self.crypto.to_string()),
            "rejection must identify the Crypto model whose evidence is invalid: {detail}"
        );
    }
}

#[test]
fn pending_rediscovery_preserves_budget() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut pending = fixture.candidates.clone();
    for candidate in &mut pending {
        if matches!(candidate.subject, FeatureParitySubject::ModelRun(_)) {
            candidate.input_witness = FeatureParityInputWitness::PendingServingEvidence;
        }
    }
    let pending_sample = deterministic_sample(FeatureParityRunKind::Sampled, pending)
        .expect("all-pending models remain eligible for a later writer attempt");
    let ready_sample =
        deterministic_sample(FeatureParityRunKind::Sampled, fixture.candidates.clone())
            .expect("later completed inputs use their actual verified eligibility");
    assert_eq!(pending_sample.len(), 20);
    assert_eq!(ready_sample.len(), 20);
    for model_id in [fixture.crypto, fixture.weather] {
        assert!(pending_sample.iter().any(|candidate| {
            candidate.subject == FeatureParitySubject::ModelRun(model_id)
                && candidate.input_witness == FeatureParityInputWitness::PendingServingEvidence
        }));
        assert!(ready_sample.iter().any(|candidate| {
            candidate.subject == FeatureParitySubject::ModelRun(model_id)
                && matches!(
                    candidate.input_witness,
                    FeatureParityInputWitness::VerifiedModelInput { .. }
                )
        }));
    }
}

#[test]
fn mixed_pending_inputs_rejects() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut candidates = fixture.candidates.clone();
    let candidate = candidates
        .iter_mut()
        .find(|candidate| candidate.subject == FeatureParitySubject::ModelRun(fixture.crypto))
        .expect("Crypto subject");
    candidate.input_witness = FeatureParityInputWitness::PendingServingEvidence;
    assert!(matches!(
        deterministic_sample(FeatureParityRunKind::Sampled, candidates),
        Err(QuantError::Research(ResearchError::Determinism { .. }))
    ));
}

#[test]
fn completed_model_requires_witness() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut candidates = fixture.candidates.clone();
    for candidate in &mut candidates {
        if candidate.subject == FeatureParitySubject::ModelRun(fixture.crypto) {
            candidate.input_witness = FeatureParityInputWitness::SelectionOnly;
        }
    }
    assert!(matches!(
        deterministic_sample(FeatureParityRunKind::Sampled, candidates),
        Err(QuantError::Research(ResearchError::Determinism { .. }))
    ));
}

#[test]
fn report_rejects_model_witness() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    for witness in [
        FeatureParityInputWitness::PendingServingEvidence,
        FeatureParityInputWitness::VerifiedModelInput {
            feature_vector_id: FeatureVectorId::new(Uuid::from_u128(3000)),
        },
    ] {
        let mut candidates = fixture.candidates.clone();
        let candidate = candidates
            .iter_mut()
            .find(|candidate| {
                candidate.subject == FeatureParitySubject::RecommendationReport(fixture.report)
            })
            .expect("report subject");
        candidate.input_witness = witness;
        assert!(matches!(
            deterministic_sample(FeatureParityRunKind::Sampled, candidates),
            Err(QuantError::Research(ResearchError::Determinism { .. }))
        ));
    }
}

#[test]
fn missing_model_input_rejects() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut attempt = fixture.replay(&fixture.candidates);
    attempt.comparisons.retain(|comparison| {
        comparison.model_run_id != Some(fixture.crypto)
            || comparison.stage != FeatureParityStage::ModelInput
    });
    fixture.rejects_model_evidence(attempt);
}

#[test]
fn missing_model_transform_rejects() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut attempt = fixture.replay(&fixture.candidates);
    for comparison in &mut attempt.comparisons {
        if comparison.model_run_id == Some(fixture.crypto)
            && comparison.stage == FeatureParityStage::ModelInput
        {
            comparison.transform_hash = None;
        }
    }
    fixture.rejects_model_evidence(attempt);
}

#[test]
fn wrong_model_input_rejects() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut attempt = fixture.replay(&fixture.candidates);
    let input = attempt
        .comparisons
        .iter_mut()
        .find(|comparison| {
            comparison.model_run_id == Some(fixture.crypto)
                && comparison.stage == FeatureParityStage::ModelInput
        })
        .expect("Crypto input under its original selected sampling key");
    input.model_run_id = Some(fixture.weather);
    fixture.rejects_model_evidence(attempt);
}

#[test]
fn wrong_model_vector_rejects() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut attempt = fixture.replay(&fixture.candidates);
    let input = attempt
        .comparisons
        .iter_mut()
        .find(|comparison| {
            comparison.model_run_id == Some(fixture.crypto)
                && comparison.stage == FeatureParityStage::ModelInput
        })
        .expect("Crypto input with an exact qualified feature vector");
    let FeatureParityDetailSource::ModelInput {
        feature_vector_id, ..
    } = &mut input.detail
    else {
        panic!("fixture input detail");
    };
    *feature_vector_id = FeatureVectorId::new(Uuid::from_u128(3000));
    fixture.rejects_model_evidence(attempt);
}

#[test]
fn wrong_input_detail_rejects() {
    let fixture = GlobalSelectionFixture::new(Utc::now());
    let mut attempt = fixture.replay(&fixture.candidates);
    let input = attempt
        .comparisons
        .iter_mut()
        .find(|comparison| {
            comparison.model_run_id == Some(fixture.crypto)
                && comparison.stage == FeatureParityStage::ModelInput
        })
        .expect("Crypto input with a stage-consistent detail");
    let FeatureParityDetailSource::ModelInput {
        feature_vector_id, ..
    } = &input.detail
    else {
        panic!("fixture input detail");
    };
    input.detail = FeatureParityDetailSource::FeatureCell {
        feature_vector_id: *feature_vector_id,
    };
    fixture.rejects_model_evidence(attempt);
}

#[tokio::test]
async fn sampled_models_require_inputs() {
    let now = Utc::now();
    let fixture = GlobalSelectionFixture::new(now);
    let mut run = run(now);
    run.kind = FeatureParityRunKind::Sampled;
    run.report_id = Some(fixture.report);
    let selected = deterministic_sample(run.kind, fixture.candidates.clone()).expect("sample");
    assert_eq!(
        selected.len(),
        20,
        "retain the original native 30-to-20 budget"
    );
    let crypto_selected = selected
        .iter()
        .filter(|candidate| candidate.subject == FeatureParitySubject::ModelRun(fixture.crypto))
        .map(|candidate| candidate.market_id.as_ref().expect("market").to_string())
        .collect::<Vec<_>>();
    let parity = Arc::new(InMemoryParityRepository::with_run(run.clone()));
    let source = Arc::new(FixedReplaySource {
        candidates: fixture.candidates.clone(),
        attempts: Mutex::new(VecDeque::from([fixture.replay(&selected)])),
    });
    let writer = Arc::new(RecordingFactWriter::default());
    let outcome = executor(
        Arc::clone(&parity),
        source,
        Arc::clone(&writer),
        Arc::new(RecordingIncidentPort::default()),
    )
    .execute(
        params(&run),
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    )
    .await
    .expect("native executor accepts the exact sampled evidence shape");
    let FeatureParityExecutionOutcome::Completed(result) = outcome else {
        panic!("fully materialized evidence must produce a terminal result");
    };
    let rows = lock(&writer.rows);
    let inputs_for = |model_id| {
        rows.iter()
            .filter(|row| {
                row.model_run_id == Some(model_id)
                    && row.stage == FeatureParityStage::ModelInput.as_str()
            })
            .count()
    };
    let crypto_inputs = inputs_for(fixture.crypto);
    let weather_inputs = inputs_for(fixture.weather);
    println!(
        "native-route-sampling selected={} crypto_selected={crypto_selected:?} crypto_inputs={crypto_inputs} weather_inputs={weather_inputs} rows={} status={:?} persisted_status={:?} transform_present={}",
        selected.len(),
        rows.len(),
        result.status,
        parity.run().status,
        result.transform_hash.is_some(),
    );
    drop(rows);
    assert!(
        weather_inputs > 0,
        "the other Route supplies actual transform-bearing rows"
    );
    assert_eq!(result.status, FeatureParityRunStatus::Passed);
    assert_eq!(parity.run().status, FeatureParityRunStatus::Passed);
    assert!(
        crypto_inputs > 0 && weather_inputs > 0,
        "Passed serving parity must cover ModelInput for each represented model; another Route's transform cannot stand in for Crypto"
    );
}
