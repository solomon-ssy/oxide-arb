//! Integration tests for [`CalibrationUpdater::tick()`].
//!
//! Exercises the full reconciliation cycle with a mock data source,
//! verifying Gamma/CTF cross-check logic, in-memory calibrator updates,
//! `MoM` prior re-estimation, and persistence calls.

use oxide_arb_algorithm::calibration::{
    CalibrationDataSource, CalibrationEntry, CalibrationUpdater, ResolutionCalibrator,
    UnresolvedOutcome,
};
use oxide_arb_error::algorithm::AlgoError;
use oxide_arb_models::{
    config::CalibrationConfig,
    domain::calibration::{BucketKey, UpsertCalibration},
    enums::calibration::{DurationBucket, PriceZone},
    enums::common::MarketCategory,
    types::MarketId,
};
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── Mock Data Source ────────────────────────────────────────────────────

/// Records all calls made to the data source for assertion.
#[derive(Debug, Default, Clone)]
struct CallLog {
    resolved_outcomes: Vec<(i64, bool)>,
    saved_buckets: Vec<Vec<UpsertCalibration>>,
}

/// Configurable mock implementing [`CalibrationDataSource`].
struct MockDataSource {
    unresolved: Mutex<Vec<UnresolvedOutcome>>,
    gamma_responses: Mutex<HashMap<String, Option<bool>>>,
    ctf_responses: Mutex<HashMap<String, Option<bool>>>,
    log: Mutex<CallLog>,
}

impl MockDataSource {
    fn new() -> Self {
        Self {
            unresolved: Mutex::new(Vec::new()),
            gamma_responses: Mutex::new(HashMap::new()),
            ctf_responses: Mutex::new(HashMap::new()),
            log: Mutex::new(CallLog::default()),
        }
    }

    fn with_unresolved(self, outcomes: Vec<UnresolvedOutcome>) -> Self {
        *self.unresolved.lock().unwrap() = outcomes;
        self
    }

    fn with_gamma(self, market_id: &str, result: Option<bool>) -> Self {
        self.gamma_responses
            .lock()
            .unwrap()
            .insert(market_id.to_owned(), result);
        self
    }

    fn with_ctf(self, market_id: &str, result: Option<bool>) -> Self {
        self.ctf_responses
            .lock()
            .unwrap()
            .insert(market_id.to_owned(), result);
        self
    }

    fn call_log(&self) -> CallLog {
        self.log.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl CalibrationDataSource for MockDataSource {
    async fn get_unresolved_outcomes(&self) -> Result<Vec<UnresolvedOutcome>, AlgoError> {
        Ok(self.unresolved.lock().unwrap().clone())
    }

    async fn check_gamma_resolution(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<bool>, AlgoError> {
        let map = self.gamma_responses.lock().unwrap();
        map.get(market_id.as_str()).map_or(Ok(None), |v| Ok(*v))
    }

    async fn check_ctf_resolution(&self, market_id: &MarketId) -> Result<Option<bool>, AlgoError> {
        let map = self.ctf_responses.lock().unwrap();
        map.get(market_id.as_str()).map_or(Ok(None), |v| Ok(*v))
    }

    async fn upsert_buckets(&self, entries: &[UpsertCalibration]) -> Result<(), AlgoError> {
        self.log
            .lock()
            .unwrap()
            .saved_buckets
            .push(entries.to_vec());
        Ok(())
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), AlgoError> {
        self.log
            .lock()
            .unwrap()
            .resolved_outcomes
            .push((outcome_id, actual_yes));
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn default_config() -> CalibrationConfig {
    CalibrationConfig {
        min_sample_size: 10,
        bootstrap_alpha: dec!(2),
        bootstrap_beta: dec!(0.2),
        ..CalibrationConfig::default()
    }
}

const fn make_bucket_key(zone: PriceZone) -> BucketKey {
    BucketKey {
        category: MarketCategory::Geopolitics,
        price_zone: zone,
        duration_bucket: DurationBucket::Medium,
    }
}

fn make_outcome(id: i64, market: &str, zone: PriceZone, predicted_yes: bool) -> UnresolvedOutcome {
    UnresolvedOutcome {
        outcome_id: id,
        market_id: MarketId::new(market),
        bucket_key: make_bucket_key(zone),
        predicted_yes,
    }
}

/// Find a specific bucket entry by key from the calibrator's entries.
fn find_entry(calibrator: &ResolutionCalibrator, key: &BucketKey) -> Option<CalibrationEntry> {
    calibrator
        .all_entries()
        .into_iter()
        .find(|e| &e.bucket_key == key)
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn happy_path_gamma_confirms_and_updates_calibrator() {
    let outcome = make_outcome(1, "mkt-1", PriceZone::Z97, true);

    let ds = Arc::new(
        MockDataSource::new()
            .with_unresolved(vec![outcome.clone()])
            .with_gamma("mkt-1", Some(true)),
    );
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.total_unresolved, 1);
    assert_eq!(stats.resolved, 1);
    assert_eq!(stats.gamma_miss, 0);

    // Calibrator should have recorded a correct outcome
    let entry = find_entry(&calibrator, &outcome.bucket_key)
        .expect("Bucket should exist after recording outcome");
    assert_eq!(entry.total_count, 1);
    assert_eq!(entry.correct_count, 1);

    // Persistence should have been called
    let log = ds.call_log();
    assert_eq!(log.resolved_outcomes, vec![(1, true)]);
    assert_eq!(log.saved_buckets.len(), 1);
}

#[tokio::test]
async fn gamma_miss_increments_counter_and_skips() {
    let outcome = make_outcome(1, "mkt-no-gamma", PriceZone::Z98, true);

    let ds = Arc::new(
        MockDataSource::new().with_unresolved(vec![outcome.clone()]),
        // No gamma response configured → returns None
    );
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.total_unresolved, 1);
    assert_eq!(stats.resolved, 0);
    assert_eq!(stats.gamma_miss, 1);

    // Calibrator should remain empty
    assert_eq!(calibrator.bucket_count(), 0);

    // No persistence calls
    let log = ds.call_log();
    assert!(log.resolved_outcomes.is_empty());
    assert!(log.saved_buckets.is_empty());
}

#[tokio::test]
async fn gamma_ctf_disagree_skips_outcome() {
    let outcome = make_outcome(1, "mkt-disagree", PriceZone::Z97, true);

    let ds = Arc::new(
        MockDataSource::new()
            .with_unresolved(vec![outcome.clone()])
            .with_gamma("mkt-disagree", Some(true))
            .with_ctf("mkt-disagree", Some(false)), // Disagrees with Gamma
    );
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.total_unresolved, 1);
    assert_eq!(stats.resolved, 0);
    assert_eq!(stats.gamma_miss, 0);

    // Calibrator unmodified
    assert_eq!(calibrator.bucket_count(), 0);

    let log = ds.call_log();
    assert!(log.resolved_outcomes.is_empty());
}

#[tokio::test]
async fn ctf_unavailable_falls_through_to_gamma() {
    let outcome = make_outcome(1, "mkt-no-ctf", PriceZone::Z99, false);

    let ds = Arc::new(
        MockDataSource::new()
            .with_unresolved(vec![outcome.clone()])
            .with_gamma("mkt-no-ctf", Some(false)),
        // CTF not configured → returns None (fallback to Gamma)
    );
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.resolved, 1);

    // predicted_yes=false, actual=false → was_correct=true
    let entry = find_entry(&calibrator, &outcome.bucket_key).expect("Bucket should exist");
    assert_eq!(entry.correct_count, 1);

    let log = ds.call_log();
    assert_eq!(log.resolved_outcomes, vec![(1, false)]);
}

#[tokio::test]
async fn incorrect_prediction_records_miss() {
    let outcome = make_outcome(1, "mkt-wrong", PriceZone::Z97, true);

    let ds = Arc::new(
        MockDataSource::new()
            .with_unresolved(vec![outcome.clone()])
            .with_gamma("mkt-wrong", Some(false)), // Predicted YES but resolved NO
    );
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.resolved, 1);

    // predicted_yes=true, actual=false → was_correct=false
    let entry = find_entry(&calibrator, &outcome.bucket_key).expect("Bucket should exist");
    assert_eq!(entry.total_count, 1);
    assert_eq!(entry.correct_count, 0);
}

#[tokio::test]
async fn zero_unresolved_does_not_trigger_prior_update() {
    let ds = Arc::new(MockDataSource::new());
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.total_unresolved, 0);
    assert_eq!(stats.resolved, 0);

    let log = ds.call_log();
    assert!(log.saved_buckets.is_empty());
}

#[tokio::test]
async fn multiple_outcomes_mixed_results() {
    let outcomes = vec![
        make_outcome(1, "mkt-a", PriceZone::Z97, true),
        make_outcome(2, "mkt-b", PriceZone::Z98, false),
        make_outcome(3, "mkt-c", PriceZone::Z99, true), // Gamma miss
    ];

    let ds = Arc::new(
        MockDataSource::new()
            .with_unresolved(outcomes)
            .with_gamma("mkt-a", Some(true)) // Correct
            .with_gamma("mkt-b", Some(true)), // Incorrect (predicted NO, actual YES)
                                              // mkt-c: no gamma → miss
    );
    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.total_unresolved, 3);
    assert_eq!(stats.resolved, 2);
    assert_eq!(stats.gamma_miss, 1);

    // mkt-a bucket: predicted YES, actual YES → correct
    let key_a = make_bucket_key(PriceZone::Z97);
    let entry_a = find_entry(&calibrator, &key_a).expect("Z97 bucket should exist");
    assert_eq!(entry_a.total_count, 1);
    assert_eq!(entry_a.correct_count, 1);

    // mkt-b bucket: predicted NO, actual YES → incorrect
    let key_b = make_bucket_key(PriceZone::Z98);
    let entry_b = find_entry(&calibrator, &key_b).expect("Z98 bucket should exist");
    assert_eq!(entry_b.total_count, 1);
    assert_eq!(entry_b.correct_count, 0);

    let log = ds.call_log();
    assert_eq!(log.resolved_outcomes.len(), 2);
    assert_eq!(log.saved_buckets.len(), 1); // Prior update triggered once
}

#[tokio::test]
async fn prior_update_modifies_sparse_buckets() {
    // MoM requires at least 3 qualified buckets (total_count >= min_sample_size)
    // with non-zero variance among their rates.
    let sparse_key = BucketKey {
        category: MarketCategory::Geopolitics,
        price_zone: PriceZone::Z95,
        duration_bucket: DurationBucket::Short,
    };
    let dense_key_1 = BucketKey {
        category: MarketCategory::Geopolitics,
        price_zone: PriceZone::Z97,
        duration_bucket: DurationBucket::Medium,
    };
    let dense_key_2 = BucketKey {
        category: MarketCategory::Geopolitics,
        price_zone: PriceZone::Z98,
        duration_bucket: DurationBucket::Medium,
    };
    let dense_key_3 = BucketKey {
        category: MarketCategory::Geopolitics,
        price_zone: PriceZone::Z99,
        duration_bucket: DurationBucket::Long,
    };
    let config = default_config(); // min_sample_size = 10

    let initial_entries = vec![
        CalibrationEntry {
            bucket_key: sparse_key.clone(),
            total_count: 3,
            correct_count: 2,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        },
        // 3 dense buckets with varying rates to ensure non-zero variance
        CalibrationEntry {
            bucket_key: dense_key_1.clone(),
            total_count: 30,
            correct_count: 27, // rate = 0.90
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        },
        CalibrationEntry {
            bucket_key: dense_key_2.clone(),
            total_count: 20,
            correct_count: 16, // rate = 0.80
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        },
        CalibrationEntry {
            bucket_key: dense_key_3.clone(),
            total_count: 25,
            correct_count: 24, // rate = 0.96
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        },
    ];
    let calibrator = Arc::new(ResolutionCalibrator::from_entries(
        initial_entries,
        config.clone(),
    ));

    // Trigger an outcome that resolves, so update_priors fires
    let outcome = UnresolvedOutcome {
        outcome_id: 99,
        market_id: MarketId::new("mkt-trigger"),
        bucket_key: dense_key_1.clone(),
        predicted_yes: true,
    };
    let ds = Arc::new(
        MockDataSource::new()
            .with_unresolved(vec![outcome])
            .with_gamma("mkt-trigger", Some(true)),
    );
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), config.clone());

    updater.tick().await.unwrap();

    // Dense buckets should keep original priors (total_count >= min_sample_size)
    let dense_entry = find_entry(&calibrator, &dense_key_2).expect("Dense bucket 2 should exist");
    assert_eq!(dense_entry.alpha_prior, dec!(2));
    assert_eq!(dense_entry.beta_prior, dec!(0.2));

    // Sparse bucket should have updated priors from MoM estimation.
    // With 3 dense buckets having rates 0.90, 0.80, 0.96 (variance > 0),
    // MoM will produce α,β different from (2, 0.2).
    let sparse_entry = find_entry(&calibrator, &sparse_key).expect("Sparse bucket should exist");
    assert!(
        sparse_entry.alpha_prior != dec!(2) || sparse_entry.beta_prior != dec!(0.2),
        "Sparse bucket priors should have been updated by MoM: α={}, β={}",
        sparse_entry.alpha_prior,
        sparse_entry.beta_prior,
    );

    // Verify upsert_buckets was called
    let log = ds.call_log();
    assert_eq!(log.saved_buckets.len(), 1);
}

#[tokio::test]
async fn batch_many_outcomes_all_resolve() {
    let outcomes: Vec<UnresolvedOutcome> = (0..50)
        .map(|i| {
            let zone = match i % 3 {
                0 => PriceZone::Z97,
                1 => PriceZone::Z98,
                _ => PriceZone::Z99,
            };
            make_outcome(i64::from(i), &format!("mkt-{i}"), zone, i % 2 == 0)
        })
        .collect();

    let ds = {
        let mut builder = MockDataSource::new().with_unresolved(outcomes.clone());
        for outcome in &outcomes {
            builder = builder.with_gamma(outcome.market_id.as_str(), Some(true));
        }
        Arc::new(builder)
    };

    let calibrator = Arc::new(ResolutionCalibrator::empty(default_config()));
    let updater = CalibrationUpdater::new(calibrator.clone(), ds.clone(), default_config());

    let stats = updater.tick().await.unwrap();

    assert_eq!(stats.total_unresolved, 50);
    assert_eq!(stats.resolved, 50);
    assert_eq!(stats.gamma_miss, 0);

    // All outcomes resolved → prior update should have been called
    let log = ds.call_log();
    assert_eq!(log.resolved_outcomes.len(), 50);
    assert_eq!(log.saved_buckets.len(), 1);

    // Verify calibrator has entries across the 3 zones
    assert!(calibrator.bucket_count() >= 3);
}
