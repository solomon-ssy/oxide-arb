//! Shared calibration-split primitives (Phase 11.3 §2/§4).
//!
//! Purge (disjoint-window) and embargo checks every empirical calibration fit
//! (`FavoriteLongshotFitService`'s `market_price_bias` fit and
//! `ModelCalibrationFitService`'s `model_score` fit) must pass before its
//! fitted artifact is trusted as an **independent held-out** calibration.
//!
//! This is the minimal, literature-standard `WalkForwardSplit`-with-embargo
//! purge primitive (López de Prado, *Advances in Financial Machine
//! Learning*, Ch. 7): a calibration/fit window must not overlap any `Built`/
//! `Ready` training-dataset window (purge), and — when calibrating a specific
//! model version — must start no earlier than that model's own training
//! window end plus a governed embargo gap (drops the serially-correlated
//! buffer immediately after training). Phase 11.5 upgrades this to full
//! combinatorial purged CV; the interface here is designed to absorb that
//! upgrade without a call-site rewrite.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{TrainingDatasetListQuery, query::TimeWindow},
    enums::quant::TrainingDatasetStatus,
    hashing::CanonicalDigest,
    types::ContentHash,
};
use quant_pivot_repository::traits::TrainingDatasetRepository;
use serde_json::json;

/// True when half-open intervals `[a_start, a_end)` and `[b_start, b_end)` intersect.
#[must_use]
pub fn half_open_windows_overlap(
    a_start: DateTime<Utc>,
    a_end: DateTime<Utc>,
    b_start: DateTime<Utc>,
    b_end: DateTime<Utc>,
) -> bool {
    a_start < b_end && b_start < a_end
}

/// Fail closed when `window` overlaps any `Built`/`Ready` training-dataset window.
///
/// This is the purge primitive shared by every calibration fit. A calibration
/// artifact must never be fit on the same spine a model trains on.
///
/// # Errors
///
/// Returns [`ResearchError::DatasetBuild`] on the first overlapping dataset.
pub async fn assert_disjoint_from_all_training_datasets(
    training_dataset_repo: &dyn TrainingDatasetRepository,
    window: &TimeWindow,
    fit_label: &str,
) -> QuantResult<()> {
    let mut query = TrainingDatasetListQuery::default();
    query.page.size = 100;
    let mut page = 1_u64;
    loop {
        query.page.page = page;
        let batch = training_dataset_repo
            .page(query.clone())
            .await
            .map_err(QuantError::from)?;
        for dataset in &batch.items {
            if !matches!(
                dataset.status,
                TrainingDatasetStatus::Built | TrainingDatasetStatus::Ready
            ) {
                continue;
            }
            if half_open_windows_overlap(
                window.from,
                window.to,
                dataset.window_start,
                dataset.window_end,
            ) {
                return Err(QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "{fit_label} window [{}, {}) overlaps training dataset {} \
                         [{}, {}) in status `{}` — fit and train windows must be disjoint",
                        window.from,
                        window.to,
                        dataset.training_dataset_id,
                        dataset.window_start,
                        dataset.window_end,
                        dataset.status,
                    ),
                }));
            }
        }
        if page.saturating_mul(query.page.size) >= batch.total {
            break;
        }
        page += 1;
    }
    Ok(())
}

/// Fail closed unless `window` starts at or after `reference_window.to + embargo_secs`.
///
/// Directional embargo gap against one specific reference window (the target
/// model's own training-dataset window).
///
/// # Errors
///
/// Returns [`ResearchError::DatasetBuild`] when the embargo gap is not met.
pub fn assert_embargoed_after(
    window: &TimeWindow,
    reference_window: &TimeWindow,
    embargo_secs: i64,
    fit_label: &str,
) -> QuantResult<()> {
    let required_start = reference_window.to + chrono::Duration::seconds(embargo_secs.max(0));
    if window.from < required_start {
        return Err(QuantError::from(ResearchError::DatasetBuild {
            detail: format!(
                "{fit_label} window starts {} but must start at/after {} \
                 (reference window end {} + {embargo_secs}s embargo) — \
                 calibration split must be embargoed from the reference window",
                window.from, required_start, reference_window.to,
            ),
        }));
    }
    Ok(())
}

/// Content hash anchoring a calibration fit to its exact sample set.
///
/// Hashes the window plus sorted, distinct `(subject_key, sampled_at)` pairs.
/// Shared by both calibration-artifact families so `calibration_split_hash`
/// always means the same provenance guarantee.
///
/// # Errors
///
/// Propagates canonical JSON hashing failures.
pub fn calibration_split_hash(
    window: &TimeWindow,
    keys: impl Iterator<Item = (String, DateTime<Utc>)>,
) -> QuantResult<ContentHash> {
    let mut deduped: Vec<(String, DateTime<Utc>)> =
        keys.collect::<HashSet<_>>().into_iter().collect();
    deduped.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let sample_keys: Vec<_> = deduped
        .iter()
        .map(|(subject_key, sampled_at)| {
            json!({
                "subject_key": subject_key,
                "sampled_at": sampled_at,
            })
        })
        .collect();
    CanonicalDigest::content_hash_json(&json!({
        "window_start": window.from,
        "window_end": window.to,
        "sample_count": deduped.len() as u64,
        "sample_keys": sample_keys,
    }))
    .map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("calibration split hash failed: {error}"),
        })
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn half_open_overlap_detects_touching_intervals() {
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mid = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 1, 3, 0, 0, 0).unwrap();
        assert!(half_open_windows_overlap(start, mid, start, end));
        assert!(!half_open_windows_overlap(start, mid, mid, end));
    }

    #[test]
    fn calibration_split_must_be_disjoint_and_embargoed_from_training() {
        let train = TimeWindow {
            from: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap(),
        };
        let overlapping = TimeWindow {
            from: Utc.with_ymd_and_hms(2026, 1, 15, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap(),
        };
        assert!(half_open_windows_overlap(
            overlapping.from,
            overlapping.to,
            train.from,
            train.to,
        ));
        let too_early = TimeWindow {
            from: Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap(),
        };
        assert_embargoed_after(&too_early, &train, 86_400, "calibration")
            .expect_err("zero-gap start at train end must fail embargo");
        let ok = TimeWindow {
            from: Utc.with_ymd_and_hms(2026, 2, 2, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap(),
        };
        assert_embargoed_after(&ok, &train, 86_400, "calibration").expect("embargo satisfied");
    }
}
