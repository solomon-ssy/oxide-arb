//! Shared calibration-split primitives.
//!
//! Purge (disjoint-window) and embargo checks every empirical calibration fit
//! (`BiasTableFitService`'s `market_price_bias` fit and
//! `ModelCalibrationFitService`'s `model_score` fit) must pass before its
//! fitted artifact is trusted as an **independent held-out** calibration.
//!
//! This is the minimal, literature-standard `WalkForwardSplit`-with-embargo
//! purge primitive (López de Prado, *Advances in Financial Machine
//! Learning*, Ch. 7): a calibration/fit window must not overlap any `Ready`
//! training-dataset window (purge), and — when calibrating a specific
//! model version — must start no earlier than that model's own training
//! window end plus a governed embargo gap (drops the serially-correlated
//! buffer immediately after training). Full combinatorial purged CV uses a
//! separate validation path; this primitive remains the single-window
//! calibration guard.

use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{api::TrainingDatasetListQuery, pagination::PageRequest, query::TimeWindow},
    enums::quant::{DatasetPurpose, TrainingDatasetStatus},
    hashing::CanonicalDigest,
    types::{ContentHash, MarketId, TokenId},
};
use quant_pivot_repository::traits::TrainingDatasetRepository;
use serde::Serialize;

/// Exact identity of one sample admitted to a calibration fit.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct CalibrationSampleKey {
    /// Primary calibrated subject.
    pub subject_key: MarketId,
    /// Optional instrument dimension. Model-score calibration binds the exact
    /// outcome token; market-price bias calibrates the market subject itself.
    pub instrument_key: Option<TokenId>,
    /// Point-in-time sample boundary.
    pub sampled_at: DateTime<Utc>,
}

impl CalibrationSampleKey {
    /// Build a market-level calibration sample identity.
    #[must_use]
    pub const fn for_subject(subject_key: MarketId, sampled_at: DateTime<Utc>) -> Self {
        Self {
            subject_key,
            instrument_key: None,
            sampled_at,
        }
    }

    /// Build an instrument-level calibration sample identity.
    #[must_use]
    pub const fn for_instrument(
        subject_key: MarketId,
        instrument_key: TokenId,
        sampled_at: DateTime<Utc>,
    ) -> Self {
        Self {
            subject_key,
            instrument_key: Some(instrument_key),
            sampled_at,
        }
    }
}

#[derive(Serialize)]
struct CalibrationSplitHashInput<'a> {
    contract: &'static str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    sample_count: u64,
    sample_keys: &'a [CalibrationSampleKey],
}

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

/// Fail closed when `window` overlaps any `Ready` training-dataset window.
///
/// This is the purge primitive shared by every calibration fit. A calibration
/// artifact must never be fit on the same spine a model trains on.
///
/// # Errors
///
/// Returns [`ResearchError::DatasetBuild`] on the first overlapping dataset.
pub async fn assert_dataset_disjoint(
    training_dataset_repo: &dyn TrainingDatasetRepository,
    window: &TimeWindow,
    fit_label: &str,
) -> QuantResult<()> {
    let mut query = TrainingDatasetListQuery {
        purpose: Some(DatasetPurpose::Training),
        page: PageRequest {
            size: PageRequest::MAX_SIZE,
            ..PageRequest::default()
        },
        ..TrainingDatasetListQuery::default()
    };
    let mut page = 1_u64;
    loop {
        query.page.page = page;
        let batch = training_dataset_repo
            .page(query.clone())
            .await
            .map_err(QuantError::from)?;
        for dataset in &batch.items {
            if dataset.status != TrainingDatasetStatus::Ready
                || dataset.purpose != DatasetPurpose::Training
            {
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
        let consumed = page.checked_mul(query.page.size).ok_or_else(|| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: "calibration dataset pagination count overflow".to_owned(),
            })
        })?;
        if consumed >= batch.total {
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
    let required_start = reference_window.to + Duration::seconds(embargo_secs.max(0));
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
/// Hashes the window plus sorted, distinct subject/instrument/time identities.
/// Shared by both calibration-artifact families so `calibration_split_hash`
/// always means the same provenance guarantee.
///
/// # Errors
///
/// Propagates canonical JSON hashing failures.
pub fn calibration_split_hash(
    window: &TimeWindow,
    keys: impl Iterator<Item = CalibrationSampleKey>,
) -> QuantResult<ContentHash> {
    let mut deduped = keys.collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();
    deduped.sort_by(|a, b| {
        a.subject_key
            .cmp(&b.subject_key)
            .then(a.instrument_key.cmp(&b.instrument_key))
            .then(a.sampled_at.cmp(&b.sampled_at))
    });
    let sample_count =
        u64::try_from(deduped.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("calibration split sample count exceeds u64: {error}"),
        })?;
    CanonicalDigest::content_hash_json(&CalibrationSplitHashInput {
        contract: "quant-pivot/calibration-split/v2",
        window_start: window.from,
        window_end: window.to,
        sample_count,
        sample_keys: &deduped,
    })
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
    fn half_open_detects_intervals() {
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mid = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 1, 3, 0, 0, 0).unwrap();
        assert!(half_open_windows_overlap(start, mid, start, end));
        assert!(!half_open_windows_overlap(start, mid, mid, end));
    }

    #[test]
    fn calibration_split_disjoint_training() {
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

    #[test]
    fn split_commits_instrument_identity() {
        let sampled_at = Utc
            .with_ymd_and_hms(2026, 2, 2, 0, 0, 0)
            .single()
            .expect("valid fixture timestamp");
        let window = TimeWindow {
            from: sampled_at,
            to: sampled_at + Duration::hours(1),
        };
        let market = MarketId::new("market");
        let yes =
            CalibrationSampleKey::for_instrument(market.clone(), TokenId::new("yes"), sampled_at);
        let no = CalibrationSampleKey::for_instrument(market, TokenId::new("no"), sampled_at);
        let one = calibration_split_hash(&window, [yes.clone()].into_iter()).expect("single key");
        let both =
            calibration_split_hash(&window, [yes.clone(), no.clone()].into_iter()).expect("both");
        let reordered = calibration_split_hash(&window, [no, yes].into_iter()).expect("reordered");
        assert_ne!(one, both, "the outcome token must enter the split hash");
        assert_eq!(
            both, reordered,
            "input order must not affect the split hash"
        );
    }
}
