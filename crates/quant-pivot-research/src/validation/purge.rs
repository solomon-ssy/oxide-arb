//! Label-horizon-aware purge + embargo splitting (Phase 11.5 §3.1/§3.2, AFML Ch.7).
//!
//! Ordinary k-fold/time-block cross-validation systematically overstates
//! out-of-sample performance whenever labels have an overlapping forward
//! horizon and the underlying series is serially correlated: a training row
//! whose label-maturity window reaches into (or past) the test window has
//! "seen" test-period information through its label. **Purging** removes every
//! such overlapping training row; **embargo** additionally removes training
//! rows immediately following the test window, because a label's *feature*
//! side can still carry serial-correlation leakage even after its own horizon
//! has closed (López de Prado, *Advances in Financial Machine Learning*,
//! Ch.7; <https://en.wikipedia.org/wiki/Purged_cross-validation>).
//!
//! [`TimelineGroup`] is deliberately abstract: it is not the same thing as an
//! `as_of` cross-section — the Buy-side backtest wiring in
//! [`crate::backtest`] happens to group rows by same-`as_of` cross-section
//! (mirroring the [`crate::model::trainer`] LTR query groups), but
//! [`PurgedSplitter`] itself has no opinion on what a group represents. Phase
//! 11.5.1 groups by lot (`position_id`) instead and reuses this module
//! unmodified.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use rust_decimal::{Decimal, prelude::ToPrimitive};

/// One atomic, purge/embargo-indivisible split unit.
///
/// A time interval `[decision_at, label_horizon_end]` inside which every member
/// row's label is not yet fully resolved. Two groups whose intervals overlap
/// must never be split across a train/test boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineGroup {
    /// The group's decision time (e.g. a same-decision-time cross-section,
    /// or a lot's first decision instant).
    pub decision_at: DateTime<Utc>,
    /// The conservative upper bound of every member row's label maturity
    /// (`TrainingLabel::matured_at`, maxed across the group's rows for the
    /// label actually being trained on).
    pub label_horizon_end: DateTime<Utc>,
}

impl TimelineGroup {
    /// Whether this group's `[decision_at, label_horizon_end]` interval overlaps `other`'s.
    #[must_use]
    fn overlaps(&self, other: &Self) -> bool {
        self.decision_at <= other.label_horizon_end && other.decision_at <= self.label_horizon_end
    }
}

/// Purge/embargo configuration.
///
/// Whether to purge by label horizon is not configurable — a training row
/// whose label overlaps the test window is leakage regardless of any operator
/// toggle, so there is deliberately no `purge_by_label_horizon` switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PurgeConfig {
    /// Embargo window as a fraction of the full timeline span (e.g. `0.02` =
    /// 2% of the span immediately after each test group's `label_horizon_end`).
    pub embargo_pct: Decimal,
    /// Absolute floor on the embargo duration in seconds (typically the max
    /// feature lookback). The effective embargo is
    /// `max(embargo_pct × span, min_embargo_secs)` so rolling features cannot
    /// leak across a short percentage window.
    pub min_embargo_secs: u64,
}

impl PurgeConfig {
    /// Percentage-only embargo (no lookback floor) — used by unit tests and
    /// callers that have not yet resolved feature lookback.
    #[must_use]
    pub const fn pct_only(embargo_pct: Decimal) -> Self {
        Self {
            embargo_pct,
            min_embargo_secs: 0,
        }
    }
}

/// The result of purging/embargoing one candidate train/test group assignment.
///
/// Every index is into the caller's `groups` slice; the four sets partition
/// it completely (`train ∪ test ∪ purged ∪ embargoed == 0..groups.len()`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PurgedSplit {
    /// Groups eligible for training after purge/embargo.
    pub train_indices: Vec<usize>,
    /// The requested test groups (echoed back, ascending).
    pub test_indices: Vec<usize>,
    /// Groups removed from training because their interval overlapped a test
    /// group's interval (label-horizon leakage).
    pub purged_indices: Vec<usize>,
    /// Groups removed from training because they fall within the embargo
    /// window immediately after a test group's `label_horizon_end`.
    pub embargoed_indices: Vec<usize>,
}

/// Splits a timeline of atomic groups into purged/embargoed train and test
/// sets. Pure, deterministic, no I/O — safe to call from any thread.
pub trait PurgedSplitter: Send + Sync {
    /// `groups` must be sorted ascending by `as_of` (callers own the sort so
    /// the same sorted slice can be reused across many combinatorial splits).
    /// `test_indices` selects which groups are the test set for this split.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] for an invalid
    /// timeline/index/config or an embargo duration that cannot be represented.
    fn split(
        &self,
        groups: &[TimelineGroup],
        test_indices: &[usize],
        config: &PurgeConfig,
    ) -> QuantResult<PurgedSplit>;
}

/// The production [`PurgedSplitter`]: label-horizon overlap purge + trailing
/// embargo, per AFML Ch.7.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultPurgedSplitter;

impl DefaultPurgedSplitter {
    /// Construct the splitter.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl PurgedSplitter for DefaultPurgedSplitter {
    fn split(
        &self,
        groups: &[TimelineGroup],
        test_indices: &[usize],
        config: &PurgeConfig,
    ) -> QuantResult<PurgedSplit> {
        validate_timeline(groups, config)?;
        let mut test_indices: Vec<usize> = test_indices.to_vec();
        test_indices.sort_unstable();
        test_indices.dedup();

        let embargo_duration = embargo_duration(groups, config)?;
        let test_groups = test_indices
            .iter()
            .map(|&index| {
                groups.get(index).copied().ok_or_else(|| {
                    methodology(format!(
                        "purge test index {index} is outside {} timeline groups",
                        groups.len()
                    ))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let embargo_ends = test_groups
            .iter()
            .map(|test| {
                test.label_horizon_end
                    .checked_add_signed(embargo_duration)
                    .ok_or_else(|| {
                        methodology(format!(
                            "embargo duration overflows test horizon {}",
                            test.label_horizon_end
                        ))
                    })
            })
            .collect::<QuantResult<Vec<_>>>()?;

        let mut train_indices = Vec::new();
        let mut purged_indices = Vec::new();
        let mut embargoed_indices = Vec::new();
        for (i, group) in groups.iter().enumerate() {
            if test_indices.contains(&i) {
                continue;
            }
            if test_groups.iter().any(|test| group.overlaps(test)) {
                purged_indices.push(i);
                continue;
            }
            let embargoed = test_groups.iter().zip(&embargo_ends).any(|(test, end)| {
                group.decision_at > test.label_horizon_end && group.decision_at <= *end
            });
            if embargoed {
                embargoed_indices.push(i);
                continue;
            }
            train_indices.push(i);
        }

        Ok(PurgedSplit {
            train_indices,
            test_indices,
            purged_indices,
            embargoed_indices,
        })
    }
}

/// The embargo window as an absolute [`Duration`]: the larger of
/// `embargo_pct × span` and `min_embargo_secs`. Zero for fewer than two groups
/// or a non-positive span when both knobs are zero.
fn embargo_duration(groups: &[TimelineGroup], config: &PurgeConfig) -> QuantResult<Duration> {
    let floor_secs = i64::try_from(config.min_embargo_secs).map_err(|error| {
        methodology(format!(
            "min_embargo_secs={} exceeds chrono duration range: {error}",
            config.min_embargo_secs
        ))
    })?;
    let floor = Duration::seconds(floor_secs);
    let (Some(first), Some(last)) = (groups.first(), groups.last()) else {
        return Ok(floor);
    };
    let span_secs = (last.label_horizon_end - first.decision_at).num_seconds();
    if span_secs <= 0 || config.embargo_pct <= Decimal::ZERO {
        return Ok(floor);
    }
    let pct_secs = Decimal::from(span_secs)
        .checked_mul(config.embargo_pct)
        .ok_or_else(|| methodology("embargo percentage multiplication overflowed".to_owned()))?
        .round()
        .to_i64()
        .ok_or_else(|| methodology("embargo duration does not fit i64 seconds".to_owned()))?;
    let pct = Duration::seconds(pct_secs);
    Ok(if pct > floor { pct } else { floor })
}

fn validate_timeline(groups: &[TimelineGroup], config: &PurgeConfig) -> QuantResult<()> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&config.embargo_pct) {
        return Err(methodology(format!(
            "embargo_pct must be in [0, 1], got {}",
            config.embargo_pct
        )));
    }
    for (index, group) in groups.iter().enumerate() {
        if group.label_horizon_end < group.decision_at {
            return Err(methodology(format!(
                "timeline group {index} label horizon precedes its decision time"
            )));
        }
        if index > 0 && groups[index - 1].decision_at > group.decision_at {
            return Err(methodology(format!(
                "timeline groups are not sorted at index {index}"
            )));
        }
    }
    Ok(())
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use super::{DefaultPurgedSplitter, PurgeConfig, PurgedSplitter, TimelineGroup};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    /// Hourly groups whose label horizon extends 90 minutes forward (i.e. each
    /// group's interval overlaps the *next* group's `as_of`) — the canonical
    /// AFML overlapping-horizon scenario: a naive time-block split would leak.
    fn overlapping_groups(n: i64) -> Vec<TimelineGroup> {
        (0..n)
            .map(|h| {
                let as_of = Utc.timestamp_opt(1_700_000_000 + h * 3_600, 0).unwrap();
                TimelineGroup {
                    decision_at: as_of,
                    label_horizon_end: as_of + chrono::Duration::minutes(90),
                }
            })
            .collect()
    }

    #[test]
    fn purge_removes_overlapping_label_horizon_rows() {
        // 6 hourly groups, each maturing 90m forward: testing group index 3
        // must purge groups 2 and 4 (their intervals overlap group 3's), but
        // must NOT purge group 1 or 5 (no overlap: gap > 90m).
        let groups = overlapping_groups(6);
        let split = DefaultPurgedSplitter::new()
            .split(&groups, &[3], &PurgeConfig::pct_only(dec!(0)))
            .expect("split");
        assert!(
            split.purged_indices.contains(&2),
            "group 2 overlaps group 3's horizon"
        );
        assert!(
            split.purged_indices.contains(&4),
            "group 4 overlaps group 3's horizon"
        );
        assert!(!split.train_indices.contains(&2));
        assert!(!split.train_indices.contains(&4));
        assert!(split.train_indices.contains(&0));
        assert!(split.train_indices.contains(&1) || split.purged_indices.contains(&1));
    }

    #[test]
    fn feature_timestamp_only_purge_is_insufficient() {
        // A splitter that ignored `label_horizon_end` (treating each group as a
        // zero-width instant) would find zero overlap between adjacent hourly
        // groups, since their `as_of`s never collide. The label-horizon-aware
        // splitter must still purge — this is the #7 regression this module closes.
        let groups = overlapping_groups(6);
        let split = DefaultPurgedSplitter::new()
            .split(&groups, &[3], &PurgeConfig::pct_only(dec!(0)))
            .expect("split");
        assert!(
            !split.purged_indices.is_empty(),
            "label-horizon-aware purge must remove overlapping neighbors even though \
             as_of timestamps themselves never collide"
        );
    }

    #[test]
    fn embargo_removes_post_test_rows() {
        // 100 groups spaced 1 hour apart, non-overlapping labels (horizon = 0),
        // 2% embargo over a ~100h span ≈ 2h ≈ 2 groups after the test block.
        let groups: Vec<TimelineGroup> = (0..100)
            .map(|h| {
                let as_of = Utc.timestamp_opt(1_700_000_000 + h * 3_600, 0).unwrap();
                TimelineGroup {
                    decision_at: as_of,
                    label_horizon_end: as_of,
                }
            })
            .collect();
        let split = DefaultPurgedSplitter::new()
            .split(&groups, &[50], &PurgeConfig::pct_only(dec!(0.02)))
            .expect("split");
        assert!(
            split.embargoed_indices.contains(&51),
            "the group immediately after the test group must be embargoed"
        );
        assert!(
            !split.embargoed_indices.contains(&49),
            "embargo is forward-only (after the test window), never backward"
        );
        assert!(split.train_indices.contains(&49));
    }

    #[test]
    fn zero_embargo_pct_embargoes_nothing() {
        let groups: Vec<TimelineGroup> = (0..10)
            .map(|h| {
                let as_of = Utc.timestamp_opt(1_700_000_000 + h * 3_600, 0).unwrap();
                TimelineGroup {
                    decision_at: as_of,
                    label_horizon_end: as_of,
                }
            })
            .collect();
        let split = DefaultPurgedSplitter::new()
            .split(&groups, &[5], &PurgeConfig::pct_only(dec!(0)))
            .expect("split");
        assert!(split.embargoed_indices.is_empty());
        assert!(split.train_indices.contains(&6));
    }

    #[test]
    fn purged_splitter_is_agnostic_to_what_a_group_represents() {
        // No field, method, or behavior here depends on "as_of" meaning a
        // cross-section — an abstract, business-meaning-free set of intervals
        // (e.g. 11.5.1's per-lot groups) must split identically to any other.
        let groups: Vec<TimelineGroup> = (0..5)
            .map(|i| {
                let start = Utc.timestamp_opt(1_000_000 + i * 1_000, 0).unwrap();
                TimelineGroup {
                    decision_at: start,
                    label_horizon_end: start + chrono::Duration::seconds(500),
                }
            })
            .collect();
        let split = DefaultPurgedSplitter::new()
            .split(&groups, &[2], &PurgeConfig::pct_only(dec!(0.1)))
            .expect("split");
        assert_eq!(split.test_indices, vec![2]);
        assert_eq!(
            split.train_indices.len()
                + split.purged_indices.len()
                + split.embargoed_indices.len()
                + 1,
            groups.len()
        );
    }

    #[test]
    fn every_index_is_partitioned_exactly_once() {
        let groups = overlapping_groups(12);
        let split = DefaultPurgedSplitter::new()
            .split(&groups, &[4, 5], &PurgeConfig::pct_only(dec!(0.05)))
            .expect("split");
        let mut seen = vec![0_u8; groups.len()];
        for &i in &split.train_indices {
            seen[i] += 1;
        }
        for &i in &split.test_indices {
            seen[i] += 1;
        }
        for &i in &split.purged_indices {
            seen[i] += 1;
        }
        for &i in &split.embargoed_indices {
            seen[i] += 1;
        }
        assert!(
            seen.iter().all(|&count| count == 1),
            "every group must land in exactly one bucket: {seen:?}"
        );
    }

    #[test]
    fn embargo_duration_overflow_is_rejected_instead_of_saturated() {
        let groups = overlapping_groups(2);
        let result = DefaultPurgedSplitter::new().split(
            &groups,
            &[0],
            &PurgeConfig {
                embargo_pct: dec!(0),
                min_embargo_secs: u64::MAX,
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn out_of_range_test_index_is_rejected_instead_of_panicking() {
        let groups = overlapping_groups(2);
        assert!(
            DefaultPurgedSplitter::new()
                .split(&groups, &[2], &PurgeConfig::pct_only(dec!(0)))
                .is_err()
        );
    }
}
