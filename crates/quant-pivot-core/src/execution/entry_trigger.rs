//! Durable entry-trigger observation state machine.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::EntryTriggerTransition,
    enums::quant::{EntryTriggerState, PriceComparison},
    types::{EntryTrigger, Price},
};

/// Process-local continuity proof. It is deliberately not reconstructed after
/// restart: a persisted `Confirming` row without this proof resets fail-closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmationProgress {
    pub confirming_since: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEvaluation {
    pub transition: Option<EntryTriggerTransition>,
    pub progress: Option<ConfirmationProgress>,
    pub ready: bool,
}

impl TriggerEvaluation {
    fn transition(
        state: EntryTriggerState,
        progress: Option<ConfirmationProgress>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            transition: Some(EntryTriggerTransition {
                state,
                confirming_since: progress.map(|value| value.confirming_since),
                last_observed_at: progress.map(|value| value.last_observed_at),
                ready_at: (state == EntryTriggerState::Ready).then_some(now),
            }),
            progress,
            ready: state == EntryTriggerState::Ready,
        }
    }
}

/// Observe one fresh best ask. The caller persists only returned transitions
/// and keeps `progress` in memory between scans.
#[must_use]
pub fn evaluate_entry_trigger(
    trigger: &EntryTrigger,
    persisted_state: EntryTriggerState,
    progress: Option<ConfirmationProgress>,
    best_ask: Option<Price>,
    book_fresh: bool,
    now: DateTime<Utc>,
) -> TriggerEvaluation {
    let EntryTrigger::PriceCondition {
        comparison,
        threshold,
        confirmation_secs,
        max_observation_gap_ms,
    } = trigger
    else {
        return TriggerEvaluation {
            transition: None,
            progress: None,
            ready: true,
        };
    };

    let satisfied = book_fresh
        && best_ask.is_some_and(|price| match comparison {
            PriceComparison::AtOrAbove => price >= *threshold,
            PriceComparison::AtOrBelow => price <= *threshold,
        });
    if !satisfied {
        return if persisted_state == EntryTriggerState::Waiting && progress.is_none() {
            TriggerEvaluation {
                transition: None,
                progress: None,
                ready: false,
            }
        } else {
            TriggerEvaluation::transition(EntryTriggerState::Waiting, None, now)
        };
    }

    if persisted_state == EntryTriggerState::Ready {
        return TriggerEvaluation {
            transition: None,
            progress,
            ready: true,
        };
    }

    if *confirmation_secs == 0 {
        return if persisted_state == EntryTriggerState::Ready {
            TriggerEvaluation {
                transition: None,
                progress: None,
                ready: true,
            }
        } else {
            TriggerEvaluation::transition(EntryTriggerState::Ready, None, now)
        };
    }

    let Some(mut progress) = progress else {
        if persisted_state == EntryTriggerState::Confirming {
            return TriggerEvaluation::transition(EntryTriggerState::Waiting, None, now);
        }
        let progress = ConfirmationProgress {
            confirming_since: now,
            last_observed_at: now,
        };
        return TriggerEvaluation::transition(EntryTriggerState::Confirming, Some(progress), now);
    };
    let max_gap = i64::try_from(*max_observation_gap_ms).unwrap_or(i64::MAX);
    if now - progress.last_observed_at > Duration::milliseconds(max_gap) {
        progress = ConfirmationProgress {
            confirming_since: now,
            last_observed_at: now,
        };
        return TriggerEvaluation::transition(EntryTriggerState::Confirming, Some(progress), now);
    }
    progress.last_observed_at = now;
    let required = i64::try_from(*confirmation_secs).unwrap_or(i64::MAX);
    if now - progress.confirming_since >= Duration::seconds(required) {
        TriggerEvaluation::transition(EntryTriggerState::Ready, Some(progress), now)
    } else {
        TriggerEvaluation {
            transition: None,
            progress: Some(progress),
            ready: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::{EntryTriggerState, PriceComparison},
        types::{EntryTrigger, Price},
    };
    use rust_decimal_macros::dec;

    use super::{ConfirmationProgress, evaluate_entry_trigger};

    fn at(second: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, second)
            .single()
            .expect("valid test time")
    }

    fn trigger() -> EntryTrigger {
        EntryTrigger::PriceCondition {
            comparison: PriceComparison::AtOrBelow,
            threshold: Price::new(dec!(0.50)),
            confirmation_secs: 5,
            max_observation_gap_ms: 2_000,
        }
    }

    #[test]
    fn confirmation_requires_continuous_process_local_observations() {
        let first = evaluate_entry_trigger(
            &trigger(),
            EntryTriggerState::Waiting,
            None,
            Some(Price::new(dec!(0.49))),
            true,
            at(0),
        );
        let progress = first.progress.expect("confirmation started");
        let gap = evaluate_entry_trigger(
            &trigger(),
            EntryTriggerState::Confirming,
            Some(progress),
            Some(Price::new(dec!(0.49))),
            true,
            at(3),
        );
        assert!(!gap.ready);
        assert_eq!(
            gap.progress
                .expect("confirmation restarted")
                .confirming_since,
            at(3)
        );
    }

    #[test]
    fn restart_without_progress_resets_confirmation() {
        let result = evaluate_entry_trigger(
            &trigger(),
            EntryTriggerState::Confirming,
            None,
            Some(Price::new(dec!(0.49))),
            true,
            at(5),
        );
        assert_eq!(
            result.transition.expect("reset transition").state,
            EntryTriggerState::Waiting
        );
    }

    #[test]
    fn false_condition_resets_to_waiting() {
        let result = evaluate_entry_trigger(
            &trigger(),
            EntryTriggerState::Confirming,
            Some(ConfirmationProgress {
                confirming_since: at(0),
                last_observed_at: at(1),
            }),
            Some(Price::new(dec!(0.51))),
            true,
            at(2),
        );
        assert_eq!(
            result.transition.expect("waiting transition").state,
            EntryTriggerState::Waiting
        );
    }
}
