//! Chainlink `AggregatorV3` composite round identifiers (`uint80`).
//!
//! OCR2-era feeds encode round ids as `(phase_id << 64) | aggregator_round_id`.
//! Phase ids live in the high bits; values with `phase_id >= 1` exceed [`u64::MAX`]
//! and must never be narrowed to `u64`.

use alloy::primitives::Uint;

/// On-chain `AggregatorV3.latestRoundData().roundId` (`uint80`).
pub type RoundId = Uint<80, 2>;

/// Smallest valid round id (phase 0, aggregator round 1).
pub const ONE: RoundId = RoundId::from_limbs([1, 0]);

/// The oldest round id an incremental gap-recovery walk may visit: `gap_cap` rounds
/// back from `latest_round`, clamped to [`ONE`].
#[must_use]
pub fn gap_recovery_floor(latest_round: RoundId, gap_cap: u32) -> RoundId {
    latest_round
        .saturating_sub(RoundId::from(u64::from(gap_cap)))
        .max(ONE)
}

/// The oldest round id a bootstrap backscan may visit.
#[must_use]
pub fn backscan_start(latest_round: RoundId, backscan: u32) -> RoundId {
    gap_recovery_floor(latest_round, backscan)
}

/// Step to the previous round id, if any (returns `None` at [`ONE`]).
#[must_use]
pub fn prev(round_id: RoundId) -> Option<RoundId> {
    if round_id <= ONE {
        None
    } else {
        Some(round_id - RoundId::from(1_u64))
    }
}

/// Whether an `eth_call` failure likely means the round id does not exist on-chain.
///
/// Chainlink proxies revert on unknown rounds; phase boundaries leave gaps where
/// decrementing the composite id lands on ids with no stored round data.
#[must_use]
pub fn is_missing_round_reason(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("revert")
        || lower.contains("no data")
        || lower.contains("missing")
        || lower.contains("invalid round")
        || lower.contains("execution reverted")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase_one_round(aggregator_round: u64) -> RoundId {
        (RoundId::from(1_u64) << 64) + RoundId::from(aggregator_round)
    }

    #[test]
    fn phase_one_round_exceeds_u64_max() {
        let round_id = phase_one_round(3_684_024);
        assert!(round_id > RoundId::from(u64::MAX));
    }

    #[test]
    fn gap_recovery_floor_within_phase_one() {
        let latest = phase_one_round(3_684_024);
        let floor = gap_recovery_floor(latest, 100);
        assert_eq!(floor, phase_one_round(3_683_924));
    }

    #[test]
    fn gap_recovery_floor_clamps_to_one_near_genesis() {
        assert_eq!(gap_recovery_floor(RoundId::from(50_u64), 100), ONE);
        assert_eq!(gap_recovery_floor(ONE, 100), ONE);
    }

    #[test]
    fn prev_steps_down_within_phase() {
        let round = phase_one_round(10);
        assert_eq!(prev(round), Some(phase_one_round(9)));
    }

    #[test]
    fn prev_returns_none_at_one() {
        assert_eq!(prev(ONE), None);
    }
}
