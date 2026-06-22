//! Pure redeem-route resolution for [`RedeemRoutingPolicy`].
//!
//! Resolution is deterministic and side-effect free so it can be unit-tested
//! and invoked from pre-trade gates, post-trade snapshots, and settlement.

use super::{
    NegRiskRedeemPolicy, RedeemClassPolicy, RedeemRoutingPolicy, ResolvedRedeemPlan,
    StandardRedeemPolicy,
};
use crate::{enums::common::RedeemResolutionSource, types::MarketId};

impl RedeemRoutingPolicy {
    /// Resolve the redeem plan for a market.
    ///
    /// Priority: per-market override → class policy (`neg_risk` ? `neg_risk` :
    /// standard). Returns `None` when no policy covers the market class.
    #[must_use]
    pub fn resolve(&self, market_id: &MarketId, neg_risk: bool) -> Option<ResolvedRedeemPlan> {
        if let Some(override_policy) = self.overrides.get(market_id) {
            if override_policy.expects_neg_risk() != neg_risk {
                return None;
            }
            return Some(plan_from_policy(
                override_policy,
                neg_risk,
                RedeemResolutionSource::Override,
                self.gas_limit,
            ));
        }

        if neg_risk {
            self.neg_risk
                .as_ref()
                .map(|policy| plan_from_neg_risk(policy, self.gas_limit))
        } else {
            self.standard
                .as_ref()
                .map(|policy| plan_from_standard(policy, self.gas_limit))
        }
    }

    /// Whether Live mode can redeem at least one market class.
    #[must_use]
    pub const fn has_any_live_class(&self) -> bool {
        self.standard.is_some() || self.neg_risk.is_some()
    }
}

fn plan_from_standard(policy: &StandardRedeemPolicy, gas_limit: u64) -> ResolvedRedeemPlan {
    ResolvedRedeemPlan {
        route: policy.route.into(),
        holder_address: policy.holder_address.clone(),
        neg_risk: false,
        gas_limit,
        resolution: RedeemResolutionSource::ClassStandard,
    }
}

fn plan_from_neg_risk(policy: &NegRiskRedeemPolicy, gas_limit: u64) -> ResolvedRedeemPlan {
    ResolvedRedeemPlan {
        route: policy.route.into(),
        holder_address: policy.holder_address.clone(),
        neg_risk: true,
        gas_limit,
        resolution: RedeemResolutionSource::ClassNegRisk,
    }
}

fn plan_from_policy(
    policy: &RedeemClassPolicy,
    neg_risk: bool,
    resolution: RedeemResolutionSource,
    gas_limit: u64,
) -> ResolvedRedeemPlan {
    ResolvedRedeemPlan {
        route: policy.route(),
        holder_address: policy.holder_address().map(str::to_owned),
        neg_risk,
        gas_limit,
        resolution,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::{
        enums::common::{NegRiskRedeemRoute, StandardRedeemRoute},
        runtime_config::{NegRiskRedeemPolicy, StandardRedeemPolicy},
    };

    fn market(id: &str) -> MarketId {
        MarketId::new(id)
    }

    #[test]
    fn resolve_class_standard() {
        let policy = RedeemRoutingPolicy::default();
        let plan = policy
            .resolve(&market("0xabc"), false)
            .expect("standard plan");
        assert_eq!(plan.route, StandardRedeemRoute::default().into());
        assert!(!plan.neg_risk);
        assert_eq!(plan.resolution, RedeemResolutionSource::ClassStandard);
    }

    #[test]
    fn resolve_class_neg_risk() {
        let policy = RedeemRoutingPolicy::default();
        let plan = policy
            .resolve(&market("0xabc"), true)
            .expect("neg-risk plan");
        assert_eq!(plan.route, NegRiskRedeemRoute::default().into());
        assert!(plan.neg_risk);
        assert_eq!(plan.resolution, RedeemResolutionSource::ClassNegRisk);
    }

    #[test]
    fn override_wins_over_class() {
        let mut policy = RedeemRoutingPolicy::default();
        policy.overrides.insert(
            market("0xoverride"),
            RedeemClassPolicy::Standard(StandardRedeemPolicy {
                route: StandardRedeemRoute::CtfCollateralAdapter,
                holder_address: None,
            }),
        );
        let plan = policy
            .resolve(&market("0xoverride"), false)
            .expect("override plan");
        assert_eq!(plan.route, StandardRedeemRoute::CtfCollateralAdapter.into());
        assert_eq!(plan.resolution, RedeemResolutionSource::Override);
    }

    #[test]
    fn override_variant_mismatch_returns_none() {
        let mut policy = RedeemRoutingPolicy::default();
        policy.overrides.insert(
            market("0xbad"),
            RedeemClassPolicy::NegRisk(NegRiskRedeemPolicy::default()),
        );
        assert!(policy.resolve(&market("0xbad"), false).is_none());
    }

    #[test]
    fn missing_class_returns_none() {
        let policy = RedeemRoutingPolicy {
            standard: None,
            neg_risk: Some(NegRiskRedeemPolicy::default()),
            overrides: HashMap::new(),
            gas_limit: 500_000,
            matic_usd_price: RedeemRoutingPolicy::default().matic_usd_price,
        };
        assert!(policy.resolve(&market("0xabc"), false).is_none());
        assert!(policy.resolve(&market("0xabc"), true).is_some());
    }

    #[test]
    fn resolved_route_round_trips_persisted_string() {
        let route: crate::enums::common::ResolvedRedeemRoute =
            "neg_risk_collateral_adapter".parse().expect("route");
        assert_eq!(route, NegRiskRedeemRoute::NegRiskCollateralAdapter.into());
        assert!(
            "unknown_route"
                .parse::<crate::enums::common::ResolvedRedeemRoute>()
                .is_err()
        );
    }
}
