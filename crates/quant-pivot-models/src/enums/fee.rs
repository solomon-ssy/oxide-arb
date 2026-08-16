//! Fee quote enums.

pg_enum! {
    type_name = "qp_fee_liquidity_role",
    /// Actual liquidity role confirmed for one venue fill.
    pub enum FeeLiquidityRole {
        Taker => "taker",
        Maker => "maker",
    }
}

pg_enum! {
    type_name = "qp_fee_measurement_stage",
    /// Monotone provenance tier for one execution-fill fee measurement.
    pub enum FeeMeasurementStage {
        PreparedExpected => "prepared_expected",
        AuthenticatedTradeDerived => "authenticated_trade_derived",
        OnChainSettled => "on_chain_settled",
    }
}

pg_enum! {
    type_name = "qp_venue_incentive_kind",
    pub enum VenueIncentiveKind {
        MakerRebate => "maker_rebate",
        TakerRebate => "taker_rebate",
    }
}

pg_enum! {
    type_name = "qp_venue_incentive_stage",
    /// Append-only incentive lifecycle event. Stages are facts, not mutable
    /// status values on a single row.
    pub enum VenueIncentiveStage {
        EstimatedAccrual => "estimated_accrual",
        VenueAwarded => "venue_awarded",
        WalletCredited => "wallet_credited",
    }
}
