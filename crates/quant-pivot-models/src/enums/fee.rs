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
    @derive(PartialOrd, Ord, schemars::JsonSchema)
    pub enum VenueIncentiveKind {
        MakerRebate => "maker_rebate",
        TakerRebate => "taker_rebate",
    }
}

pg_enum! {
    type_name = "qp_venue_incentive_stage",
    /// Append-only incentive lifecycle event. Stages are facts, not mutable
    /// status values on a single row.
    @derive(PartialOrd, Ord, schemars::JsonSchema)
    pub enum VenueIncentiveStage {
        EstimatedAccrual => "estimated_accrual",
        VenueReportedAccrual => "venue_reported_accrual",
        WalletCredited => "wallet_credited",
    }
}

pg_enum! {
    type_name = "qp_venue_incentive_reconciliation_scan_status",
    /// Durable outcome of one upstream reconciliation partition scan.
    @derive(schemars::JsonSchema)
    pub enum VenueIncentiveReconciliationScanStatus {
        Succeeded => "succeeded",
        Failed => "failed",
    }
}
