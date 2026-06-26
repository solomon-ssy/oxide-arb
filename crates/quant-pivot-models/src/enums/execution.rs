//! Execution-layer Postgres enums (`quant_order_intent`, `quant_execution_order`).

crate::pg_enum! {
    type_name = "qp_order_intent_kind",
    pub enum OrderIntentKind {
        Buy => "buy",
    }
}

crate::pg_enum! {
    type_name = "qp_execution_order_phase",
    pub enum ExecutionOrderPhase {
        Entry => "entry",
        Exit => "exit",
    }
}

crate::pg_enum! {
    type_name = "qp_order_type_kind",
    pub enum OrderTypeKind {
        Fok => "fok",
        Gtc => "gtc",
        Gtd => "gtd",
    }
}

crate::pg_enum! {
    type_name = "qp_venue_order_status",
    pub enum VenueOrderStatus {
        Filled => "filled",
        PartiallyFilled => "partially_filled",
        Rejected => "rejected",
        Cancelled => "cancelled",
        Open => "open",
        Expired => "expired",
    }
}
