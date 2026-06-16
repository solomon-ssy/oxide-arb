use super::{EnumItemView, enum_items};

pub(super) fn items() -> Vec<EnumItemView> {
    enum_items(&[
        (
            "neg_risk_legacy_adapter",
            ("Neg-risk legacy adapter", "Neg-risk 旧版适配器"),
        ),
        (
            "neg_risk_collateral_adapter",
            ("Neg-risk collateral adapter", "Neg-risk 抵押品适配器"),
        ),
    ])
}
