use super::{EnumItemView, enum_items};

pub(super) fn items() -> Vec<EnumItemView> {
    enum_items(&[
        ("disabled", ("Disabled", "禁用")),
        ("standard_ctf", ("Standard CTF", "标准 CTF")),
        (
            "neg_risk_legacy_adapter",
            ("Neg-risk legacy adapter", "Neg-risk 旧版适配器"),
        ),
        (
            "ctf_collateral_adapter",
            ("CTF collateral adapter", "CTF 抵押品适配器"),
        ),
        (
            "neg_risk_collateral_adapter",
            ("Neg-risk collateral adapter", "Neg-risk 抵押品适配器"),
        ),
        ("proxy_safe", ("Gnosis Safe (proxy)", "Gnosis Safe（代理）")),
    ])
}
