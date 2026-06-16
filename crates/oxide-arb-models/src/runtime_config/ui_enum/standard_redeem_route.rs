use super::{EnumItemView, enum_items};

pub(super) fn items() -> Vec<EnumItemView> {
    enum_items(&[
        ("standard_ctf", ("Standard CTF", "标准 CTF")),
        (
            "ctf_collateral_adapter",
            ("CTF collateral adapter", "CTF 抵押品适配器"),
        ),
    ])
}
