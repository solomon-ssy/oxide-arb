use super::{EnumItemView, enum_items};

pub(super) fn items() -> Vec<EnumItemView> {
    enum_items(&[
        (
            "conservative_reject",
            ("Conservative reject (fail-closed)", "保守拒绝（失败关闭）"),
        ),
        ("manual_ack", ("Manual acknowledgement", "人工确认")),
    ])
}
