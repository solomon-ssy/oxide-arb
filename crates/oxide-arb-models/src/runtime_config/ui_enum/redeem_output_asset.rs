use super::{EnumItemView, enum_items};

pub(super) fn items() -> Vec<EnumItemView> {
    enum_items(&[("usdc_e", ("USDC.e", "USDC.e")), ("pusd", ("PUSD", "PUSD"))])
}
