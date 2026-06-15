use super::{EnumItemView, enum_items};

pub(super) fn items() -> Vec<EnumItemView> {
    enum_items(&[
        ("crypto", ("Crypto", "加密货币")),
        ("culture", ("Culture", "文化")),
        ("economics", ("Economics", "经济")),
        ("finance", ("Finance", "金融")),
        ("geopolitics", ("Geopolitics", "地缘政治")),
        ("other", ("Other", "其他")),
        ("politics", ("Politics", "政治")),
        ("sports", ("Sports", "体育")),
        ("tech", ("Tech", "科技")),
        ("weather", ("Weather", "天气")),
    ])
}
