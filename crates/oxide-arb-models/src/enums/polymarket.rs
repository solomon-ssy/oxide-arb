//! Polymarket SDK conversion tests (implementations live on [`Side`] in [`super::common`]).

pub use super::common::SdkSideConversionError;

#[cfg(test)]
mod tests {
    use super::super::common::Side;
    use super::SdkSideConversionError;
    use polymarket_client_sdk_v2::clob::types::Side as SdkSide;

    #[test]
    fn side_into_sdk() {
        assert_eq!(SdkSide::from(Side::Buy), SdkSide::Buy);
        assert_eq!(SdkSide::from(Side::Sell), SdkSide::Sell);
    }

    #[test]
    fn sdk_side_try_into_domain() {
        assert_eq!(Side::try_from(SdkSide::Buy).unwrap(), Side::Buy);
        assert_eq!(
            Side::try_from(SdkSide::Unknown).unwrap_err(),
            SdkSideConversionError
        );
    }
}
