//! Conversions between domain [`Side`] and Polymarket SDK CLOB types.

use oxide_arb_models::enums::common::Side;
use polymarket_client_sdk_v2::clob::types::Side as SdkSide;
use thiserror::Error;

/// Failed to map CLOB SDK [`SdkSide`] into domain [`Side`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("unsupported Polymarket CLOB side")]
pub struct SdkSideConversionError;

/// Local newtype so [`From`] / [`TryFrom`] impls satisfy orphan rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClobSide(pub Side);

impl From<Side> for ClobSide {
    fn from(side: Side) -> Self {
        Self(side)
    }
}

impl From<ClobSide> for SdkSide {
    fn from(side: ClobSide) -> Self {
        match side.0 {
            Side::Buy => Self::Buy,
            Side::Sell => Self::Sell,
        }
    }
}

impl TryFrom<SdkSide> for ClobSide {
    type Error = SdkSideConversionError;

    fn try_from(side: SdkSide) -> Result<Self, SdkSideConversionError> {
        match side {
            SdkSide::Buy => Ok(Self(Side::Buy)),
            SdkSide::Sell => Ok(Self(Side::Sell)),
            _ => Err(SdkSideConversionError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clob_side_into_sdk_maps_both_variants() {
        assert_eq!(SdkSide::from(ClobSide::from(Side::Buy)), SdkSide::Buy);
        assert_eq!(SdkSide::from(ClobSide::from(Side::Sell)), SdkSide::Sell);
    }

    #[test]
    fn sdk_side_try_into_clob_side_rejects_unknown() {
        assert_eq!(ClobSide::try_from(SdkSide::Buy).unwrap().0, Side::Buy);
        assert_eq!(
            ClobSide::try_from(SdkSide::Unknown).unwrap_err(),
            SdkSideConversionError
        );
    }
}
