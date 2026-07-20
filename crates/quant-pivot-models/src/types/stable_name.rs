//! Stable names shared by feature, factor, model-artifact and persistence code.

use std::{
    borrow::Cow,
    fmt::{self, Display, Formatter},
};

use serde::{Deserialize, Serialize};

macro_rules! stable_name {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Construct from a compile-time-known stable name without allocation.
            #[must_use]
            pub const fn from_static(name: &'static str) -> Self {
                Self(Cow::Borrowed(name))
            }

            /// Construct from an owned runtime name.
            #[must_use]
            pub fn new(name: impl Into<String>) -> Self {
                Self(Cow::Owned(name.into()))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

stable_name! {
    /// Stable, governed feature name such as `spread_bps`.
    FeatureName
}

impl FeatureName {
    #[must_use]
    pub fn ts_return(window_secs: u64) -> Self {
        Self::new(format!("ts.return_{window_secs}s"))
    }

    #[must_use]
    pub fn ts_spread_trend(window_secs: u64) -> Self {
        Self::new(format!("ts.spread_trend_{window_secs}s"))
    }

    #[must_use]
    pub fn ts_depth_trend(window_secs: u64) -> Self {
        Self::new(format!("ts.depth_trend_{window_secs}s"))
    }

    #[must_use]
    pub fn ts_momentum_roc(window_secs: u64) -> Self {
        Self::new(format!("ts.momentum_roc_{window_secs}s"))
    }

    #[must_use]
    pub fn ts_ema_slope(window_secs: u64) -> Self {
        Self::new(format!("ts.ema_slope_{window_secs}s"))
    }

    #[must_use]
    pub fn ts_vol_adjusted_return(window_secs: u64) -> Self {
        Self::new(format!("ts.vol_adjusted_return_{window_secs}s"))
    }

    #[must_use]
    pub fn ts_realized_vol(window_secs: u64) -> Self {
        Self::new(format!("ts.realized_vol_{window_secs}s"))
    }

    #[must_use]
    pub fn book_depth_top(level: u32) -> Self {
        Self::new(format!("book.depth_top{level}_usd"))
    }
}

stable_name! {
    /// Stable, governed factor name such as `liquidity_depth`.
    FactorName
}

stable_name! {
    /// Stable model-ready metric/encoded-column name.
    ModelMetricName
}
