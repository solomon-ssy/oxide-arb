//! Generic-slice feature group builders (platform-computable, always present).
//!
//! Every builder here consumes only platform data (order books, Gamma
//! metadata, microstructure windows, trade tape, neg-risk sibling legs) and
//! populates the fixed-width generic slice of the two-layer
//! [`FeatureVector`](crate::features::FeatureVector). External-vertical
//! builders live in [`crate::features::domain`] and populate the optional
//! domain slice — the two planes never mix.

pub mod book;
pub mod market;
pub mod microstructure;
pub(crate) mod stats;
pub mod structural;
pub mod timeseries;
