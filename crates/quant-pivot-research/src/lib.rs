//! Quant research plane: market selection, feature/factor computation, model
//! runtime, training, backtest, and quality governance.
//!
//! This crate owns every **computation trait** and **compute-domain value type**
//! of the research plane. Persistence DTOs (`*Info` / `New*`), typed IDs, the
//! `enums::quant` lifecycle enums, and the content-addressing newtypes
//! (`ContentHash` / `ArtifactUri` / `SchemaVersion`) live in `quant-pivot-models`;
//! this crate depends on them and maps compute types to persistence rows at
//! explicit boundaries (never by merging the two families).
//!
//! # Module map
//!
//! - [`selection`] / [`features`] / [`factors`] / [`model`] — the **online
//!   closure**: `MarketSelection → FeatureVector → FactorValue → SignalCandidate`.
//! - [`pit`] / [`training`] / [`backtest`] / [`gates`] / [`governance`] — the
//!   **offline closure**: historical point-in-time access, dataset/label
//!   construction, training, backtest, and quality gating.
//! - [`artifact`] — content-addressed artifact storage (`ArtifactStore` +
//!   `LocalArtifactStore`).
//! - [`hashing`] — `ResearchHasher`, the canonical `blake3:` content hasher for
//!   research artifacts (order-independent for sets).
//!
//! # Feature flags
//!
//! Heavy numeric / ML dependencies are feature-gated so the default
//! `report_only` build stays light. `default = ["stats"]` links only the
//! pure-Rust numeric stack; `dataframe` / `optimize` / `ml-classical` opt into
//! polars / argmin / smartcore respectively and never appear in default builds.

#![deny(unsafe_code)]

mod naming;

pub mod artifact;
pub mod backtest;
pub mod factors;
pub mod features;
pub mod gates;
pub mod governance;
pub mod hashing;
pub mod model;
pub mod pit;
pub mod selection;
pub mod training;

#[cfg(test)]
mod feature_guard_tests {
    /// The default build must never link the heavy ML stack: only the pure-Rust
    /// `stats` group is on by default. `cargo test --workspace` runs with default
    /// features, so this guards against an accidental `default = [...]` that
    /// pulls polars / argmin / smartcore into the `report_only` build.
    #[test]
    fn default_build_excludes_heavy_features() {
        // `black_box` hides the cfg constants from the optimizer so this stays a
        // runtime assertion (not a compile-time one that would break the
        // heavy-feature clippy gate).
        let heavy = std::hint::black_box(
            cfg!(feature = "dataframe")
                || cfg!(feature = "optimize")
                || cfg!(feature = "ml-classical"),
        );
        assert!(
            !heavy,
            "default build must exclude polars / argmin / smartcore"
        );
    }
}
