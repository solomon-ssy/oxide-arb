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
//! The base build links only the pure-Rust numeric stack (`ndarray` /
//! `ndarray-stats` / `statrs` / `rayon`), which the online feature plane needs
//! for rolling statistics and parallel batch build. The heavy / native-adjacent
//! stacks are feature-gated and never appear in the default `report_only` build:
//! `dataframe` (polars / arrow / parquet), `optimize` (argmin), `ml-classical`
//! (smartcore).

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
pub mod vertical;

#[cfg(test)]
mod acceptance_tests {
    use std::path::Path;

    /// Phase 3.0 §11: default build must not link polars / smartcore / argmin.
    #[test]
    fn research_default_build_excludes_heavy_deps() {
        let output = std::process::Command::new("cargo")
            .args(["tree", "-p", "quant-pivot-research", "--depth", "1"])
            .output()
            .expect("cargo tree must succeed");
        assert!(
            output.status.success(),
            "cargo tree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
        for forbidden in ["polars", "smartcore", "argmin"] {
            assert!(
                !stdout.contains(forbidden),
                "default build must not list `{forbidden}`:\n{stdout}"
            );
        }
    }

    /// Phase 3.0 §11: the models-domain `SignalCandidate` stub must stay deleted.
    #[test]
    fn signal_candidate_typed_replaces_stub() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let quant_mod = manifest_dir.join("../quant-pivot-models/src/domain/quant/mod.rs");
        let signal_rs = manifest_dir.join("../quant-pivot-models/src/domain/quant/signal.rs");
        assert!(
            !signal_rs.exists(),
            "domain/quant/signal.rs stub must remain deleted"
        );
        let mod_src = std::fs::read_to_string(&quant_mod).expect("read domain/quant/mod.rs");
        assert!(
            !mod_src.contains("mod signal"),
            "domain/quant/mod.rs must not declare mod signal"
        );
        assert!(
            !mod_src.contains("SignalCandidate"),
            "domain/quant must not reference SignalCandidate"
        );
    }
}

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
