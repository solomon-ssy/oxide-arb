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
//! The base build links only the pure-Rust numeric stack (`ndarray` / `statrs` /
//! `rayon`), which the online feature plane needs
//! for rolling statistics and parallel batch build. The heavy / native-adjacent
//! stacks are feature-gated and never appear in the default `report_only` build:
//! `dataframe` (polars / parquet), `optimize` (argmin), `ml-classical`
//! (smartcore).

#![deny(unsafe_code)]

mod naming;
mod parallel;
pub mod precision;
pub mod stats;
pub mod structural_volatility;

pub mod artifact;
pub mod backtest;
pub mod domain;
pub mod execution_semantics;
pub mod factors;
pub mod features;
pub mod gates;
pub mod governance;
pub mod hashing;
pub mod linkage;
pub mod model;
pub mod pit;
#[cfg(feature = "dataframe")]
pub mod policy_evidence;
pub mod policy_replay;
pub mod policy_validation;
pub mod portfolio;
pub mod selection;
#[cfg(feature = "dataframe")]
pub mod source_slice;
pub mod trade_tape;
pub mod training;
pub mod validation;
pub mod weather_proxy_validation;

#[cfg(test)]
mod acceptance_tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process::Command,
    };

    /// Phase 3.0 §11: default build must not link polars / smartcore / argmin.
    #[test]
    fn research_default_build_excludes_heavy_deps() {
        let output = Command::new("cargo")
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

    /// Phase 3.6 boundary: no `smartcore` concrete type may leak into the
    /// business layers (core / web / models). Inside this crate it may appear
    /// only behind the `ml-classical` adapter / runtime modules.
    #[test]
    fn business_layer_has_no_smartcore_concrete_type() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let crates = manifest_dir.parent().expect("crates dir");
        for crate_name in ["quant-pivot-core", "quant-pivot-web", "quant-pivot-models"] {
            let src = crates.join(crate_name).join("src");
            assert_no_token(&src, "smartcore::");
        }
        // Within research, the `smartcore::` concrete path is confined to the
        // classical adapter / runtime modules.
        let research_src = manifest_dir.join("src");
        for entry in walk_rs(&research_src) {
            let name = entry.to_string_lossy();
            // Skip the classical modules (where it legitimately lives) and this
            // acceptance file (which names the token in its assertions).
            if name.contains("classical") || name.ends_with("lib.rs") {
                continue;
            }
            let body = fs::read_to_string(&entry).unwrap_or_default();
            assert!(
                !body.contains("smartcore::"),
                "smartcore concrete type leaked into non-classical research file {name}"
            );
        }
    }

    /// Assert no `.rs` file under `dir` mentions `token`.
    fn assert_no_token(dir: &Path, token: &str) {
        for entry in walk_rs(dir) {
            let body = fs::read_to_string(&entry).unwrap_or_default();
            assert!(
                !body.contains(token),
                "`{token}` must not appear in {}",
                entry.to_string_lossy()
            );
        }
    }

    /// Recursively collect `.rs` files under `dir`.
    fn walk_rs(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_rs(&path));
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
        out
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
        let mod_src = fs::read_to_string(&quant_mod).expect("read domain/quant/mod.rs");
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
    /// The optimizer (`argmin`) and classical-ML (`smartcore`) stacks stay gated
    /// until Phase 3.6 and must never be linked by default.
    ///
    /// `dataframe` (polars/parquet) is **intentionally excluded** from this
    /// guard: Phase 3.5 links it workspace-wide so `quant-pivot-core` can
    /// materialize offline training datasets (the `always_compile` decision —
    /// `quant-pivot-core` enables `quant-pivot-research/dataframe`). Under
    /// `cargo test --workspace` feature unification therefore turns `dataframe`
    /// on here, which is expected.
    //
    // Gated to the default build: heavy-feature CI jobs legitimately enable
    // `optimize` / `ml-classical`, so the guard only asserts the default build.
    #[cfg(not(any(feature = "optimize", feature = "ml-classical")))]
    use std::hint;

    #[cfg(not(any(feature = "optimize", feature = "ml-classical")))]
    #[test]
    fn default_build_excludes_unlinked_heavy_features() {
        // `black_box` hides the cfg constants from const-eval so this stays a
        // runtime assertion rather than a (clippy-flagged) constant one.
        let heavy = hint::black_box(cfg!(feature = "optimize"))
            || hint::black_box(cfg!(feature = "ml-classical"));
        assert!(!heavy, "default build must exclude argmin / smartcore");
    }
}
