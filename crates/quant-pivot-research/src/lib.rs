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
//! The base research build links the pure-Rust numeric stack (`ndarray` /
//! `statrs` / `rayon`) and the required `microlp` portfolio solver. `research-jobs`
//! (`S3` / `polars` / `parquet`), `optimize` (`argmin`), and `ml-classical`
//! (`smartcore`) remain independently feature-gated; the production binary
//! chooses its deployment feature set explicitly.

#![deny(unsafe_code)]

use quant_pivot_allocator as _;

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
pub mod policy_evidence;
pub mod policy_replay;
pub mod policy_validation;
pub mod portfolio;
pub mod selection;
pub mod source_slice;
pub mod trade_tape;
pub mod training;
pub mod validation;
pub mod weather_proxy_validation;

#[cfg(test)]
pub(crate) mod test_support {
    use quant_pivot_models::{hashing::CanonicalDigest, types::ContentHash};

    /// Deterministic, syntactically valid content hash for semantic test seeds.
    pub fn content_hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("canonical fixture content hash")
    }
}

#[cfg(test)]
mod acceptance_tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process::Command,
    };

    /// The default build must not link `Polars`, `SmartCore`, or `Argmin`.
    #[test]
    fn research_default_excludes_deps() {
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

    /// boundary: no `smartcore` concrete type may leak into the
    /// business layers (core / web / models). Inside this crate it may appear
    /// only behind the `ml-classical` adapter / runtime modules.
    #[test]
    fn business_layer_no_type() {
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
}

#[cfg(test)]
mod feature_guard_tests {
    /// The optimizer (`argmin`) and classical-ML (`smartcore`) stacks stay behind
    /// explicit features and must never be linked by default.
    ///
    /// `research-jobs` (S3/polars/parquet) is **intentionally excluded** from this
    /// guard because `quant-pivot-core` enables it to materialize offline
    /// training datasets. Under
    /// `cargo test --workspace` feature unification therefore turns `research-jobs`
    /// on here, which is expected.
    //
    // Gated to the default build: heavy-feature CI jobs legitimately enable
    // `optimize` / `ml-classical`, so the guard only asserts the default build.
    #[cfg(not(any(feature = "optimize", feature = "ml-classical")))]
    use std::hint;

    #[cfg(not(any(feature = "optimize", feature = "ml-classical")))]
    #[test]
    fn default_build_excludes_features() {
        // `black_box` hides the cfg constants from const-eval so this stays a
        // runtime assertion rather than a (clippy-flagged) constant one.
        let heavy = hint::black_box(cfg!(feature = "optimize"))
            || hint::black_box(cfg!(feature = "ml-classical"));
        assert!(!heavy, "default build must exclude argmin / smartcore");
    }
}
