#!/usr/bin/env bash
# Import-style lint — enforce file/module-preamble `use` discipline from
# `.cursor/rules/quant-pivot-rust-style.mdc`:
#
#   1. No deep paths in item bodies (at most one `::` after imports).
#   2. No `use` inside functions / block items — only module preambles.
#   3. One non-`pub` `use` tree per (root, leading-attrs) in each module
#      scope — merge siblings into a single brace tree:
#        use std::{cmp::Ordering, panic::{self, AssertUnwindSafe}};
#
#   Canonical barrel `pub use` declarations are left alone. Compatibility
#   re-exports are rejected by architecture/domain lints.
#   Distinct `#[cfg(...)]` (or other attrs) on a `use` keep a separate tree —
#   attrs cannot appear inside use-tree braces on stable Rust.
#
# Scope: crates/*/src/**/*.rs (excludes quant-pivot-macros).
#
# Usage:
#   bash scripts/lint-import-style.sh
#   bash scripts/lint-import-style.sh --fix    # merge duplicate-root uses
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export LINT_IMPORT_STYLE_ROOT="$ROOT"
exec python3 "$ROOT/scripts/lint_import_style.py" "$@"
