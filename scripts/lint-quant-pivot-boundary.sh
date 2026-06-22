#!/usr/bin/env bash
set -euo pipefail

# Phase 0 symbol gate — forbid Endgame / deleted-crate references from re-entering.

FORBIDDEN='EndgameDetector|ScoredOpportunity(?!Snapshot)|OpportunityPipeline|\bExecutionMode::(DryRun|Paper|Live)|oxide_arb_algorithm|oxide_arb_risk|oxide_arb_control'

if rg -n -P "$FORBIDDEN" crates/ config/ --glob '!**/lint-quant-pivot-boundary.sh' 2>/dev/null; then
  echo "ERROR: forbidden legacy Endgame symbols detected (see lint-quant-pivot-boundary.sh)"
  exit 1
fi

if rg -n 'quant_pivot_(algorithm|risk|control)::|use quant_pivot_(algorithm|risk|control)\b' crates/ config/ 2>/dev/null; then
  echo "ERROR: forbidden deleted-crate import detected"
  exit 1
fi

if rg -n 'pub use .*::Opportunity;|pub use .*ScoredOpportunity;' \
  crates/**/lib.rs crates/**/mod.rs 2>/dev/null; then
  echo "ERROR: forbidden re-export of legacy symbols"
  exit 1
fi

echo "Quant pivot boundary checks passed."
