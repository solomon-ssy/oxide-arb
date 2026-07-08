#!/usr/bin/env bash
# Fail when views use raw Tailwind palette classes for semantic state colors.
# Allowed: theme tokens (text-success, text-destructive, text-muted-foreground, etc.)
# and shared/components primitives under review.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${ROOT}/ui/apps/web-antdv-next/src/views"

PATTERN='text-(red|green|blue|amber|emerald|rose|gray|yellow|orange|lime|cyan|purple|pink|violet|fuchsia|sky|teal|indigo)-[0-9]+|bg-(red|green|blue|amber|emerald|rose|gray|yellow|orange|lime|cyan|purple|pink|violet|fuchsia|sky|teal|indigo)-[0-9]+(/[0-9]+)?|border-(red|green|blue|amber|emerald|rose|gray|yellow|orange)-[0-9]+(/[0-9]+)?'

if [[ ! -d "${TARGET}" ]]; then
  echo "lint-ui-semantic-colors: views directory not found: ${TARGET}" >&2
  exit 1
fi

MATCHES="$(rg -n "${PATTERN}" "${TARGET}" --glob '*.vue' || true)"

if [[ -n "${MATCHES}" ]]; then
  echo "Hardcoded Tailwind palette classes found in views (use semantic tokens or shared components):" >&2
  echo "${MATCHES}" >&2
  exit 1
fi

echo "lint-ui-semantic-colors: OK"
