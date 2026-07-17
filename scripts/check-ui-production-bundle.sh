#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/ui/apps/web-antdv-next/dist"
MANIFEST="$DIST/.vite/manifest.json"
MAX_DASHBOARD_GZIP_BYTES=307200
BASELINE_FILE="$ROOT/ui/apps/web-antdv-next/bundle-baselines/dashboard-gzip-bytes.txt"

if [[ ! -d "$DIST" ]]; then
  echo "ERROR: UI production dist is missing; run pnpm build:antdv-next first" >&2
  exit 1
fi

if rg -n -i 'mock-napi|[?&]token=|Bearer%20|VITE_APP_STORE_SECURE_KEY|REPLACE_WITH|hm\.baidu\.com|@vbenjs/static-source|Oxide Arb|Vben Admin|www\.vben\.pro|ann\.vben|open\.dingtalk' "$DIST"; then
  echo "ERROR: forbidden production bundle pattern found" >&2
  exit 1
fi

if [[ ! -f "$MANIFEST" ]]; then
  echo "ERROR: Vite manifest is missing: $MANIFEST" >&2
  exit 1
fi

DASHBOARD_FILE="$(
  jq -r '
    to_entries[]
    | select(.key | endswith("src/views/dashboard/index.vue"))
    | .value.file
  ' "$MANIFEST"
)"
if [[ -z "$DASHBOARD_FILE" || "$DASHBOARD_FILE" == "null" ]]; then
  echo "ERROR: dashboard route chunk is absent from the Vite manifest" >&2
  exit 1
fi

DASHBOARD_PATH="$DIST/$DASHBOARD_FILE"
DASHBOARD_GZIP_BYTES="$(gzip -c "$DASHBOARD_PATH" | wc -c | tr -d ' ')"
if (( DASHBOARD_GZIP_BYTES > MAX_DASHBOARD_GZIP_BYTES )); then
  echo "ERROR: dashboard gzip chunk is ${DASHBOARD_GZIP_BYTES} bytes; budget is ${MAX_DASHBOARD_GZIP_BYTES}" >&2
  exit 1
fi

if [[ -f "$BASELINE_FILE" ]]; then
  BASELINE_BYTES="$(tr -d '[:space:]' < "$BASELINE_FILE")"
  if [[ ! "$BASELINE_BYTES" =~ ^[0-9]+$ ]]; then
    echo "ERROR: invalid dashboard bundle baseline: $BASELINE_FILE" >&2
    exit 1
  fi
  MAX_REGRESSION_BYTES=$(( BASELINE_BYTES + BASELINE_BYTES / 10 ))
  if (( DASHBOARD_GZIP_BYTES > MAX_REGRESSION_BYTES )); then
    echo "ERROR: dashboard gzip chunk regressed by more than 10% (${DASHBOARD_GZIP_BYTES} > ${MAX_REGRESSION_BYTES})" >&2
    exit 1
  fi
fi

echo "dashboard gzip chunk: ${DASHBOARD_GZIP_BYTES} bytes"
