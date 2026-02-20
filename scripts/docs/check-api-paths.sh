#!/usr/bin/env bash
#
# check-api-paths.sh — compare documented API paths in docs/*.md against
# registered routes in internal/http/admin_routes.go. Warns about documented
# paths that are not found in route registrations.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DOCS_DIR="$REPO_ROOT/project/docs"
ROUTES_FILE="$REPO_ROOT/project/internal/http/admin_routes.go"
ALLOWLIST_FILE="$(dirname "$0")/api-path-allowlist.txt"

if [ ! -d "$DOCS_DIR" ]; then
  echo "SKIP: docs directory not found at $DOCS_DIR"
  exit 0
fi

if [ ! -f "$ROUTES_FILE" ]; then
  echo "SKIP: admin_routes.go not found at $ROUTES_FILE"
  exit 0
fi

# clean_path strips trailing backticks, quotes, periods, and query strings.
clean_path() {
  echo "$1" | sed -E 's/[`"'\''.:]+$//; s/\?.*$//; s/\*$//; s|/+$||'
}

# Extract documented API paths from docs/*.md route tables.
# Table format: | `METHOD` | `/api/path` | ...
table_paths=$(grep -ohP '\|\s*`(?:GET|POST|PUT|PATCH|DELETE)`\s*\|\s*`(/(?:api|ui)/[^`]+)`' "$DOCS_DIR"/*.md 2>/dev/null \
  | grep -oP '/(?:api|ui)/[^`]+' || true)

# Also capture inline path references like: POST /api/... or GET /api/...
inline_paths=$(grep -ohP '(?:GET|POST|PUT|PATCH|DELETE)\s+`?(/(?:api|ui)/[a-zA-Z0-9_./{}-]+)' "$DOCS_DIR"/*.md 2>/dev/null \
  | grep -oP '/(?:api|ui)/[a-zA-Z0-9_./{}-]+' || true)

# Merge, clean, and deduplicate.
all_doc_paths=$(printf '%s\n%s' "$table_paths" "$inline_paths" \
  | while IFS= read -r p; do
      [ -z "$p" ] && continue
      clean_path "$p"
    done \
  | sort -u | grep -v '^$' || true)

if [ -z "$all_doc_paths" ]; then
  echo "SKIP: no API/UI paths found in docs"
  exit 0
fi

# Extract registered routes from admin_routes.go.
# Matches patterns like: mux.Handle("GET /api/...", ...)
registered_routes=$(grep -oP 'mux\.Handle\(\s*"(?:GET|POST|PUT|PATCH|DELETE)\s+(/[^"]+)"' "$ROUTES_FILE" 2>/dev/null \
  | grep -oP '/[^"]+' \
  | sort -u || true)

# Load allowlist entries (one path per line, # comments allowed).
allowed_paths=""
if [ -f "$ALLOWLIST_FILE" ]; then
  allowed_paths=$(grep -v '^\s*#' "$ALLOWLIST_FILE" | grep -v '^\s*$' | sed 's/[[:space:]]*$//' | sort -u || true)
fi

# Normalize a path by replacing {param} segments with a common placeholder.
normalize_path() {
  echo "$1" | sed 's/{[^}]*}/{PARAM}/g'
}

errors=0

while IFS= read -r doc_path; do
  [ -z "$doc_path" ] && continue

  normalized_doc=$(normalize_path "$doc_path")

  # Check against registered routes.
  found=0
  while IFS= read -r route; do
    [ -z "$route" ] && continue
    normalized_route=$(normalize_path "$route")
    if [ "$normalized_doc" = "$normalized_route" ]; then
      found=1
      break
    fi
  done <<< "$registered_routes"

  if [ "$found" -eq 0 ]; then
    # Check allowlist.
    in_allowlist=0
    if [ -n "$allowed_paths" ]; then
      while IFS= read -r allowed; do
        [ -z "$allowed" ] && continue
        normalized_allowed=$(normalize_path "$allowed")
        if [ "$normalized_doc" = "$normalized_allowed" ]; then
          in_allowlist=1
          break
        fi
      done <<< "$allowed_paths"
    fi

    if [ "$in_allowlist" -eq 0 ]; then
      echo "WARN: documented path not found in route registration: $doc_path"
      errors=$((errors + 1))
    fi
  fi
done <<< "$all_doc_paths"

if [ "$errors" -gt 0 ]; then
  echo ""
  echo "FAILED: $errors documented API path(s) not found in route registrations"
  exit 1
fi

echo "OK: all documented API/UI paths match registered routes"
exit 0
