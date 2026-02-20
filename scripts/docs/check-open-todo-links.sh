#!/usr/bin/env bash
#
# check-open-todo-links.sh — verify that TODO-*.md references in docs/ are
# still open (not marked done) in outstanding-todo.md and exist on disk.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DOCS_DIR="$REPO_ROOT/project/docs"
TODO_FILE="$REPO_ROOT/outstanding-todo.md"

if [ ! -d "$DOCS_DIR" ]; then
  echo "SKIP: docs directory not found at $DOCS_DIR"
  exit 0
fi

if [ ! -f "$TODO_FILE" ]; then
  echo "SKIP: outstanding-todo.md not found at $TODO_FILE"
  exit 0
fi

errors=0

# Collect unique TODO-*.md references from docs/*.md files.
refs=$(grep -ohP 'TODO-[a-z0-9-]+\.md' "$DOCS_DIR"/*.md 2>/dev/null | sort -u || true)

if [ -z "$refs" ]; then
  echo "OK: no TODO-*.md references found in docs"
  exit 0
fi

for ref in $refs; do
  # Check if the TODO file exists on disk (at repo root).
  if [ ! -f "$REPO_ROOT/$ref" ]; then
    echo "FAIL: $ref referenced in docs but file does not exist on disk"
    errors=$((errors + 1))
    continue
  fi

  # Check if any line in outstanding-todo.md mentions this TODO and is done.
  # Done = line contains [x] (checked checkbox) OR [done] (case-insensitive).
  while IFS= read -r line; do
    # Check for done markers.
    is_done=0
    if echo "$line" | grep -qP '^\s*-\s*\[x\]\s+'; then
      is_done=1
    elif echo "$line" | grep -qi '\[done\]'; then
      is_done=1
    fi

    if [ "$is_done" -eq 1 ]; then
      echo "FAIL: $ref referenced in docs but marked done in outstanding-todo.md"
      errors=$((errors + 1))
      break
    fi
  done < <(grep -F "$ref" "$TODO_FILE" 2>/dev/null || true)
done

if [ "$errors" -gt 0 ]; then
  echo ""
  echo "FAILED: $errors stale/missing TODO reference(s) found in docs"
  exit 1
fi

echo "OK: all TODO-*.md references in docs are open and exist on disk"
exit 0
