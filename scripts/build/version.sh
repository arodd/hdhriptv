#!/usr/bin/env sh
set -eu

# If release workflow explicitly sets RELEASE_TAG, always honor it.
if [ -n "${RELEASE_TAG:-}" ]; then
  printf '%s\n' "${RELEASE_TAG}"
  exit 0
fi

if ! command -v git >/dev/null 2>&1; then
  printf '%s\n' "v0.0.0-dev"
  exit 0
fi

# Exact release tag build.
exact_tag="$(git describe --tags --exact-match --match 'v[0-9]*' 2>/dev/null || true)"
if [ -n "$exact_tag" ]; then
  printf '%s\n' "$exact_tag"
  exit 0
fi

latest_local_tags="$(git tag --list 'v[0-9]*' || true)"

# Also query public GitHub tags and choose the highest semantic version across
# local + remote sources. This avoids stale internal-only local tags anchoring
# dev builds to an older base version.
latest_remote_tags="$(git ls-remote --refs --tags https://github.com/arodd/hdhriptv.git "refs/tags/v*" 2>/dev/null \
  | awk '{print $2}' \
  | sed 's#refs/tags/##' || true)"

latest_tag="$(printf '%s\n%s\n' "$latest_local_tags" "$latest_remote_tags" \
  | awk 'NF' \
  | sort -uV \
  | tail -n1 || true)"
if [ -z "$latest_tag" ]; then
  latest_tag="v0.0.0"
fi

short_sha="$(git rev-parse --short=12 HEAD 2>/dev/null || true)"
if [ -z "$short_sha" ]; then
  short_sha="unknown"
fi

dirty_suffix=""
if ! git diff --quiet --ignore-submodules HEAD -- >/dev/null 2>&1; then
  dirty_suffix=".dirty"
fi

printf '%s\n' "${latest_tag}-dev+${short_sha}${dirty_suffix}"
