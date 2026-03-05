#!/usr/bin/env sh
set -eu

build_version="${BUILD_VERSION:-}"
build_commit="${BUILD_COMMIT:-}"
build_time="${BUILD_TIME:-}"

if [ -z "$build_version" ]; then
  build_version="$(./scripts/build/version.sh)"
fi

if [ -z "$build_commit" ]; then
  if command -v git >/dev/null 2>&1; then
    build_commit="$(git rev-parse --short=12 HEAD 2>/dev/null || true)"
  fi
fi
if [ -z "$build_commit" ]; then
  build_commit="unknown"
fi

if [ -z "$build_time" ]; then
  build_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fi

printf '%s' "-s -w -X github.com/arodd/hdhriptv/internal/version.Version=${build_version} -X github.com/arodd/hdhriptv/internal/version.Commit=${build_commit} -X github.com/arodd/hdhriptv/internal/version.BuildTime=${build_time}"
