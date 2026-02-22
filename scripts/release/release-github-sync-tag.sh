#!/usr/bin/env bash
set -euo pipefail

INTERNAL_REMOTE="${INTERNAL_REMOTE:-origin}"
INTERNAL_REPO_URL="${INTERNAL_REPO_URL:-git@gitlab.lan:arodd/hdhriptv.git}"
PUBLIC_REMOTE="${PUBLIC_REMOTE:-github}"
PUBLIC_REPO_URL="${PUBLIC_REPO_URL:-git@github.com:arodd/hdhriptv.git}"
SYNC_BRANCH="${SYNC_BRANCH:-main}"
RELEASE_TAG="${RELEASE_TAG:-}"
RELEASE_TITLE="${RELEASE_TITLE:-}"
RELEASE_NOTES_FILE="${RELEASE_NOTES_FILE:-}"
RELEASE_DIST_DIR="${RELEASE_DIST_DIR:-dist}"
GITHUB_RELEASE_REPO="${GITHUB_RELEASE_REPO:-}"
PUBLISH_GITHUB_COMMIT_MESSAGE="${PUBLISH_GITHUB_COMMIT_MESSAGE:-}"

log() {
  echo "[release-github-sync-tag] $*"
}

die() {
  echo "[release-github-sync-tag] ERROR: $*" >&2
  exit 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "required command not found: $1"
  fi
}

ensure_remote_url() {
  remote_name="$1"
  remote_url="$2"

  if ! git remote get-url "$remote_name" >/dev/null 2>&1; then
    log "Adding remote ${remote_name} -> ${remote_url}"
    git remote add "$remote_name" "$remote_url"
    return
  fi

  current_url="$(git remote get-url "$remote_name")"
  if [ "$current_url" != "$remote_url" ]; then
    log "Updating remote ${remote_name} URL to ${remote_url}"
    git remote set-url "$remote_name" "$remote_url"
  fi
}

remote_tag_commit() {
  remote_name="$1"
  tag_name="$2"

  peeled="$(git ls-remote --tags "$remote_name" "refs/tags/${tag_name}^{}" | awk 'NR==1 {print $1}')"
  if [ -n "$peeled" ]; then
    printf '%s\n' "$peeled"
    return
  fi

  lightweight="$(git ls-remote --refs --tags "$remote_name" "refs/tags/${tag_name}" | awk 'NR==1 {print $1}')"
  if [ -n "$lightweight" ]; then
    printf '%s\n' "$lightweight"
  fi
}

push_tag_if_needed() {
  remote_name="$1"
  tag_name="$2"
  expected_commit="$3"

  existing_commit="$(remote_tag_commit "$remote_name" "$tag_name")"
  if [ -n "$existing_commit" ] && [ "$existing_commit" != "$expected_commit" ]; then
    die "remote tag ${remote_name}/${tag_name} points to ${existing_commit}, expected ${expected_commit}"
  fi

  if [ -n "$existing_commit" ]; then
    log "Tag ${remote_name}/${tag_name} already points to ${expected_commit}"
    return
  fi

  log "Pushing tag ${tag_name} to ${remote_name} at ${expected_commit}"
  git push "$remote_name" "${expected_commit}:refs/tags/${tag_name}"
}

parse_github_repo_from_url() {
  repo_url="$1"

  case "$repo_url" in
    git@github.com:*)
      repo="${repo_url#git@github.com:}"
      ;;
    ssh://git@github.com/*)
      repo="${repo_url#ssh://git@github.com/}"
      ;;
    https://github.com/*)
      repo="${repo_url#https://github.com/}"
      ;;
    http://github.com/*)
      repo="${repo_url#http://github.com/}"
      ;;
    *)
      return 1
      ;;
  esac

  repo="${repo%.git}"
  repo="${repo%/}"
  case "$repo" in
    */*)
      printf '%s\n' "$repo"
      ;;
    *)
      return 1
      ;;
  esac
}

if [ -z "$RELEASE_TAG" ]; then
  die "RELEASE_TAG is required (example: v1.2.3)"
fi

require_cmd git
require_cmd go
require_cmd make
require_cmd gh
require_cmd sha256sum
require_cmd awk

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  die "tracked changes detected; commit/stash tracked changes before running a release"
fi

ensure_remote_url "$INTERNAL_REMOTE" "$INTERNAL_REPO_URL"
ensure_remote_url "$PUBLIC_REMOTE" "$PUBLIC_REPO_URL"

log "Fetching internal/public refs"
git fetch --no-tags "$INTERNAL_REMOTE" "$SYNC_BRANCH"
git fetch --no-tags "$PUBLIC_REMOTE"

if ! git show-ref --verify --quiet "refs/heads/${SYNC_BRANCH}"; then
  die "local branch ${SYNC_BRANCH} does not exist"
fi

head_tip="$(git rev-parse HEAD)"
local_tip="$(git rev-parse "refs/heads/${SYNC_BRANCH}")"
if [ "$head_tip" != "$local_tip" ]; then
  die "checked out commit ${head_tip} is not refs/heads/${SYNC_BRANCH}; checkout ${SYNC_BRANCH} before releasing"
fi

internal_tip="$(git rev-parse "${INTERNAL_REMOTE}/${SYNC_BRANCH}")"
if [ "$local_tip" != "$internal_tip" ]; then
  set -- $(git rev-list --left-right --count "refs/heads/${SYNC_BRANCH}...${INTERNAL_REMOTE}/${SYNC_BRANCH}")
  ahead="$1"
  behind="$2"
  die "local ${SYNC_BRANCH} is out of sync with ${INTERNAL_REMOTE}/${SYNC_BRANCH} (ahead=${ahead}, behind=${behind})"
fi

log "Building release binaries from ${internal_tip}"
mkdir -p "$RELEASE_DIST_DIR"
go mod download

build_target() {
  goos="$1"
  goarch="$2"
  output="$3"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -ldflags="-s -w" -o "${RELEASE_DIST_DIR}/${output}" ./cmd/hdhriptv
  test -f "${RELEASE_DIST_DIR}/${output}"
}

release_artifacts=(
  hdhriptv-linux-amd64
  hdhriptv-linux-arm64
  hdhriptv-darwin-amd64
  hdhriptv-darwin-arm64
)

build_target linux amd64 hdhriptv-linux-amd64
build_target linux arm64 hdhriptv-linux-arm64
build_target darwin amd64 hdhriptv-darwin-amd64
build_target darwin arm64 hdhriptv-darwin-arm64

(
  cd "$RELEASE_DIST_DIR"
  sha256sum "${release_artifacts[@]}" > SHA256SUMS
)
test -f "${RELEASE_DIST_DIR}/SHA256SUMS"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  die "tracked files changed during build; stop and inspect before publishing"
fi

log "Publishing mirrored public branch commit via make publish-github"
make publish-github \
  INTERNAL_REMOTE="$INTERNAL_REMOTE" \
  INTERNAL_REPO_URL="$INTERNAL_REPO_URL" \
  PUBLIC_REMOTE="$PUBLIC_REMOTE" \
  PUBLIC_REPO_URL="$PUBLIC_REPO_URL" \
  SYNC_BRANCH="$SYNC_BRANCH" \
  PUBLISH_GITHUB_COMMIT_MESSAGE="$PUBLISH_GITHUB_COMMIT_MESSAGE"

git fetch --no-tags "$PUBLIC_REMOTE" "$SYNC_BRANCH"
public_tip="$(git rev-parse "${PUBLIC_REMOTE}/${SYNC_BRANCH}")"
internal_tree="$(git rev-parse "${internal_tip}^{tree}")"
public_tree="$(git rev-parse "${public_tip}^{tree}")"
if [ "$internal_tree" != "$public_tree" ]; then
  die "tree mismatch after publish-github: ${INTERNAL_REMOTE}/${SYNC_BRANCH} tree=${internal_tree}, ${PUBLIC_REMOTE}/${SYNC_BRANCH} tree=${public_tree}"
fi

push_tag_if_needed "$INTERNAL_REMOTE" "$RELEASE_TAG" "$internal_tip"
push_tag_if_needed "$PUBLIC_REMOTE" "$RELEASE_TAG" "$public_tip"

release_repo="$GITHUB_RELEASE_REPO"
if [ -z "$release_repo" ]; then
  if ! release_repo="$(parse_github_repo_from_url "$PUBLIC_REPO_URL")"; then
    die "unable to derive GITHUB_RELEASE_REPO from PUBLIC_REPO_URL (${PUBLIC_REPO_URL}); set GITHUB_RELEASE_REPO explicitly (owner/repo)"
  fi
fi

if ! gh auth status -h github.com >/dev/null 2>&1; then
  die "gh is not authenticated for github.com"
fi

if [ -z "$RELEASE_TITLE" ]; then
  RELEASE_TITLE="$RELEASE_TAG"
fi

if [ -n "$RELEASE_NOTES_FILE" ] && [ ! -f "$RELEASE_NOTES_FILE" ]; then
  die "RELEASE_NOTES_FILE does not exist: ${RELEASE_NOTES_FILE}"
fi

if gh release view "$RELEASE_TAG" --repo "$release_repo" >/dev/null 2>&1; then
  log "GitHub release ${RELEASE_TAG} already exists in ${release_repo}; updating metadata"
  if [ -n "$RELEASE_NOTES_FILE" ]; then
    gh release edit "$RELEASE_TAG" --repo "$release_repo" --title "$RELEASE_TITLE" --notes-file "$RELEASE_NOTES_FILE"
  else
    gh release edit "$RELEASE_TAG" --repo "$release_repo" --title "$RELEASE_TITLE"
  fi
else
  log "Creating GitHub release ${RELEASE_TAG} in ${release_repo}"
  if [ -n "$RELEASE_NOTES_FILE" ]; then
    gh release create "$RELEASE_TAG" --repo "$release_repo" --title "$RELEASE_TITLE" --verify-tag --notes-file "$RELEASE_NOTES_FILE"
  else
    gh release create "$RELEASE_TAG" --repo "$release_repo" --title "$RELEASE_TITLE" --verify-tag --generate-notes
  fi
fi

log "Uploading release assets to GitHub (clobber enabled)"
release_upload_paths=()
for artifact in "${release_artifacts[@]}"; do
  release_upload_paths+=("${RELEASE_DIST_DIR}/${artifact}")
done
release_upload_paths+=("${RELEASE_DIST_DIR}/SHA256SUMS")

gh release upload "$RELEASE_TAG" "${release_upload_paths[@]}" --repo "$release_repo" --clobber

log "Release complete"
log "Internal commit: ${internal_tip}"
log "Public commit:   ${public_tip}"
log "GitLab tag:      ${INTERNAL_REMOTE}/${RELEASE_TAG}"
log "GitHub release:  ${release_repo}#${RELEASE_TAG}"
