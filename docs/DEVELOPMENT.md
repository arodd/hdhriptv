# Development

## GitHub Mirror Publish Workflow

`make publish-github` squashes the current internal `main` tree into a single
public commit and updates a sync marker tag on the internal remote.

Required variables (with defaults from `Makefile`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `INTERNAL_REMOTE` | `origin` | Internal git remote name to read from and write marker tags to. |
| `INTERNAL_REPO_URL` | `git@gitlab.lan:arodd/hdhriptv.git` | Canonical URL for `INTERNAL_REMOTE`. |
| `PUBLIC_REMOTE` | `github` | Public git remote name to publish squashed commits to. |
| `PUBLIC_REPO_URL` | `git@github.com:arodd/hdhriptv.git` | Canonical URL for `PUBLIC_REMOTE`. |
| `SYNC_BRANCH` | `main` | Branch synchronized from internal to public. |
| `PUBLIC_SYNC_TAG` | `public-sync/latest` | Marker tag on the internal remote that records the last published internal commit. |
| `PUBLISH_GITHUB_COMMIT_MESSAGE` | empty | Optional squash commit subject override. |

Example:

```bash
make publish-github \
  INTERNAL_REMOTE=origin \
  INTERNAL_REPO_URL=git@gitlab.lan:arodd/hdhriptv.git \
  PUBLIC_REMOTE=github \
  PUBLIC_REPO_URL=git@github.com:arodd/hdhriptv.git \
  SYNC_BRANCH=main \
  PUBLIC_SYNC_TAG=public-sync/latest
```

Behavior summary:

- Refuses to run unless local `main` exactly matches `INTERNAL_REMOTE/main`.
- Uses the internal marker tag to decide whether there are unpublished internal
  commits.
- Publishes a squash commit to `PUBLIC_REMOTE/main` using the internal tree
  (initial publish has no parent; later publishes parent onto current public tip).
- Updates `PUBLIC_SYNC_TAG` on the internal remote only after successful public
  publish.
- If public branch tree already matches the internal tip tree (for example after
  a prior public push succeeded but marker-tag push failed), reruns update only
  the marker tag and do not create duplicate public squash commits.

## GitHub Binary Release with GitLab Tag

`make release-github-sync-tag` builds release binaries once from internal
`main`, ensures the public mirror branch is synced, pushes the same tag name to
both remotes, and publishes release binaries on GitHub only.

GitLab behavior for this workflow:

- Pushes a matching tag name to internal remote.
- Does not create a GitLab release object or upload release assets.

Additional variables (with defaults from `Makefile`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `RELEASE_TAG` | empty (required) | Release tag name (for example `v1.2.3`). |
| `RELEASE_TITLE` | empty | GitHub release title (defaults to `RELEASE_TAG`). |
| `RELEASE_NOTES_FILE` | empty | Optional notes preface merged ahead of auto-generated changelog highlights. |
| `RELEASE_DIST_DIR` | `dist` | Output directory for release binaries/checksums. |
| `GITHUB_RELEASE_REPO` | empty | Optional override for GitHub repo in `owner/repo` format (auto-derived from `PUBLIC_REPO_URL` when empty). |
| `RELEASE_IMAGE` | `arodd/hdhriptv` | Container image repository pushed by the release workflow (`<repo>:<RELEASE_TAG>` and `<repo>:latest`). |
| `BUILDX_BUILDER` | `hdhriptv-multiarch` | Docker Buildx builder name used for multi-arch image publishing. |
| `PUBLISH_GITHUB_COMMIT_MESSAGE` | empty | Optional override for the mirror squash commit message. If empty, release workflow defaults to `public(<SYNC_BRANCH>): release <RELEASE_TAG>`. |

Example:

```bash
make release-github-sync-tag \
  RELEASE_TAG=v1.2.3 \
  RELEASE_TITLE="v1.2.3" \
  INTERNAL_REMOTE=origin \
  INTERNAL_REPO_URL=git@gitlab.lan:arodd/hdhriptv.git \
  PUBLIC_REMOTE=github \
  PUBLIC_REPO_URL=git@github.com:arodd/hdhriptv.git \
  SYNC_BRANCH=main
```

Behavior summary:

- Refuses to run with tracked local changes or branch divergence from internal
  `SYNC_BRANCH`.
- Builds the following release binaries plus `dist/SHA256SUMS`:
  - `dist/hdhriptv-linux-amd64`
  - `dist/hdhriptv-linux-arm64`
  - `dist/hdhriptv-darwin-amd64`
  - `dist/hdhriptv-darwin-arm64`
  - `dist/hdhriptv-windows-amd64.exe`
- Uses `public(<SYNC_BRANCH>): release <RELEASE_TAG>` for the mirror squash
  commit subject unless `PUBLISH_GITHUB_COMMIT_MESSAGE` is explicitly set.
- Runs `make publish-github` and verifies internal/public trees match.
- Builds and pushes multi-arch container image tags for both
  `<RELEASE_IMAGE>:<RELEASE_TAG>` and `<RELEASE_IMAGE>:latest`.
- Ensures `RELEASE_TAG` points to internal tip on GitLab remote and public tip
  on GitHub remote, failing fast if an existing remote tag points elsewhere.
- Auto-generates pretty release-note sections from `CHANGELOG.md` entries added
  since the previous internal release tag (grouped into Features, Bug Fixes,
  Docs, and Other), then merges any optional `RELEASE_NOTES_FILE` preface.
- Creates or updates the GitHub release and uploads assets with clobber enabled
  for idempotent reruns.

## Testing

## Build Versioning

Builds now stamp runtime metadata into the binary via ldflags:

- `internal/version.Version`
- `internal/version.Commit`
- `internal/version.BuildTime`

Version behavior:

- Release builds (for example `make release-github-sync-tag RELEASE_TAG=v1.2.3`) stamp the exact release tag.
- Non-tag/interim builds stamp `<latest-release-tag>-dev+<shortsha>` (for example `v1.2.3-dev+abc123def456`).

At runtime this metadata is:

- Logged on startup (`app_version`, `app_commit`, `app_build_time`).
- Persisted in SQLite settings (`app.version`, `app.commit`, `app.build_time`).

### Automated Tests

```bash
go test ./...
```

### Manual Smoke Checks

```bash
curl -s http://127.0.0.1:5004/healthz
curl -s http://127.0.0.1:5004/discover.json
curl -s http://127.0.0.1:5004/lineup_status.json
```

### Client Compatibility

Run [../deploy/testing/compatibility-checklist.md](../deploy/testing/compatibility-checklist.md) against real clients on your LAN (Plex, Emby, Jellyfin, VLC).

### Recovery Continuity

```bash
./deploy/testing/recovery-slate-av-ffmpeg-copy.sh
./deploy/testing/recovery-slate-av-ffmpeg-copy-ffplay.sh
```
