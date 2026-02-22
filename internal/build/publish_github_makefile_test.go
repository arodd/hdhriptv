package build

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const testSyncTag = "public-sync/test"

type publishFixture struct {
	internalBare string
	publicBare   string
	workDir      string
	makefilePath string
}

func TestPublishGithubInitialIncrementalAndIdempotent(t *testing.T) {
	fixture := newPublishFixture(t)

	out := fixture.runPublish(t, true)
	if !strings.Contains(out, "Published ") {
		t.Fatalf("publish-github output = %q, want published summary", out)
	}
	internalTip := fixture.internalTip(t)
	publicTip := fixture.publicTip(t)
	if fixture.commitTree(t, fixture.internalBare, internalTip) != fixture.commitTree(t, fixture.publicBare, publicTip) {
		t.Fatal("initial publish tree mismatch between internal and public tips")
	}
	if got := fixture.internalTagTip(t, testSyncTag); got != internalTip {
		t.Fatalf("internal tag tip = %s, want %s", got, internalTip)
	}

	_ = fixture.addAndPushInternalCommit(t, "incremental.txt", "incremental sync\n")
	out = fixture.runPublish(t, true)
	if !strings.Contains(out, "Published ") {
		t.Fatalf("publish-github incremental output = %q, want published summary", out)
	}
	internalTip = fixture.internalTip(t)
	publicTip = fixture.publicTip(t)
	if fixture.commitTree(t, fixture.internalBare, internalTip) != fixture.commitTree(t, fixture.publicBare, publicTip) {
		t.Fatal("incremental publish tree mismatch between internal and public tips")
	}
	if got := fixture.internalTagTip(t, testSyncTag); got != internalTip {
		t.Fatalf("internal tag tip after incremental publish = %s, want %s", got, internalTip)
	}

	publicTipBefore := publicTip
	out = fixture.runPublish(t, true)
	if !strings.Contains(out, "No new internal commits to publish.") {
		t.Fatalf("idempotent rerun output = %q, want no-new-commits message", out)
	}
	publicTipAfter := fixture.publicTip(t)
	if publicTipAfter != publicTipBefore {
		t.Fatalf("public tip changed on idempotent rerun: before=%s after=%s", publicTipBefore, publicTipAfter)
	}
}

func TestPublishGithubRejectsLocalDivergence(t *testing.T) {
	fixture := newPublishFixture(t)
	fixture.addLocalOnlyCommit(t, "local-divergence.txt", "local divergence\n")

	out := fixture.runPublish(t, false)
	if !strings.Contains(out, "Refusing to publish: local main is out of sync with origin/main.") {
		t.Fatalf("divergence rejection output = %q, want out-of-sync message", out)
	}
	if fixture.hasBranch(t, fixture.publicBare, "main") {
		t.Fatal("public main branch exists after divergence rejection, want no publish")
	}
}

func TestPublishGithubRecoversFromTagPushFailureWithoutDuplicatePublicCommit(t *testing.T) {
	fixture := newPublishFixture(t)
	fixture.installRejectTagPushOnceHook(t, testSyncTag)

	out := fixture.runPublish(t, false)
	if !strings.Contains(out, "rejecting sync tag update once") {
		t.Fatalf("first publish output = %q, want injected hook rejection", out)
	}

	internalTip := fixture.internalTip(t)
	publicTipAfterFailure := fixture.publicTip(t)
	if fixture.commitTree(t, fixture.internalBare, internalTip) != fixture.commitTree(t, fixture.publicBare, publicTipAfterFailure) {
		t.Fatal("public tip tree mismatch after tag-push failure")
	}
	if fixture.hasTag(t, fixture.internalBare, testSyncTag) {
		t.Fatal("sync tag exists on internal remote after forced tag-push failure, want absent")
	}

	out = fixture.runPublish(t, true)
	if !strings.Contains(out, "already matches internal tree; updating marker only.") {
		t.Fatalf("recovery output = %q, want tree-match marker update message", out)
	}

	publicTipAfterRecovery := fixture.publicTip(t)
	if publicTipAfterRecovery != publicTipAfterFailure {
		t.Fatalf(
			"public tip changed on recovery rerun: after failure=%s after recovery=%s",
			publicTipAfterFailure,
			publicTipAfterRecovery,
		)
	}
	if got := fixture.internalTagTip(t, testSyncTag); got != internalTip {
		t.Fatalf("internal sync tag tip after recovery = %s, want %s", got, internalTip)
	}

	count := strings.TrimSpace(fixture.gitDirMust(t, fixture.publicBare, "rev-list", "--count", "refs/heads/main"))
	if count != "1" {
		t.Fatalf("public commit count = %s, want 1 (no duplicate squash commit)", count)
	}
}

func newPublishFixture(t *testing.T) publishFixture {
	t.Helper()
	requireTool(t, "git")
	requireTool(t, "make")

	tmpRoot := t.TempDir()
	internalBare := filepath.Join(tmpRoot, "internal.git")
	publicBare := filepath.Join(tmpRoot, "public.git")
	seedDir := filepath.Join(tmpRoot, "seed")
	workDir := filepath.Join(tmpRoot, "work")

	gitMust(t, tmpRoot, "init", "--bare", internalBare)
	gitMust(t, tmpRoot, "init", "--bare", publicBare)

	gitMust(t, tmpRoot, "init", seedDir)
	configureGitIdentity(t, seedDir)
	writeFile(t, filepath.Join(seedDir, "README.md"), "# seed\n")
	gitMust(t, seedDir, "add", "README.md")
	gitMust(t, seedDir, "commit", "-m", "seed")
	gitMust(t, seedDir, "branch", "-M", "main")
	gitMust(t, seedDir, "remote", "add", "origin", internalBare)
	gitMust(t, seedDir, "push", "origin", "main")
	gitDirMust(t, internalBare, "symbolic-ref", "HEAD", "refs/heads/main")

	gitMust(t, tmpRoot, "clone", internalBare, workDir)
	configureGitIdentity(t, workDir)

	return publishFixture{
		internalBare: internalBare,
		publicBare:   publicBare,
		workDir:      workDir,
		makefilePath: repoMakefilePath(t),
	}
}

func repoMakefilePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "Makefile"))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat makefile %s: %v", path, err)
	}
	return path
}

func requireTool(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not available: %v", name, err)
	}
}

func (f publishFixture) runPublish(t *testing.T, wantSuccess bool) string {
	t.Helper()
	args := []string{
		"-f", f.makefilePath,
		"publish-github",
		"INTERNAL_REMOTE=origin",
		"INTERNAL_REPO_URL=" + f.internalBare,
		"PUBLIC_REMOTE=public",
		"PUBLIC_REPO_URL=" + f.publicBare,
		"SYNC_BRANCH=main",
		"PUBLIC_SYNC_TAG=" + testSyncTag,
	}
	out, err := runCmd(f.workDir, "make", args...)
	if wantSuccess && err != nil {
		t.Fatalf("make publish-github failed: %v\n%s", err, out)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("make publish-github unexpectedly succeeded\n%s", out)
	}
	return out
}

func (f publishFixture) addAndPushInternalCommit(t *testing.T, relPath, contents string) string {
	t.Helper()
	writeFile(t, filepath.Join(f.workDir, relPath), contents)
	gitMust(t, f.workDir, "add", relPath)
	gitMust(t, f.workDir, "commit", "-m", "update "+relPath)
	gitMust(t, f.workDir, "push", "origin", "main")
	return strings.TrimSpace(gitMust(t, f.workDir, "rev-parse", "HEAD"))
}

func (f publishFixture) addLocalOnlyCommit(t *testing.T, relPath, contents string) {
	t.Helper()
	writeFile(t, filepath.Join(f.workDir, relPath), contents)
	gitMust(t, f.workDir, "add", relPath)
	gitMust(t, f.workDir, "commit", "-m", "local-only "+relPath)
}

func (f publishFixture) internalTip(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(f.gitDirMust(t, f.internalBare, "rev-parse", "refs/heads/main"))
}

func (f publishFixture) publicTip(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(f.gitDirMust(t, f.publicBare, "rev-parse", "refs/heads/main"))
}

func (f publishFixture) internalTagTip(t *testing.T, tag string) string {
	t.Helper()
	return strings.TrimSpace(f.gitDirMust(t, f.internalBare, "rev-parse", "refs/tags/"+tag+"^{commit}"))
}

func (f publishFixture) commitTree(t *testing.T, bareRepo, commit string) string {
	t.Helper()
	return strings.TrimSpace(f.gitDirMust(t, bareRepo, "rev-parse", commit+"^{tree}"))
}

func (f publishFixture) hasBranch(t *testing.T, bareRepo, branch string) bool {
	t.Helper()
	_, err := runCmd("", "git", "--git-dir", bareRepo, "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	return err == nil
}

func (f publishFixture) hasTag(t *testing.T, bareRepo, tag string) bool {
	t.Helper()
	_, err := runCmd("", "git", "--git-dir", bareRepo, "show-ref", "--verify", "--quiet", "refs/tags/"+tag)
	return err == nil
}

func (f publishFixture) installRejectTagPushOnceHook(t *testing.T, tag string) {
	t.Helper()
	hookPath := filepath.Join(f.internalBare, "hooks", "pre-receive")
	sentinelPath := filepath.Join(f.internalBare, "hooks", "reject-sync-tag-once")
	script := fmt.Sprintf(`#!/usr/bin/env bash
set -eu
sentinel="%s"
if [ ! -f "$sentinel" ]; then
  exit 0
fi
while read -r old new ref; do
  if [ "$ref" = "refs/tags/%s" ]; then
    rm -f "$sentinel"
    echo "rejecting sync tag update once" >&2
    exit 1
  fi
done
exit 0
`, sentinelPath, tag)
	writeFileMode(t, hookPath, script, 0o755)
	writeFile(t, sentinelPath, "reject once\n")
}

func (f publishFixture) gitDirMust(t *testing.T, bareRepo string, args ...string) string {
	t.Helper()
	return gitDirMust(t, bareRepo, args...)
}

func gitDirMust(t *testing.T, bareRepo string, args ...string) string {
	t.Helper()
	fullArgs := append([]string{"--git-dir", bareRepo}, args...)
	out, err := runCmd("", "git", fullArgs...)
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(fullArgs, " "), err, out)
	}
	return out
}

func configureGitIdentity(t *testing.T, dir string) {
	t.Helper()
	gitMust(t, dir, "config", "user.name", "hdhriptv-test")
	gitMust(t, dir, "config", "user.email", "hdhriptv-test@example.com")
}

func gitMust(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := runCmd(dir, "git", args...)
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func runCmd(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	writeFileMode(t, path, contents, 0o644)
}

func writeFileMode(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
