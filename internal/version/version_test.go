package version

import "testing"

func TestCurrentReleaseVersion(t *testing.T) {
	restore := snapshotBuildVars()
	t.Cleanup(restore)

	Version = "v1.2.3"
	Commit = "abcdef1234567890"
	BuildTime = "2026-03-03T22:00:00Z"

	got := Current()
	if got.Version != "v1.2.3" {
		t.Fatalf("Version = %q, want v1.2.3", got.Version)
	}
	if got.Source != "release" {
		t.Fatalf("Source = %q, want release", got.Source)
	}
	if got.Commit != "abcdef123456" {
		t.Fatalf("Commit = %q, want abcdef123456", got.Commit)
	}
	if got.BuildTime != "2026-03-03T22:00:00Z" {
		t.Fatalf("BuildTime = %q, want 2026-03-03T22:00:00Z", got.BuildTime)
	}
}

func TestCurrentNormalizesPlainSemver(t *testing.T) {
	restore := snapshotBuildVars()
	t.Cleanup(restore)

	Version = "1.2.3"
	Commit = "abc123"
	BuildTime = "2026-03-03T22:00:00Z"

	got := Current()
	if got.Version != "v1.2.3" {
		t.Fatalf("Version = %q, want v1.2.3", got.Version)
	}
	if got.Source != "release" {
		t.Fatalf("Source = %q, want release", got.Source)
	}
}

func TestCurrentDefaultsToDevWhenUnset(t *testing.T) {
	restore := snapshotBuildVars()
	t.Cleanup(restore)

	Version = ""
	Commit = ""
	BuildTime = ""

	got := Current()
	if got.Version != "v0.0.0-dev" {
		t.Fatalf("Version = %q, want v0.0.0-dev", got.Version)
	}
	if got.Source != "dev" {
		t.Fatalf("Source = %q, want dev", got.Source)
	}
	if got.Commit != "unknown" {
		t.Fatalf("Commit = %q, want unknown", got.Commit)
	}
	if got.BuildTime != "unknown" {
		t.Fatalf("BuildTime = %q, want unknown", got.BuildTime)
	}
}

func TestCurrentDevVersionClassification(t *testing.T) {
	restore := snapshotBuildVars()
	t.Cleanup(restore)

	Version = "v1.2.3-dev+abc123"
	Commit = "abc123"
	BuildTime = "2026-03-03T22:00:00Z"

	got := Current()
	if got.Source != "dev" {
		t.Fatalf("Source = %q, want dev", got.Source)
	}
}

func snapshotBuildVars() func() {
	origVersion := Version
	origCommit := Commit
	origBuildTime := BuildTime
	return func() {
		Version = origVersion
		Commit = origCommit
		BuildTime = origBuildTime
	}
}
