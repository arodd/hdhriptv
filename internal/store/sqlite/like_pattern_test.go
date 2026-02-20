package sqlite

import "testing"

func TestSQLLikeContainsPatternEscapesWildcardAndEscapeChars(t *testing.T) {
	got := sqlLikeContainsPattern("a!b%c_d")
	const want = "%a!!b!%c!_d%"
	if got != want {
		t.Fatalf("sqlLikeContainsPattern() = %q, want %q", got, want)
	}
}
