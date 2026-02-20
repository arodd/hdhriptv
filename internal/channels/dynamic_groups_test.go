package channels

import "testing"

func TestNormalizeGroupNamesLegacyFallbackAndDeterministicOrdering(t *testing.T) {
	got := NormalizeGroupNames("  US News  ", []string{" sports ", "US News", "Sports"})
	if len(got) != 2 {
		t.Fatalf("len(NormalizeGroupNames) = %d, want 2", len(got))
	}
	if got[0] != "sports" && got[0] != "Sports" {
		t.Fatalf("NormalizeGroupNames()[0] = %q, want Sports-like value", got[0])
	}
	if got[1] != "US News" {
		t.Fatalf("NormalizeGroupNames()[1] = %q, want US News", got[1])
	}
}

func TestNormalizeGroupNamesUsesLegacyAliasWhenListMissing(t *testing.T) {
	got := NormalizeGroupNames("  US News  ", nil)
	if len(got) != 1 || got[0] != "US News" {
		t.Fatalf("NormalizeGroupNames(legacy only) = %#v, want [US News]", got)
	}
}

func TestNormalizeGroupNamesReturnsEmptyWhenNoSelectors(t *testing.T) {
	got := NormalizeGroupNames("   ", []string{})
	if len(got) != 0 {
		t.Fatalf("NormalizeGroupNames(empty) = %#v, want empty", got)
	}
	if alias := GroupNameAlias(got); alias != "" {
		t.Fatalf("GroupNameAlias(empty) = %q, want empty", alias)
	}
}
