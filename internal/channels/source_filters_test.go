package channels

import (
	"reflect"
	"testing"
)

func TestValidateSourceIDsAcceptsEmptyAndPositiveValues(t *testing.T) {
	for _, input := range [][]int64{
		nil,
		{},
		{1, 2, 9},
	} {
		if err := ValidateSourceIDs(input, "source_ids"); err != nil {
			t.Fatalf("ValidateSourceIDs(%v) error = %v, want nil", input, err)
		}
	}
}

func TestValidateSourceIDsRejectsNonPositiveValues(t *testing.T) {
	tests := []struct {
		name  string
		ids   []int64
		field string
		want  string
	}{
		{
			name:  "zero",
			ids:   []int64{0, 1},
			field: "source_ids",
			want:  "source_ids must contain only positive integers",
		},
		{
			name:  "negative",
			ids:   []int64{-1, 4},
			field: "dynamic_rule.source_ids",
			want:  "dynamic_rule.source_ids must contain only positive integers",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSourceIDs(tc.ids, tc.field)
			if err == nil {
				t.Fatalf("ValidateSourceIDs(%v, %q) error = nil, want %q", tc.ids, tc.field, tc.want)
			}
			if err.Error() != tc.want {
				t.Fatalf("ValidateSourceIDs(%v, %q) error = %q, want %q", tc.ids, tc.field, err.Error(), tc.want)
			}
		})
	}
}

func TestNormalizeSourceIDsDedupesAndSorts(t *testing.T) {
	got := NormalizeSourceIDs([]int64{5, 2, 5, 3, 2})
	want := []int64{2, 3, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeSourceIDs() = %#v, want %#v", got, want)
	}
}

func TestNormalizeSourceIDsDropsInvalidValues(t *testing.T) {
	got := NormalizeSourceIDs([]int64{0, -1, 7})
	want := []int64{7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeSourceIDs() = %#v, want %#v", got, want)
	}
}

func TestNormalizeSourceIDsEmptyInputReturnsEmptySlice(t *testing.T) {
	got := NormalizeSourceIDs(nil)
	if got == nil {
		t.Fatal("NormalizeSourceIDs(nil) returned nil slice, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("len(NormalizeSourceIDs(nil)) = %d, want 0", len(got))
	}
}
