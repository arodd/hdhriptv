package playlist

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGroupJSONUsesLowercaseKeys(t *testing.T) {
	body, err := json.Marshal(Group{
		Name:  "US Sports",
		Count: 42,
	})
	if err != nil {
		t.Fatalf("json.Marshal(Group) error = %v", err)
	}

	raw := string(body)
	if !strings.Contains(raw, `"name":"US Sports"`) {
		t.Fatalf("group json = %s, want lowercase name key", raw)
	}
	if !strings.Contains(raw, `"count":42`) {
		t.Fatalf("group json = %s, want lowercase count key", raw)
	}
	if strings.Contains(raw, `"Name"`) || strings.Contains(raw, `"Count"`) {
		t.Fatalf("group json = %s, want lowercase keys only", raw)
	}
}
