package stream

import (
	"testing"
	"time"
)

func TestTuneBackoffGateScopesCooldownPerChannel(t *testing.T) {
	gate := newTuneBackoffGate(1, time.Minute, 2*time.Second)
	now := time.Unix(1_700_000_000, 0).UTC()

	if decision := gate.allow(101, now); !decision.Allowed {
		t.Fatalf("allow(101) = %#v, want allowed", decision)
	}

	failure := gate.recordFailure(101, now)
	if failure.Allowed {
		t.Fatalf("recordFailure(101) = %#v, want cooldown activation", failure)
	}
	if failure.RetryAfter != 2*time.Second {
		t.Fatalf("recordFailure(101) RetryAfter = %s, want %s", failure.RetryAfter, 2*time.Second)
	}

	if decision := gate.allow(101, now.Add(250*time.Millisecond)); decision.Allowed {
		t.Fatalf("allow(101) during cooldown = %#v, want rejected", decision)
	}

	if decision := gate.allow(202, now.Add(250*time.Millisecond)); !decision.Allowed {
		t.Fatalf("allow(202) while 101 cooling = %#v, want allowed", decision)
	}
}

func TestTuneBackoffGateSuccessClearsFailureBudget(t *testing.T) {
	gate := newTuneBackoffGate(2, time.Minute, 2*time.Second)
	now := time.Unix(1_700_000_000, 0).UTC()

	if decision := gate.allow(101, now); !decision.Allowed {
		t.Fatalf("allow(101) = %#v, want allowed", decision)
	}
	if decision := gate.recordFailure(101, now); !decision.Allowed {
		t.Fatalf("recordFailure(101) first = %#v, want failure budget to remain open", decision)
	}

	gate.recordSuccess(101, now.Add(250*time.Millisecond))

	if decision := gate.recordFailure(101, now.Add(500*time.Millisecond)); !decision.Allowed {
		t.Fatalf("recordFailure(101) after success = %#v, want budget reset", decision)
	}
	if decision := gate.allow(101, now.Add(750*time.Millisecond)); !decision.Allowed {
		t.Fatalf("allow(101) after fail-success-fail = %#v, want still allowed", decision)
	}
}

func TestTuneBackoffGatePrunesExpiredScopesWithoutDirectRevisit(t *testing.T) {
	gate := newTuneBackoffGate(1, time.Second, time.Second)
	now := time.Unix(1_700_000_000, 0).UTC()

	for _, scopeKey := range []int64{101, 202, 303} {
		decision := gate.recordFailure(scopeKey, now)
		if decision.Allowed {
			t.Fatalf("recordFailure(%d) = %#v, want cooldown activation", scopeKey, decision)
		}
	}

	if got := tuneBackoffScopeCount(gate); got != 3 {
		t.Fatalf("scope count after setup = %d, want 3", got)
	}

	later := now.Add(3 * time.Second)
	if decision := gate.allow(404, later); !decision.Allowed {
		t.Fatalf("allow(404) after stale scopes expire = %#v, want allowed", decision)
	}

	if got := tuneBackoffScopeCount(gate); got != 0 {
		t.Fatalf("scope count after opportunistic prune = %d, want 0", got)
	}
}

func TestTuneBackoffGateScopeStateBoundedUnderHighChurn(t *testing.T) {
	gate := newTuneBackoffGate(100, time.Minute, time.Minute)
	gate.scopeCap = 4
	now := time.Unix(1_700_000_000, 0).UTC()

	for i := int64(1); i <= 12; i++ {
		decision := gate.recordFailure(i, now.Add(time.Duration(i)*time.Millisecond))
		if !decision.Allowed {
			t.Fatalf("recordFailure(%d) = %#v, want failure window still open", i, decision)
		}
	}

	if got := tuneBackoffScopeCount(gate); got != 4 {
		t.Fatalf("scope count after churn = %d, want cap of 4", got)
	}

	gate.mu.Lock()
	defer gate.mu.Unlock()

	for _, scopeKey := range []int64{9, 10, 11, 12} {
		if _, ok := gate.scopes[scopeKey]; !ok {
			t.Fatalf("expected recent scope %d to be retained", scopeKey)
		}
	}
	for _, scopeKey := range []int64{1, 2, 3, 4} {
		if _, ok := gate.scopes[scopeKey]; ok {
			t.Fatalf("expected oldest scope %d to be evicted", scopeKey)
		}
	}
}

func tuneBackoffScopeCount(gate *tuneBackoffGate) int {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return len(gate.scopes)
}
