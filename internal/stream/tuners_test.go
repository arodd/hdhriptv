package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPoolAcquireRelease(t *testing.T) {
	pool := NewPool(1)

	lease, err := pool.Acquire(context.Background(), "101", "127.0.0.1:4000")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if lease.ID != 0 {
		t.Fatalf("lease.ID = %d, want 0", lease.ID)
	}
	if got := pool.InUseCount(); got != 1 {
		t.Fatalf("InUseCount() = %d, want 1", got)
	}

	if _, err := pool.Acquire(context.Background(), "102", "127.0.0.1:4001"); !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("Acquire() when full error = %v, want ErrNoTunersAvailable", err)
	}

	lease.Release()
	lease.Release()

	if got := pool.InUseCount(); got != 0 {
		t.Fatalf("InUseCount() after release = %d, want 0", got)
	}

	lease2, err := pool.Acquire(context.Background(), "103", "127.0.0.1:4002")
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}
	lease2.Release()
}

func TestPoolAcquireCanceledContext(t *testing.T) {
	pool := NewPool(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := pool.Acquire(ctx, "101", "127.0.0.1:5000"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(canceled) error = %v, want context.Canceled", err)
	}
}

func TestPoolClientPreemptsProbeLease(t *testing.T) {
	pool := NewPool(1)

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	probeLease, err := pool.AcquireProbe(probeCtx, "src:test", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbe() error = %v", err)
	}

	probeDone := make(chan struct{})
	go func() {
		<-probeCtx.Done()
		probeLease.Release()
		close(probeDone)
	}()

	clientLease, err := pool.AcquireClient(context.Background(), "101", "127.0.0.1:7000")
	if err != nil {
		t.Fatalf("AcquireClient() error = %v", err)
	}
	defer clientLease.Release()

	select {
	case <-probeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for probe lease preemption")
	}

	if !errors.Is(context.Cause(probeCtx), ErrProbePreempted) {
		t.Fatalf("probe context cause = %v, want ErrProbePreempted", context.Cause(probeCtx))
	}
}

func TestPoolClientPreemptsIdleGraceLease(t *testing.T) {
	pool := NewPool(1)

	idleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() setup error = %v", err)
	}

	preempted := make(chan struct{}, 1)
	pool.markClientPreemptible(idleLease.ID, idleLease.token, func() bool {
		idleLease.Release()
		select {
		case preempted <- struct{}{}:
		default:
		}
		return true
	})

	clientLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() error = %v", err)
	}
	defer clientLease.Release()

	select {
	case <-preempted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for idle-grace lease preemption")
	}
}

func TestPoolMarkClientPreemptibleIgnoresStaleLeaseOwnership(t *testing.T) {
	pool := NewPool(1)

	staleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient(stale setup) error = %v", err)
	}
	staleToken := staleLease.token
	staleLease.Release()

	currentLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient(current setup) error = %v", err)
	}
	if currentLease.token == staleToken {
		t.Fatal("current lease token unexpectedly matches stale lease token")
	}

	currentPreempted := make(chan struct{}, 1)
	stalePreempted := make(chan struct{}, 1)
	pool.markClientPreemptible(currentLease.ID, currentLease.token, func() bool {
		currentLease.Release()
		select {
		case currentPreempted <- struct{}{}:
		default:
		}
		return true
	})

	// Simulate late stale callback registration for a previous lifecycle owner.
	pool.markClientPreemptible(currentLease.ID, staleToken, func() bool {
		select {
		case stalePreempted <- struct{}{}:
		default:
		}
		return false
	})

	replacementLease, err := pool.AcquireClient(context.Background(), "103", "shared:103")
	if err != nil {
		t.Fatalf("AcquireClient(replacement) error = %v, want success via current callback", err)
	}
	replacementLease.Release()

	select {
	case <-currentPreempted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for current idle callback preemption")
	}

	select {
	case <-stalePreempted:
		t.Fatal("stale preempt callback executed, want ignored stale ownership")
	default:
	}
}

func TestPoolClearClientPreemptibleIgnoresStaleLeaseOwnership(t *testing.T) {
	pool := NewPool(1)

	// Acquire and release a stale lease, capturing its token.
	staleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient(stale setup) error = %v", err)
	}
	staleToken := staleLease.token
	staleLease.Release()

	// Acquire the current lease on the same slot.
	currentLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient(current setup) error = %v", err)
	}
	if currentLease.token == staleToken {
		t.Fatal("current lease token unexpectedly matches stale lease token")
	}

	// Register a preempt callback for the current lease.
	currentPreempted := make(chan struct{}, 1)
	pool.markClientPreemptible(currentLease.ID, currentLease.token, func() bool {
		currentLease.Release()
		select {
		case currentPreempted <- struct{}{}:
		default:
		}
		return true
	})

	// Simulate a late stale session calling clearClientPreemptible with its
	// old token — this should be a no-op and not remove the current callback.
	pool.clearClientPreemptible(currentLease.ID, staleToken)

	// A third acquire should succeed via the current preempt callback.
	replacementLease, err := pool.AcquireClient(context.Background(), "103", "shared:103")
	if err != nil {
		t.Fatalf("AcquireClient(replacement) error = %v, want success via current callback", err)
	}
	replacementLease.Release()

	select {
	case <-currentPreempted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for current idle callback preemption")
	}
}

func TestPoolPreemptSettleDelayAppliedAfterPreemption(t *testing.T) {
	pool := NewPool(1)
	pool.SetPreemptSettleDelay(120 * time.Millisecond)

	idleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() setup error = %v", err)
	}
	pool.markClientPreemptible(idleLease.ID, idleLease.token, func() bool {
		idleLease.Release()
		return true
	})

	started := time.Now()
	clientLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() with preempt settle error = %v", err)
	}
	defer clientLease.Release()

	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("preempted acquire completed in %s, want at least 100ms settle delay", elapsed)
	}
}

func TestPoolPreemptSettleDelayAppliedOnFullReleaseBeforeRefill(t *testing.T) {
	pool := NewPool(2)
	pool.SetPreemptSettleDelay(220 * time.Millisecond)

	lease1, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() lease1 setup error = %v", err)
	}
	defer lease1.Release()

	lease2, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() lease2 setup error = %v", err)
	}

	lease2.Release() // full(2) -> partial(1)

	started := time.Now()
	lease3, err := pool.AcquireClient(context.Background(), "103", "shared:103") // partial(1) -> full(2)
	if err != nil {
		t.Fatalf("AcquireClient() refill error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 170*time.Millisecond {
		t.Fatalf("refill acquire completed in %s, want settle delay after full->partial transition", elapsed)
	}
	lease3.Release()
}

func TestPoolFailoverSettleDelayWhenRecentlyDroppedFromFull(t *testing.T) {
	pool := NewPool(2)
	pool.SetPreemptSettleDelay(220 * time.Millisecond)

	lease1, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() lease1 setup error = %v", err)
	}
	defer lease1.Release()

	lease2, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() lease2 setup error = %v", err)
	}

	lease2.Release() // full(2) -> partial(1), arms settle window.

	if wait := pool.failoverSettleDelayWhenFull(); wait < 150*time.Millisecond {
		t.Fatalf("failoverSettleDelayWhenFull() = %s, want at least 150ms after full->partial", wait)
	}

	time.Sleep(260 * time.Millisecond)
	if wait := pool.failoverSettleDelayWhenFull(); wait > 0 {
		t.Fatalf("failoverSettleDelayWhenFull() after window = %s, want 0", wait)
	}
}

func TestPoolPreemptSettleDelayPersistsAcrossCanceledRefill(t *testing.T) {
	pool := NewPool(2)
	pool.SetPreemptSettleDelay(300 * time.Millisecond)

	lease1, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() lease1 setup error = %v", err)
	}
	defer lease1.Release()

	lease2, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() lease2 setup error = %v", err)
	}
	lease2.Release() // full(2) -> partial(1), arms settle

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	if _, err := pool.AcquireClient(ctx, "103", "shared:103"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireClient() canceled refill error = %v, want context deadline exceeded", err)
	}

	started := time.Now()
	lease3, err := pool.AcquireClient(context.Background(), "104", "shared:104")
	if err != nil {
		t.Fatalf("AcquireClient() follow-up refill error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 190*time.Millisecond {
		t.Fatalf("follow-up refill completed in %s, want remaining settle delay after canceled attempt", elapsed)
	}
	lease3.Release()
}

func TestPoolPreemptSettleDelayPersistsAfterContextCancellation(t *testing.T) {
	pool := NewPool(1)
	pool.SetPreemptSettleDelay(300 * time.Millisecond)

	idleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() setup error = %v", err)
	}
	pool.markClientPreemptible(idleLease.ID, idleLease.token, func() bool {
		idleLease.Release()
		return true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if _, err := pool.AcquireClient(ctx, "102", "shared:102"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireClient() with canceled settle delay error = %v, want context deadline exceeded", err)
	}
	if got := pool.InUseCount(); got != 0 {
		t.Fatalf("InUseCount() after canceled acquire = %d, want 0", got)
	}

	started := time.Now()
	lease, err := pool.AcquireClient(context.Background(), "103", "shared:103")
	if err != nil {
		t.Fatalf("AcquireClient() follow-up error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 180*time.Millisecond {
		t.Fatalf("follow-up acquire completed in %s, want remaining settle delay after cancellation", elapsed)
	}
	lease.Release()

	started = time.Now()
	lease, err = pool.AcquireClient(context.Background(), "104", "shared:104")
	if err != nil {
		t.Fatalf("AcquireClient() after settle consumed error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 240*time.Millisecond {
		t.Fatalf("post-release refill completed in %s, want full->partial->full settle delay", elapsed)
	}
	lease.Release()
}

func TestPoolPreemptSettleDelayTracksPreemptedSlot(t *testing.T) {
	pool := NewPool(2)
	pool.SetPreemptSettleDelay(220 * time.Millisecond)

	activeLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() active setup error = %v", err)
	}
	defer activeLease.Release()

	idleLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() idle setup error = %v", err)
	}
	defer idleLease.Release()

	pool.markClientPreemptible(idleLease.ID, idleLease.token, func() bool {
		go func() {
			time.Sleep(40 * time.Millisecond)
			idleLease.Release()
		}()
		return true
	})

	go func() {
		time.Sleep(10 * time.Millisecond)
		activeLease.Release()
	}()

	started := time.Now()
	lease, err := pool.AcquireClient(context.Background(), "103", "shared:103")
	if err != nil {
		t.Fatalf("AcquireClient() replacing stream error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 170*time.Millisecond {
		t.Fatalf("acquire completed in %s, want refill settle delay before returning to full usage", elapsed)
	}
	defer lease.Release()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if pool.InUseCount() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pool.InUseCount(); got != 1 {
		t.Fatalf("InUseCount() before second acquire = %d, want 1", got)
	}

	started = time.Now()
	lease2, err := pool.AcquireClient(context.Background(), "104", "shared:104")
	if err != nil {
		t.Fatalf("AcquireClient() preempted-slot reuse error = %v", err)
	}
	lease2.Release()
}

// ---------------------------------------------------------------------------
// Regression matrix: acquire error-classification and preempt-race fixes
// Covers all branches from the consolidated child issues:
//   - probe/no-preempt no-slot fallthrough (context expiry misclassified)
//   - preempt-scan lock-delay fallthrough (context expiry misclassified)
//   - preempt-callback fallthrough (context expiry misclassified)
//   - preempt-wait timeout vs caller deadline differentiation
//   - freed-slot re-check after failed preempt callbacks
// ---------------------------------------------------------------------------

// TestPoolAcquireProbeNoSlotContextCanceled verifies that probe acquire (no
// preempt allowed) returns context.Canceled rather than ErrNoTunersAvailable
// when the caller context is canceled and no slot is free.
// Child: TODO-stream-tuner-probe-acquire-context-expiry-misclassified-no-tuners.md
func TestPoolAcquireProbeNoSlotContextCanceled(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	_, err = pool.AcquireProbe(ctx, "probe:test", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireProbe(canceled) error = %v, want context.Canceled", err)
	}
}

// TestPoolAcquireProbeNoSlotContextDeadline verifies that probe acquire returns
// context.DeadlineExceeded rather than ErrNoTunersAvailable when the caller
// deadline has expired.
// Child: TODO-stream-tuner-probe-acquire-context-expiry-misclassified-no-tuners.md
func TestPoolAcquireProbeNoSlotContextDeadline(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err = pool.AcquireProbe(ctx, "probe:test", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireProbe(deadline) error = %v, want context.DeadlineExceeded", err)
	}
}

// TestPoolAcquireNoSlotTrueCapacityExhaustion verifies that when the caller
// context is still active and no slot is free, ErrNoTunersAvailable is returned.
// This covers the "true capacity exhaustion" baseline for all no-slot branches.
func TestPoolAcquireNoSlotTrueCapacityExhaustion(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	// Probe path (no preempt) with active context → true capacity exhaustion.
	_, err = pool.AcquireProbe(context.Background(), "probe:test", nil)
	if !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireProbe(active ctx) error = %v, want ErrNoTunersAvailable", err)
	}
}

// TestPoolAcquirePreemptCallbackContextCanceled verifies that when a preempt
// callback returns false and the caller context has expired, the acquire path
// returns context.Canceled instead of ErrNoTunersAvailable.
// Child: TODO-stream-tuner-preempt-callback-context-expiry-misclassified-no-tuners.md
func TestPoolAcquirePreemptCallbackContextCanceled(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	ctx, cancel := context.WithCancel(context.Background())

	// Callback cancels the caller context and returns false (failed preempt).
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		cancel()
		return false
	})

	_, err = pool.AcquireClient(ctx, "102", "shared:102")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireClient(preempt callback cancel) error = %v, want context.Canceled", err)
	}
}

// TestPoolAcquirePreemptCallbackContextDeadline verifies that when a preempt
// callback takes time and the caller deadline expires during the callback,
// acquire returns context.DeadlineExceeded.
// Child: TODO-stream-tuner-preempt-callback-context-expiry-misclassified-no-tuners.md
func TestPoolAcquirePreemptCallbackContextDeadline(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// Callback sleeps past the caller deadline and returns false.
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		time.Sleep(80 * time.Millisecond)
		return false
	})

	_, err = pool.AcquireClient(ctx, "102", "shared:102")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireClient(preempt callback deadline) error = %v, want context.DeadlineExceeded", err)
	}
}

// TestPoolAcquirePreemptCallbackTrueNoTuner verifies that when a preempt
// callback returns false but the caller context is still active, acquire
// correctly returns ErrNoTunersAvailable (true capacity exhaustion).
func TestPoolAcquirePreemptCallbackTrueNoTuner(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	// Callback returns false immediately; caller context stays active.
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		return false
	})

	_, err = pool.AcquireClient(context.Background(), "102", "shared:102")
	if !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireClient(preempt callback active ctx) error = %v, want ErrNoTunersAvailable", err)
	}
}

// TestPoolAcquirePreemptWaitCallerDeadlineBeforeInternalTimeout verifies that
// when a caller deadline fires before the internal preempt-wait timeout, the
// caller's context error is returned instead of ErrNoTunersAvailable.
// Child: TODO-stream-tuner-preempt-wait-parent-deadline-misclassified-no-tuners.md
func TestPoolAcquirePreemptWaitCallerDeadlineBeforeInternalTimeout(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}

	// Successful preempt callback but releases slot after a delay.
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		go func() {
			time.Sleep(200 * time.Millisecond)
			held.Release()
		}()
		return true
	})

	// Caller deadline is shorter than the clientPreemptWait + release delay.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	_, err = pool.AcquireClient(ctx, "102", "shared:102")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireClient(caller deadline < preempt wait) error = %v, want context.DeadlineExceeded", err)
	}
}

// TestPoolAcquirePreemptWaitCallerCanceledBeforeInternalTimeout verifies the
// same branch for explicit cancellation instead of deadline.
// Child: TODO-stream-tuner-preempt-wait-parent-deadline-misclassified-no-tuners.md
func TestPoolAcquirePreemptWaitCallerCanceledBeforeInternalTimeout(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}

	pool.markClientPreemptible(held.ID, held.token, func() bool {
		go func() {
			time.Sleep(200 * time.Millisecond)
			held.Release()
		}()
		return true
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	_, err = pool.AcquireClient(ctx, "102", "shared:102")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireClient(caller cancel < preempt wait) error = %v, want context.Canceled", err)
	}
}

// TestPoolAcquirePreemptWaitInternalTimeoutTrueNoTuner verifies that when the
// internal preempt-wait timeout fires but the caller context is still active,
// ErrNoTunersAvailable is returned (true capacity exhaustion).
// Child: TODO-stream-tuner-preempt-wait-parent-deadline-misclassified-no-tuners.md
func TestPoolAcquirePreemptWaitInternalTimeoutTrueNoTuner(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}

	// Successful preempt but never releases the slot.
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		return true
	})

	// Long caller deadline — internal preempt-wait timeout fires first.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = pool.AcquireClient(ctx, "102", "shared:102")
	if !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireClient(internal timeout, active ctx) error = %v, want ErrNoTunersAvailable", err)
	}
}

// TestPoolAcquireFreedSlotAfterFailedPreemptCallback verifies that when a
// preempt callback releases a slot but returns false, the acquire path
// re-checks free capacity and succeeds instead of returning false no-tuner.
// Child: TODO-stream-tuner-acquire-preempt-race-misses-freed-slot.md
func TestPoolAcquireFreedSlotAfterFailedPreemptCallback(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}

	// Callback releases the slot but reports not-idle (returns false).
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		held.Release()
		return false
	})

	lease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient(freed slot after failed preempt) error = %v, want success", err)
	}
	lease.Release()
}

// TestPoolAcquireFreedSlotConcurrentReleaseAfterFailedPreempt verifies that a
// concurrent release during the preempt scan window is picked up by the
// post-preempt free-capacity re-check.
// Child: TODO-stream-tuner-acquire-preempt-race-misses-freed-slot.md
func TestPoolAcquireFreedSlotConcurrentReleaseAfterFailedPreempt(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}

	// Callback takes time (simulating lock contention), during which the
	// slot is released concurrently. Callback returns false.
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		go held.Release()
		time.Sleep(20 * time.Millisecond) // let the release complete
		return false
	})

	lease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient(concurrent release during failed preempt) error = %v, want success", err)
	}
	lease.Release()
}

// TestPoolAcquirePreemptScanContextDeadline verifies that when the caller
// context expires during a slow preempt-scan callback (lock contention
// simulation), the acquire path returns context.DeadlineExceeded.
// Child: TODO-stream-tuner-preempt-scan-context-expiry-misclassified-no-tuners.md
func TestPoolAcquirePreemptScanContextDeadline(t *testing.T) {
	pool := NewPool(1)
	held, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("setup AcquireClient error = %v", err)
	}
	defer held.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// Callback sleeps past the deadline and returns false, simulating
	// lock-delay during preempt scan.
	pool.markClientPreemptible(held.ID, held.token, func() bool {
		time.Sleep(80 * time.Millisecond)
		return false
	})

	_, err = pool.AcquireClient(ctx, "102", "shared:102")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireClient(preempt scan deadline) error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPoolClientPreemptWaitIncludesSettleDelay(t *testing.T) {
	pool := NewPool(1)
	pool.SetPreemptSettleDelay(500 * time.Millisecond)

	idleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() setup error = %v", err)
	}
	defer idleLease.Release()

	pool.markClientPreemptible(idleLease.ID, idleLease.token, func() bool {
		go func() {
			time.Sleep(2100 * time.Millisecond)
			idleLease.Release()
		}()
		return true
	})

	started := time.Now()
	lease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() long preempt wait error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 2500*time.Millisecond {
		t.Fatalf("AcquireClient() completed in %s, want wait to include release + settle delay", elapsed)
	}
	lease.Release()
}
