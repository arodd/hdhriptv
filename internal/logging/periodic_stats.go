package logging

import (
	"sync"
	"time"
)

// PeriodicStatsSnapshot captures a flushed periodic stats window.
type PeriodicStatsSnapshot[T any] struct {
	Stats          T
	WindowStart    time.Time
	WindowEnd      time.Time
	WindowDuration time.Duration
}

// PeriodicStatsWindow tracks stats samples and emits a snapshot at a bounded
// cadence.
//
// When emitFirst is false, the first sample starts the cadence window and the
// first flush occurs once interval has elapsed.
//
// When emitFirst is true, the first sample flushes immediately, then subsequent
// samples follow the interval cadence.
type PeriodicStatsWindow[T any] struct {
	mu         sync.Mutex
	mergeFn    func(*T, T)
	emitFirst  bool
	lastEmit   time.Time
	pending    T
	pendingAt  time.Time
	hasPending bool
}

// NewPeriodicStatsWindow builds a periodic tracker.
func NewPeriodicStatsWindow[T any](emitFirst bool, mergeFn func(*T, T)) *PeriodicStatsWindow[T] {
	if mergeFn == nil {
		mergeFn = func(dst *T, sample T) {
			if dst == nil {
				return
			}
			*dst = sample
		}
	}
	return &PeriodicStatsWindow[T]{
		mergeFn:   mergeFn,
		emitFirst: emitFirst,
	}
}

// Record adds a sample into the pending window and returns a snapshot when the
// cadence interval is reached.
func (w *PeriodicStatsWindow[T]) Record(now time.Time, interval time.Duration, sample T) (PeriodicStatsSnapshot[T], bool) {
	if w == nil {
		return PeriodicStatsSnapshot[T]{}, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if interval < 0 {
		interval = 0
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.hasPending {
		w.pendingAt = now
		w.hasPending = true
	} else if now.Before(w.pendingAt) {
		w.pendingAt = now
	}
	w.mergeFn(&w.pending, sample)

	shouldFlush := false
	if w.lastEmit.IsZero() {
		if w.emitFirst {
			shouldFlush = true
		} else {
			w.lastEmit = now
		}
	} else {
		elapsed := now.Sub(w.lastEmit)
		switch {
		case elapsed < 0:
			w.lastEmit = now
		case elapsed >= interval:
			shouldFlush = true
		}
	}

	if !shouldFlush {
		return PeriodicStatsSnapshot[T]{}, false
	}

	start := w.pendingAt
	stats := w.pending
	var zero T
	w.pending = zero
	w.pendingAt = time.Time{}
	w.hasPending = false
	w.lastEmit = now

	return PeriodicStatsSnapshot[T]{
		Stats:          stats,
		WindowStart:    start,
		WindowEnd:      now,
		WindowDuration: now.Sub(start),
	}, true
}

// Reset clears pending samples and cadence state.
func (w *PeriodicStatsWindow[T]) Reset() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	var zero T
	w.pending = zero
	w.pendingAt = time.Time{}
	w.hasPending = false
	w.lastEmit = time.Time{}
}
