package stream

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"
)

var ErrNoTunersAvailable = errors.New("no tuners available")
var ErrProbePreempted = errors.New("probe preempted by client stream")

const (
	sessionKindClient = "client"
	sessionKindProbe  = "probe"

	clientPreemptWait = 2 * time.Second
)

// Session describes an active tuner lease.
type Session struct {
	ID          int
	GuideNumber string
	ClientAddr  string
	Kind        string
	StartedAt   time.Time
	// PlaylistSourceID/Name identify which virtual source pool owns this lease.
	PlaylistSourceID   int64
	PlaylistSourceName string
	// VirtualTunerSlot is the slot index inside the source-local pool.
	VirtualTunerSlot int
	leaseToken       uint64
}

// VirtualTunerPoolSnapshot reports one source-level virtual tuner pool.
type VirtualTunerPoolSnapshot struct {
	PlaylistSourceID    int64
	PlaylistSourceName  string
	PlaylistSourceOrder int
	TunerCount          int
	InUseCount          int
	IdleCount           int
}

// Pool tracks active tuner sessions and enforces a max concurrent stream count.
type Pool struct {
	free chan int

	mu              sync.Mutex
	sessions        map[int]Session
	cancels         map[int]context.CancelCauseFunc
	clientPreempts  map[int]func() bool
	nextLeaseToken  uint64
	preemptSettle   time.Duration
	preemptPending  map[int]struct{}
	settleNotBefore map[int]time.Time
	fullSettleUntil time.Time
}

// Lease represents an acquired tuner slot.
type Lease struct {
	ID int
	// PlaylistSourceID/Name identify which virtual source pool owns this lease.
	PlaylistSourceID   int64
	PlaylistSourceName string
	// VirtualTunerSlot is the slot index inside the source-local pool.
	VirtualTunerSlot int

	once      sync.Once
	token     uint64
	releaseFn func()
}

func NewPool(count int) *Pool {
	if count < 1 {
		count = 1
	}

	p := &Pool{
		free:            make(chan int, count),
		sessions:        make(map[int]Session, count),
		cancels:         make(map[int]context.CancelCauseFunc, count),
		clientPreempts:  make(map[int]func() bool, count),
		preemptSettle:   0,
		preemptPending:  make(map[int]struct{}, count),
		settleNotBefore: make(map[int]time.Time, count),
	}
	for i := 0; i < count; i++ {
		p.free <- i
	}
	return p
}

// SetPreemptSettleDelay configures a delay after preemption before reusing the
// reclaimed tuner slot for a new lease.
func (p *Pool) SetPreemptSettleDelay(delay time.Duration) {
	if p == nil {
		return
	}
	if delay < 0 {
		delay = 0
	}
	p.mu.Lock()
	p.preemptSettle = delay
	p.mu.Unlock()
}

func (p *Pool) Acquire(ctx context.Context, guideNumber, clientAddr string) (*Lease, error) {
	return p.AcquireClient(ctx, guideNumber, clientAddr)
}

func (p *Pool) AcquireClient(ctx context.Context, guideNumber, clientAddr string) (*Lease, error) {
	return p.acquire(ctx, guideNumber, clientAddr, sessionKindClient, nil, true)
}

// AcquireClientForSource keeps API parity with VirtualTunerManager. The base
// single-pool implementation ignores source selection and acquires from the
// shared pool.
func (p *Pool) AcquireClientForSource(
	ctx context.Context,
	_ int64,
	guideNumber,
	clientAddr string,
) (*Lease, error) {
	return p.AcquireClient(ctx, guideNumber, clientAddr)
}

func (p *Pool) AcquireProbe(ctx context.Context, label string, cancel context.CancelCauseFunc) (*Lease, error) {
	return p.acquire(ctx, "probe:"+label, "auto-prioritize", sessionKindProbe, cancel, false)
}

// AcquireProbeForSource keeps API parity with VirtualTunerManager. The base
// single-pool implementation ignores source selection and acquires from the
// shared pool.
func (p *Pool) AcquireProbeForSource(
	ctx context.Context,
	_ int64,
	label string,
	cancel context.CancelCauseFunc,
) (*Lease, error) {
	return p.AcquireProbe(ctx, label, cancel)
}

func (p *Pool) acquire(
	ctx context.Context,
	guideNumber,
	clientAddr,
	kind string,
	cancel context.CancelCauseFunc,
	allowClientPreempt bool,
) (*Lease, error) {
	if p == nil {
		return nil, errors.New("tuner pool is not configured")
	}

	attemptStartedAt := time.Now().UTC()
	slog.Info(
		"tuner acquire attempt started",
		"kind", kind,
		"guide_number", guideNumber,
		"client_addr", clientAddr,
		"attempt_started_at", attemptStartedAt,
	)

	select {
	case <-ctx.Done():
		slog.Info(
			"tuner acquire attempt canceled before start",
			"kind", kind,
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"attempt_started_at", attemptStartedAt,
			"error", ctx.Err(),
		)
		return nil, ctx.Err()
	default:
	}

	id, ok := p.tryTakeFree()
	preempted := false
	if !ok && allowClientPreempt {
		preempted = p.preemptOneProbe()
		if !preempted {
			preempted = p.preemptOneIdleClient()
		}
		if preempted {
			waitCtx, waitCancel := context.WithTimeout(ctx, p.clientPreemptWaitTimeout())
			defer waitCancel()
			select {
			case <-waitCtx.Done():
				// Distinguish caller context expiry from internal preempt-wait
				// timeout: if the caller context is already done, surface its
				// error so cancellation/deadline is not misclassified as
				// capacity exhaustion.
				acquireErr := ErrNoTunersAvailable
				if ctxErr := ctx.Err(); ctxErr != nil {
					acquireErr = ctxErr
				}
				slog.Info(
					"tuner acquire preempt wait failed",
					"kind", kind,
					"guide_number", guideNumber,
					"client_addr", clientAddr,
					"preempted", preempted,
					"attempt_started_at", attemptStartedAt,
					"error", acquireErr,
					"wait_ctx_error", waitCtx.Err(),
					"elapsed", time.Since(attemptStartedAt).String(),
				)
				return nil, acquireErr
			case id = <-p.free:
				ok = true
			}
		}
		// Re-check free capacity after failed preempt attempts: a callback
		// may have released a slot (returning false) or a concurrent release
		// may have occurred during the preempt scan window.
		if !ok {
			id, ok = p.tryTakeFree()
		}
	}
	if !ok {
		// Preserve caller cancellation/deadline when context expired during
		// the acquire attempt instead of misclassifying as capacity exhaustion.
		if ctxErr := ctx.Err(); ctxErr != nil {
			slog.Info(
				"tuner acquire failed: caller context expired",
				"kind", kind,
				"guide_number", guideNumber,
				"client_addr", clientAddr,
				"attempt_started_at", attemptStartedAt,
				"error", ctxErr,
				"elapsed", time.Since(attemptStartedAt).String(),
			)
			return nil, ctxErr
		}
		slog.Info(
			"tuner acquire failed: no slots available",
			"kind", kind,
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"attempt_started_at", attemptStartedAt,
			"elapsed", time.Since(attemptStartedAt).String(),
		)
		return nil, ErrNoTunersAvailable
	}
	slotWait, err := p.waitForSlotSettle(ctx, id)
	if err != nil {
		p.free <- id
		slog.Info(
			"tuner acquire failed during slot settle wait",
			"kind", kind,
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"slot_id", id,
			"slot_settle_wait", slotWait.String(),
			"attempt_started_at", attemptStartedAt,
			"error", err,
			"elapsed", time.Since(attemptStartedAt).String(),
		)
		return nil, err
	}
	capacityWait, err := p.waitForCapacityRefillSettle(ctx)
	if err != nil {
		p.free <- id
		slog.Info(
			"tuner acquire failed during capacity refill settle wait",
			"kind", kind,
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"slot_id", id,
			"slot_settle_wait", slotWait.String(),
			"capacity_settle_wait", capacityWait.String(),
			"attempt_started_at", attemptStartedAt,
			"error", err,
			"elapsed", time.Since(attemptStartedAt).String(),
		)
		return nil, err
	}
	if slotWait > 0 || capacityWait > 0 {
		slog.Info(
			"tuner acquire settle waits applied",
			"kind", kind,
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"slot_id", id,
			"slot_settle_wait", slotWait.String(),
			"capacity_settle_wait", capacityWait.String(),
			"preempted", preempted,
			"attempt_started_at", attemptStartedAt,
			"elapsed", time.Since(attemptStartedAt).String(),
		)
	}
	select {
	case <-ctx.Done():
		p.free <- id
		slog.Info(
			"tuner acquire canceled after waits",
			"kind", kind,
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"slot_id", id,
			"slot_settle_wait", slotWait.String(),
			"capacity_settle_wait", capacityWait.String(),
			"attempt_started_at", attemptStartedAt,
			"error", ctx.Err(),
			"elapsed", time.Since(attemptStartedAt).String(),
		)
		return nil, ctx.Err()
	default:
	}

	p.mu.Lock()
	p.nextLeaseToken++
	if p.nextLeaseToken == 0 {
		p.nextLeaseToken++
	}
	leaseToken := p.nextLeaseToken
	p.sessions[id] = Session{
		ID:                 id,
		GuideNumber:        guideNumber,
		ClientAddr:         clientAddr,
		Kind:               kind,
		StartedAt:          time.Now().UTC(),
		PlaylistSourceID:   defaultPlaylistSourceID,
		PlaylistSourceName: defaultPlaylistSourceName,
		// Base single-pool mode uses one implicit source pool.
		VirtualTunerSlot: id,
		leaseToken:       leaseToken,
	}
	if kind == sessionKindProbe && cancel != nil {
		p.cancels[id] = cancel
	}
	p.mu.Unlock()

	slog.Info(
		"tuner acquire attempt succeeded",
		"kind", kind,
		"guide_number", guideNumber,
		"client_addr", clientAddr,
		"slot_id", id,
		"preempted", preempted,
		"slot_settle_wait", slotWait.String(),
		"capacity_settle_wait", capacityWait.String(),
		"attempt_started_at", attemptStartedAt,
		"acquired_at", time.Now().UTC(),
		"elapsed", time.Since(attemptStartedAt).String(),
	)

	return &Lease{
		ID:                 id,
		PlaylistSourceID:   defaultPlaylistSourceID,
		PlaylistSourceName: defaultPlaylistSourceName,
		VirtualTunerSlot:   id,
		token:              leaseToken,
		releaseFn: func() {
			releasedAt := time.Now().UTC()
			p.mu.Lock()
			wasFull := len(p.sessions) == cap(p.free)
			previousSession, hadSession := p.sessions[id]
			delete(p.sessions, id)
			delete(p.cancels, id)
			delete(p.clientPreempts, id)
			p.applyPreemptSettleOnReleaseLocked(id)
			p.applyCapacityRefillSettleOnReleaseLocked(wasFull)
			p.mu.Unlock()

			if hadSession {
				elapsed := releasedAt.Sub(previousSession.StartedAt)
				if elapsed < 0 {
					elapsed = 0
				}
				slog.Info(
					"tuner lease released",
					"tuner_id", id,
					"kind", previousSession.Kind,
					"guide_number", previousSession.GuideNumber,
					"client_addr", previousSession.ClientAddr,
					"session_started_at", previousSession.StartedAt,
					"released_at", releasedAt,
					"duration", elapsed.String(),
				)
			}
			p.free <- id
		},
	}, nil
}

func (p *Pool) clientPreemptWaitTimeout() time.Duration {
	if p == nil {
		return clientPreemptWait
	}
	p.mu.Lock()
	delay := p.preemptSettle
	p.mu.Unlock()
	if delay <= 0 {
		return clientPreemptWait
	}
	return clientPreemptWait + delay
}

func (p *Pool) waitForSlotSettle(ctx context.Context, id int) (time.Duration, error) {
	totalWait := time.Duration(0)
	for {
		wait := p.slotSettleRemaining(id)
		if wait <= 0 {
			return totalWait, nil
		}
		timer := time.NewTimer(wait)
		started := time.Now()
		select {
		case <-ctx.Done():
			timer.Stop()
			return totalWait + time.Since(started), ctx.Err()
		case <-timer.C:
			totalWait += time.Since(started)
		}
	}
}

func (p *Pool) waitForCapacityRefillSettle(ctx context.Context) (time.Duration, error) {
	totalWait := time.Duration(0)
	for {
		wait := p.capacityRefillSettleRemaining()
		if wait <= 0 {
			return totalWait, nil
		}
		timer := time.NewTimer(wait)
		started := time.Now()
		select {
		case <-ctx.Done():
			timer.Stop()
			return totalWait + time.Since(started), ctx.Err()
		case <-timer.C:
			totalWait += time.Since(started)
		}
	}
}

func (p *Pool) slotSettleRemaining(id int) time.Duration {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	readyAt, ok := p.settleNotBefore[id]
	if !ok {
		return 0
	}

	remaining := time.Until(readyAt)
	if remaining <= 0 {
		delete(p.settleNotBefore, id)
		return 0
	}
	return remaining
}

func (p *Pool) capacityRefillSettleRemaining() time.Duration {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	capacity := cap(p.free)
	if capacity <= 0 {
		return 0
	}

	// This acquire has already reserved one free tuner id; only delay when
	// assigning it would refill back to full capacity.
	if len(p.sessions)+1 != capacity {
		return 0
	}

	if p.fullSettleUntil.IsZero() {
		return 0
	}

	remaining := time.Until(p.fullSettleUntil)
	if remaining <= 0 {
		p.fullSettleUntil = time.Time{}
		return 0
	}
	return remaining
}

func (p *Pool) applyPreemptSettleOnReleaseLocked(id int) {
	if p == nil {
		return
	}

	if _, pending := p.preemptPending[id]; !pending {
		return
	}
	delete(p.preemptPending, id)

	if p.preemptSettle <= 0 {
		return
	}

	readyAt := time.Now().Add(p.preemptSettle)
	current, ok := p.settleNotBefore[id]
	if !ok || readyAt.After(current) {
		p.settleNotBefore[id] = readyAt
	}
}

func (p *Pool) applyCapacityRefillSettleOnReleaseLocked(wasFull bool) {
	if p == nil || !wasFull || p.preemptSettle <= 0 {
		return
	}

	// Only arm the refill settle window when the release actually moved the
	// pool from full usage to one-below-full usage.
	if len(p.sessions) != cap(p.free)-1 {
		return
	}

	readyAt := time.Now().Add(p.preemptSettle)
	if p.fullSettleUntil.IsZero() || readyAt.After(p.fullSettleUntil) {
		p.fullSettleUntil = readyAt
	}
}

func (p *Pool) clearPreemptPending(id int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.preemptPending, id)
	p.mu.Unlock()
}

func (p *Pool) Snapshot() []Session {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]Session, 0, len(p.sessions))
	for _, sess := range p.sessions {
		out = append(out, sess)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func (p *Pool) InUseCount() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sessions)
}

// InUseCountForSource keeps API parity with VirtualTunerManager. The base
// single-pool implementation reports global in-use count for any source.
func (p *Pool) InUseCountForSource(_ int64) int {
	return p.InUseCount()
}

func (p *Pool) Capacity() int {
	if p == nil {
		return 0
	}
	return cap(p.free)
}

// CapacityForSource keeps API parity with VirtualTunerManager. The base
// single-pool implementation reports global capacity for any source.
func (p *Pool) CapacityForSource(_ int64) int {
	return p.Capacity()
}

// hasPreemptibleLeaseForSource keeps API parity with VirtualTunerManager.
// The base single-pool implementation is source-agnostic.
func (p *Pool) hasPreemptibleLeaseForSource(_ int64) bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for id, sess := range p.sessions {
		switch sess.Kind {
		case sessionKindProbe:
			if cancel, ok := p.cancels[id]; ok && cancel != nil {
				return true
			}
		case sessionKindClient:
			if preemptFn, ok := p.clientPreempts[id]; ok && preemptFn != nil {
				return true
			}
		}
	}
	return false
}

// VirtualTunerSnapshot reports a single implicit pool for legacy one-pool mode.
func (p *Pool) VirtualTunerSnapshot() []VirtualTunerPoolSnapshot {
	if p == nil {
		return nil
	}
	tunerCount := p.Capacity()
	inUseCount := p.InUseCount()
	idleCount := tunerCount - inUseCount
	if idleCount < 0 {
		idleCount = 0
	}

	return []VirtualTunerPoolSnapshot{
		{
			PlaylistSourceID:    defaultPlaylistSourceID,
			PlaylistSourceName:  defaultPlaylistSourceName,
			PlaylistSourceOrder: 0,
			TunerCount:          tunerCount,
			InUseCount:          inUseCount,
			IdleCount:           idleCount,
		},
	}
}

func (p *Pool) failoverSettleDelayWhenFull() time.Duration {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	capacity := cap(p.free)
	if capacity <= 1 || p.preemptSettle <= 0 {
		return 0
	}

	// Under full pressure, keep spacing source retries by the configured settle delay.
	if len(p.sessions) >= capacity {
		return p.preemptSettle
	}

	// If the pool just transitioned from full -> partial, preserve a short settle
	// window so immediate retry loops do not race through channels without delay.
	if len(p.sessions) != capacity-1 || p.fullSettleUntil.IsZero() {
		return 0
	}

	remaining := time.Until(p.fullSettleUntil)
	if remaining <= 0 {
		p.fullSettleUntil = time.Time{}
		return 0
	}
	return remaining
}

// failoverSettleDelayWhenFullForSource keeps API parity with
// VirtualTunerManager. The base single-pool implementation is source-agnostic.
func (p *Pool) failoverSettleDelayWhenFullForSource(_ int64) time.Duration {
	return p.failoverSettleDelayWhenFull()
}

func (p *Pool) tryTakeFree() (int, bool) {
	select {
	case id := <-p.free:
		return id, true
	default:
		return 0, false
	}
}

func (p *Pool) preemptOneProbe() bool {
	if p == nil {
		return false
	}

	var (
		selectedID     int
		selectedCancel context.CancelCauseFunc
		selectedSess   Session
		selectedAt     time.Time
		found          bool
	)

	p.mu.Lock()
	for id, sess := range p.sessions {
		if sess.Kind != sessionKindProbe {
			continue
		}
		cancel, ok := p.cancels[id]
		if !ok || cancel == nil {
			continue
		}
		if !found || sess.StartedAt.Before(selectedAt) {
			selectedID = id
			selectedCancel = cancel
			selectedSess = sess
			selectedAt = sess.StartedAt
			found = true
		}
	}
	if found {
		delete(p.cancels, selectedID)
		p.preemptPending[selectedID] = struct{}{}
	}
	p.mu.Unlock()

	if !found || selectedCancel == nil {
		return false
	}
	selectedCancel(ErrProbePreempted)
	slog.Info(
		"tuner probe preempted",
		"tuner_id", selectedID,
		"kind", selectedSess.Kind,
		"guide_number", selectedSess.GuideNumber,
		"client_addr", selectedSess.ClientAddr,
		"session_started_at", selectedSess.StartedAt,
		"result", "preempted",
	)
	return true
}

func (p *Pool) markClientPreemptible(id int, leaseToken uint64, preemptFn func() bool) {
	if p == nil || leaseToken == 0 || preemptFn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	sess, ok := p.sessions[id]
	if !ok || sess.Kind != sessionKindClient || sess.leaseToken != leaseToken {
		return
	}
	p.clientPreempts[id] = preemptFn
}

func (p *Pool) clearClientPreemptible(id int, leaseToken uint64) {
	if p == nil || leaseToken == 0 {
		return
	}

	p.mu.Lock()
	sess, ok := p.sessions[id]
	if !ok || sess.Kind != sessionKindClient || sess.leaseToken != leaseToken {
		p.mu.Unlock()
		return
	}
	delete(p.clientPreempts, id)
	p.mu.Unlock()
}

func (p *Pool) preemptOneIdleClient() bool {
	if p == nil {
		return false
	}

	for {
		var (
			selectedID       int
			selectedPreempt  func() bool
			selectedSession  Session
			selectedStarted  time.Time
			foundPreemptible bool
		)

		p.mu.Lock()
		for id, sess := range p.sessions {
			if sess.Kind != sessionKindClient {
				continue
			}

			preemptFn, ok := p.clientPreempts[id]
			if !ok || preemptFn == nil {
				continue
			}

			if !foundPreemptible || sess.StartedAt.Before(selectedStarted) {
				selectedID = id
				selectedPreempt = preemptFn
				selectedSession = sess
				selectedStarted = sess.StartedAt
				foundPreemptible = true
			}
		}
		if foundPreemptible {
			delete(p.clientPreempts, selectedID)
			p.preemptPending[selectedID] = struct{}{}
		}
		p.mu.Unlock()

		if !foundPreemptible || selectedPreempt == nil {
			return false
		}

		if selectedPreempt() {
			slog.Info(
				"tuner idle-client preempted",
				"tuner_id", selectedID,
				"kind", selectedSession.Kind,
				"guide_number", selectedSession.GuideNumber,
				"client_addr", selectedSession.ClientAddr,
				"session_started_at", selectedSession.StartedAt,
				"result", "preempted",
			)
			return true
		}
		slog.Info(
			"tuner idle-client preempted",
			"tuner_id", selectedID,
			"kind", selectedSession.Kind,
			"guide_number", selectedSession.GuideNumber,
			"client_addr", selectedSession.ClientAddr,
			"session_started_at", selectedSession.StartedAt,
			"result", "not_idle",
		)
		p.clearPreemptPending(selectedID)
	}
}

func (l *Lease) Release() {
	if l == nil || l.releaseFn == nil {
		return
	}
	l.once.Do(l.releaseFn)
}
