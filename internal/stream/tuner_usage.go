package stream

import (
	"context"
	"time"
)

// tunerUsage captures tuner operations used by streaming runtime components.
// Pool and VirtualTunerManager both satisfy this interface.
type tunerUsage interface {
	Acquire(ctx context.Context, guideNumber, clientAddr string) (*Lease, error)
	AcquireClient(ctx context.Context, guideNumber, clientAddr string) (*Lease, error)
	AcquireClientForSource(ctx context.Context, sourceID int64, guideNumber, clientAddr string) (*Lease, error)
	AcquireProbe(ctx context.Context, label string, cancel context.CancelCauseFunc) (*Lease, error)
	AcquireProbeForSource(ctx context.Context, sourceID int64, label string, cancel context.CancelCauseFunc) (*Lease, error)
	Snapshot() []Session
	InUseCount() int
	InUseCountForSource(sourceID int64) int
	Capacity() int
	CapacityForSource(sourceID int64) int
	hasPreemptibleLeaseForSource(sourceID int64) bool
	clearClientPreemptible(id int, leaseToken uint64)
	markClientPreemptible(id int, leaseToken uint64, preemptFn func() bool)
	failoverSettleDelayWhenFull() time.Duration
	failoverSettleDelayWhenFullForSource(sourceID int64) time.Duration
}

var _ tunerUsage = (*Pool)(nil)
var _ tunerUsage = (*VirtualTunerManager)(nil)
