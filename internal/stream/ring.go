package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultChunkRingCapacity     = 256
	maxRetainedChunkDataCapacity = 256 * 1024
	maxRetainedReadResultEntries = 1024
	readResultPoolInitialCap     = 8
	startupPrewarmMaxSlots       = 256
	startupPrewarmMaxBytes       = 4 * 1024 * 1024
)

var ErrRingClosed = errors.New("chunk ring is closed")

// RingChunk is one published chunk in a channel session ring.
type RingChunk struct {
	Seq         uint64
	PublishedAt time.Time
	Data        []byte
}

// ChunkPublisher receives chunks emitted by the stream pump.
type ChunkPublisher interface {
	PublishChunk(data []byte, publishedAt time.Time) error
}

// ChunkPublisherCopyAware advertises whether PublishChunk always deep-copies
// input bytes before returning.
type ChunkPublisherCopyAware interface {
	ChunkPublisher
	CopiesPublishedChunkData() bool
}

// ChunkRing stores recently published chunks in an in-memory ring.
type ChunkRing struct {
	mu sync.RWMutex

	maxChunks        int
	maxBytes         int
	startupChunkHint int

	chunks           []RingChunk
	slotRefs         []int32
	start            int
	count            int
	bufferedBytes    int
	nextSeq          uint64
	closed           bool
	closeErr         error
	waitSeq          uint64
	waitCond         *sync.Cond
	readResultPool   sync.Pool
	readResultToken  uint64
	startupPrewarmed bool
}

// NewChunkRing creates a bounded in-memory chunk ring.
func NewChunkRing(maxChunks int) *ChunkRing {
	return NewChunkRingWithLimitsAndStartupHint(maxChunks, 0, 0)
}

// NewChunkRingWithLimits creates a bounded in-memory chunk ring with optional
// chunk-count and byte-budget eviction limits.
func NewChunkRingWithLimits(maxChunks, maxBytes int) *ChunkRing {
	return NewChunkRingWithLimitsAndStartupHint(maxChunks, maxBytes, 0)
}

// NewChunkRingWithLimitsAndStartupHint creates a bounded in-memory chunk ring
// with optional chunk-count/byte-budget limits plus a startup payload hint used
// to prewarm a bounded number of slot buffers.
func NewChunkRingWithLimitsAndStartupHint(maxChunks, maxBytes, startupChunkHint int) *ChunkRing {
	if maxChunks < 1 {
		maxChunks = defaultChunkRingCapacity
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	startupChunkHint = normalizeStartupChunkHint(startupChunkHint)

	ring := &ChunkRing{
		maxChunks:        maxChunks,
		maxBytes:         maxBytes,
		startupChunkHint: startupChunkHint,
		chunks:           make([]RingChunk, maxChunks),
		slotRefs:         make([]int32, maxChunks),
	}
	ring.waitCond = sync.NewCond(&ring.mu)
	ring.readResultPool.New = func() any {
		return &readResultState{
			chunks: make([]RingChunk, 0, readResultPoolInitialCap),
			slots:  make([]int, 0, readResultPoolInitialCap),
		}
	}
	return ring
}

// CopiesPublishedChunkData reports that PublishChunk deep-copies chunk bytes.
func (r *ChunkRing) CopiesPublishedChunkData() bool {
	return true
}

// PublishChunk appends one chunk and evicts the oldest chunk when full.
func (r *ChunkRing) PublishChunk(data []byte, publishedAt time.Time) error {
	if r == nil || len(data) == 0 {
		return nil
	}
	if publishedAt.IsZero() {
		publishedAt = time.Now().UTC()
	} else {
		publishedAt = publishedAt.UTC()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRingClosed
	}
	chunkLen := len(data)
	r.prewarmStartupSlotBuffersLocked(chunkLen)
	for r.count > 0 && r.maxBytes > 0 && r.bufferedBytes+chunkLen > r.maxBytes {
		r.evictOldestLocked()
	}
	if r.count == r.maxChunks {
		r.evictOldestLocked()
	}

	insertAt := (r.start + r.count) % r.maxChunks
	r.chunks[insertAt] = RingChunk{
		Seq:         r.nextSeq,
		PublishedAt: publishedAt,
		Data:        r.copyIntoSlotLocked(insertAt, data),
	}
	r.nextSeq++
	r.count++
	r.bufferedBytes += chunkLen
	r.signalLocked()
	return nil
}

// Snapshot returns a deep copy of currently buffered chunks.
func (r *ChunkRing) Snapshot() []RingChunk {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]RingChunk, r.count)
	for i := 0; i < r.count; i++ {
		chunk := r.chunkAtOffset(i)
		out[i] = RingChunk{
			Seq:         chunk.Seq,
			PublishedAt: chunk.PublishedAt,
			Data:        append([]byte(nil), chunk.Data...),
		}
	}
	return out
}

// Close stops the ring and wakes readers waiting for new chunks.
func (r *ChunkRing) Close(err error) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}

	r.closed = true
	r.closeErr = err
	r.signalLocked()
}

// StartSeqByLagBytes returns the sequence to start reading from for a new subscriber.
// When lagBytes is zero or negative, subscribers start at the live tail.
func (r *ChunkRing) StartSeqByLagBytes(lagBytes int) uint64 {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.count == 0 {
		return r.nextSeq
	}
	if lagBytes <= 0 {
		return r.nextSeq
	}

	total := 0
	startOffset := r.count - 1
	for startOffset > 0 && total < lagBytes {
		total += len(r.chunkAtOffset(startOffset).Data)
		startOffset--
	}
	if total < lagBytes {
		startOffset = 0
	} else if startOffset < r.count-1 {
		startOffset++
	}

	return r.chunkAtOffset(startOffset).Seq
}

// ReadResult is one read pass over ring data starting at a requested sequence.
type ReadResult struct {
	Chunks         []RingChunk
	NextSeq        uint64
	Behind         bool
	Closed         bool
	Err            error
	WaitSeq        uint64
	RequestedSeq   uint64
	OldestSeq      uint64
	RingNextSeq    uint64
	BufferedChunks int
	BufferedBytes  int
	state          *readResultState
	token          uint64
}

type readResultState struct {
	ring     *ChunkRing
	chunks   []RingChunk
	slots    []int
	released uint32
	token    uint64
}

// Release signals that callers are done reading chunk payload slices returned
// by ReadFrom.
func (r *ReadResult) Release() {
	if r == nil || r.state == nil {
		return
	}
	state := r.state
	r.state = nil
	r.Chunks = nil

	if state == nil || r.token == 0 {
		return
	}
	if atomic.LoadUint64(&state.token) != r.token {
		return
	}
	if !atomic.CompareAndSwapUint32(&state.released, 0, 1) {
		return
	}

	ring := state.ring
	if ring == nil {
		return
	}
	for _, idx := range state.slots {
		atomic.AddInt32(&ring.slotRefs[idx], -1)
	}
	ring.recycleReadResultState(state)
}

// ReadFrom returns available chunks for the requested sequence.
// If no chunks are available yet, WaitSeq can be passed to WaitForChange.
func (r *ChunkRing) ReadFrom(seq uint64) ReadResult {
	if r == nil {
		return ReadResult{Closed: true, Err: ErrRingClosed}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	oldest := r.nextSeq
	if r.count > 0 {
		oldest = r.chunkAtOffset(0).Seq
	}

	result := ReadResult{
		NextSeq:        seq,
		WaitSeq:        r.waitSeq,
		RequestedSeq:   seq,
		OldestSeq:      oldest,
		RingNextSeq:    r.nextSeq,
		BufferedChunks: r.count,
		BufferedBytes:  r.bufferedBytes,
	}

	if seq < oldest {
		result.Behind = true
		result.NextSeq = oldest
		return result
	}

	if seq >= r.nextSeq {
		if r.closed {
			result.Closed = true
			result.Err = r.closeErr
		}
		return result
	}

	start := int(seq - oldest)
	if start < 0 {
		start = 0
	}
	if start > r.count {
		start = r.count
	}
	state := r.acquireReadResultState(r.count - start)
	for i := start; i < r.count; i++ {
		idx := (r.start + i) % r.maxChunks
		chunk := r.chunks[idx]
		state.chunks = append(state.chunks, RingChunk{
			Seq:         chunk.Seq,
			PublishedAt: chunk.PublishedAt,
			Data:        chunk.Data,
		})
		state.slots = append(state.slots, idx)
		atomic.AddInt32(&r.slotRefs[idx], 1)
	}

	result.Chunks = state.chunks
	if len(state.chunks) > 0 {
		result.NextSeq = state.chunks[len(state.chunks)-1].Seq + 1
		result.state = state
		result.token = atomic.LoadUint64(&state.token)
		return result
	}
	r.recycleReadResultState(state)
	return result
}

// WaitForChange blocks until ring state changes from waitSeq, ring closes, or ctx ends.
func (r *ChunkRing) WaitForChange(ctx context.Context, waitSeq uint64) error {
	if r == nil {
		return ErrRingClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	if waitSeq != r.waitSeq || r.closed {
		r.mu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}

	// Wake Wait() when ctx is canceled; publish/close also broadcasts.
	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.waitCond.Broadcast()
		r.mu.Unlock()
	})
	defer stop()

	for waitSeq == r.waitSeq && !r.closed {
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		r.waitCond.Wait()
	}
	r.mu.Unlock()
	return nil
}

func (r *ChunkRing) signalLocked() {
	r.waitSeq++
	if r.waitCond != nil {
		r.waitCond.Broadcast()
	}
}

func (r *ChunkRing) chunkAtOffset(offset int) RingChunk {
	idx := (r.start + offset) % r.maxChunks
	return r.chunks[idx]
}

func (r *ChunkRing) evictOldestLocked() {
	if r.count <= 0 {
		return
	}

	evicted := r.chunks[r.start]
	r.bufferedBytes -= len(evicted.Data)
	if r.bufferedBytes < 0 {
		r.bufferedBytes = 0
	}
	retainedData := evicted.Data
	if shouldRetainChunkDataCapacity(cap(retainedData)) {
		retainedData = retainedData[:0]
	} else {
		retainedData = nil
	}
	r.chunks[r.start] = RingChunk{Data: retainedData}
	r.start = (r.start + 1) % r.maxChunks
	r.count--
}

func (r *ChunkRing) copyIntoSlotLocked(slot int, data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	if slot < 0 || slot >= len(r.chunks) {
		copied := make([]byte, len(data))
		copy(copied, data)
		return copied
	}

	slotData := r.chunks[slot].Data
	if atomic.LoadInt32(&r.slotRefs[slot]) == 0 &&
		cap(slotData) >= len(data) &&
		shouldRetainChunkDataCapacity(cap(slotData)) {
		slotData = slotData[:len(data)]
		copy(slotData, data)
		return slotData
	}

	copied := make([]byte, len(data))
	copy(copied, data)
	return copied
}

func shouldRetainChunkDataCapacity(capacity int) bool {
	return capacity > 0 && capacity <= maxRetainedChunkDataCapacity
}

func normalizeStartupChunkHint(chunkBytes int) int {
	if !shouldRetainChunkDataCapacity(chunkBytes) {
		return 0
	}
	return chunkBytes
}

func startupPrewarmSlotCount(maxChunks, slotCapacity int) int {
	if maxChunks <= 0 || slotCapacity <= 0 {
		return 0
	}

	slots := maxChunks
	if slots > startupPrewarmMaxSlots {
		slots = startupPrewarmMaxSlots
	}
	budgetSlots := startupPrewarmMaxBytes / slotCapacity
	if budgetSlots <= 0 {
		return 0
	}
	if budgetSlots < slots {
		slots = budgetSlots
	}
	return slots
}

func (r *ChunkRing) prewarmStartupSlotBuffersLocked(chunkLen int) {
	if r == nil || r.startupPrewarmed {
		return
	}
	r.startupPrewarmed = true

	slotCapacity := chunkLen
	if r.startupChunkHint > slotCapacity {
		slotCapacity = r.startupChunkHint
	}
	if !shouldRetainChunkDataCapacity(slotCapacity) {
		return
	}

	slots := startupPrewarmSlotCount(r.maxChunks, slotCapacity)
	if slots <= 0 {
		return
	}

	slab := make([]byte, slots*slotCapacity)
	for i := 0; i < slots; i++ {
		start := i * slotCapacity
		slot := slab[start : start+slotCapacity : start+slotCapacity]
		r.chunks[i].Data = slot[:0]
	}
}

func (r *ChunkRing) acquireReadResultState(requiredCap int) *readResultState {
	state, _ := r.readResultPool.Get().(*readResultState)
	if state == nil {
		state = &readResultState{}
	}

	state.ring = r
	state.chunks = state.chunks[:0]
	state.slots = state.slots[:0]
	if cap(state.chunks) < requiredCap {
		state.chunks = make([]RingChunk, 0, requiredCap)
	}
	if cap(state.slots) < requiredCap {
		state.slots = make([]int, 0, requiredCap)
	}

	token := atomic.AddUint64(&r.readResultToken, 1)
	atomic.StoreUint64(&state.token, token)
	atomic.StoreUint32(&state.released, 0)
	return state
}

func (r *ChunkRing) recycleReadResultState(state *readResultState) {
	if r == nil || state == nil {
		return
	}

	state.ring = nil
	if shouldRetainReadResultEntriesCapacity(cap(state.chunks)) {
		state.chunks = state.chunks[:0]
	} else {
		state.chunks = nil
	}
	if shouldRetainReadResultEntriesCapacity(cap(state.slots)) {
		state.slots = state.slots[:0]
	} else {
		state.slots = nil
	}
	atomic.StoreUint32(&state.released, 1)
	r.readResultPool.Put(state)
}

func shouldRetainReadResultEntriesCapacity(capacity int) bool {
	return capacity > 0 && capacity <= maxRetainedReadResultEntries
}
