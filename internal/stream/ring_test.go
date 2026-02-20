package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestChunkRingReadFromOrderedAcrossWrapBoundary(t *testing.T) {
	ring := NewChunkRing(3)
	base := time.Unix(1700000000, 0).UTC()

	for i, payload := range []string{"a", "b", "c", "d", "e"} {
		if err := ring.PublishChunk([]byte(payload), base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("PublishChunk(%q) error = %v", payload, err)
		}
	}

	snapshot := ring.Snapshot()
	if len(snapshot) != 3 {
		t.Fatalf("len(snapshot) = %d, want 3", len(snapshot))
	}

	wantSeq := []uint64{2, 3, 4}
	wantData := []string{"c", "d", "e"}
	for i := range snapshot {
		if snapshot[i].Seq != wantSeq[i] {
			t.Fatalf("snapshot[%d].Seq = %d, want %d", i, snapshot[i].Seq, wantSeq[i])
		}
		if got := string(snapshot[i].Data); got != wantData[i] {
			t.Fatalf("snapshot[%d].Data = %q, want %q", i, got, wantData[i])
		}
	}

	read := ring.ReadFrom(2)
	defer read.Release()
	if read.Behind {
		t.Fatal("ReadFrom(2) Behind = true, want false")
	}
	if read.NextSeq != 5 {
		t.Fatalf("ReadFrom(2) NextSeq = %d, want 5", read.NextSeq)
	}
	if len(read.Chunks) != 3 {
		t.Fatalf("ReadFrom(2) len(Chunks) = %d, want 3", len(read.Chunks))
	}
	for i := range read.Chunks {
		if read.Chunks[i].Seq != wantSeq[i] {
			t.Fatalf("ReadFrom(2) chunk[%d].Seq = %d, want %d", i, read.Chunks[i].Seq, wantSeq[i])
		}
		if got := string(read.Chunks[i].Data); got != wantData[i] {
			t.Fatalf("ReadFrom(2) chunk[%d].Data = %q, want %q", i, got, wantData[i])
		}
	}
}

func TestChunkRingReadFromBehindAfterMultipleOverflows(t *testing.T) {
	ring := NewChunkRing(2)
	now := time.Unix(1700000100, 0).UTC()

	for _, payload := range []string{"a", "b", "c", "d"} {
		if err := ring.PublishChunk([]byte(payload), now); err != nil {
			t.Fatalf("PublishChunk(%q) error = %v", payload, err)
		}
	}

	behind := ring.ReadFrom(0)
	defer behind.Release()
	if !behind.Behind {
		t.Fatal("ReadFrom(0) Behind = false, want true")
	}
	if behind.NextSeq != 2 {
		t.Fatalf("ReadFrom(0) NextSeq = %d, want 2", behind.NextSeq)
	}
	if len(behind.Chunks) != 0 {
		t.Fatalf("ReadFrom(0) len(Chunks) = %d, want 0", len(behind.Chunks))
	}

	current := ring.ReadFrom(2)
	defer current.Release()
	if current.Behind {
		t.Fatal("ReadFrom(2) Behind = true, want false")
	}
	if current.NextSeq != 4 {
		t.Fatalf("ReadFrom(2) NextSeq = %d, want 4", current.NextSeq)
	}
	if len(current.Chunks) != 2 {
		t.Fatalf("ReadFrom(2) len(Chunks) = %d, want 2", len(current.Chunks))
	}
	if got := string(current.Chunks[0].Data); got != "c" {
		t.Fatalf("ReadFrom(2) chunk[0].Data = %q, want c", got)
	}
	if got := string(current.Chunks[1].Data); got != "d" {
		t.Fatalf("ReadFrom(2) chunk[1].Data = %q, want d", got)
	}
}

func TestChunkRingStartSeqByLagBytesAfterWrap(t *testing.T) {
	ring := NewChunkRing(4)
	now := time.Unix(1700000200, 0).UTC()

	for _, payload := range []string{"aa", "bbb", "cccc", "ddddd", "eeeeee"} {
		if err := ring.PublishChunk([]byte(payload), now); err != nil {
			t.Fatalf("PublishChunk(%q) error = %v", payload, err)
		}
	}

	if got := ring.StartSeqByLagBytes(0); got != 5 {
		t.Fatalf("StartSeqByLagBytes(0) = %d, want 5 (live tail)", got)
	}
	if got := ring.StartSeqByLagBytes(7); got != 3 {
		t.Fatalf("StartSeqByLagBytes(7) = %d, want 3", got)
	}
	if got := ring.StartSeqByLagBytes(100); got != 1 {
		t.Fatalf("StartSeqByLagBytes(100) = %d, want 1 (oldest buffered)", got)
	}
}

func TestChunkRingEvictsByByteBudgetWithMixedChunkSizes(t *testing.T) {
	ring := NewChunkRingWithLimits(12, 7)
	now := time.Unix(1700000250, 0).UTC()

	publish := func(payload string) {
		t.Helper()
		if err := ring.PublishChunk([]byte(payload), now); err != nil {
			t.Fatalf("PublishChunk(%q) error = %v", payload, err)
		}
	}

	publish("AAAA")
	publish("b")
	publish("c")
	publish("d")
	publish("EEEE")

	snapshot := ring.Snapshot()
	if len(snapshot) != 4 {
		t.Fatalf("len(snapshot) = %d, want 4", len(snapshot))
	}

	wantSeq := []uint64{1, 2, 3, 4}
	wantData := []string{"b", "c", "d", "EEEE"}
	for i := range snapshot {
		if snapshot[i].Seq != wantSeq[i] {
			t.Fatalf("snapshot[%d].Seq = %d, want %d", i, snapshot[i].Seq, wantSeq[i])
		}
		if got := string(snapshot[i].Data); got != wantData[i] {
			t.Fatalf("snapshot[%d].Data = %q, want %q", i, got, wantData[i])
		}
	}

	behind := ring.ReadFrom(0)
	defer behind.Release()
	if !behind.Behind {
		t.Fatal("ReadFrom(0) Behind = false, want true after byte-budget eviction")
	}
	if behind.NextSeq != 1 {
		t.Fatalf("ReadFrom(0) NextSeq = %d, want 1", behind.NextSeq)
	}
}

func TestChunkRingByteBudgetRetainsOversizedChunk(t *testing.T) {
	ring := NewChunkRingWithLimits(8, 3)
	now := time.Unix(1700000260, 0).UTC()

	for _, payload := range []string{"ab", "c", "WXYZ"} {
		if err := ring.PublishChunk([]byte(payload), now); err != nil {
			t.Fatalf("PublishChunk(%q) error = %v", payload, err)
		}
	}

	snapshot := ring.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("len(snapshot) = %d, want 1", len(snapshot))
	}
	if got := string(snapshot[0].Data); got != "WXYZ" {
		t.Fatalf("snapshot[0].Data = %q, want WXYZ", got)
	}
	if snapshot[0].Seq != 2 {
		t.Fatalf("snapshot[0].Seq = %d, want 2", snapshot[0].Seq)
	}
}

func TestChunkRingPublishChunkCopiesCallerBuffer(t *testing.T) {
	ring := NewChunkRing(2)
	now := time.Unix(1700000270, 0).UTC()
	payload := []byte("ABCD")

	if err := ring.PublishChunk(payload, now); err != nil {
		t.Fatalf("PublishChunk() error = %v", err)
	}
	payload[0] = 'Z'

	read := ring.ReadFrom(0)
	defer read.Release()
	if len(read.Chunks) != 1 {
		t.Fatalf("len(read.Chunks) = %d, want 1", len(read.Chunks))
	}
	if got := string(read.Chunks[0].Data); got != "ABCD" {
		t.Fatalf("read chunk data = %q, want ABCD", got)
	}
}

func TestChunkRingPublishChunkPreservesHeldReadChunkOnSlotOverwrite(t *testing.T) {
	ring := NewChunkRing(1)
	now := time.Unix(1700000280, 0).UTC()

	if err := ring.PublishChunk([]byte("first"), now); err != nil {
		t.Fatalf("PublishChunk(first) error = %v", err)
	}

	held := ring.ReadFrom(0)
	defer held.Release()
	if len(held.Chunks) != 1 {
		t.Fatalf("len(held.Chunks) = %d, want 1", len(held.Chunks))
	}

	if err := ring.PublishChunk([]byte("second"), now.Add(time.Second)); err != nil {
		t.Fatalf("PublishChunk(second) error = %v", err)
	}

	if got := string(held.Chunks[0].Data); got != "first" {
		t.Fatalf("held chunk data after overwrite = %q, want first", got)
	}

	current := ring.ReadFrom(1)
	defer current.Release()
	if len(current.Chunks) != 1 {
		t.Fatalf("len(current.Chunks) = %d, want 1", len(current.Chunks))
	}
	if got := string(current.Chunks[0].Data); got != "second" {
		t.Fatalf("current chunk data = %q, want second", got)
	}
}

func TestChunkRingReadResultCopiedAfterReleaseDoesNotClobberNewRefs(t *testing.T) {
	ring := NewChunkRing(1)
	now := time.Unix(1700000285, 0).UTC()

	if err := ring.PublishChunk([]byte("first"), now); err != nil {
		t.Fatalf("PublishChunk(first) error = %v", err)
	}

	first := ring.ReadFrom(0)
	copiedFirst := first
	first.Release()

	if err := ring.PublishChunk([]byte("second"), now.Add(time.Second)); err != nil {
		t.Fatalf("PublishChunk(second) error = %v", err)
	}

	second := ring.ReadFrom(1)
	defer second.Release()
	if len(second.Chunks) != 1 {
		t.Fatalf("len(second.Chunks) = %d, want 1", len(second.Chunks))
	}

	copiedFirst.Release()

	if err := ring.PublishChunk([]byte("third"), now.Add(2*time.Second)); err != nil {
		t.Fatalf("PublishChunk(third) error = %v", err)
	}
	if got := string(second.Chunks[0].Data); got != "second" {
		t.Fatalf("held second chunk data after stale release + overwrite = %q, want second", got)
	}
}

func TestChunkRingPublishChunkReuseShorterPayloadNoTrailingBytes(t *testing.T) {
	ring := NewChunkRing(1)
	now := time.Unix(1700000290, 0).UTC()

	if err := ring.PublishChunk([]byte("LONGPAYLOAD"), now); err != nil {
		t.Fatalf("PublishChunk(LONGPAYLOAD) error = %v", err)
	}
	if err := ring.PublishChunk([]byte("xy"), now.Add(time.Second)); err != nil {
		t.Fatalf("PublishChunk(xy) error = %v", err)
	}

	read := ring.ReadFrom(1)
	defer read.Release()
	if len(read.Chunks) != 1 {
		t.Fatalf("len(read.Chunks) = %d, want 1", len(read.Chunks))
	}
	if got := string(read.Chunks[0].Data); got != "xy" {
		t.Fatalf("read chunk data = %q, want xy", got)
	}
	if got := len(read.Chunks[0].Data); got != 2 {
		t.Fatalf("len(read chunk data) = %d, want 2", got)
	}
}

func TestChunkRingPublishChunkCopiesCallerBufferWithStartupHint(t *testing.T) {
	ring := NewChunkRingWithLimitsAndStartupHint(4, 0, 8)
	now := time.Unix(1700000295, 0).UTC()
	payload := []byte("ABCD")

	if err := ring.PublishChunk(payload, now); err != nil {
		t.Fatalf("PublishChunk() error = %v", err)
	}
	payload[0] = 'Z'

	read := ring.ReadFrom(0)
	defer read.Release()
	if len(read.Chunks) != 1 {
		t.Fatalf("len(read.Chunks) = %d, want 1", len(read.Chunks))
	}
	if got := string(read.Chunks[0].Data); got != "ABCD" {
		t.Fatalf("read chunk data = %q, want ABCD", got)
	}
}

func TestChunkRingPublishChunkStartupHintPreservesHeldReadChunkOnSlotOverwrite(t *testing.T) {
	ring := NewChunkRingWithLimitsAndStartupHint(1, 0, 16)
	now := time.Unix(1700000297, 0).UTC()

	if err := ring.PublishChunk([]byte("first"), now); err != nil {
		t.Fatalf("PublishChunk(first) error = %v", err)
	}

	held := ring.ReadFrom(0)
	defer held.Release()
	if len(held.Chunks) != 1 {
		t.Fatalf("len(held.Chunks) = %d, want 1", len(held.Chunks))
	}

	if err := ring.PublishChunk([]byte("second"), now.Add(time.Second)); err != nil {
		t.Fatalf("PublishChunk(second) error = %v", err)
	}

	if got := string(held.Chunks[0].Data); got != "first" {
		t.Fatalf("held chunk data after overwrite = %q, want first", got)
	}

	current := ring.ReadFrom(1)
	defer current.Release()
	if len(current.Chunks) != 1 {
		t.Fatalf("len(current.Chunks) = %d, want 1", len(current.Chunks))
	}
	if got := string(current.Chunks[0].Data); got != "second" {
		t.Fatalf("current chunk data = %q, want second", got)
	}
}

func TestChunkRingStartupPrewarmSlotCountBoundedBySlotsAndBytes(t *testing.T) {
	const slotCapacity = 64 * 1024
	wantMax := startupPrewarmMaxBytes / slotCapacity
	if wantMax > startupPrewarmMaxSlots {
		wantMax = startupPrewarmMaxSlots
	}
	if wantMax <= 0 {
		t.Fatalf("wantMax = %d, expected > 0", wantMax)
	}

	if got := startupPrewarmSlotCount(0, slotCapacity); got != 0 {
		t.Fatalf("startupPrewarmSlotCount(0, %d) = %d, want 0", slotCapacity, got)
	}
	if got := startupPrewarmSlotCount(1024, slotCapacity); got != wantMax {
		t.Fatalf("startupPrewarmSlotCount(1024, %d) = %d, want %d", slotCapacity, got, wantMax)
	}
	if got := startupPrewarmSlotCount(3, slotCapacity); got != 3 {
		t.Fatalf("startupPrewarmSlotCount(3, %d) = %d, want 3", slotCapacity, got)
	}
}

func TestChunkRingStartupHintPrewarmsBoundedSlotCapacity(t *testing.T) {
	const (
		maxChunks   = 1024
		slotCapHint = 64 * 1024
	)
	ring := NewChunkRingWithLimitsAndStartupHint(maxChunks, 0, slotCapHint)

	payload := bytes.Repeat([]byte{0x47}, slotCapHint)
	if err := ring.PublishChunk(payload, time.Unix(1700000299, 0).UTC()); err != nil {
		t.Fatalf("PublishChunk() error = %v", err)
	}

	wantSlots := startupPrewarmSlotCount(maxChunks, slotCapHint)
	gotSlots := 0
	for i := range ring.chunks {
		if cap(ring.chunks[i].Data) == slotCapHint {
			gotSlots++
		}
	}
	if gotSlots != wantSlots {
		t.Fatalf("prewarmed slot count = %d, want %d", gotSlots, wantSlots)
	}
}

func TestChunkRingCloseWakesWaiters(t *testing.T) {
	ring := NewChunkRing(2)

	waiting := ring.ReadFrom(0)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ring.WaitForChange(context.Background(), waiting.WaitSeq)
	}()

	ring.Close(io.EOF)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WaitForChange() error = %v, want nil", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WaitForChange() did not unblock after Close")
	}

	closed := ring.ReadFrom(0)
	defer closed.Release()
	if !closed.Closed {
		t.Fatal("ReadFrom(0) Closed = false, want true")
	}
	if !errors.Is(closed.Err, io.EOF) {
		t.Fatalf("ReadFrom(0) Err = %v, want io.EOF", closed.Err)
	}
}

func TestChunkRingWaitForChangeWakesAllWaitersOnPublish(t *testing.T) {
	ring := NewChunkRing(4)
	waiting := ring.ReadFrom(0)

	const waiterCount = 4
	entered := make(chan struct{}, waiterCount)
	errCh := make(chan error, waiterCount)
	var wg sync.WaitGroup
	for i := 0; i < waiterCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entered <- struct{}{}
			errCh <- ring.WaitForChange(context.Background(), waiting.WaitSeq)
		}()
	}
	for i := 0; i < waiterCount; i++ {
		select {
		case <-entered:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("waiter %d did not enter WaitForChange", i)
		}
	}

	if err := ring.PublishChunk([]byte("x"), time.Now().UTC()); err != nil {
		t.Fatalf("PublishChunk() error = %v", err)
	}

	for i := 0; i < waiterCount; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("waiter %d WaitForChange() error = %v, want nil", i, err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("waiter %d did not wake after publish", i)
		}
	}
	wg.Wait()
}

func TestChunkRingWaitForChangeReturnsOnContextCancel(t *testing.T) {
	ring := NewChunkRing(2)
	waiting := ring.ReadFrom(0)

	waitCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- ring.WaitForChange(waitCtx, waiting.WaitSeq)
	}()

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitForChange() error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WaitForChange() did not return after context cancellation")
	}
}

func TestChunkRingWaitForChangeNoMissedWakeupWhenPublishedBeforeWait(t *testing.T) {
	ring := NewChunkRing(2)
	waiting := ring.ReadFrom(0)

	if err := ring.PublishChunk([]byte("x"), time.Now().UTC()); err != nil {
		t.Fatalf("PublishChunk() error = %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := ring.WaitForChange(waitCtx, waiting.WaitSeq); err != nil {
		t.Fatalf("WaitForChange() error = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("WaitForChange() returned in %s, want immediate stale-generation return", elapsed)
	}
}

func BenchmarkChunkRingPublishChunkAtCapacity(b *testing.B) {
	payload := bytes.Repeat([]byte{0x47}, 188*7)
	publishedAt := time.Unix(1700000300, 0).UTC()

	for _, maxChunks := range []int{32, 128, 512} {
		b.Run(fmt.Sprintf("max_chunks_%d", maxChunks), func(b *testing.B) {
			ring := NewChunkRing(maxChunks)
			for i := 0; i < maxChunks; i++ {
				if err := ring.PublishChunk(payload, publishedAt); err != nil {
					b.Fatalf("warmup PublishChunk() error = %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ring.PublishChunk(payload, publishedAt); err != nil {
					b.Fatalf("PublishChunk() error = %v", err)
				}
			}
		})
	}
}

func BenchmarkChunkRingPublishChunkAtCapacityPayload64K(b *testing.B) {
	payload := bytes.Repeat([]byte{0x47}, 64*1024)
	publishedAt := time.Unix(1700000310, 0).UTC()

	for _, maxChunks := range []int{32, 128, 512} {
		b.Run(fmt.Sprintf("max_chunks_%d", maxChunks), func(b *testing.B) {
			ring := NewChunkRing(maxChunks)
			for i := 0; i < maxChunks; i++ {
				if err := ring.PublishChunk(payload, publishedAt); err != nil {
					b.Fatalf("warmup PublishChunk() error = %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ring.PublishChunk(payload, publishedAt); err != nil {
					b.Fatalf("PublishChunk() error = %v", err)
				}
			}
		})
	}
}

func BenchmarkChunkRingPublishChunkStartupFillPayload64K(b *testing.B) {
	payload := bytes.Repeat([]byte{0x47}, 64*1024)
	publishedAt := time.Unix(1700000315, 0).UTC()

	benchmarks := []struct {
		name             string
		maxChunks        int
		startupPublishes int
	}{
		{
			name:             "max_chunks_32_fill_32",
			maxChunks:        32,
			startupPublishes: 32,
		},
		{
			name:             "max_chunks_128_fill_96",
			maxChunks:        128,
			startupPublishes: 96,
		},
		{
			name:             "max_chunks_512_fill_96",
			maxChunks:        512,
			startupPublishes: 96,
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ring := NewChunkRingWithLimitsAndStartupHint(
					benchmark.maxChunks,
					0,
					len(payload),
				)
				for j := 0; j < benchmark.startupPublishes; j++ {
					if err := ring.PublishChunk(payload, publishedAt); err != nil {
						b.Fatalf("PublishChunk() error = %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkChunkRingReadFromOneChunk(b *testing.B) {
	ring := NewChunkRing(8)
	publishedAt := time.Unix(1700000320, 0).UTC()
	if err := ring.PublishChunk(bytes.Repeat([]byte{0x47}, 188*7), publishedAt); err != nil {
		b.Fatalf("PublishChunk() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read := ring.ReadFrom(0)
		if len(read.Chunks) != 1 {
			b.Fatalf("len(read.Chunks) = %d, want 1", len(read.Chunks))
		}
		read.Release()
	}
}

func BenchmarkChunkRingReadFromBurst(b *testing.B) {
	ring := NewChunkRing(64)
	publishedAt := time.Unix(1700000330, 0).UTC()
	payload := bytes.Repeat([]byte{0x47}, 188*7)
	for i := 0; i < 64; i++ {
		if err := ring.PublishChunk(payload, publishedAt); err != nil {
			b.Fatalf("warmup PublishChunk() error = %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read := ring.ReadFrom(0)
		if len(read.Chunks) != 64 {
			b.Fatalf("len(read.Chunks) = %d, want 64", len(read.Chunks))
		}
		read.Release()
	}
}
