package stream

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

type aliasingPublisher struct {
	chunks [][]byte
}

func (p *aliasingPublisher) PublishChunk(data []byte, _ time.Time) error {
	p.chunks = append(p.chunks, data)
	return nil
}

type discardCopyAwarePublisher struct{}

func (p *discardCopyAwarePublisher) PublishChunk(_ []byte, _ time.Time) error {
	return nil
}

func (p *discardCopyAwarePublisher) CopiesPublishedChunkData() bool {
	return true
}

func TestPumpPublishesByChunkSize(t *testing.T) {
	ring := NewChunkRing(16)
	pump := NewPump(PumpConfig{
		ChunkBytes:           4,
		PublishFlushInterval: 5 * time.Second,
	}, ring)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pump.Run(ctx, io.NopCloser(strings.NewReader("abcdefghij")))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	chunks := ring.Snapshot()
	if len(chunks) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(chunks))
	}
	if string(chunks[0].Data) != "abcd" {
		t.Fatalf("chunk[0] = %q, want abcd", string(chunks[0].Data))
	}
	if string(chunks[1].Data) != "efgh" {
		t.Fatalf("chunk[1] = %q, want efgh", string(chunks[1].Data))
	}
	if string(chunks[2].Data) != "ij" {
		t.Fatalf("chunk[2] = %q, want ij", string(chunks[2].Data))
	}

	stats := pump.Stats()
	if stats.BytesRead != 10 {
		t.Fatalf("BytesRead = %d, want 10", stats.BytesRead)
	}
	if stats.BytesPublished != 10 {
		t.Fatalf("BytesPublished = %d, want 10", stats.BytesPublished)
	}
	if stats.ChunksPublished != 3 {
		t.Fatalf("ChunksPublished = %d, want 3", stats.ChunksPublished)
	}
	if stats.LastByteReadAt.IsZero() {
		t.Fatal("LastByteReadAt is zero")
	}
	if stats.LastPublishAt.IsZero() {
		t.Fatal("LastPublishAt is zero")
	}
}

func TestPumpFlushesPartialChunkOnTimer(t *testing.T) {
	ring := NewChunkRing(16)
	pump := NewPump(PumpConfig{
		ChunkBytes:           64,
		PublishFlushInterval: 10 * time.Millisecond,
	}, ring)

	reader, writer := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = writer.Write([]byte("abc"))
		time.Sleep(60 * time.Millisecond)
		_, _ = writer.Write([]byte("def"))
		_ = writer.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pump.Run(ctx, reader)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	<-done

	chunks := ring.Snapshot()
	if len(chunks) < 2 {
		t.Fatalf("len(chunks) = %d, want at least 2 (timer flush + final flush)", len(chunks))
	}
	if string(chunks[0].Data) != "abc" {
		t.Fatalf("chunk[0] = %q, want abc", string(chunks[0].Data))
	}
	if string(chunks[1].Data) != "def" {
		t.Fatalf("chunk[1] = %q, want def", string(chunks[1].Data))
	}
}

func TestPumpTSAlignmentCarriesRemainderWithoutDataLoss(t *testing.T) {
	ring := NewChunkRing(32)
	pump := NewPump(PumpConfig{
		ChunkBytes:           200,
		PublishFlushInterval: 5 * time.Second,
		TSAlign188:           true,
	}, ring)

	input := make([]byte, mpegTSPacketSize*3+17)
	for i := range input {
		input[i] = byte(i % 251)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := pump.Run(ctx, io.NopCloser(bytes.NewReader(input))); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	chunks := ring.Snapshot()
	if len(chunks) < 2 {
		t.Fatalf("len(chunks) = %d, want at least 2", len(chunks))
	}

	var merged []byte
	for i, chunk := range chunks {
		if i < len(chunks)-1 && len(chunk.Data)%mpegTSPacketSize != 0 {
			t.Fatalf("chunk[%d] length = %d, want multiple of %d", i, len(chunk.Data), mpegTSPacketSize)
		}
		merged = append(merged, chunk.Data...)
	}
	if !bytes.Equal(merged, input) {
		t.Fatalf("merged output length = %d, want %d with identical bytes", len(merged), len(input))
	}
}

func TestPumpCopiesWhenPublisherIsNotCopyAware(t *testing.T) {
	publisher := &aliasingPublisher{}
	pump := NewPump(PumpConfig{
		ChunkBytes:           4,
		PublishFlushInterval: 5 * time.Second,
	}, publisher)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := pump.Run(ctx, io.NopCloser(strings.NewReader("abcdefghij"))); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(publisher.chunks) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(publisher.chunks))
	}
	if got := string(publisher.chunks[0]); got != "abcd" {
		t.Fatalf("chunk[0] = %q, want abcd", got)
	}
	if got := string(publisher.chunks[1]); got != "efgh" {
		t.Fatalf("chunk[1] = %q, want efgh", got)
	}
	if got := string(publisher.chunks[2]); got != "ij" {
		t.Fatalf("chunk[2] = %q, want ij", got)
	}
}

// TestPumpRunCancelBlocksOnSourceClose verifies that Pump.Run() returns
// promptly when ctx is canceled even if src.Close() blocks. The deferred
// close at pump.go:111 uses bounded close so a stalled Close() does not
// block pump completion and upstream lifecycle waiters.
//
// Regression: TODO-stream-pump-cancel-blocking-source-close-lifecycle-stall.md
func TestPumpRunCancelBlocksOnSourceClose(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh) // unblock close on test exit

	src := &blockingCloseReadCloser{
		blockCh: blockCh,
	}

	ring := NewChunkRing(16)
	pump := NewPump(PumpConfig{
		ChunkBytes:           4,
		PublishFlushInterval: 5 * time.Second,
	}, ring)

	ctx, cancel := context.WithCancel(context.Background())

	pumpDone := make(chan error, 1)
	go func() {
		pumpDone <- pump.Run(ctx, src)
	}()

	// Let the pump start its read loop, then cancel context.
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Pump.Run() should return promptly even though Close() is still blocked,
	// because bounded close runs in background.
	select {
	case err := <-pumpDone:
		if err != context.Canceled {
			t.Fatalf("Pump.Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Pump.Run() did not return within 1s after cancel — bounded close may not be working")
	}
}

func TestPumpRunCancelRepeatedSourceCloseDoesNotCreateUnboundedGoroutines(t *testing.T) {
	waitForStableGoroutines := func() int {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(80 * time.Millisecond)
		return runtime.NumGoroutine()
	}

	before := waitForStableGoroutines()

	const attempts = boundedCloseWorkerBudget + 8
	blockChs := make([]chan struct{}, 0, attempts)
	for i := 0; i < attempts; i++ {
		blockCh := make(chan struct{})
		blockChs = append(blockChs, blockCh)
		ring := NewChunkRing(16)
		pump := NewPump(PumpConfig{
			ChunkBytes:           4,
			PublishFlushInterval: 5 * time.Second,
		}, ring)

		ctx, cancel := context.WithCancel(context.Background())
		src := &blockingCloseReadCloser{
			blockCh: blockCh,
		}

		pumpDone := make(chan error, 1)
		go func() {
			pumpDone <- pump.Run(ctx, src)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()

		// The pump should return promptly even with blocked close behavior.
		select {
		case err := <-pumpDone:
			if err != context.Canceled {
				t.Fatalf("Pump.Run() error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Pump.Run() did not return within 1s after cancel — detached close path may be stuck")
		}

		// Keep the close goroutine blocked for test coverage until end.
	}

	after := waitForStableGoroutines()
	maxExpectedGrowth := attempts + (2 * boundedCloseWorkerBudget) + 8 // +8 for runtime jitter.
	if after > before+maxExpectedGrowth {
		t.Fatalf(
			"pump close worker accumulation exceeded bounded bound: before=%d after=%d max_growth=%d",
			before,
			after,
			maxExpectedGrowth,
		)
	}

	// unblock all deferred close paths, then allow cleanup paths to drain.
	for _, blockCh := range blockChs {
		close(blockCh)
	}
	runtime.GC()
	runtime.Gosched()
	time.Sleep(120 * time.Millisecond)

	afterRelease := waitForStableGoroutines()
	if afterRelease > before+2 {
		t.Fatalf("blocked close workers did not drain after release: before=%d afterRelease=%d", before, afterRelease)
	}
}

// blockingCloseReadCloser is a synthetic io.ReadCloser whose Read blocks
// until ctx or close, and whose Close blocks until blockCh is closed.
type blockingCloseReadCloser struct {
	blockCh chan struct{}
}

func (r *blockingCloseReadCloser) Read(p []byte) (int, error) {
	// Block forever until close is requested — simulates a stalled upstream.
	<-r.blockCh
	return 0, io.EOF
}

func (r *blockingCloseReadCloser) Close() error {
	<-r.blockCh
	return nil
}

func BenchmarkPumpRun(b *testing.B) {
	// Guardrail benchmark: includes both warmup-heavy and steady-state-at-capacity
	// cases. The steady-state case forces ring overwrites so publish-path
	// allocation regressions are visible after initial slot warmup.
	payload := bytes.Repeat([]byte{0x47}, 10*1024*1024)
	ctx := context.Background()

	benchmarks := []struct {
		name          string
		chunkBytes    int
		readBufBytes  int
		maxRingChunks int
		discardOnly   bool
	}{
		{
			name:          "chunk32k_read32k",
			chunkBytes:    32 * 1024,
			readBufBytes:  32 * 1024,
			maxRingChunks: 400,
		},
		{
			name:          "chunk32k_read32k_startup_fill_ring400",
			chunkBytes:    32 * 1024,
			readBufBytes:  32 * 1024,
			maxRingChunks: 400,
		},
		{
			name:          "chunk64k_read32k",
			chunkBytes:    64 * 1024,
			readBufBytes:  32 * 1024,
			maxRingChunks: 220,
		},
		{
			name:          "chunk64k_read32k_steady_state_ring32",
			chunkBytes:    64 * 1024,
			readBufBytes:  32 * 1024,
			maxRingChunks: 32,
		},
		{
			name:         "chunk64k_read32k_discard_publisher",
			chunkBytes:   64 * 1024,
			readBufBytes: 32 * 1024,
			discardOnly:  true,
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var publisher ChunkPublisher
				if benchmark.discardOnly {
					publisher = &discardCopyAwarePublisher{}
				} else {
					publisher = NewChunkRingWithLimitsAndStartupHint(
						benchmark.maxRingChunks,
						0,
						benchmark.chunkBytes,
					)
				}
				pump := NewPump(PumpConfig{
					ChunkBytes:           benchmark.chunkBytes,
					ReadBufferBytes:      benchmark.readBufBytes,
					PublishFlushInterval: time.Hour,
				}, publisher)
				if err := pump.Run(ctx, io.NopCloser(bytes.NewReader(payload))); err != nil {
					b.Fatalf("Run() error = %v", err)
				}
			}
		})
	}
}
