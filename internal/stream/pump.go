package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

const (
	defaultPumpChunkBytes      = 64 * 1024
	defaultPumpReadBufferBytes = 32 * 1024
	defaultPumpFlushInterval   = 100 * time.Millisecond
	mpegTSPacketSize           = 188
)

// PumpConfig controls chunking and flush behavior for producer output.
type PumpConfig struct {
	ChunkBytes           int
	ReadBufferBytes      int
	PublishFlushInterval time.Duration
	TSAlign188           bool
}

// PumpStats exposes byte-flow telemetry used by stall detection.
type PumpStats struct {
	LastByteReadAt  time.Time
	LastPublishAt   time.Time
	BytesRead       int64
	BytesPublished  int64
	ChunksPublished int64
}

type pumpReadBuffer struct {
	data []byte
}

type pumpReadEvent struct {
	buf *pumpReadBuffer
	n   int
	err error
}

// Pump copies producer bytes into chunk publications using size-or-time flush.
type Pump struct {
	publisher           ChunkPublisher
	publisherCopiesData bool
	chunkBytes          int
	readBufBytes        int
	flushInterval       time.Duration
	tsAlign188          bool

	nowFn func() time.Time

	readBufPool sync.Pool

	mu    sync.RWMutex
	stats PumpStats
}

// NewPump builds a new chunk pump for producer output.
func NewPump(cfg PumpConfig, publisher ChunkPublisher) *Pump {
	chunkBytes := cfg.ChunkBytes
	if chunkBytes < 1 {
		chunkBytes = defaultPumpChunkBytes
	}

	readBufBytes := cfg.ReadBufferBytes
	if readBufBytes < 1 {
		readBufBytes = defaultPumpReadBufferBytes
	}

	flushInterval := cfg.PublishFlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultPumpFlushInterval
	}

	pump := &Pump{
		publisher:     publisher,
		chunkBytes:    chunkBytes,
		readBufBytes:  readBufBytes,
		flushInterval: flushInterval,
		tsAlign188:    cfg.TSAlign188,
		nowFn: func() time.Time {
			return time.Now().UTC()
		},
	}

	if aware, ok := publisher.(ChunkPublisherCopyAware); ok {
		pump.publisherCopiesData = aware.CopiesPublishedChunkData()
	}
	pump.readBufPool.New = func() any {
		return &pumpReadBuffer{
			data: make([]byte, pump.readBufBytes),
		}
	}

	return pump
}

// Run consumes producer output and publishes chunks until EOF or cancellation.
func (p *Pump) Run(ctx context.Context, src io.ReadCloser) error {
	if p == nil || p.publisher == nil {
		return errors.New("pump publisher is not configured")
	}
	if src == nil {
		return errors.New("pump source is required")
	}

	// Close source on all non-cancel exit paths. On the cancel path, use
	// bounded close so a blocked Close() does not stall pump completion and
	// upstream lifecycle waiters (shared_session runCycle, recovery, shutdown).
	defer func() {
		go closeWithTimeout(src, boundedCloseTimeout)
	}()

	events := make(chan pumpReadEvent, 1)
	readDone := make(chan struct{})
	go p.readLoop(src, events, readDone)
	defer close(readDone)

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	chunk := make([]byte, 0, p.chunkBytes+mpegTSPacketSize)
	publish := func(now time.Time, desired int, final bool) (int, error) {
		n := p.publishLength(len(chunk), desired, final)
		if n <= 0 {
			return 0, nil
		}

		data := chunk[:n]
		if !p.publisherCopiesData {
			data = append([]byte(nil), data...)
		}
		if err := p.publisher.PublishChunk(data, now); err != nil {
			return 0, err
		}
		p.recordPublish(now, n)
		if n == len(chunk) {
			chunk = chunk[:0]
		} else {
			copy(chunk, chunk[n:])
			chunk = chunk[:len(chunk)-n]
		}
		return n, nil
	}

	flush := func(now time.Time, final bool) error {
		for len(chunk) > 0 {
			desired := len(chunk)
			if !final && desired > p.chunkBytes {
				desired = p.chunkBytes
			}

			n, err := publish(now, desired, final)
			if err != nil {
				return err
			}
			if n == 0 {
				return nil
			}

			if !final && len(chunk) < p.chunkBytes {
				return nil
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush(p.now(), true)
			return ctx.Err()
		case <-ticker.C:
			if len(chunk) == 0 {
				continue
			}

			now := p.now()
			lastPublish := p.lastPublishAt()
			if lastPublish.IsZero() || now.Sub(lastPublish) >= p.flushInterval {
				if err := flush(now, false); err != nil {
					return err
				}
			}
		case event := <-events:
			now := p.now()
			if event.n > 0 && event.buf != nil {
				data := event.buf.data[:event.n]
				p.recordRead(now, len(data))
				chunk = append(chunk, data...)
				p.putReadBuffer(event.buf)
			}

			for len(chunk) >= p.chunkBytes {
				n, err := publish(now, p.chunkBytes, false)
				if err != nil {
					return err
				}
				if n == 0 {
					break
				}
			}

			if event.err == nil {
				continue
			}
			if err := flush(now, true); err != nil {
				return err
			}
			if errors.Is(event.err, io.EOF) {
				return nil
			}
			return event.err
		}
	}
}

// Stats returns the latest telemetry snapshot.
func (p *Pump) Stats() PumpStats {
	if p == nil {
		return PumpStats{}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

func (p *Pump) readLoop(src io.Reader, events chan<- pumpReadEvent, done <-chan struct{}) {
	for {
		buf := p.getReadBuffer()
		n, err := src.Read(buf.data)
		if n > 0 {
			select {
			case events <- pumpReadEvent{buf: buf, n: n}:
			case <-done:
				p.putReadBuffer(buf)
				return
			}
		} else {
			p.putReadBuffer(buf)
		}

		if err != nil {
			select {
			case events <- pumpReadEvent{err: err}:
			case <-done:
			}
			return
		}
	}
}

func (p *Pump) getReadBuffer() *pumpReadBuffer {
	if p == nil || p.readBufBytes <= 0 {
		return &pumpReadBuffer{
			data: make([]byte, defaultPumpReadBufferBytes),
		}
	}

	raw := p.readBufPool.Get()
	if raw == nil {
		return &pumpReadBuffer{
			data: make([]byte, p.readBufBytes),
		}
	}
	buf, ok := raw.(*pumpReadBuffer)
	if !ok || buf == nil || cap(buf.data) < p.readBufBytes {
		return &pumpReadBuffer{
			data: make([]byte, p.readBufBytes),
		}
	}
	buf.data = buf.data[:p.readBufBytes]
	return buf
}

func (p *Pump) putReadBuffer(buf *pumpReadBuffer) {
	if p == nil || p.readBufBytes <= 0 || buf == nil || cap(buf.data) < p.readBufBytes {
		return
	}
	buf.data = buf.data[:p.readBufBytes]
	p.readBufPool.Put(buf)
}

func (p *Pump) recordRead(at time.Time, n int) {
	if p == nil || n <= 0 {
		return
	}

	p.mu.Lock()
	p.stats.LastByteReadAt = at.UTC()
	p.stats.BytesRead += int64(n)
	p.mu.Unlock()
}

func (p *Pump) recordPublish(at time.Time, n int) {
	if p == nil || n <= 0 {
		return
	}

	p.mu.Lock()
	p.stats.LastPublishAt = at.UTC()
	p.stats.BytesPublished += int64(n)
	p.stats.ChunksPublished++
	p.mu.Unlock()
}

func (p *Pump) lastPublishAt() time.Time {
	if p == nil {
		return time.Time{}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats.LastPublishAt
}

func (p *Pump) now() time.Time {
	if p == nil || p.nowFn == nil {
		return time.Now().UTC()
	}
	return p.nowFn().UTC()
}

func (p *Pump) publishLength(bufferLen, desired int, final bool) int {
	if p == nil || bufferLen <= 0 {
		return 0
	}

	if desired <= 0 || desired > bufferLen {
		desired = bufferLen
	}
	if !p.tsAlign188 || final {
		return desired
	}

	aligned := desired - (desired % mpegTSPacketSize)
	if aligned > 0 {
		return aligned
	}

	return bufferLen - (bufferLen % mpegTSPacketSize)
}
