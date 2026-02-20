package jobs

import (
	"bytes"
	"sync"
)

// testLogBuffer serializes slog writes and test reads for race-safe assertions.
type testLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newTestLogBuffer() *testLogBuffer {
	return &testLogBuffer{}
}

func (b *testLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *testLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
