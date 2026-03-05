package stream

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

type streamTestTimingProfile struct {
	startupTimeout             time.Duration
	failoverTotalTimeout       time.Duration
	bufferPublishFlushInterval time.Duration
	sessionIdleTimeout         time.Duration
	upstreamChunkInterval      time.Duration
	progressWindow             time.Duration
	pollInterval               time.Duration
	thirdClientAttemptsDirect  int
	thirdClientAttemptsFFmpeg  int
}

var fastStreamTestTiming = streamTestTimingProfile{
	startupTimeout:             350 * time.Millisecond,
	failoverTotalTimeout:       1200 * time.Millisecond,
	bufferPublishFlushInterval: 5 * time.Millisecond,
	sessionIdleTimeout:         120 * time.Millisecond,
	upstreamChunkInterval:      4 * time.Millisecond,
	progressWindow:             140 * time.Millisecond,
	pollInterval:               5 * time.Millisecond,
	thirdClientAttemptsDirect:  20,
	thirdClientAttemptsFFmpeg:  10,
}

// testVideoAudioStartupFixtureChunk prepends a valid MPEG-TS startup probe
// containing both video and audio PMT entries so strict startup validation
// accepts legacy plain-text stream fixtures.
func testVideoAudioStartupFixtureChunk(payload []byte) []byte {
	if len(payload) == 0 {
		payload = mpegTSNullPacketChunk(1)
	}
	probe := startupTestProbeWithPMTStreams(0x1B, 0x0F)
	chunk := make([]byte, 0, len(probe)+len(payload))
	chunk = append(chunk, probe...)
	chunk = append(chunk, payload...)
	return chunk
}

func testVideoAudioStartupFixtureText(payload string) []byte {
	return testVideoAudioStartupFixtureChunk([]byte(payload))
}

func testVideoAudioStartupMinProbeBytes() int {
	return len(startupTestProbeWithPMTStreams(0x1B, 0x0F))
}

func (p streamTestTimingProfile) handlerConfig(mode string) Config {
	return Config{
		Mode:                       mode,
		StartupTimeout:             p.startupTimeout,
		FailoverTotalTimeout:       p.failoverTotalTimeout,
		MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: p.bufferPublishFlushInterval,
		SessionIdleTimeout:         p.sessionIdleTimeout,
		RecoveryFillerEnabled:      false,
		RecoveryFillerMode:         recoveryFillerModeNull,
		RecoveryFillerInterval:     300 * time.Millisecond,
	}
}

func (p streamTestTimingProfile) sessionManagerConfig(mode string) SessionManagerConfig {
	return SessionManagerConfig{
		Mode:                       mode,
		StartupTimeout:             p.startupTimeout,
		FailoverTotalTimeout:       p.failoverTotalTimeout,
		MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: p.bufferPublishFlushInterval,
		SessionIdleTimeout:         p.sessionIdleTimeout,
		RecoveryFillerEnabled:      false,
		RecoveryFillerMode:         recoveryFillerModeNull,
		RecoveryFillerInterval:     300 * time.Millisecond,
	}
}

func assertWriterProgressWithoutUnexpectedEnd(
	t *testing.T,
	writer *recordingResponseWriter,
	errCh <-chan error,
	window time.Duration,
	label string,
) {
	t.Helper()
	if writer == nil {
		t.Fatalf("%s: writer is nil", label)
	}
	if window <= 0 {
		window = fastStreamTestTiming.progressWindow
	}

	startWrites := writer.Writes()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: stream ended unexpectedly: %v", label, err)
			}
			return
		default:
		}
		if writer.Writes() > startWrites {
			return
		}
		time.Sleep(fastStreamTestTiming.pollInterval)
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s: stream ended unexpectedly: %v", label, err)
		}
	default:
	}
	if writer.Writes() <= startWrites {
		t.Fatalf("%s: stream made no forward progress within %s", label, window)
	}
}

type deadlineResponseRecorder struct {
	*httptest.ResponseRecorder
}

func newDeadlineResponseRecorder() *deadlineResponseRecorder {
	return &deadlineResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
}

func (r *deadlineResponseRecorder) SetWriteDeadline(time.Time) error {
	return nil
}
