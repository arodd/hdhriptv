package stream

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

func TestSanitizeStreamURLForLog(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		want  string
		block []string
	}{
		{
			name:  "removes user info query and fragment",
			raw:   "https://user:pass@example.com/live/stream.m3u8?token=abc123&expires=456#frag",
			want:  "https://example.com/live/stream.m3u8",
			block: []string{"user:pass@", "token=abc123", "#frag"},
		},
		{
			name:  "preserves route shape when already clean",
			raw:   "http://example.net/channels/news.ts",
			want:  "http://example.net/channels/news.ts",
			block: []string{"?"},
		},
		{
			name:  "fallback strips malformed query escapes",
			raw:   "http://user:secret@example.org/live/index.m3u8?sig=%ZZ",
			want:  "http://example.org/live/index.m3u8",
			block: []string{"user:secret@", "sig=%ZZ"},
		},
		{
			name:  "trims blank input",
			raw:   "   ",
			want:  "",
			block: []string{},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeStreamURLForLog(tc.raw)
			if got != tc.want {
				t.Fatalf("sanitizeStreamURLForLog(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			for _, blocked := range tc.block {
				if blocked != "" && strings.Contains(got, blocked) {
					t.Fatalf("sanitizeStreamURLForLog(%q) leaked %q in %q", tc.raw, blocked, got)
				}
			}
		})
	}
}

func TestSetSourceStateLogsSanitizedSourceURL(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	session := &sharedRuntimeSession{
		manager: &SessionManager{
			logger: logger,
			cfg: sessionManagerConfig{
				mode: "direct",
			},
		},
		channel: channels.Channel{
			ChannelID:   91,
			GuideNumber: "191",
			GuideName:   "US Secure News",
		},
		startedAt: time.Now().UTC(),
	}

	rawURL := "https://alice:secret@example.test/live/news.m3u8?token=signed-token&exp=1700000000#frag"
	session.setSourceState(channels.Source{
		SourceID:  8,
		ItemKey:   "src:secure:news",
		StreamURL: rawURL,
	}, "ffmpeg-copy:source=8", "initial_startup")

	text := logs.String()
	if !strings.Contains(text, "shared session selected source") {
		t.Fatalf("expected selected-source log event, got %q", text)
	}
	if strings.Contains(text, "signed-token") || strings.Contains(text, "alice:secret@") {
		t.Fatalf("selected-source log leaked sensitive URL fields: %q", text)
	}
	if !strings.Contains(text, "source_url=https://example.test/live/news.m3u8") {
		t.Fatalf("selected-source log missing sanitized URL: %q", text)
	}
}

func TestRunStreamProfileProbeFailureLogsSanitizedSourceURL(t *testing.T) {
	tmp := t.TempDir()
	writeExecutable(t, tmp, "ffprobe", `#!/usr/bin/env bash
echo "simulated ffprobe failure" >&2
exit 2
`)
	t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rawURL := "https://probe:secret@example.test/stream/live.m3u8?token=abc123#frag"

	session := &sharedRuntimeSession{
		manager: &SessionManager{
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   77,
			GuideNumber: "177",
			GuideName:   "US Probe Test",
		},
		sourceID:               123,
		sourceStreamURL:        rawURL,
		profileProbeGeneration: 1,
	}

	probeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	session.runStreamProfileProbe(probeCtx, nil, 1, 123, rawURL, 0)

	text := logs.String()
	if !strings.Contains(text, "shared session stream profile probe failed") {
		t.Fatalf("expected stream profile probe failure log, got %q", text)
	}
	if strings.Contains(text, "probe:secret@") || strings.Contains(text, "token=abc123") {
		t.Fatalf("probe failure log leaked sensitive URL fields: %q", text)
	}
	if !strings.Contains(text, "source_url=https://example.test/stream/live.m3u8") {
		t.Fatalf("probe failure log missing sanitized URL: %q", text)
	}
}
