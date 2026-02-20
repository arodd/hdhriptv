package analyzer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeMetadataModeParsesFFprobeMetrics(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-metadata.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","width":1920,"height":1080,"avg_frame_rate":"30000/1001","r_frame_rate":"0/0","bit_rate":"3500000","tags":{"variant_bitrate":"0"}},{"codec_type":"audio","codec_name":"aac","width":0,"height":0,"avg_frame_rate":"0/0","r_frame_rate":"0/0","bit_rate":"128000","tags":{"variant_bitrate":"0"}}],"format":{"bit_rate":"0"}}
JSON
`)

	a, err := NewFFmpegAnalyzer(Config{
		FFprobePath:   ffprobePath,
		BitrateMode:   BitrateModeMetadata,
		ProbeTimeout:  2 * time.Second,
		SampleSeconds: 1,
	})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	metrics, err := a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}

	if metrics.Width != 1920 || metrics.Height != 1080 {
		t.Fatalf("resolution = %dx%d, want 1920x1080", metrics.Width, metrics.Height)
	}
	if metrics.FPS < 29.96 || metrics.FPS > 29.98 {
		t.Fatalf("FPS = %f, want ~29.97", metrics.FPS)
	}
	if metrics.VideoCodec != "h264" {
		t.Fatalf("VideoCodec = %q, want h264", metrics.VideoCodec)
	}
	if metrics.AudioCodec != "aac" {
		t.Fatalf("AudioCodec = %q, want aac", metrics.AudioCodec)
	}
	if metrics.BitrateBPS != 3_500_000 {
		t.Fatalf("BitrateBPS = %d, want 3500000", metrics.BitrateBPS)
	}
}

func TestAnalyzeMetadataModeIgnoresFFprobeStderrNoise(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-stderr-noise.sh", `#!/usr/bin/env bash
echo "[h264 @ 0x1234] non-existing SPS 0 referenced in buffering period" >&2
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","width":1280,"height":720,"avg_frame_rate":"30000/1001","r_frame_rate":"0/0","bit_rate":"0","tags":{"variant_bitrate":"0"}}],"format":{"bit_rate":"0"}}
JSON
`)

	a, err := NewFFmpegAnalyzer(Config{
		FFprobePath:   ffprobePath,
		BitrateMode:   BitrateModeMetadata,
		ProbeTimeout:  2 * time.Second,
		SampleSeconds: 1,
	})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	metrics, err := a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if metrics.Width != 1280 || metrics.Height != 720 {
		t.Fatalf("resolution = %dx%d, want 1280x720", metrics.Width, metrics.Height)
	}
}

func TestAnalyzeMetadataThenSampleFallsBackWhenBitrateMissing(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-no-bitrate.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","width":1280,"height":720,"avg_frame_rate":"60/1","r_frame_rate":"0/0","bit_rate":"0","tags":{"variant_bitrate":"0"}}],"format":{"bit_rate":"0"}}
JSON
`)
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-sample.sh", `#!/usr/bin/env bash
sleep 1
head -c 2000 /dev/zero
`)

	a, err := NewFFmpegAnalyzer(Config{
		FFprobePath:   ffprobePath,
		FFmpegPath:    ffmpegPath,
		BitrateMode:   BitrateModeMetadataThenSample,
		ProbeTimeout:  4 * time.Second,
		SampleSeconds: 1,
	})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	metrics, err := a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if metrics.BitrateBPS <= 0 {
		t.Fatalf("BitrateBPS = %d, want > 0", metrics.BitrateBPS)
	}
	if metrics.BitrateBPS < 5_000 || metrics.BitrateBPS > 40_000 {
		t.Fatalf("BitrateBPS = %d, expected sampled range [5000, 40000]", metrics.BitrateBPS)
	}
}

func TestAnalyzeSampleModeDrainsStderrConcurrently(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-no-bitrate.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","width":1280,"height":720,"avg_frame_rate":"60/1","r_frame_rate":"0/0","bit_rate":"0","tags":{"variant_bitrate":"0"}}],"format":{"bit_rate":"0"}}
JSON
`)
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-stderr-heavy-sample.sh", `#!/usr/bin/env bash
set -euo pipefail
dd if=/dev/zero bs=1024 count=384 status=none | tr '\0' 'E' >&2
sleep 1
head -c 40000 /dev/zero
`)

	a, err := NewFFmpegAnalyzer(Config{
		FFprobePath:   ffprobePath,
		FFmpegPath:    ffmpegPath,
		BitrateMode:   BitrateModeSample,
		ProbeTimeout:  4 * time.Second,
		SampleSeconds: 1,
	})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	start := time.Now()
	metrics, err := a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if metrics.BitrateBPS <= 0 {
		t.Fatalf("BitrateBPS = %d, want > 0", metrics.BitrateBPS)
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("Analyze() elapsed = %s, want < 3s for stderr-heavy sample", elapsed)
	}
}

func TestAnalyzeSampleModeFailureIncludesStderrContext(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-no-bitrate.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","width":1280,"height":720,"avg_frame_rate":"60/1","r_frame_rate":"0/0","bit_rate":"0","tags":{"variant_bitrate":"0"}}],"format":{"bit_rate":"0"}}
JSON
`)
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-fail-stderr.sh", `#!/usr/bin/env bash
set -euo pipefail
echo "fatal-stderr-token provider rejected stream" >&2
dd if=/dev/zero bs=1024 count=256 status=none | tr '\0' 'Z' >&2
exit 1
`)

	a, err := NewFFmpegAnalyzer(Config{
		FFprobePath:   ffprobePath,
		FFmpegPath:    ffmpegPath,
		BitrateMode:   BitrateModeSample,
		ProbeTimeout:  4 * time.Second,
		SampleSeconds: 1,
	})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	_, err = a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err == nil {
		t.Fatal("Analyze() error = nil, want ffmpeg sample failure")
	}
	if !strings.Contains(err.Error(), "fatal-stderr-token") {
		t.Fatalf("Analyze() error = %q, want stderr token", err.Error())
	}
	if len(err.Error()) > 900 {
		t.Fatalf("Analyze() error length = %d, want bounded message length", len(err.Error()))
	}
}

func TestAnalyzeReturnsProbeError(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-fail.sh", `#!/usr/bin/env bash
echo "probe failed" >&2
exit 1
`)

	a, err := NewFFmpegAnalyzer(Config{FFprobePath: ffprobePath, BitrateMode: BitrateModeMetadata})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	_, err = a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err == nil {
		t.Fatal("Analyze() error = nil, want probe failure")
	}
}

func TestAnalyzeMetadataModeSelectsVideoWhenAudioStreamAppearsFirst(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-audio-first.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"audio","codec_name":"mp2","width":0,"height":0,"avg_frame_rate":"0/0","r_frame_rate":"0/0","bit_rate":"128000","tags":{"variant_bitrate":"0"}},{"codec_type":"video","codec_name":"mpeg2video","width":720,"height":480,"avg_frame_rate":"30000/1001","r_frame_rate":"0/0","bit_rate":"2500000","tags":{"variant_bitrate":"0"}}],"format":{"bit_rate":"0"}}
JSON
`)

	a, err := NewFFmpegAnalyzer(Config{
		FFprobePath:  ffprobePath,
		BitrateMode:  BitrateModeMetadata,
		ProbeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	metrics, err := a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}

	if metrics.Width != 720 || metrics.Height != 480 {
		t.Fatalf("resolution = %dx%d, want 720x480", metrics.Width, metrics.Height)
	}
	if metrics.VideoCodec != "mpeg2video" {
		t.Fatalf("VideoCodec = %q, want mpeg2video", metrics.VideoCodec)
	}
	if metrics.AudioCodec != "mp2" {
		t.Fatalf("AudioCodec = %q, want mp2", metrics.AudioCodec)
	}
}

func TestAnalyzeDecodeErrorIncludesFFprobeOutputSnippet(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeExecutable(t, tmp, "ffprobe-invalid-json.sh", `#!/usr/bin/env bash
echo "https://provider.example/tokenized/stream"
`)

	a, err := NewFFmpegAnalyzer(Config{FFprobePath: ffprobePath, BitrateMode: BitrateModeMetadata})
	if err != nil {
		t.Fatalf("NewFFmpegAnalyzer() error = %v", err)
	}

	_, err = a.Analyze(context.Background(), "http://example.com/stream.m3u8")
	if err == nil {
		t.Fatal("Analyze() error = nil, want decode failure")
	}
	if !strings.Contains(err.Error(), "decode ffprobe JSON") {
		t.Fatalf("Analyze() error = %q, want decode ffprobe JSON", err.Error())
	}
	if !strings.Contains(err.Error(), "https://provider.example/tokenized/stream") {
		t.Fatalf("Analyze() error = %q, want output snippet", err.Error())
	}
}

func TestParseFPS(t *testing.T) {
	value, err := parseFPS("30000/1001")
	if err != nil {
		t.Fatalf("parseFPS() error = %v", err)
	}
	if value < 29.96 || value > 29.98 {
		t.Fatalf("value = %f, want ~29.97", value)
	}

	value, err = parseFPS("0/0")
	if err != nil {
		t.Fatalf("parseFPS(0/0) error = %v", err)
	}
	if value != 0 {
		t.Fatalf("value = %f, want 0", value)
	}
}

func writeExecutable(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	return path
}
