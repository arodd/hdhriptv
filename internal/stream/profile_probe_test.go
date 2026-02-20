package stream

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

func TestProbeStreamProfileParsesVideoAudioDetails(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-profile-ok.sh", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {
      "codec_type":"video",
      "codec_name":"h264",
      "width":1920,
      "height":1080,
      "avg_frame_rate":"30000/1001",
      "r_frame_rate":"0/0",
      "bit_rate":"4100000"
    },
    {
      "codec_type":"audio",
      "codec_name":"aac",
      "sample_rate":"48000",
      "channels":2,
      "bit_rate":"128000"
    }
  ],
  "format":{"bit_rate":"4200000"}
}
JSON
`)

	profile, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/live.m3u8", 2*time.Second)
	if err != nil {
		t.Fatalf("probeStreamProfile() error = %v", err)
	}

	if got, want := profile.Width, 1920; got != want {
		t.Fatalf("profile.Width = %d, want %d", got, want)
	}
	if got, want := profile.Height, 1080; got != want {
		t.Fatalf("profile.Height = %d, want %d", got, want)
	}
	if profile.FrameRate < 29.96 || profile.FrameRate > 29.98 {
		t.Fatalf("profile.FrameRate = %f, want ~29.97", profile.FrameRate)
	}
	if got, want := profile.VideoCodec, "h264"; got != want {
		t.Fatalf("profile.VideoCodec = %q, want %q", got, want)
	}
	if got, want := profile.AudioCodec, "aac"; got != want {
		t.Fatalf("profile.AudioCodec = %q, want %q", got, want)
	}
	if got, want := profile.AudioSampleRate, 48000; got != want {
		t.Fatalf("profile.AudioSampleRate = %d, want %d", got, want)
	}
	if got, want := profile.AudioChannels, 2; got != want {
		t.Fatalf("profile.AudioChannels = %d, want %d", got, want)
	}
	if got, want := profile.BitrateBPS, int64(4_100_000); got != want {
		t.Fatalf("profile.BitrateBPS = %d, want %d", got, want)
	}
}

func TestProbeStreamProfileFallsBackBitrateWhenStreamBitrateMissing(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-profile-fallback.sh", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {
      "codec_type":"video",
      "codec_name":"hevc",
      "width":1280,
      "height":720,
	      "avg_frame_rate":"60/1",
	      "r_frame_rate":"0/0",
	      "bit_rate":"0"
	    },
	    {
	      "codec_type":"audio",
	      "codec_name":"aac",
	      "sample_rate":"48000",
	      "channels":2
	    }
	  ],
	  "format":{"bit_rate":"2500000"}
}
JSON
`)

	profile, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/live2.m3u8", 2*time.Second)
	if err != nil {
		t.Fatalf("probeStreamProfile() error = %v", err)
	}

	if got, want := profile.VideoCodec, "hevc"; got != want {
		t.Fatalf("profile.VideoCodec = %q, want %q", got, want)
	}
	if got, want := profile.AudioCodec, "aac"; got != want {
		t.Fatalf("profile.AudioCodec = %q, want %q", got, want)
	}
	if got, want := profile.AudioSampleRate, 48000; got != want {
		t.Fatalf("profile.AudioSampleRate = %d, want %d", got, want)
	}
	if got, want := profile.AudioChannels, 2; got != want {
		t.Fatalf("profile.AudioChannels = %d, want %d", got, want)
	}
	if got, want := profile.BitrateBPS, int64(2_500_000); got != want {
		t.Fatalf("profile.BitrateBPS = %d, want %d", got, want)
	}
}

func TestProbeStreamProfileFallsBackToVariantBitrate(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-profile-variant-bitrate.sh", `#!/usr/bin/env bash
cat <<'JSON'
{
	  "streams":[
    {
      "codec_type":"video",
      "codec_name":"h264",
      "width":1280,
      "height":720,
      "avg_frame_rate":"30000/1001",
	      "r_frame_rate":"0/0",
	      "bit_rate":"0",
	      "tags":{"variant_bitrate":"1800000"}
	    },
	    {
	      "codec_type":"audio",
	      "codec_name":"aac",
	      "sample_rate":"48000",
	      "channels":2
	    }
	  ],
	  "format":{"bit_rate":"0"}
}
JSON
`)

	profile, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/live-variant.m3u8", 2*time.Second)
	if err != nil {
		t.Fatalf("probeStreamProfile() error = %v", err)
	}
	if got, want := profile.BitrateBPS, int64(1_800_000); got != want {
		t.Fatalf("profile.BitrateBPS = %d, want %d", got, want)
	}
}

func TestProbeStreamProfileReturnsPartialWhenVideoMissing(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-profile-no-video.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"audio","codec_name":"aac"}],"format":{"bit_rate":"0"}}
JSON
`)

	profile, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/audio-only", 2*time.Second)
	if err != nil {
		t.Fatalf("probeStreamProfile(audio-only) error = %v, want nil (partial profile)", err)
	}
	if got, want := profile.AudioCodec, "aac"; got != want {
		t.Fatalf("AudioCodec = %q, want %q", got, want)
	}
	if profile.VideoCodec != "" {
		t.Fatalf("VideoCodec = %q, want empty for audio-only", profile.VideoCodec)
	}
}

func TestProbeStreamProfileReturnsPartialWhenAudioMissing(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-profile-no-audio.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","width":1280,"height":720}],"format":{"bit_rate":"0"}}
JSON
`)

	profile, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/video-only", 2*time.Second)
	if err != nil {
		t.Fatalf("probeStreamProfile(video-only) error = %v, want nil (partial profile)", err)
	}
	if got, want := profile.VideoCodec, "h264"; got != want {
		t.Fatalf("VideoCodec = %q, want %q", got, want)
	}
	if got, want := profile.Width, 1280; got != want {
		t.Fatalf("Width = %d, want %d", got, want)
	}
	if profile.AudioCodec != "" {
		t.Fatalf("AudioCodec = %q, want empty for video-only", profile.AudioCodec)
	}
}

// TestProbeStreamProfileAudioOnlyReturnsPartialProfile verifies that
// audio-only ffprobe payloads return a partial profile with audio codec
// metadata populated and zero-valued video fields. This allows audio-only
// sources to persist profile hints for recovery inference.
func TestProbeStreamProfileAudioOnlyReturnsPartialProfile(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-audio-only.sh", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {
      "codec_type":"audio",
      "codec_name":"aac",
      "sample_rate":"48000",
      "channels":2,
      "bit_rate":"128000"
    }
  ],
  "format":{"bit_rate":"128000"}
}
JSON
`)

	profile, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/audio-only.m3u8", 2*time.Second)
	if err != nil {
		t.Fatalf("probeStreamProfile(audio-only) error = %v, want nil (partial profile)", err)
	}

	// Audio fields should be populated.
	if got, want := profile.AudioCodec, "aac"; got != want {
		t.Fatalf("AudioCodec = %q, want %q", got, want)
	}
	if got, want := profile.AudioSampleRate, 48000; got != want {
		t.Fatalf("AudioSampleRate = %d, want %d", got, want)
	}
	if got, want := profile.AudioChannels, 2; got != want {
		t.Fatalf("AudioChannels = %d, want %d", got, want)
	}

	// Video fields should be zero-valued for audio-only streams.
	if profile.Width != 0 || profile.Height != 0 || profile.FrameRate != 0 {
		t.Fatalf("video fields = %dx%d@%f, want all zero for audio-only", profile.Width, profile.Height, profile.FrameRate)
	}
	if profile.VideoCodec != "" {
		t.Fatalf("VideoCodec = %q, want empty for audio-only", profile.VideoCodec)
	}

	// Bitrate should fall back to format bitrate.
	if got, want := profile.BitrateBPS, int64(128_000); got != want {
		t.Fatalf("BitrateBPS = %d, want %d", got, want)
	}
}

// TestProbeStreamProfileRejectsNoStreams verifies that ffprobe payloads with
// no streams at all are still rejected as hard errors.
func TestProbeStreamProfileRejectsNoStreams(t *testing.T) {
	tmp := t.TempDir()
	ffprobePath := writeProbeExecutable(t, tmp, "ffprobe-no-streams.sh", `#!/usr/bin/env bash
cat <<'JSON'
{"streams":[],"format":{"bit_rate":"0"}}
JSON
`)

	_, err := probeStreamProfile(context.Background(), ffprobePath, "http://example.com/empty", 2*time.Second)
	if err == nil {
		t.Fatal("probeStreamProfile(no streams) error = nil, want error")
	}
	if !strings.Contains(err.Error(), "no video or audio streams") {
		t.Fatalf("probeStreamProfile(no streams) error = %q, want 'no video or audio streams'", err.Error())
	}
}

// TestInferSourceComponentStateFromPersistedProfileProbe tests the inference
// helper directly in the profile_probe_test.go file alongside the other probe
// helpers.
func TestInferSourceComponentStateFromPersistedProfileProbe(t *testing.T) {
	source := channels.Source{
		ProfileVideoCodec: "h264",
		ProfileAudioCodec: "aac",
	}
	if got := inferSourceComponentState(source); got != "video_audio" {
		t.Fatalf("inferSourceComponentState(h264/aac) = %q, want video_audio", got)
	}

	source = channels.Source{}
	if got := inferSourceComponentState(source); got != "undetected" {
		t.Fatalf("inferSourceComponentState(empty) = %q, want undetected", got)
	}
}

func TestEstimateCurrentBitrateBPS(t *testing.T) {
	now := time.Unix(1_770_000_120, 0).UTC()
	selectedAt := now.Add(-2 * time.Second)
	got := estimateCurrentBitrateBPS(now, selectedAt, 1_000_000, 500_000)
	if want := int64(2_000_000); got != want {
		t.Fatalf("estimateCurrentBitrateBPS() = %d, want %d", got, want)
	}

	// No bytes since source-select should report zero current bitrate.
	got = estimateCurrentBitrateBPS(now, selectedAt, 500_000, 500_000)
	if want := int64(0); got != want {
		t.Fatalf("zero-bytes estimateCurrentBitrateBPS() = %d, want %d", got, want)
	}
}

func writeProbeExecutable(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	return path
}
