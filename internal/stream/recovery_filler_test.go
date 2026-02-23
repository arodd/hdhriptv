package stream

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeRecoveryFillerProfileDefaults(t *testing.T) {
	got := normalizeRecoveryFillerProfile(streamProfile{}, true)

	if got.Width != defaultRecoveryFillerWidth {
		t.Fatalf("Width = %d, want %d", got.Width, defaultRecoveryFillerWidth)
	}
	if got.Height != defaultRecoveryFillerHeight {
		t.Fatalf("Height = %d, want %d", got.Height, defaultRecoveryFillerHeight)
	}
	if got.FrameRate != defaultRecoveryFillerFrameRate {
		t.Fatalf("FrameRate = %v, want %v", got.FrameRate, defaultRecoveryFillerFrameRate)
	}
	if got.AudioSampleRate != defaultRecoveryFillerAudioSampleRate {
		t.Fatalf("AudioSampleRate = %d, want %d", got.AudioSampleRate, defaultRecoveryFillerAudioSampleRate)
	}
	if got.AudioChannels != defaultRecoveryFillerAudioChannels {
		t.Fatalf("AudioChannels = %d, want %d", got.AudioChannels, defaultRecoveryFillerAudioChannels)
	}
}

func TestNormalizeRecoveryFillerProfileNormalizesOddDimensions(t *testing.T) {
	got := normalizeRecoveryFillerProfile(
		streamProfile{
			Width:  853,
			Height: 481,
		},
		true,
	)

	if got.Width != 852 {
		t.Fatalf("Width = %d, want 852", got.Width)
	}
	if got.Height != 480 {
		t.Fatalf("Height = %d, want 480", got.Height)
	}
}

func TestNormalizeRecoveryFillerProfileMinimumEvenDimensions(t *testing.T) {
	got := normalizeRecoveryFillerProfile(
		streamProfile{
			Width:  1,
			Height: 1,
		},
		true,
	)

	if got.Width != 2 {
		t.Fatalf("Width = %d, want 2", got.Width)
	}
	if got.Height != 2 {
		t.Fatalf("Height = %d, want 2", got.Height)
	}
}

func TestRecoveryFillerResolutionNormalizationReason(t *testing.T) {
	tests := []struct {
		name             string
		originalWidth    int
		originalHeight   int
		normalizedWidth  int
		normalizedHeight int
		want             string
	}{
		{
			name:             "unchanged",
			originalWidth:    1920,
			originalHeight:   1080,
			normalizedWidth:  1920,
			normalizedHeight: 1080,
			want:             "",
		},
		{
			name:             "defaulted",
			originalWidth:    0,
			originalHeight:   480,
			normalizedWidth:  1280,
			normalizedHeight: 480,
			want:             "profile_dimension_default",
		},
		{
			name:             "odd width",
			originalWidth:    853,
			originalHeight:   480,
			normalizedWidth:  852,
			normalizedHeight: 480,
			want:             "profile_dimension_odd_width",
		},
		{
			name:             "odd height",
			originalWidth:    854,
			originalHeight:   481,
			normalizedWidth:  854,
			normalizedHeight: 480,
			want:             "profile_dimension_odd_height",
		},
		{
			name:             "odd width and height",
			originalWidth:    853,
			originalHeight:   481,
			normalizedWidth:  852,
			normalizedHeight: 480,
			want:             "profile_dimension_odd_width_height",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := recoveryFillerResolutionNormalizationReason(
				tt.originalWidth,
				tt.originalHeight,
				tt.normalizedWidth,
				tt.normalizedHeight,
			)
			if got != tt.want {
				t.Fatalf("reason = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSlateAVFillerProducerArgsMatchProfile(t *testing.T) {
	producer, err := newSlateAVFillerProducer(slateAVFillerConfig{
		FFmpegPath: "ffmpeg",
		Profile: streamProfile{
			Width:           1920,
			Height:          1080,
			FrameRate:       59.94,
			AudioSampleRate: 44100,
			AudioChannels:   2,
		},
		Text:        "Recovering: please wait",
		EnableAudio: true,
	})
	if err != nil {
		t.Fatalf("newSlateAVFillerProducer() error = %v", err)
	}

	typed, ok := producer.(*slateAVFillerProducer)
	if !ok {
		t.Fatalf("producer type = %T, want *slateAVFillerProducer", producer)
	}

	args := typed.args()
	assertArgContains(t, args, "color=c=black:s=1920x1080:r=60000/1001")
	assertArgContains(t, args, "anullsrc=r=44100:cl=stereo")
	assertArgContains(t, args, "drawtext=text='Recovering\\: please wait'")
	assertArgContains(t, args, "-ar")
	assertArgContains(t, args, "44100")
	assertArgContains(t, args, "-ac")
	assertArgContains(t, args, "2")
	assertArgContains(t, args, "-g")
	assertArgContains(t, args, "120")
	assertArgContains(t, args, "-x264-params")
	assertArgContains(t, args, "repeat-headers=1")
	assertArgContains(t, args, "aud=1")
}

func TestSlateAVFillerProducerArgsApplyPTSOffsetToVideoAndAudio(t *testing.T) {
	producer, err := newSlateAVFillerProducer(slateAVFillerConfig{
		FFmpegPath: "ffmpeg",
		Profile: streamProfile{
			Width:           1280,
			Height:          720,
			FrameRate:       30000.0 / 1001.0,
			AudioSampleRate: 48000,
			AudioChannels:   2,
		},
		Text:        "Recovering",
		EnableAudio: true,
		PTSOffset:   1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("newSlateAVFillerProducer() error = %v", err)
	}

	typed, ok := producer.(*slateAVFillerProducer)
	if !ok {
		t.Fatalf("producer type = %T, want *slateAVFillerProducer", producer)
	}

	args := typed.args()
	assertArgContains(t, args, "setpts=PTS+1500000/1000000/TB")
	assertArgContains(t, args, "-af")
	assertArgContains(t, args, "asetpts=PTS+1500000/1000000/TB")
}

func TestSlateAVFillerProducerNegativePTSOffsetClampsToZero(t *testing.T) {
	producer, err := newSlateAVFillerProducer(slateAVFillerConfig{
		FFmpegPath: "ffmpeg",
		Profile: streamProfile{
			Width:           1280,
			Height:          720,
			FrameRate:       30000.0 / 1001.0,
			AudioSampleRate: 48000,
			AudioChannels:   2,
		},
		Text:        "Recovering",
		EnableAudio: true,
		PTSOffset:   -500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("newSlateAVFillerProducer() error = %v", err)
	}

	typed, ok := producer.(*slateAVFillerProducer)
	if !ok {
		t.Fatalf("producer type = %T, want *slateAVFillerProducer", producer)
	}
	if typed.ptsOffset != 0 {
		t.Fatalf("ptsOffset = %s, want 0", typed.ptsOffset)
	}

	args := typed.args()
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "setpts=") {
		t.Fatalf("args unexpectedly contained pts offset filters: %s", joined)
	}
	if containsArg(args, "-af") {
		t.Fatalf("args unexpectedly contained -af for zero pts offset: %#v", args)
	}
}

func TestSlateAVFillerProducerArgsInputPacingBeforeInput(t *testing.T) {
	producer, err := newSlateAVFillerProducer(slateAVFillerConfig{
		FFmpegPath: "ffmpeg",
		Profile: streamProfile{
			Width:     1280,
			Height:    720,
			FrameRate: 30000.0 / 1001.0,
		},
		EnableAudio: false,
	})
	if err != nil {
		t.Fatalf("newSlateAVFillerProducer() error = %v", err)
	}

	typed, ok := producer.(*slateAVFillerProducer)
	if !ok {
		t.Fatalf("producer type = %T, want *slateAVFillerProducer", producer)
	}

	args := typed.args()
	idxReadRate := argIndex(args, "-readrate")
	idxBurst := argIndex(args, "-readrate_initial_burst")
	idxInput := argIndex(args, "-i")

	if idxReadRate == -1 {
		t.Fatalf("args missing -readrate: %#v", args)
	}
	if idxBurst == -1 {
		t.Fatalf("args missing -readrate_initial_burst: %#v", args)
	}
	if idxInput == -1 {
		t.Fatalf("args missing -i: %#v", args)
	}
	if idxReadRate > idxInput {
		t.Fatalf("-readrate index = %d, expected before -i index %d: %#v", idxReadRate, idxInput, args)
	}
	if idxBurst > idxInput {
		t.Fatalf("-readrate_initial_burst index = %d, expected before -i index %d: %#v", idxBurst, idxInput, args)
	}
	if got := args[idxReadRate+1]; got != "1" {
		t.Fatalf("-readrate value = %q, want 1", got)
	}
	if got := args[idxBurst+1]; got != "1" {
		t.Fatalf("-readrate_initial_burst value = %q, want 1", got)
	}
}

func TestSlateAVFillerProducerArgsDisableAudio(t *testing.T) {
	producer, err := newSlateAVFillerProducer(slateAVFillerConfig{
		FFmpegPath: "ffmpeg",
		Profile: streamProfile{
			Width:           1280,
			Height:          720,
			FrameRate:       30,
			AudioSampleRate: 48000,
			AudioChannels:   2,
		},
		EnableAudio: false,
	})
	if err != nil {
		t.Fatalf("newSlateAVFillerProducer() error = %v", err)
	}

	typed, ok := producer.(*slateAVFillerProducer)
	if !ok {
		t.Fatalf("producer type = %T, want *slateAVFillerProducer", producer)
	}

	args := typed.args()
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "anullsrc=") {
		t.Fatalf("args unexpectedly contained anullsrc input: %s", joined)
	}
	if !containsArg(args, "-an") {
		t.Fatalf("args missing -an audio disable flag: %#v", args)
	}
}

func TestSlateAVFillerProducerArgsNormalizeOddResolution(t *testing.T) {
	producer, err := newSlateAVFillerProducer(slateAVFillerConfig{
		FFmpegPath: "ffmpeg",
		Profile: streamProfile{
			Width:     853,
			Height:    481,
			FrameRate: 30000.0 / 1001.0,
		},
		EnableAudio: false,
	})
	if err != nil {
		t.Fatalf("newSlateAVFillerProducer() error = %v", err)
	}

	typed, ok := producer.(*slateAVFillerProducer)
	if !ok {
		t.Fatalf("producer type = %T, want *slateAVFillerProducer", producer)
	}

	args := typed.args()
	assertArgContains(t, args, "color=c=black:s=852x480:r=30000/1001")
}

func assertArgContains(t *testing.T, args []string, want string) {
	t.Helper()
	for _, arg := range args {
		if arg == want || strings.Contains(arg, want) {
			return
		}
	}
	t.Fatalf("args did not contain %q: %#v", want, args)
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}
