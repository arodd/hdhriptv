package stream

import "testing"

func TestNewFFmpegProducerDefaults(t *testing.T) {
	producer, err := NewFFmpegProducer(FFmpegProducerConfig{
		StreamURL: "http://example.com/live.m3u8",
	})
	if err != nil {
		t.Fatalf("NewFFmpegProducer() error = %v", err)
	}

	if producer.ffmpegPath != "ffmpeg" {
		t.Fatalf("ffmpegPath = %q, want ffmpeg", producer.ffmpegPath)
	}
	if producer.mode != "ffmpeg-copy" {
		t.Fatalf("mode = %q, want ffmpeg-copy", producer.mode)
	}
	if producer.readRate != 1 {
		t.Fatalf("readRate = %v, want 1", producer.readRate)
	}
	if producer.initialBurstSeconds != 1 {
		t.Fatalf("initialBurstSeconds = %d, want 1", producer.initialBurstSeconds)
	}
}

func TestFFmpegProducerArgsInputPacingBeforeInputURL(t *testing.T) {
	producer, err := NewFFmpegProducer(FFmpegProducerConfig{
		FFmpegPath:          "/usr/bin/ffmpeg",
		Mode:                "ffmpeg-copy",
		StreamURL:           "http://example.com/live.m3u8?token=123",
		ReadRate:            1.25,
		InitialBurstSeconds: 2,
		LogLevel:            "error",
	})
	if err != nil {
		t.Fatalf("NewFFmpegProducer() error = %v", err)
	}

	args, err := producer.args()
	if err != nil {
		t.Fatalf("args() error = %v", err)
	}

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
	if got := args[idxReadRate+1]; got != "1.25" {
		t.Fatalf("-readrate value = %q, want 1.25", got)
	}
	if got := args[idxBurst+1]; got != "2" {
		t.Fatalf("-readrate_initial_burst value = %q, want 2", got)
	}
}

func TestNewFFmpegProducerRejectsInvalidMode(t *testing.T) {
	_, err := NewFFmpegProducer(FFmpegProducerConfig{
		Mode:      "direct",
		StreamURL: "http://example.com/stream.ts",
	})
	if err == nil {
		t.Fatal("expected mode validation error")
	}
}

func TestNewFFmpegProducerRejectsMissingStreamURL(t *testing.T) {
	_, err := NewFFmpegProducer(FFmpegProducerConfig{})
	if err == nil {
		t.Fatal("expected stream URL validation error")
	}
}

func argIndex(args []string, needle string) int {
	for i := range args {
		if args[i] == needle {
			return i
		}
	}
	return -1
}
