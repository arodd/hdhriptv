package analyzer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	BitrateModeMetadata           = "metadata"
	BitrateModeSample             = "sample"
	BitrateModeMetadataThenSample = "metadata_then_sample"
	ffmpegSampleStderrCaptureMax  = 64 * 1024
)

// Config controls ffprobe/ffmpeg stream analysis behavior.
type Config struct {
	FFprobePath       string
	FFmpegPath        string
	ProbeTimeout      time.Duration
	AnalyzeDurationUS int64
	ProbeSizeBytes    int64
	BitrateMode       string
	SampleSeconds     int
}

// Metrics captures one source probe result.
type Metrics struct {
	Width      int
	Height     int
	FPS        float64
	VideoCodec string
	AudioCodec string
	BitrateBPS int64
	VariantBPS int64
}

// FFmpegAnalyzer probes stream metadata using ffprobe and optional ffmpeg sampling.
type FFmpegAnalyzer struct {
	cfg Config
}

// NewFFmpegAnalyzer builds an analyzer with validated defaults.
func NewFFmpegAnalyzer(cfg Config) (*FFmpegAnalyzer, error) {
	cfg.FFprobePath = strings.TrimSpace(cfg.FFprobePath)
	cfg.FFmpegPath = strings.TrimSpace(cfg.FFmpegPath)
	cfg.BitrateMode = strings.ToLower(strings.TrimSpace(cfg.BitrateMode))

	if cfg.FFprobePath == "" {
		cfg.FFprobePath = "ffprobe"
	}
	if cfg.FFmpegPath == "" {
		cfg.FFmpegPath = "ffmpeg"
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 7 * time.Second
	}
	if cfg.AnalyzeDurationUS <= 0 {
		cfg.AnalyzeDurationUS = 1_500_000
	}
	if cfg.ProbeSizeBytes <= 0 {
		cfg.ProbeSizeBytes = 1_000_000
	}
	if cfg.SampleSeconds <= 0 {
		cfg.SampleSeconds = 3
	}
	if cfg.BitrateMode == "" {
		cfg.BitrateMode = BitrateModeMetadataThenSample
	}

	switch cfg.BitrateMode {
	case BitrateModeMetadata, BitrateModeSample, BitrateModeMetadataThenSample:
	default:
		return nil, fmt.Errorf("invalid bitrate mode %q", cfg.BitrateMode)
	}

	return &FFmpegAnalyzer{cfg: cfg}, nil
}

// Analyze probes one stream URL and returns quality metrics.
func (a *FFmpegAnalyzer) Analyze(ctx context.Context, streamURL string) (Metrics, error) {
	if a == nil {
		return Metrics{}, fmt.Errorf("analyzer is not configured")
	}
	streamURL = strings.TrimSpace(streamURL)
	if streamURL == "" {
		return Metrics{}, fmt.Errorf("stream URL is required")
	}

	metrics, err := a.probeWithFFprobe(ctx, streamURL)
	if err != nil {
		return Metrics{}, err
	}

	switch a.cfg.BitrateMode {
	case BitrateModeMetadata:
		return metrics, nil
	case BitrateModeSample:
		sampledBPS, sampleErr := a.measureBitrate(ctx, streamURL)
		if sampleErr != nil {
			return metrics, sampleErr
		}
		metrics.BitrateBPS = sampledBPS
		return metrics, nil
	case BitrateModeMetadataThenSample:
		if metrics.BitrateBPS > 0 {
			return metrics, nil
		}
		sampledBPS, sampleErr := a.measureBitrate(ctx, streamURL)
		if sampleErr != nil {
			return metrics, sampleErr
		}
		metrics.BitrateBPS = sampledBPS
		return metrics, nil
	default:
		return Metrics{}, fmt.Errorf("invalid bitrate mode %q", a.cfg.BitrateMode)
	}
}

func (a *FFmpegAnalyzer) probeWithFFprobe(ctx context.Context, streamURL string) (Metrics, error) {
	probeCtx, cancel := context.WithTimeout(ctx, a.cfg.ProbeTimeout)
	defer cancel()

	cmd := exec.CommandContext(probeCtx, a.cfg.FFprobePath,
		"-v", "error",
		"-of", "json",
		"-show_entries", "stream=codec_type,codec_name,width,height,avg_frame_rate,r_frame_rate,bit_rate:stream_tags=variant_bitrate:format=bit_rate",
		"-analyzeduration", strconv.FormatInt(a.cfg.AnalyzeDurationUS, 10),
		"-probesize", strconv.FormatInt(a.cfg.ProbeSizeBytes, 10),
		streamURL,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		detail := compactOutput(stderr.Bytes())
		if detail == "no output" {
			detail = compactOutput(out)
		}
		return Metrics{}, fmt.Errorf("ffprobe failed: %w: %s", err, detail)
	}

	var payload ffprobeOutput
	if err := json.Unmarshal(out, &payload); err != nil {
		return Metrics{}, fmt.Errorf("decode ffprobe JSON: %w: %s", err, compactOutput(out))
	}
	if len(payload.Streams) == 0 {
		return Metrics{}, fmt.Errorf("ffprobe returned no video streams")
	}

	videoStream, audioCodec, ok := selectProbeStreams(payload.Streams)
	if !ok {
		return Metrics{}, fmt.Errorf("ffprobe returned no video streams")
	}

	fps, err := parseFPS(videoStream.AvgFrameRate)
	if err != nil {
		return Metrics{}, fmt.Errorf("parse avg_frame_rate %q: %w", videoStream.AvgFrameRate, err)
	}
	if fps == 0 {
		fps, err = parseFPS(videoStream.RFrameRate)
		if err != nil {
			return Metrics{}, fmt.Errorf("parse r_frame_rate %q: %w", videoStream.RFrameRate, err)
		}
	}

	variantBPS, err := parseInt64(videoStream.Tags.VariantBitrate)
	if err != nil {
		return Metrics{}, fmt.Errorf("parse stream.tags.variant_bitrate %q: %w", videoStream.Tags.VariantBitrate, err)
	}
	streamBPS, err := parseInt64(videoStream.BitRate)
	if err != nil {
		return Metrics{}, fmt.Errorf("parse stream.bit_rate %q: %w", videoStream.BitRate, err)
	}
	formatBPS, err := parseInt64(payload.Format.BitRate)
	if err != nil {
		return Metrics{}, fmt.Errorf("parse format.bit_rate %q: %w", payload.Format.BitRate, err)
	}

	metrics := Metrics{
		Width:      videoStream.Width,
		Height:     videoStream.Height,
		FPS:        fps,
		VideoCodec: strings.TrimSpace(videoStream.CodecName),
		AudioCodec: strings.TrimSpace(audioCodec),
		BitrateBPS: firstPositive(streamBPS, formatBPS, variantBPS),
		VariantBPS: variantBPS,
	}
	return metrics, nil
}

func selectProbeStreams(streams []ffprobeStream) (ffprobeStream, string, bool) {
	var (
		video     ffprobeStream
		audioName string
		found     bool
	)
	for _, stream := range streams {
		codecType := strings.ToLower(strings.TrimSpace(stream.CodecType))
		if audioName == "" && codecType == "audio" {
			audioName = strings.TrimSpace(stream.CodecName)
		}
		if found {
			continue
		}
		if codecType == "video" || (codecType == "" && stream.Width > 0 && stream.Height > 0) {
			video = stream
			found = true
		}
	}
	return video, audioName, found
}

func (a *FFmpegAnalyzer) measureBitrate(ctx context.Context, streamURL string) (int64, error) {
	timeout := a.cfg.ProbeTimeout
	minTimeout := time.Duration(a.cfg.SampleSeconds+2) * time.Second
	if timeout < minTimeout {
		timeout = minTimeout
	}
	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(sampleCtx, a.cfg.FFmpegPath,
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-i", streamURL,
		"-t", strconv.Itoa(a.cfg.SampleSeconds),
		"-c", "copy",
		"-f", "mpegts",
		"pipe:1",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, fmt.Errorf("open ffmpeg stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return 0, fmt.Errorf("open ffmpeg stderr: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start ffmpeg sample: %w", err)
	}

	stderrCapture := newBoundedBuffer(ffmpegSampleStderrCaptureMax)
	stdoutDone := make(chan copyResult, 1)
	stderrDone := make(chan error, 1)

	go func() {
		n, err := io.Copy(io.Discard, stdout)
		stdoutDone <- copyResult{n: n, err: err}
	}()
	go func() {
		_, err := io.Copy(stderrCapture, stderr)
		stderrDone <- err
	}()

	start := time.Now()
	waitErr := cmd.Wait()
	stdoutResult := <-stdoutDone
	stderrErr := <-stderrDone

	if waitErr != nil {
		return 0, fmt.Errorf("ffmpeg sample failed: %w: %s", waitErr, compactOutput(stderrCapture.Bytes()))
	}
	if stdoutResult.err != nil && !errors.Is(stdoutResult.err, io.EOF) {
		return 0, fmt.Errorf("read ffmpeg sample output: %w", stdoutResult.err)
	}
	if stderrErr != nil && !errors.Is(stderrErr, io.EOF) {
		return 0, fmt.Errorf("read ffmpeg sample stderr: %w", stderrErr)
	}

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0.1 {
		return 0, fmt.Errorf("ffmpeg sample elapsed time too short")
	}
	if stdoutResult.n <= 0 {
		return 0, fmt.Errorf("ffmpeg sample produced no output")
	}

	return int64(float64(stdoutResult.n*8) / elapsed), nil
}

type copyResult struct {
	n   int64
	err error
}

type boundedBuffer struct {
	max       int
	buf       bytes.Buffer
	truncated bool
}

func newBoundedBuffer(max int) *boundedBuffer {
	if max < 0 {
		max = 0
	}
	return &boundedBuffer{max: max}
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.max == 0 {
		b.truncated = true
		return len(p), nil
	}
	remaining := b.max - b.buf.Len()
	if remaining > 0 {
		toWrite := len(p)
		if toWrite > remaining {
			toWrite = remaining
		}
		_, _ = b.buf.Write(p[:toWrite])
		if len(p) > toWrite {
			b.truncated = true
		}
		return len(p), nil
	}
	b.truncated = true
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte {
	if !b.truncated {
		return b.buf.Bytes()
	}
	out := make([]byte, 0, b.buf.Len()+12)
	out = append(out, b.buf.Bytes()...)
	out = append(out, " [truncated]"...)
	return out
}

func parseFPS(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" || raw == "0/0" {
		return 0, nil
	}

	if !strings.Contains(raw, "/") {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, err
		}
		if value < 0 {
			return 0, fmt.Errorf("fps must be non-negative")
		}
		return value, nil
	}

	parts := strings.Split(raw, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid ratio")
	}
	denominator, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, err
	}
	if denominator == 0 {
		return 0, nil
	}
	numerator, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, err
	}
	if numerator < 0 || denominator < 0 {
		return 0, fmt.Errorf("fps must be non-negative")
	}
	return numerator / denominator, nil
}

func parseInt64(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "N/A" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("must be non-negative")
	}
	return value, nil
}

func firstPositive(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func compactOutput(out []byte) string {
	text := strings.TrimSpace(string(out))
	if text == "" {
		return "no output"
	}
	text = strings.ReplaceAll(text, "\n", " ")
	if len(text) > 300 {
		return text[:300] + "..."
	}
	return text
}
