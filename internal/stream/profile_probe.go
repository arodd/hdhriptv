package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

const (
	defaultStreamProfileFFprobePath         = "ffprobe"
	defaultStreamProfileProbeTimeout        = 4 * time.Second
	defaultStreamProfileAnalyzeDurationUS   = int64(1_500_000)
	defaultStreamProfileProbeSizeBytes      = int64(1_000_000)
	defaultStreamProfileOutputSnippetLength = 240
)

type streamProfileProbeFunc func(
	ctx context.Context,
	ffprobePath string,
	streamURL string,
	timeout time.Duration,
) (streamProfile, error)

var streamProfileProbeRunner streamProfileProbeFunc = probeStreamProfile

type streamProfile struct {
	Width           int
	Height          int
	FrameRate       float64
	VideoCodec      string
	AudioCodec      string
	AudioSampleRate int
	AudioChannels   int
	BitrateBPS      int64
}

type ffprobeProfileOutput struct {
	Streams []ffprobeProfileStream `json:"streams"`
	Format  ffprobeProfileFormat   `json:"format"`
}

type ffprobeProfileStream struct {
	CodecType    string                   `json:"codec_type"`
	CodecName    string                   `json:"codec_name"`
	Width        int                      `json:"width"`
	Height       int                      `json:"height"`
	AvgFrameRate string                   `json:"avg_frame_rate"`
	RFrameRate   string                   `json:"r_frame_rate"`
	BitRate      string                   `json:"bit_rate"`
	SampleRate   string                   `json:"sample_rate"`
	Channels     int                      `json:"channels"`
	Tags         ffprobeProfileStreamTags `json:"tags"`
}

type ffprobeProfileStreamTags struct {
	VariantBitrate string `json:"variant_bitrate"`
}

type ffprobeProfileFormat struct {
	BitRate string `json:"bit_rate"`
}

func probeStreamProfile(ctx context.Context, ffprobePath, streamURL string, timeout time.Duration) (streamProfile, error) {
	streamURL = strings.TrimSpace(streamURL)
	if streamURL == "" {
		return streamProfile{}, fmt.Errorf("stream URL is required")
	}

	ffprobePath = strings.TrimSpace(ffprobePath)
	if ffprobePath == "" {
		ffprobePath = defaultStreamProfileFFprobePath
	}
	if timeout <= 0 {
		timeout = defaultStreamProfileProbeTimeout
	}

	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(
		probeCtx,
		ffprobePath,
		"-v", "error",
		"-of", "json",
		"-show_streams",
		"-show_format",
		"-analyzeduration", strconv.FormatInt(defaultStreamProfileAnalyzeDurationUS, 10),
		"-probesize", strconv.FormatInt(defaultStreamProfileProbeSizeBytes, 10),
		streamURL,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		detail := compactProfileProbeOutput(stderr.Bytes())
		if detail == "no output" {
			detail = compactProfileProbeOutput(out)
		}
		return streamProfile{}, fmt.Errorf("ffprobe stream profile failed: %w: %s", err, detail)
	}

	var payload ffprobeProfileOutput
	if err := json.Unmarshal(out, &payload); err != nil {
		return streamProfile{}, fmt.Errorf(
			"decode ffprobe stream profile JSON: %w: %s",
			err,
			compactProfileProbeOutput(out),
		)
	}

	video, audio, err := selectProfileStreams(payload.Streams)
	if err != nil {
		return streamProfile{}, err
	}

	fps, err := parseProfileFPS(video.AvgFrameRate)
	if err != nil {
		return streamProfile{}, fmt.Errorf("parse avg_frame_rate %q: %w", video.AvgFrameRate, err)
	}
	if fps == 0 {
		fps, err = parseProfileFPS(video.RFrameRate)
		if err != nil {
			return streamProfile{}, fmt.Errorf("parse r_frame_rate %q: %w", video.RFrameRate, err)
		}
	}

	videoBitrate, err := parseProfileInt64(video.BitRate)
	if err != nil {
		return streamProfile{}, fmt.Errorf("parse video bit_rate %q: %w", video.BitRate, err)
	}
	formatBitrate, err := parseProfileInt64(payload.Format.BitRate)
	if err != nil {
		return streamProfile{}, fmt.Errorf("parse format bit_rate %q: %w", payload.Format.BitRate, err)
	}
	variantBitrate, err := parseProfileInt64(video.Tags.VariantBitrate)
	if err != nil {
		return streamProfile{}, fmt.Errorf("parse stream.tags.variant_bitrate %q: %w", video.Tags.VariantBitrate, err)
	}
	audioSampleRate, err := parseProfileInt64(audio.SampleRate)
	if err != nil {
		return streamProfile{}, fmt.Errorf("parse audio sample_rate %q: %w", audio.SampleRate, err)
	}
	audioChannels := audio.Channels
	if audioChannels < 0 {
		return streamProfile{}, fmt.Errorf("audio channels must be non-negative")
	}

	return streamProfile{
		Width:           video.Width,
		Height:          video.Height,
		FrameRate:       fps,
		VideoCodec:      strings.TrimSpace(video.CodecName),
		AudioCodec:      strings.TrimSpace(audio.CodecName),
		AudioSampleRate: int(audioSampleRate),
		AudioChannels:   audioChannels,
		BitrateBPS:      firstPositiveInt64(videoBitrate, formatBitrate, variantBitrate),
	}, nil
}

func selectProfileStreams(streams []ffprobeProfileStream) (ffprobeProfileStream, ffprobeProfileStream, error) {
	var video ffprobeProfileStream
	var audio ffprobeProfileStream
	hasVideo := false
	hasAudio := false

	for _, stream := range streams {
		switch strings.ToLower(strings.TrimSpace(stream.CodecType)) {
		case "video":
			if !hasVideo {
				video = stream
				hasVideo = true
			}
		case "audio":
			if !hasAudio {
				audio = stream
				hasAudio = true
			}
		}
	}

	// At least one stream type must be present. Audio-only payloads are
	// allowed so that persisted profile metadata can capture audio codec
	// hints for sources that only provide audio. Video fields in the
	// returned profile will be zero-valued for audio-only streams.
	if !hasVideo && !hasAudio {
		return ffprobeProfileStream{}, ffprobeProfileStream{}, fmt.Errorf("ffprobe returned no video or audio streams")
	}
	return video, audio, nil
}

func formatResolution(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	return strconv.Itoa(width) + "x" + strconv.Itoa(height)
}

func sourceProfileIsEmpty(profile streamProfile) bool {
	if profile.Width > 0 || profile.Height > 0 || profile.FrameRate > 0 {
		return false
	}
	if strings.TrimSpace(profile.VideoCodec) != "" || strings.TrimSpace(profile.AudioCodec) != "" {
		return false
	}
	if profile.AudioSampleRate > 0 || profile.AudioChannels > 0 {
		return false
	}
	return profile.BitrateBPS <= 0
}

// inferSourceComponentState derives a component-state hint from persisted
// source profile codec metadata. This allows recovery startup to use
// historical probe data instead of defaulting to "undetected" when no live
// startup inventory is available. Phase B2 will wire this into the recovery
// candidate startup path.
func inferSourceComponentState(source channels.Source) string {
	hasVideo := strings.TrimSpace(source.ProfileVideoCodec) != ""
	hasAudio := strings.TrimSpace(source.ProfileAudioCodec) != ""
	switch {
	case hasVideo && hasAudio:
		return "video_audio"
	case hasVideo:
		return "video_only"
	case hasAudio:
		return "audio_only"
	default:
		return "undetected"
	}
}

func parseProfileFPS(raw string) (float64, error) {
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
	numerator, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, err
	}
	denominator, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, err
	}
	if denominator == 0 {
		return 0, nil
	}
	if numerator < 0 || denominator < 0 {
		return 0, fmt.Errorf("fps must be non-negative")
	}
	return numerator / denominator, nil
}

func parseProfileInt64(raw string) (int64, error) {
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

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func compactProfileProbeOutput(raw []byte) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "no output"
	}
	fields := strings.Fields(trimmed)
	compact := strings.Join(fields, " ")
	runes := []rune(compact)
	if len(runes) > defaultStreamProfileOutputSnippetLength {
		return strings.TrimSpace(string(runes[:defaultStreamProfileOutputSnippetLength])) + "..."
	}
	return compact
}

func estimateCurrentBitrateBPS(
	now time.Time,
	sourceSelectedAt time.Time,
	bytesPushed int64,
	sourceBytesBaseline int64,
) int64 {
	if sourceSelectedAt.IsZero() {
		return 0
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	elapsed := now.Sub(sourceSelectedAt)
	if elapsed <= 0 {
		return 0
	}

	bytesSinceSelect := bytesPushed - sourceBytesBaseline
	if bytesSinceSelect < 0 {
		bytesSinceSelect = 0
	}
	if bytesSinceSelect == 0 {
		return 0
	}

	bps := int64(float64(bytesSinceSelect*8) / elapsed.Seconds())
	if bps < 0 {
		return 0
	}
	return bps
}
