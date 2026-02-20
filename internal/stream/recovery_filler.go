package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

const (
	recoveryFillerModeSlateAV = "slate_av"
	heartbeatModeSlateAV      = recoveryFillerModeSlateAV

	defaultRecoveryFillerText            = "Channel recovering..."
	defaultRecoveryFillerWidth           = 1280
	defaultRecoveryFillerHeight          = 720
	defaultRecoveryFillerFrameRate       = 30000.0 / 1001.0
	defaultRecoveryFillerAudioSampleRate = 48000
	defaultRecoveryFillerAudioChannels   = 2
	defaultRecoveryFillerReadRate        = 1.0
	defaultRecoveryFillerInitialBurst    = 1
)

type slateAVFillerConfig struct {
	FFmpegPath  string
	Profile     streamProfile
	Text        string
	EnableAudio bool
}

type slateAVProducerFactory func(cfg slateAVFillerConfig) (Producer, error)

type recoveryFillerProfile struct {
	Width           int
	Height          int
	FrameRate       float64
	AudioSampleRate int
	AudioChannels   int
}

type slateAVFillerProducer struct {
	ffmpegPath  string
	profile     recoveryFillerProfile
	text        string
	enableAudio bool
}

func defaultSlateAVProducerFactory(cfg slateAVFillerConfig) (Producer, error) {
	return newSlateAVFillerProducer(cfg)
}

func newSlateAVFillerProducer(cfg slateAVFillerConfig) (Producer, error) {
	ffmpegPath := strings.TrimSpace(cfg.FFmpegPath)
	if ffmpegPath == "" {
		ffmpegPath = defaultProducerFFmpegPath
	}

	text := strings.TrimSpace(cfg.Text)
	if text == "" {
		text = defaultRecoveryFillerText
	}

	return &slateAVFillerProducer{
		ffmpegPath:  ffmpegPath,
		profile:     normalizeRecoveryFillerProfile(cfg.Profile, cfg.EnableAudio),
		text:        text,
		enableAudio: cfg.EnableAudio,
	}, nil
}

func (p *slateAVFillerProducer) Start(ctx context.Context) (io.ReadCloser, error) {
	if p == nil {
		return nil, fmt.Errorf("recovery slate AV producer is not configured")
	}

	args := p.args()
	cmd := exec.CommandContext(ctx, p.ffmpegPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create slate AV filler stdout pipe: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start slate AV filler producer: %w", err)
	}

	return &producerReadCloser{
		stdout: stdout,
		cmd:    cmd,
		stderr: stderr,
	}, nil
}

func (p *slateAVFillerProducer) Describe() string {
	if p == nil {
		return "recovery_filler:slate_av:<nil>"
	}
	return "recovery_filler:slate_av"
}

func (p *slateAVFillerProducer) args() []string {
	if p == nil {
		return nil
	}

	fpsExpr := recoveryFillerFPSExpression(p.profile.FrameRate)
	gop := recoveryFillerGOPSize(p.profile.FrameRate)
	colorInput := fmt.Sprintf(
		"color=c=black:s=%dx%d:r=%s",
		p.profile.Width,
		p.profile.Height,
		fpsExpr,
	)

	args := []string{
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-fflags", "+genpts",
		"-readrate", formatReadRate(defaultRecoveryFillerReadRate),
		"-readrate_initial_burst", strconv.Itoa(defaultRecoveryFillerInitialBurst),
		"-f", "lavfi",
		"-i", colorInput,
	}

	if p.enableAudio && p.profile.AudioChannels > 0 {
		audioInput := fmt.Sprintf(
			"anullsrc=r=%d:cl=%s",
			p.profile.AudioSampleRate,
			recoveryFillerAudioLayout(p.profile.AudioChannels),
		)
		args = append(args, "-f", "lavfi", "-i", audioInput)
	}

	filterText := strings.TrimSpace(p.text)
	if filterText != "" {
		args = append(
			args,
			"-vf",
			"drawtext=text='"+escapeDrawtextText(filterText)+"':"+
				"x=(w-text_w)/2:y=(h-text_h)/2:fontsize=42:fontcolor=white:"+
				"box=1:boxcolor=black@0.45:boxborderw=12",
		)
	}

	args = append(
		args,
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-tune", "stillimage",
		"-pix_fmt", "yuv420p",
		"-g", strconv.Itoa(gop),
		"-keyint_min", strconv.Itoa(gop),
		"-sc_threshold", "0",
		"-x264-params", fmt.Sprintf(
			"keyint=%d:min-keyint=%d:scenecut=0:repeat-headers=1:aud=1",
			gop,
			gop,
		),
	)

	if p.enableAudio && p.profile.AudioChannels > 0 {
		args = append(
			args,
			"-c:a", "aac",
			"-b:a", "96k",
			"-ar", strconv.Itoa(p.profile.AudioSampleRate),
			"-ac", strconv.Itoa(p.profile.AudioChannels),
		)
	} else {
		args = append(args, "-an")
	}

	args = append(
		args,
		"-muxpreload", "0",
		"-muxdelay", "0",
		"-mpegts_flags", "+resend_headers",
		"-f", "mpegts",
		"pipe:1",
	)

	return args
}

func normalizeRecoveryFillerProfile(profile streamProfile, enableAudio bool) recoveryFillerProfile {
	width := normalizeRecoveryFillerDimension(profile.Width, defaultRecoveryFillerWidth)
	height := normalizeRecoveryFillerDimension(profile.Height, defaultRecoveryFillerHeight)

	frameRate := profile.FrameRate
	if frameRate <= 0 {
		frameRate = defaultRecoveryFillerFrameRate
	}

	audioSampleRate := profile.AudioSampleRate
	if audioSampleRate <= 0 {
		audioSampleRate = defaultRecoveryFillerAudioSampleRate
	}

	audioChannels := profile.AudioChannels
	if audioChannels <= 0 {
		audioChannels = defaultRecoveryFillerAudioChannels
	}
	if audioChannels > 8 {
		audioChannels = 8
	}

	if !enableAudio {
		audioSampleRate = 0
		audioChannels = 0
	}

	return recoveryFillerProfile{
		Width:           width,
		Height:          height,
		FrameRate:       frameRate,
		AudioSampleRate: audioSampleRate,
		AudioChannels:   audioChannels,
	}
}

func normalizeRecoveryFillerDimension(value, fallback int) int {
	if value <= 0 {
		value = fallback
	}
	if value < 2 {
		value = 2
	}
	if value%2 != 0 {
		value--
	}
	if value < 2 {
		value = 2
	}
	return value
}

func recoveryFillerResolutionNormalizationReason(
	originalWidth int,
	originalHeight int,
	normalizedWidth int,
	normalizedHeight int,
) string {
	if originalWidth == normalizedWidth && originalHeight == normalizedHeight {
		return ""
	}

	widthMissing := originalWidth <= 0
	heightMissing := originalHeight <= 0
	if widthMissing || heightMissing {
		return "profile_dimension_default"
	}

	widthOdd := originalWidth%2 != 0
	heightOdd := originalHeight%2 != 0
	switch {
	case widthOdd && heightOdd:
		return "profile_dimension_odd_width_height"
	case widthOdd:
		return "profile_dimension_odd_width"
	case heightOdd:
		return "profile_dimension_odd_height"
	default:
		return "profile_dimension_adjusted"
	}
}

func recoveryFillerFPSExpression(frameRate float64) string {
	if frameRate <= 0 {
		return "30000/1001"
	}

	switch {
	case approxFrameRate(frameRate, 23.976):
		return "24000/1001"
	case approxFrameRate(frameRate, 29.97):
		return "30000/1001"
	case approxFrameRate(frameRate, 59.94):
		return "60000/1001"
	}

	roundedWhole := math.Round(frameRate)
	if math.Abs(frameRate-roundedWhole) <= 0.01 {
		return strconv.Itoa(int(roundedWhole))
	}

	rounded := math.Round(frameRate*100) / 100
	encoded := strconv.FormatFloat(rounded, 'f', 2, 64)
	return strings.TrimRight(strings.TrimRight(encoded, "0"), ".")
}

func recoveryFillerGOPSize(frameRate float64) int {
	if frameRate <= 0 {
		return 60
	}

	gop := int(math.Round(frameRate * 2))
	if gop < 24 {
		gop = 24
	}
	if gop > 120 {
		gop = 120
	}
	return gop
}

func recoveryFillerAudioLayout(channels int) string {
	switch channels {
	case 1:
		return "mono"
	case 2:
		return "stereo"
	case 6:
		return "5.1"
	case 8:
		return "7.1"
	default:
		return "stereo"
	}
}

func escapeDrawtextText(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		":", "\\:",
		"'", "\\'",
		"%", "\\%",
	)
	return replacer.Replace(raw)
}

func approxFrameRate(got, want float64) bool {
	return math.Abs(got-want) <= 0.02
}
