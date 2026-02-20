package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultProducerMode                = "ffmpeg-copy"
	defaultProducerFFmpegPath          = "ffmpeg"
	defaultProducerReadRate            = 1.0
	defaultProducerInitialBurstSeconds = 1
	defaultProducerLogLevel            = "error"
)

// Producer starts an upstream MPEG-TS source for a shared channel session.
type Producer interface {
	Start(ctx context.Context) (io.ReadCloser, error)
	Describe() string
}

// FFmpegProducerConfig controls ffmpeg-backed producer behavior.
type FFmpegProducerConfig struct {
	FFmpegPath          string
	Mode                string
	StreamURL           string
	ReadRate            float64
	InitialBurstSeconds int
	LogLevel            string
}

// FFmpegProducer starts ffmpeg and exposes its stdout as MPEG-TS bytes.
type FFmpegProducer struct {
	ffmpegPath          string
	mode                string
	streamURL           string
	readRate            float64
	initialBurstSeconds int
	logLevel            string
}

// NewFFmpegProducer validates config and creates a producer.
func NewFFmpegProducer(cfg FFmpegProducerConfig) (*FFmpegProducer, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = defaultProducerMode
	}
	if mode != "ffmpeg-copy" && mode != "ffmpeg-transcode" {
		return nil, fmt.Errorf("unsupported producer mode %q", mode)
	}

	streamURL := strings.TrimSpace(cfg.StreamURL)
	if streamURL == "" {
		return nil, fmt.Errorf("producer stream URL is required")
	}

	ffmpegPath := strings.TrimSpace(cfg.FFmpegPath)
	if ffmpegPath == "" {
		ffmpegPath = defaultProducerFFmpegPath
	}

	readRate := cfg.ReadRate
	if readRate <= 0 {
		readRate = defaultProducerReadRate
	}

	initialBurstSeconds := cfg.InitialBurstSeconds
	if initialBurstSeconds < 0 {
		return nil, fmt.Errorf("initial burst seconds must be zero or positive")
	}
	if initialBurstSeconds == 0 {
		initialBurstSeconds = defaultProducerInitialBurstSeconds
	}

	logLevel := strings.ToLower(strings.TrimSpace(cfg.LogLevel))
	if logLevel == "" {
		logLevel = defaultProducerLogLevel
	}

	return &FFmpegProducer{
		ffmpegPath:          ffmpegPath,
		mode:                mode,
		streamURL:           streamURL,
		readRate:            readRate,
		initialBurstSeconds: initialBurstSeconds,
		logLevel:            logLevel,
	}, nil
}

// Start launches ffmpeg and returns a read closer for stdout bytes.
func (p *FFmpegProducer) Start(ctx context.Context) (io.ReadCloser, error) {
	if p == nil {
		return nil, fmt.Errorf("producer is not configured")
	}

	args, err := p.args()
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, p.ffmpegPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create ffmpeg stdout pipe: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start ffmpeg producer: %w", err)
	}

	return &producerReadCloser{
		stdout: stdout,
		cmd:    cmd,
		stderr: stderr,
	}, nil
}

// Describe returns a log-friendly producer descriptor.
func (p *FFmpegProducer) Describe() string {
	if p == nil {
		return "ffmpeg:<nil>"
	}
	return "ffmpeg:" + p.mode + ":" + p.streamURL
}

func (p *FFmpegProducer) args() ([]string, error) {
	if p == nil {
		return nil, fmt.Errorf("producer is not configured")
	}

	args := []string{
		"-nostdin",
		"-hide_banner",
		"-loglevel", p.logLevel,
		"-readrate", formatReadRate(p.readRate),
	}
	if p.initialBurstSeconds > 0 {
		args = append(args, "-readrate_initial_burst", strconv.Itoa(p.initialBurstSeconds))
	}

	// Input options must be declared before the corresponding -i stream URL.
	args = append(args, "-i", p.streamURL)

	switch p.mode {
	case "ffmpeg-copy":
		args = append(args,
			"-c", "copy",
			"-f", "mpegts",
			"pipe:1",
		)
	case "ffmpeg-transcode":
		args = append(args,
			"-map", "0:v:0?",
			"-map", "0:a:0?",
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-tune", "zerolatency",
			"-c:a", "aac",
			"-ar", "48000",
			"-ac", "2",
			"-f", "mpegts",
			"pipe:1",
		)
	default:
		return nil, fmt.Errorf("unsupported producer mode %q", p.mode)
	}

	return args, nil
}

func formatReadRate(v float64) string {
	if v <= 0 {
		v = defaultProducerReadRate
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

type producerReadCloser struct {
	stdout io.ReadCloser
	cmd    *exec.Cmd
	stderr *bytes.Buffer

	once sync.Once
	err  error
}

func (p *producerReadCloser) Read(b []byte) (int, error) {
	if p == nil || p.stdout == nil {
		return 0, io.EOF
	}
	return p.stdout.Read(b)
}

func (p *producerReadCloser) Close() error {
	if p == nil {
		return nil
	}

	p.once.Do(func() {
		if p.cmd != nil && p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		if p.stdout != nil {
			_ = p.stdout.Close()
		}

		if p.cmd == nil {
			return
		}
		waitErr := p.cmd.Wait()
		if waitErr == nil {
			return
		}

		stderr := ""
		if p.stderr != nil {
			stderr = strings.TrimSpace(p.stderr.String())
		}
		if stderr != "" {
			p.err = fmt.Errorf("%w: %s", waitErr, stderr)
			return
		}
		p.err = waitErr
	})

	return p.err
}
