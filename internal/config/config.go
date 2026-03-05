package config

import (
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/robfig/cron/v3"
	"github.com/spf13/pflag"
)

const (
	streamModeDirect          = "direct"
	streamModeFFmpegCopy      = "ffmpeg-copy"
	streamModeFFmpegTranscode = "ffmpeg-transcode"

	defaultPrimaryPlaylistSourceName = "Primary"
	maxDiscoveryTunerAdvertisedCount = 255

	defaultUPnPContentDirectoryUpdateIDCacheTTL = time.Second
	defaultDVRLineupReloadTimeout               = 30 * time.Second
	defaultTraditionalGuideStart                = 100
	dynamicGuideStart                           = 10000
	maxFFmpegInputBufferSize                    = 64 << 20

	defaultCatalogSearchMaxTerms      = 12
	defaultCatalogSearchMaxDisjuncts  = 6
	defaultCatalogSearchMaxTermRunes  = 64
	maxCatalogSearchMaxTermsLimit     = 256
	maxCatalogSearchMaxDisjunctsLimit = 128
	maxCatalogSearchMaxTermRunesLimit = 256

	defaultPlaylistSyncSourceConcurrency = 1
	maxPlaylistSyncSourceConcurrency     = 16

	defaultRecoveryFillerText                   = "Channel recovering..."
	defaultFFmpegStartupProbeSizeBytes          = 1_000_000
	defaultFFmpegStartupAnalyzeDuration         = 1500 * time.Millisecond
	defaultFFmpegRWTimeout                      = 3 * time.Second
	defaultFFmpegSourceLogLevel                 = "warning"
	defaultFFmpegSourceStderrLogLevel           = "info"
	defaultFFmpegSourceStderrMaxLineBytes       = 2048
	defaultFFmpegSourceStderrPassthroughEnabled = true
	minFFmpegStartupProbeSizeBytes              = 128_000
	minFFmpegStartupAnalyzeDuration             = 1 * time.Second
)

// PlaylistSourceConfig describes one resolved playlist source from startup config.
type PlaylistSourceConfig struct {
	Name        string
	PlaylistURL string
	TunerCount  int
	Enabled     bool
}

// Config carries runtime settings from CLI flags and environment variables.
type Config struct {
	PlaylistURL                          string
	DBPath                               string
	LogDir                               string
	HTTPAddr                             string
	LegacyHTTPAddr                       string
	UPnPEnabled                          bool
	UPnPAddr                             string
	UPnPNotifyInterval                   time.Duration
	UPnPMaxAge                           time.Duration
	UPnPContentDirectoryUpdateIDCacheTTL time.Duration
	PrimaryTunerCount                    int
	TunerCount                           int
	PlaylistSources                      []PlaylistSourceConfig
	PlaylistSourcesStartupAuthoritative  bool
	TraditionalGuideStart                int
	FriendlyName                         string
	DeviceID                             string
	DeviceAuth                           string
	RefreshSchedule                      string
	PlaylistSyncSourceConcurrency        int
	ReconcileDynamicRulePaged            bool
	FFmpegPath                           string
	FFprobePath                          string
	StreamMode                           string
	StartupTimeout                       time.Duration
	StartupRandomAccessRecoveryOnly      bool
	MinProbeBytes                        int
	MaxFailovers                         int
	FailoverTotalTimeout                 time.Duration
	UpstreamOverlimitCooldown            time.Duration
	FFmpegReconnectEnabled               bool
	FFmpegReconnectDelayMax              time.Duration
	FFmpegReconnectMaxRetries            int
	FFmpegReconnectHTTPErrors            string
	FFmpegRWTimeout                      time.Duration
	FFmpegStartupProbeSize               int
	FFmpegStartupAnalyzeDuration         time.Duration
	FFmpegInputBufferSize                int
	FFmpegDiscardCorrupt                 bool
	FFmpegCopyRegenerateTimestamps       bool
	FFmpegSourceLogLevel                 string
	FFmpegSourceStderrPassthroughEnabled bool
	FFmpegSourceStderrLogLevel           string
	FFmpegSourceStderrMaxLineBytes       int
	ProducerReadRate                     float64
	ProducerReadRateCatchup              float64
	ProducerInitialBurst                 int
	BufferChunkBytes                     int
	BufferFlushInterval                  time.Duration
	BufferTSAlign188                     bool
	StallDetect                          time.Duration
	StallHardDeadline                    time.Duration
	StallPolicy                          string
	StallMaxFailovers                    int
	CycleFailureMinHealth                time.Duration
	RecoveryFillerEnabled                bool
	RecoveryFillerMode                   string
	RecoveryFillerInterval               time.Duration
	RecoveryFillerText                   string
	RecoveryFillerEnableAudio            bool
	SubscriberJoinLag                    int
	SubscriberSlowPolicy                 string
	SubscriberMaxBlocked                 time.Duration
	SessionIdleTimeout                   time.Duration
	SessionDrainTimeout                  time.Duration
	SessionMaxSubscribers                int
	SessionHistoryLimit                  int
	SessionSourceHistoryLimit            int
	SessionSubscriberHistoryLimit        int
	SourceHealthDrainTimeout             time.Duration
	PreemptSettleDelay                   time.Duration
	AutoPrioritizeProbeTuneDelay         time.Duration
	AutoPrioritizeWorkers                string
	ProbeInterval                        time.Duration
	ProbeTimeout                         time.Duration
	AdminBasicAuth                       string
	AdminJSONBodyLimitBytes              int64
	DVRLineupReloadTimeout               time.Duration
	RequestTimeout                       time.Duration
	RateLimitRPS                         float64
	RateLimitBurst                       int
	RateLimitMaxClients                  int
	RateLimitTrustedProxies              []string
	TuneBackoffMaxTunes                  int
	TuneBackoffInterval                  time.Duration
	TuneBackoffCooldown                  time.Duration
	EnableMetrics                        bool
	HTTPRequestLogEnabled                bool
	LogLevel                             string
	CatalogSearchMaxTerms                int
	CatalogSearchMaxDisjuncts            int
	CatalogSearchMaxTermRunes            int

	playlistSourceSpecsRaw []string
}

// RedactedConfig is safe for logs and startup output.
type RedactedConfig struct {
	PlaylistURLConfigured                bool     `json:"playlist_url_configured"`
	DBPath                               string   `json:"db_path"`
	LogDir                               string   `json:"log_dir"`
	HTTPAddr                             string   `json:"http_addr"`
	LegacyHTTPAddr                       string   `json:"legacy_http_addr,omitempty"`
	UPnPEnabled                          bool     `json:"upnp_enabled"`
	UPnPAddr                             string   `json:"upnp_addr"`
	UPnPNotifyInterval                   string   `json:"upnp_notify_interval"`
	UPnPMaxAge                           string   `json:"upnp_max_age"`
	UPnPContentDirectoryUpdateIDCacheTTL string   `json:"upnp_content_directory_update_id_cache_ttl"`
	PrimaryTunerCount                    int      `json:"primary_tuner_count"`
	TunerCount                           int      `json:"tuner_count"`
	PlaylistSourceCount                  int      `json:"playlist_source_count"`
	PlaylistSourcesStartupAuthoritative  bool     `json:"playlist_sources_startup_authoritative"`
	DiscoveryTunerCount                  int      `json:"discovery_tuner_count"`
	DiscoveryTunerCountCapped            bool     `json:"discovery_tuner_count_capped"`
	TraditionalGuideStart                int      `json:"traditional_guide_start"`
	FriendlyName                         string   `json:"friendly_name"`
	DeviceID                             string   `json:"device_id"`
	DeviceAuthConfigured                 bool     `json:"device_auth_configured"`
	RefreshSchedule                      string   `json:"refresh_schedule,omitempty"`
	PlaylistSyncSourceConcurrency        int      `json:"playlist_sync_source_concurrency"`
	ReconcileDynamicRulePaged            bool     `json:"reconcile_dynamic_rule_paged"`
	FFmpegPath                           string   `json:"ffmpeg_path"`
	FFprobePath                          string   `json:"ffprobe_path"`
	StreamMode                           string   `json:"stream_mode"`
	StartupTimeout                       string   `json:"startup_timeout"`
	StartupRandomAccessRecoveryOnly      bool     `json:"startup_random_access_recovery_only"`
	MinProbeBytes                        int      `json:"min_probe_bytes"`
	MaxFailovers                         int      `json:"max_failovers"`
	FailoverTotalTimeout                 string   `json:"failover_total_timeout"`
	UpstreamOverlimitCooldown            string   `json:"upstream_overlimit_cooldown"`
	FFmpegReconnectEnabled               bool     `json:"ffmpeg_reconnect_enabled"`
	FFmpegReconnectDelayMax              string   `json:"ffmpeg_reconnect_delay_max"`
	FFmpegReconnectMaxRetries            int      `json:"ffmpeg_reconnect_max_retries"`
	FFmpegReconnectHTTPErrors            string   `json:"ffmpeg_reconnect_http_errors"`
	FFmpegRWTimeout                      string   `json:"ffmpeg_rw_timeout"`
	FFmpegStartupProbeSize               int      `json:"ffmpeg_startup_probesize"`
	FFmpegStartupAnalyzeDuration         string   `json:"ffmpeg_startup_analyzeduration"`
	FFmpegInputBufferSize                int      `json:"ffmpeg_input_buffer_size"`
	FFmpegDiscardCorrupt                 bool     `json:"ffmpeg_discard_corrupt"`
	FFmpegCopyRegenerateTimestamps       bool     `json:"ffmpeg_copy_regenerate_timestamps"`
	FFmpegSourceLogLevel                 string   `json:"ffmpeg_source_log_level"`
	FFmpegSourceStderrPassthroughEnabled bool     `json:"ffmpeg_source_stderr_passthrough_enabled"`
	FFmpegSourceStderrLogLevel           string   `json:"ffmpeg_source_stderr_log_level"`
	FFmpegSourceStderrMaxLineBytes       int      `json:"ffmpeg_source_stderr_max_line_bytes"`
	ProducerReadRate                     string   `json:"producer_readrate"`
	ProducerReadRateCatchup              string   `json:"producer_readrate_catchup"`
	ProducerInitialBurst                 int      `json:"producer_initial_burst"`
	BufferChunkBytes                     int      `json:"buffer_chunk_bytes"`
	BufferFlushInterval                  string   `json:"buffer_flush_interval"`
	BufferTSAlign188                     bool     `json:"buffer_ts_align_188"`
	StallDetect                          string   `json:"stall_detect"`
	StallHardDeadline                    string   `json:"stall_hard_deadline"`
	StallPolicy                          string   `json:"stall_policy"`
	StallMaxFailovers                    int      `json:"stall_max_failovers"`
	CycleFailureMinHealth                string   `json:"cycle_failure_min_health"`
	RecoveryFillerEnabled                bool     `json:"recovery_filler_enabled"`
	RecoveryFillerMode                   string   `json:"recovery_filler_mode"`
	RecoveryFillerInterval               string   `json:"recovery_filler_interval"`
	RecoveryFillerText                   string   `json:"recovery_filler_text"`
	RecoveryFillerEnableAudio            bool     `json:"recovery_filler_enable_audio"`
	SubscriberJoinLag                    int      `json:"subscriber_join_lag"`
	SubscriberSlowPolicy                 string   `json:"subscriber_slow_policy"`
	SubscriberMaxBlocked                 string   `json:"subscriber_max_blocked"`
	SessionIdleTimeout                   string   `json:"session_idle_timeout"`
	SessionDrainTimeout                  string   `json:"session_drain_timeout"`
	SessionMaxSubscribers                int      `json:"session_max_subscribers"`
	SessionHistoryLimit                  int      `json:"session_history_limit"`
	SessionSourceHistoryLimit            int      `json:"session_source_history_limit"`
	SessionSubscriberHistoryLimit        int      `json:"session_subscriber_history_limit"`
	SourceHealthDrainTimeout             string   `json:"source_health_drain_timeout"`
	PreemptSettleDelay                   string   `json:"preempt_settle_delay"`
	AutoPrioritizeProbeTuneDelay         string   `json:"auto_prioritize_probe_tune_delay"`
	AutoPrioritizeWorkers                string   `json:"auto_prioritize_workers"`
	ProbeInterval                        string   `json:"probe_interval"`
	ProbeTimeout                         string   `json:"probe_timeout"`
	AdminAuthConfigured                  bool     `json:"admin_auth_configured"`
	AdminJSONBodyLimitBytes              int64    `json:"admin_json_body_limit_bytes"`
	DVRLineupReloadTimeout               string   `json:"dvr_lineup_reload_timeout"`
	RequestTimeout                       string   `json:"request_timeout"`
	RateLimitRPS                         string   `json:"rate_limit_rps"`
	RateLimitBurst                       int      `json:"rate_limit_burst"`
	RateLimitMaxClients                  int      `json:"rate_limit_max_clients"`
	RateLimitTrustedProxies              []string `json:"rate_limit_trusted_proxies,omitempty"`
	TuneBackoffMaxTunes                  int      `json:"tune_backoff_max_tunes"`
	TuneBackoffInterval                  string   `json:"tune_backoff_interval"`
	TuneBackoffCooldown                  string   `json:"tune_backoff_cooldown"`
	EnableMetrics                        bool     `json:"enable_metrics"`
	HTTPRequestLogEnabled                bool     `json:"http_request_log_enabled"`
	LogLevel                             string   `json:"log_level"`
	CatalogSearchMaxTerms                int      `json:"catalog_search_max_terms"`
	CatalogSearchMaxDisjuncts            int      `json:"catalog_search_max_disjuncts"`
	CatalogSearchMaxTermRunes            int      `json:"catalog_search_max_term_runes"`
}

func (c Config) Redacted() RedactedConfig {
	return RedactedConfig{
		PlaylistURLConfigured:                strings.TrimSpace(c.PlaylistURL) != "",
		DBPath:                               c.DBPath,
		LogDir:                               c.LogDir,
		HTTPAddr:                             c.HTTPAddr,
		LegacyHTTPAddr:                       c.LegacyHTTPAddr,
		UPnPEnabled:                          c.UPnPEnabled,
		UPnPAddr:                             c.UPnPAddr,
		UPnPNotifyInterval:                   c.UPnPNotifyInterval.String(),
		UPnPMaxAge:                           c.UPnPMaxAge.String(),
		UPnPContentDirectoryUpdateIDCacheTTL: c.UPnPContentDirectoryUpdateIDCacheTTL.String(),
		PrimaryTunerCount:                    c.PrimaryTunerCount,
		TunerCount:                           c.TunerCount,
		PlaylistSourceCount:                  len(c.PlaylistSources),
		PlaylistSourcesStartupAuthoritative:  c.PlaylistSourcesStartupAuthoritative,
		DiscoveryTunerCount:                  c.DiscoveryAdvertisedTunerCount(),
		DiscoveryTunerCountCapped:            c.DiscoveryTunerCountCapped(),
		TraditionalGuideStart:                c.TraditionalGuideStart,
		FriendlyName:                         c.FriendlyName,
		DeviceID:                             c.DeviceID,
		DeviceAuthConfigured:                 strings.TrimSpace(c.DeviceAuth) != "",
		RefreshSchedule:                      c.RefreshSchedule,
		PlaylistSyncSourceConcurrency:        c.PlaylistSyncSourceConcurrency,
		ReconcileDynamicRulePaged:            c.ReconcileDynamicRulePaged,
		FFmpegPath:                           c.FFmpegPath,
		FFprobePath:                          c.FFprobePath,
		StreamMode:                           c.StreamMode,
		StartupTimeout:                       c.StartupTimeout.String(),
		StartupRandomAccessRecoveryOnly:      c.StartupRandomAccessRecoveryOnly,
		MinProbeBytes:                        c.MinProbeBytes,
		MaxFailovers:                         c.MaxFailovers,
		FailoverTotalTimeout:                 c.FailoverTotalTimeout.String(),
		UpstreamOverlimitCooldown:            c.UpstreamOverlimitCooldown.String(),
		FFmpegReconnectEnabled:               c.FFmpegReconnectEnabled,
		FFmpegReconnectDelayMax:              c.FFmpegReconnectDelayMax.String(),
		FFmpegReconnectMaxRetries:            c.FFmpegReconnectMaxRetries,
		FFmpegReconnectHTTPErrors:            c.FFmpegReconnectHTTPErrors,
		FFmpegRWTimeout:                      c.FFmpegRWTimeout.String(),
		FFmpegStartupProbeSize:               c.FFmpegStartupProbeSize,
		FFmpegStartupAnalyzeDuration:         c.FFmpegStartupAnalyzeDuration.String(),
		FFmpegInputBufferSize:                c.FFmpegInputBufferSize,
		FFmpegDiscardCorrupt:                 c.FFmpegDiscardCorrupt,
		FFmpegCopyRegenerateTimestamps:       c.FFmpegCopyRegenerateTimestamps,
		FFmpegSourceLogLevel:                 c.FFmpegSourceLogLevel,
		FFmpegSourceStderrPassthroughEnabled: c.FFmpegSourceStderrPassthroughEnabled,
		FFmpegSourceStderrLogLevel:           c.FFmpegSourceStderrLogLevel,
		FFmpegSourceStderrMaxLineBytes:       c.FFmpegSourceStderrMaxLineBytes,
		ProducerReadRate:                     strconv.FormatFloat(c.ProducerReadRate, 'f', -1, 64),
		ProducerReadRateCatchup:              strconv.FormatFloat(c.ProducerReadRateCatchup, 'f', -1, 64),
		ProducerInitialBurst:                 c.ProducerInitialBurst,
		BufferChunkBytes:                     c.BufferChunkBytes,
		BufferFlushInterval:                  c.BufferFlushInterval.String(),
		BufferTSAlign188:                     c.BufferTSAlign188,
		StallDetect:                          c.StallDetect.String(),
		StallHardDeadline:                    c.StallHardDeadline.String(),
		StallPolicy:                          c.StallPolicy,
		StallMaxFailovers:                    c.StallMaxFailovers,
		CycleFailureMinHealth:                c.CycleFailureMinHealth.String(),
		RecoveryFillerEnabled:                c.RecoveryFillerEnabled,
		RecoveryFillerMode:                   c.RecoveryFillerMode,
		RecoveryFillerInterval:               c.RecoveryFillerInterval.String(),
		RecoveryFillerText:                   c.RecoveryFillerText,
		RecoveryFillerEnableAudio:            c.RecoveryFillerEnableAudio,
		SubscriberJoinLag:                    c.SubscriberJoinLag,
		SubscriberSlowPolicy:                 c.SubscriberSlowPolicy,
		SubscriberMaxBlocked:                 c.SubscriberMaxBlocked.String(),
		SessionIdleTimeout:                   c.SessionIdleTimeout.String(),
		SessionDrainTimeout:                  c.SessionDrainTimeout.String(),
		SessionMaxSubscribers:                c.SessionMaxSubscribers,
		SessionHistoryLimit:                  c.SessionHistoryLimit,
		SessionSourceHistoryLimit:            c.SessionSourceHistoryLimit,
		SessionSubscriberHistoryLimit:        c.SessionSubscriberHistoryLimit,
		SourceHealthDrainTimeout:             c.SourceHealthDrainTimeout.String(),
		PreemptSettleDelay:                   c.PreemptSettleDelay.String(),
		AutoPrioritizeProbeTuneDelay:         c.AutoPrioritizeProbeTuneDelay.String(),
		AutoPrioritizeWorkers:                c.AutoPrioritizeWorkers,
		ProbeInterval:                        c.ProbeInterval.String(),
		ProbeTimeout:                         c.ProbeTimeout.String(),
		AdminAuthConfigured:                  strings.TrimSpace(c.AdminBasicAuth) != "",
		AdminJSONBodyLimitBytes:              c.AdminJSONBodyLimitBytes,
		DVRLineupReloadTimeout:               c.DVRLineupReloadTimeout.String(),
		RequestTimeout:                       c.RequestTimeout.String(),
		RateLimitRPS:                         strconv.FormatFloat(c.RateLimitRPS, 'f', -1, 64),
		RateLimitBurst:                       c.RateLimitBurst,
		RateLimitMaxClients:                  c.RateLimitMaxClients,
		RateLimitTrustedProxies:              append([]string(nil), c.RateLimitTrustedProxies...),
		TuneBackoffMaxTunes:                  c.TuneBackoffMaxTunes,
		TuneBackoffInterval:                  c.TuneBackoffInterval.String(),
		TuneBackoffCooldown:                  c.TuneBackoffCooldown.String(),
		CatalogSearchMaxTerms:                c.CatalogSearchMaxTerms,
		CatalogSearchMaxDisjuncts:            c.CatalogSearchMaxDisjuncts,
		CatalogSearchMaxTermRunes:            c.CatalogSearchMaxTermRunes,
		EnableMetrics:                        c.EnableMetrics,
		HTTPRequestLogEnabled:                c.HTTPRequestLogEnabled,
		LogLevel:                             c.LogLevel,
	}
}

func Load(args []string) (Config, error) {
	refreshSchedule := strings.TrimSpace(getenv("REFRESH_SCHEDULE", ""))
	defaultLogDir := defaultWorkingDirectory()
	playlistSourceSpecsRaw, err := splitPlaylistSourceSpecs(strings.TrimSpace(os.Getenv("PLAYLIST_SOURCES")))
	if err != nil {
		return Config{}, fmt.Errorf("parse PLAYLIST_SOURCES: %w", err)
	}
	legacyRefreshIntervalRaw := strings.TrimSpace(os.Getenv("REFRESH_INTERVAL"))
	legacyRefreshInterval := time.Duration(0)
	if refreshSchedule == "" && legacyRefreshIntervalRaw != "" {
		parsed, err := time.ParseDuration(legacyRefreshIntervalRaw)
		if err != nil {
			return Config{}, fmt.Errorf("parse REFRESH_INTERVAL: %w", err)
		}
		legacyRefreshInterval = parsed
	}

	cfg := Config{
		PlaylistURL:                          getenv("PLAYLIST_URL", ""),
		DBPath:                               getenv("DB_PATH", "./hdhr-iptv.db"),
		LogDir:                               getenv("LOG_DIR", defaultLogDir),
		HTTPAddr:                             getenv("HTTP_ADDR", ":5004"),
		LegacyHTTPAddr:                       getenv("HTTP_ADDR_LEGACY", ""),
		UPnPEnabled:                          getenvBool("UPNP_ENABLED", true),
		UPnPAddr:                             getenv("UPNP_ADDR", ":1900"),
		UPnPNotifyInterval:                   getenvDuration("UPNP_NOTIFY_INTERVAL", 5*time.Minute),
		UPnPMaxAge:                           getenvDuration("UPNP_MAX_AGE", 30*time.Minute),
		UPnPContentDirectoryUpdateIDCacheTTL: getenvDuration("UPNP_CONTENT_DIRECTORY_UPDATE_ID_CACHE_TTL", defaultUPnPContentDirectoryUpdateIDCacheTTL),
		TunerCount:                           getenvInt("TUNER_COUNT", 2),
		PlaylistSourcesStartupAuthoritative:  getenvBool("PLAYLIST_SOURCES_STARTUP_AUTHORITATIVE", false),
		TraditionalGuideStart:                getenvInt("TRADITIONAL_GUIDE_START", defaultTraditionalGuideStart),
		FriendlyName:                         getenv("FRIENDLY_NAME", "HDHR IPTV"),
		DeviceID:                             getenv("DEVICE_ID", ""),
		DeviceAuth:                           getenv("DEVICE_AUTH", ""),
		RefreshSchedule:                      refreshSchedule,
		PlaylistSyncSourceConcurrency:        getenvInt("PLAYLIST_SYNC_SOURCE_CONCURRENCY", defaultPlaylistSyncSourceConcurrency),
		ReconcileDynamicRulePaged:            getenvBool("RECONCILE_DYNAMIC_RULE_PAGED", false),
		FFmpegPath:                           getenv("FFMPEG_PATH", "ffmpeg"),
		FFprobePath:                          getenv("FFPROBE_PATH", "ffprobe"),
		StreamMode:                           getenv("STREAM_MODE", streamModeFFmpegCopy),
		StartupTimeout:                       getenvDuration("STARTUP_TIMEOUT", 12*time.Second),
		StartupRandomAccessRecoveryOnly:      getenvBool("STARTUP_RANDOM_ACCESS_RECOVERY_ONLY", true),
		MinProbeBytes:                        getenvInt("MIN_PROBE_BYTES", 940),
		MaxFailovers:                         getenvInt("MAX_FAILOVERS", 3),
		FailoverTotalTimeout:                 getenvDuration("FAILOVER_TOTAL_TIMEOUT", 32*time.Second),
		UpstreamOverlimitCooldown:            getenvDuration("UPSTREAM_OVERLIMIT_COOLDOWN", 3*time.Second),
		FFmpegReconnectEnabled:               getenvBool("FFMPEG_RECONNECT_ENABLED", true),
		FFmpegReconnectDelayMax:              getenvDuration("FFMPEG_RECONNECT_DELAY_MAX", 3*time.Second),
		FFmpegReconnectMaxRetries:            getenvInt("FFMPEG_RECONNECT_MAX_RETRIES", 1),
		FFmpegReconnectHTTPErrors:            getenv("FFMPEG_RECONNECT_HTTP_ERRORS", ""),
		FFmpegRWTimeout:                      getenvDuration("FFMPEG_RW_TIMEOUT", defaultFFmpegRWTimeout),
		FFmpegStartupProbeSize:               getenvInt("FFMPEG_STARTUP_PROBESIZE_BYTES", defaultFFmpegStartupProbeSizeBytes),
		FFmpegStartupAnalyzeDuration:         getenvDuration("FFMPEG_STARTUP_ANALYZEDURATION", defaultFFmpegStartupAnalyzeDuration),
		FFmpegInputBufferSize:                getenvInt("FFMPEG_INPUT_BUFFER_SIZE", 0),
		FFmpegDiscardCorrupt:                 getenvBool("FFMPEG_DISCARD_CORRUPT", true),
		FFmpegCopyRegenerateTimestamps:       getenvBool("FFMPEG_COPY_REGENERATE_TIMESTAMPS", true),
		FFmpegSourceLogLevel:                 getenv("FFMPEG_SOURCE_LOG_LEVEL", defaultFFmpegSourceLogLevel),
		FFmpegSourceStderrPassthroughEnabled: getenvBool("FFMPEG_SOURCE_STDERR_PASSTHROUGH_ENABLED", defaultFFmpegSourceStderrPassthroughEnabled),
		FFmpegSourceStderrLogLevel:           getenv("FFMPEG_SOURCE_STDERR_LOG_LEVEL", defaultFFmpegSourceStderrLogLevel),
		FFmpegSourceStderrMaxLineBytes:       getenvInt("FFMPEG_SOURCE_STDERR_MAX_LINE_BYTES", defaultFFmpegSourceStderrMaxLineBytes),
		ProducerReadRate:                     getenvFloat("PRODUCER_READRATE", 1),
		ProducerReadRateCatchup:              getenvFloat("PRODUCER_READRATE_CATCHUP", 1.75),
		ProducerInitialBurst:                 getenvInt("PRODUCER_INITIAL_BURST", 10),
		BufferChunkBytes:                     getenvInt("BUFFER_CHUNK_BYTES", 64*1024),
		BufferFlushInterval:                  getenvDuration("BUFFER_PUBLISH_FLUSH_INTERVAL", 20*time.Millisecond),
		BufferTSAlign188:                     getenvBool("BUFFER_TS_ALIGN_188", true),
		StallDetect:                          getenvDuration("STALL_DETECT", 4*time.Second),
		StallHardDeadline:                    getenvDuration("STALL_HARD_DEADLINE", 32*time.Second),
		StallPolicy:                          getenv("STALL_POLICY", "failover_source"),
		StallMaxFailovers:                    getenvInt("STALL_MAX_FAILOVERS_PER_STALL", 3),
		CycleFailureMinHealth:                getenvDuration("CYCLE_FAILURE_MIN_HEALTH", 20*time.Second),
		RecoveryFillerEnabled:                getenvBool("RECOVERY_FILLER_ENABLED", true),
		RecoveryFillerMode:                   getenv("RECOVERY_FILLER_MODE", "slate_av"),
		RecoveryFillerInterval:               getenvDuration("RECOVERY_FILLER_INTERVAL", 200*time.Millisecond),
		RecoveryFillerText:                   getenv("RECOVERY_FILLER_TEXT", defaultRecoveryFillerText),
		RecoveryFillerEnableAudio:            getenvBool("RECOVERY_FILLER_ENABLE_AUDIO", true),
		SubscriberJoinLag:                    getenvInt("SUBSCRIBER_JOIN_LAG_BYTES", 8*1024*1024),
		SubscriberSlowPolicy:                 getenv("SUBSCRIBER_SLOW_CLIENT_POLICY", "disconnect"),
		SubscriberMaxBlocked:                 getenvDuration("SUBSCRIBER_MAX_BLOCKED_WRITE", 6*time.Second),
		SessionIdleTimeout:                   getenvDuration("SESSION_IDLE_TIMEOUT", 5*time.Second),
		SessionDrainTimeout:                  getenvDuration("SESSION_DRAIN_TIMEOUT", 2*time.Second),
		SessionMaxSubscribers:                getenvInt("SESSION_MAX_SUBSCRIBERS", 0),
		SessionHistoryLimit:                  getenvInt("SESSION_HISTORY_LIMIT", 0),
		SessionSourceHistoryLimit:            getenvInt("SESSION_SOURCE_HISTORY_LIMIT", 0),
		SessionSubscriberHistoryLimit:        getenvInt("SESSION_SUBSCRIBER_HISTORY_LIMIT", 0),
		SourceHealthDrainTimeout:             getenvDuration("SOURCE_HEALTH_DRAIN_TIMEOUT", 0),
		PreemptSettleDelay:                   getenvDuration("PREEMPT_SETTLE_DELAY", 500*time.Millisecond),
		AutoPrioritizeProbeTuneDelay:         getenvDuration("AUTO_PRIORITIZE_PROBE_TUNE_DELAY", 1*time.Second),
		AutoPrioritizeWorkers:                getenv("AUTO_PRIORITIZE_WORKERS", "2"),
		ProbeInterval:                        getenvDuration("PROBE_INTERVAL", 0),
		ProbeTimeout:                         getenvDuration("PROBE_TIMEOUT", 3*time.Second),
		AdminBasicAuth:                       getenv("ADMIN_AUTH", ""),
		AdminJSONBodyLimitBytes:              getenvInt64("ADMIN_JSON_BODY_LIMIT_BYTES", 1<<20),
		DVRLineupReloadTimeout:               getenvDuration("DVR_LINEUP_RELOAD_TIMEOUT", defaultDVRLineupReloadTimeout),
		RequestTimeout:                       getenvDuration("REQUEST_TIMEOUT", 15*time.Second),
		RateLimitRPS:                         getenvFloat("RATE_LIMIT_RPS", 8),
		RateLimitBurst:                       getenvInt("RATE_LIMIT_BURST", 32),
		RateLimitMaxClients:                  getenvInt("RATE_LIMIT_MAX_CLIENTS", 4096),
		RateLimitTrustedProxies:              getenvCSV("RATE_LIMIT_TRUSTED_PROXIES"),
		TuneBackoffMaxTunes:                  getenvInt("TUNE_BACKOFF_MAX_TUNES", 8),
		TuneBackoffInterval:                  getenvDuration("TUNE_BACKOFF_INTERVAL", 1*time.Minute),
		TuneBackoffCooldown:                  getenvDuration("TUNE_BACKOFF_COOLDOWN", 20*time.Second),
		CatalogSearchMaxTerms:                getenvInt("CATALOG_SEARCH_MAX_TERMS", defaultCatalogSearchMaxTerms),
		CatalogSearchMaxDisjuncts:            getenvInt("CATALOG_SEARCH_MAX_DISJUNCTS", defaultCatalogSearchMaxDisjuncts),
		CatalogSearchMaxTermRunes:            getenvInt("CATALOG_SEARCH_MAX_TERM_RUNES", defaultCatalogSearchMaxTermRunes),
		EnableMetrics:                        getenvBool("ENABLE_METRICS", false),
		HTTPRequestLogEnabled:                getenvBool("HTTP_REQUEST_LOG_ENABLED", false),
		LogLevel:                             getenv("LOG_LEVEL", "info"),
		playlistSourceSpecsRaw:               append([]string(nil), playlistSourceSpecsRaw...),
	}

	fs := pflag.NewFlagSet("hdhriptv", pflag.ContinueOnError)
	fs.StringVar(&cfg.PlaylistURL, "playlist-url", cfg.PlaylistURL, "M3U playlist URL")
	fs.StringArrayVar(
		&cfg.playlistSourceSpecsRaw,
		"playlist-source",
		cfg.playlistSourceSpecsRaw,
		"Additional playlist source spec (repeatable): url=<playlist_url>,tuners=<count>[,name=<label>][,enabled=<bool>]",
	)
	fs.StringVar(&cfg.DBPath, "db-path", cfg.DBPath, "SQLite DB path")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP listen address")
	fs.StringVar(&cfg.LegacyHTTPAddr, "http-addr-legacy", cfg.LegacyHTTPAddr, "Legacy HTTP listen address (often :80)")
	fs.BoolVar(&cfg.UPnPEnabled, "upnp-enabled", cfg.UPnPEnabled, "Enable UPnP/SSDP discovery responder")
	fs.StringVar(&cfg.UPnPAddr, "upnp-addr", cfg.UPnPAddr, "UPnP/SSDP UDP listen address")
	fs.DurationVar(&cfg.UPnPNotifyInterval, "upnp-notify-interval", cfg.UPnPNotifyInterval, "UPnP NOTIFY alive interval (0 disables periodic announcements)")
	fs.DurationVar(&cfg.UPnPMaxAge, "upnp-max-age", cfg.UPnPMaxAge, "UPnP SSDP max-age advertised in CACHE-CONTROL")
	fs.DurationVar(&cfg.UPnPContentDirectoryUpdateIDCacheTTL, "upnp-content-directory-update-id-cache-ttl", cfg.UPnPContentDirectoryUpdateIDCacheTTL, "UPnP ContentDirectory GetSystemUpdateID cache TTL")
	fs.IntVar(&cfg.TunerCount, "tuner-count", cfg.TunerCount, "Number of emulated tuners")
	fs.BoolVar(
		&cfg.PlaylistSourcesStartupAuthoritative,
		"playlist-sources-startup-authoritative",
		cfg.PlaylistSourcesStartupAuthoritative,
		"When --playlist-source/PLAYLIST_SOURCES is provided, prune persisted non-primary sources not declared in startup config",
	)
	fs.IntVar(&cfg.TraditionalGuideStart, "traditional-guide-start", cfg.TraditionalGuideStart, "First guide number used when assigning traditional channels")
	fs.StringVar(&cfg.FriendlyName, "friendly-name", cfg.FriendlyName, "Device friendly name")
	fs.StringVar(&cfg.DeviceID, "device-id", cfg.DeviceID, "8 hex character device ID")
	fs.StringVar(&cfg.DeviceAuth, "device-auth", cfg.DeviceAuth, "Device auth token")
	fs.StringVar(&cfg.RefreshSchedule, "refresh-schedule", cfg.RefreshSchedule, "Playlist sync cron schedule (5-field or optional-seconds 6-field)")
	fs.IntVar(&cfg.PlaylistSyncSourceConcurrency, "playlist-sync-source-concurrency", cfg.PlaylistSyncSourceConcurrency, "Bounded worker count for playlist source refreshes during all-source sync runs (1 keeps sequential behavior)")
	fs.BoolVar(&cfg.ReconcileDynamicRulePaged, "reconcile-dynamic-rule-paged", cfg.ReconcileDynamicRulePaged, "Enable paged catalog-filter sync for one-off dynamic reconcile rules (default false uses legacy slice mode)")
	fs.DurationVar(&legacyRefreshInterval, "refresh-interval", legacyRefreshInterval, "Deprecated: playlist refresh interval duration (converted to --refresh-schedule when representable)")
	fs.StringVar(&cfg.FFmpegPath, "ffmpeg-path", cfg.FFmpegPath, "Path to ffmpeg executable")
	fs.StringVar(&cfg.FFprobePath, "ffprobe-path", cfg.FFprobePath, "Path to ffprobe executable")
	fs.StringVar(&cfg.StreamMode, "stream-mode", cfg.StreamMode, "Stream mode: direct|ffmpeg-copy|ffmpeg-transcode")
	fs.DurationVar(&cfg.StartupTimeout, "startup-timeout", cfg.StartupTimeout, "Per-source startup timeout before failover")
	fs.BoolVar(&cfg.StartupRandomAccessRecoveryOnly, "startup-random-access-recovery-only", cfg.StartupRandomAccessRecoveryOnly, "When enabled, enforce startup random-access gating only during recovery cycles (not initial startup)")
	fs.IntVar(&cfg.MinProbeBytes, "min-probe-bytes", cfg.MinProbeBytes, "Minimum startup bytes required before committing stream")
	fs.IntVar(&cfg.MaxFailovers, "max-failovers", cfg.MaxFailovers, "Maximum fallback attempts after primary source (0 to try all)")
	fs.DurationVar(&cfg.FailoverTotalTimeout, "failover-total-timeout", cfg.FailoverTotalTimeout, "Total timeout budget for startup failover attempts")
	fs.DurationVar(&cfg.UpstreamOverlimitCooldown, "upstream-overlimit-cooldown", cfg.UpstreamOverlimitCooldown, "Per-provider-scope cooldown after upstream 429 startup failures before retrying the same provider scope")
	fs.BoolVar(&cfg.FFmpegReconnectEnabled, "ffmpeg-reconnect-enabled", cfg.FFmpegReconnectEnabled, "Enable ffmpeg input reconnect flags for ffmpeg stream modes")
	fs.DurationVar(&cfg.FFmpegReconnectDelayMax, "ffmpeg-reconnect-delay-max", cfg.FFmpegReconnectDelayMax, "Maximum ffmpeg reconnect delay between attempts for ffmpeg stream modes")
	fs.IntVar(&cfg.FFmpegReconnectMaxRetries, "ffmpeg-reconnect-max-retries", cfg.FFmpegReconnectMaxRetries, "Maximum ffmpeg reconnect attempts per input URL (-1 uses ffmpeg defaults/unlimited)")
	fs.StringVar(&cfg.FFmpegReconnectHTTPErrors, "ffmpeg-reconnect-http-errors", cfg.FFmpegReconnectHTTPErrors, "Comma-separated HTTP classes/codes for ffmpeg reconnect_on_http_error (for example 404,429,5xx)")
	fs.DurationVar(&cfg.FFmpegRWTimeout, "ffmpeg-rw-timeout", cfg.FFmpegRWTimeout, "FFmpeg input rw_timeout for I/O operations in ffmpeg stream modes (0 disables)")
	fs.IntVar(&cfg.FFmpegStartupProbeSize, "ffmpeg-startup-probesize-bytes", cfg.FFmpegStartupProbeSize, "FFmpeg input probesize in bytes used during startup analysis for ffmpeg stream modes (runtime floor applies)")
	fs.DurationVar(&cfg.FFmpegStartupAnalyzeDuration, "ffmpeg-startup-analyzeduration", cfg.FFmpegStartupAnalyzeDuration, "FFmpeg input analyzeduration during startup for ffmpeg stream modes (runtime floor applies)")
	fs.IntVar(&cfg.FFmpegInputBufferSize, "ffmpeg-input-buffer-size", cfg.FFmpegInputBufferSize, "FFmpeg input buffer size in bytes for ffmpeg stream modes (0 disables, max 64 MiB)")
	fs.BoolVar(&cfg.FFmpegDiscardCorrupt, "ffmpeg-discard-corrupt", cfg.FFmpegDiscardCorrupt, "Enable ffmpeg input discard-corrupt handling using -fflags +discardcorrupt")
	fs.BoolVar(&cfg.FFmpegCopyRegenerateTimestamps, "ffmpeg-copy-regenerate-timestamps", cfg.FFmpegCopyRegenerateTimestamps, "Enable ffmpeg-copy timestamp regeneration using -fflags +genpts")
	fs.StringVar(&cfg.FFmpegSourceLogLevel, "ffmpeg-source-log-level", cfg.FFmpegSourceLogLevel, "Source-session ffmpeg input log level: error|warning|info|debug")
	fs.BoolVar(&cfg.FFmpegSourceStderrPassthroughEnabled, "ffmpeg-source-stderr-passthrough-enabled", cfg.FFmpegSourceStderrPassthroughEnabled, "Emit source-session ffmpeg stderr lines into application logs")
	fs.StringVar(&cfg.FFmpegSourceStderrLogLevel, "ffmpeg-source-stderr-log-level", cfg.FFmpegSourceStderrLogLevel, "Application log level for source-session ffmpeg stderr pass-through: debug|info|warn")
	fs.IntVar(&cfg.FFmpegSourceStderrMaxLineBytes, "ffmpeg-source-stderr-max-line-bytes", cfg.FFmpegSourceStderrMaxLineBytes, "Maximum bytes retained per source-session ffmpeg stderr line before truncation")
	fs.Float64Var(&cfg.ProducerReadRate, "producer-readrate", cfg.ProducerReadRate, "FFmpeg producer readrate value for shared sessions")
	fs.Float64Var(&cfg.ProducerReadRateCatchup, "producer-readrate-catchup", cfg.ProducerReadRateCatchup, "FFmpeg producer catch-up readrate value for shared sessions (-readrate_catchup)")
	fs.IntVar(&cfg.ProducerInitialBurst, "producer-initial-burst", cfg.ProducerInitialBurst, "FFmpeg producer initial burst seconds for shared sessions")
	fs.IntVar(&cfg.BufferChunkBytes, "buffer-chunk-bytes", cfg.BufferChunkBytes, "Shared stream chunk publish size in bytes")
	fs.DurationVar(&cfg.BufferFlushInterval, "buffer-publish-flush-interval", cfg.BufferFlushInterval, "Shared stream chunk flush interval")
	fs.BoolVar(&cfg.BufferTSAlign188, "buffer-ts-align-188", cfg.BufferTSAlign188, "Align shared-stream chunk publishes to 188-byte MPEG-TS packet boundaries")
	fs.DurationVar(&cfg.StallDetect, "stall-detect", cfg.StallDetect, "No-publish duration threshold before shared stream recovery")
	fs.DurationVar(&cfg.StallHardDeadline, "stall-hard-deadline", cfg.StallHardDeadline, "Maximum recovery window after stall detection before closing session")
	fs.StringVar(&cfg.StallPolicy, "stall-policy", cfg.StallPolicy, "Shared stream stall policy: failover_source|restart_same|close_session")
	fs.IntVar(&cfg.StallMaxFailovers, "stall-max-failovers-per-stall", cfg.StallMaxFailovers, "Maximum failover attempts per detected shared-session stall")
	fs.DurationVar(&cfg.CycleFailureMinHealth, "cycle-failure-min-health", cfg.CycleFailureMinHealth, "Minimum healthy duration for recovery-selected sources before cycle-close failures are persisted (0 disables)")
	fs.BoolVar(&cfg.RecoveryFillerEnabled, "recovery-filler-enabled", cfg.RecoveryFillerEnabled, "Enable recovery keepalive filler while shared sessions recover")
	fs.StringVar(&cfg.RecoveryFillerMode, "recovery-filler-mode", cfg.RecoveryFillerMode, "Recovery keepalive mode: null|psi|slate_av")
	fs.DurationVar(&cfg.RecoveryFillerInterval, "recovery-filler-interval", cfg.RecoveryFillerInterval, "Interval between packet-based keepalive chunks (null/psi modes)")
	fs.StringVar(&cfg.RecoveryFillerText, "recovery-filler-text", cfg.RecoveryFillerText, "Text rendered by slate_av recovery filler")
	fs.BoolVar(&cfg.RecoveryFillerEnableAudio, "recovery-filler-enable-audio", cfg.RecoveryFillerEnableAudio, "Emit silent AAC audio in slate_av recovery filler mode")
	fs.IntVar(&cfg.SubscriberJoinLag, "subscriber-join-lag-bytes", cfg.SubscriberJoinLag, "Initial shared stream lag window in bytes for new subscribers")
	fs.StringVar(&cfg.SubscriberSlowPolicy, "subscriber-slow-client-policy", cfg.SubscriberSlowPolicy, "Slow client policy: disconnect|skip")
	fs.DurationVar(&cfg.SubscriberMaxBlocked, "subscriber-max-blocked-write", cfg.SubscriberMaxBlocked, "Max blocked write duration per chunk for shared stream subscribers (0 to disable)")
	fs.DurationVar(&cfg.SessionIdleTimeout, "session-idle-timeout", cfg.SessionIdleTimeout, "Shared channel session idle timeout after last subscriber leaves")
	fs.DurationVar(&cfg.SessionDrainTimeout, "session-drain-timeout", cfg.SessionDrainTimeout, "Timeout budget for bounded shared-session reader close operations")
	fs.IntVar(&cfg.SessionMaxSubscribers, "session-max-subscribers", cfg.SessionMaxSubscribers, "Maximum subscribers per shared channel session (0 for unlimited)")
	fs.IntVar(&cfg.SessionHistoryLimit, "session-history-limit", cfg.SessionHistoryLimit, "Shared-session history retention cap for lifecycle snapshots (0 uses runtime default)")
	fs.IntVar(&cfg.SessionSourceHistoryLimit, "session-source-history-limit", cfg.SessionSourceHistoryLimit, "Per-session source history retention cap (0 falls back to session-history-limit/runtime defaults)")
	fs.IntVar(&cfg.SessionSubscriberHistoryLimit, "session-subscriber-history-limit", cfg.SessionSubscriberHistoryLimit, "Per-session subscriber history retention cap (0 falls back to session-history-limit/runtime defaults)")
	fs.DurationVar(&cfg.SourceHealthDrainTimeout, "source-health-drain-timeout", cfg.SourceHealthDrainTimeout, "Timeout budget for source-health persistence drain during session teardown (0 uses runtime default)")
	fs.DurationVar(&cfg.PreemptSettleDelay, "preempt-settle-delay", cfg.PreemptSettleDelay, "Delay before refilling from full tuner usage back to full usage (also applied to preempted slot reuse)")
	fs.DurationVar(&cfg.AutoPrioritizeProbeTuneDelay, "auto-prioritize-probe-tune-delay", cfg.AutoPrioritizeProbeTuneDelay, "Delay between automated probe tune attempts (auto-prioritize and background source probing)")
	fs.StringVar(&cfg.AutoPrioritizeWorkers, "auto-prioritize-workers", cfg.AutoPrioritizeWorkers, "Auto-prioritize worker policy: auto or a fixed positive integer")
	fs.DurationVar(&cfg.ProbeInterval, "probe-interval", cfg.ProbeInterval, "Background source probe interval (0 to disable)")
	fs.DurationVar(&cfg.ProbeTimeout, "probe-timeout", cfg.ProbeTimeout, "Per-source timeout for background probes")
	fs.StringVar(&cfg.AdminBasicAuth, "admin-auth", cfg.AdminBasicAuth, "Admin basic auth in user:pass format")
	fs.Int64Var(&cfg.AdminJSONBodyLimitBytes, "admin-json-body-limit-bytes", cfg.AdminJSONBodyLimitBytes, "Maximum JSON request body bytes accepted by admin mutation endpoints")
	fs.DurationVar(&cfg.DVRLineupReloadTimeout, "dvr-lineup-reload-timeout", cfg.DVRLineupReloadTimeout, "Timeout budget for coalesced DVR lineup reload runs triggered by admin lineup-changing mutations")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "HTTP request timeout for metadata/admin routes (0 to disable)")
	fs.Float64Var(&cfg.RateLimitRPS, "rate-limit-rps", cfg.RateLimitRPS, "Per-client request rate limit in requests/second (0 to disable)")
	fs.IntVar(&cfg.RateLimitBurst, "rate-limit-burst", cfg.RateLimitBurst, "Per-client request burst size")
	fs.IntVar(&cfg.RateLimitMaxClients, "rate-limit-max-clients", cfg.RateLimitMaxClients, "Maximum in-memory distinct client IP limiter entries (0 disables cap)")
	fs.StringSliceVar(&cfg.RateLimitTrustedProxies, "rate-limit-trusted-proxies", cfg.RateLimitTrustedProxies, "Comma-separated trusted proxy CIDR/IP entries used to honor Forwarded/X-Forwarded-For/X-Real-IP for rate-limit identity")
	fs.IntVar(&cfg.TuneBackoffMaxTunes, "tune-backoff-max-tunes", cfg.TuneBackoffMaxTunes, "Per-channel startup-failure threshold within tune-backoff-interval before tune-backoff-cooldown is applied (0 to disable)")
	fs.DurationVar(&cfg.TuneBackoffInterval, "tune-backoff-interval", cfg.TuneBackoffInterval, "Rolling window used to count per-channel startup failures for tune backoff")
	fs.DurationVar(&cfg.TuneBackoffCooldown, "tune-backoff-cooldown", cfg.TuneBackoffCooldown, "Per-channel cooldown duration applied when tune backoff threshold is exceeded")
	fs.IntVar(&cfg.CatalogSearchMaxTerms, "catalog-search-max-terms", cfg.CatalogSearchMaxTerms, "Maximum token-mode search terms (0 uses default)")
	fs.IntVar(&cfg.CatalogSearchMaxDisjuncts, "catalog-search-max-disjuncts", cfg.CatalogSearchMaxDisjuncts, "Maximum OR disjunct clauses in token-mode search (0 uses default)")
	fs.IntVar(&cfg.CatalogSearchMaxTermRunes, "catalog-search-max-term-runes", cfg.CatalogSearchMaxTermRunes, "Maximum runes retained per token in token-mode search (0 uses default)")
	fs.BoolVar(&cfg.EnableMetrics, "enable-metrics", cfg.EnableMetrics, "Expose Prometheus metrics endpoint at /metrics")
	fs.BoolVar(&cfg.HTTPRequestLogEnabled, "http-request-log-enabled", cfg.HTTPRequestLogEnabled, "Enable per-request HTTP access logs")
	fs.StringVar(&cfg.LogDir, "log-dir", cfg.LogDir, "Directory for startup timestamped log files")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: trace|debug|info|warn|error")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.Changed("refresh-schedule") && fs.Changed("refresh-interval") {
		return Config{}, fmt.Errorf("cannot set both --refresh-schedule and --refresh-interval")
	}

	if fs.Changed("refresh-interval") {
		schedule, err := intervalToCron(legacyRefreshInterval)
		if err != nil {
			return Config{}, fmt.Errorf("convert --refresh-interval to --refresh-schedule: %w", err)
		}
		cfg.RefreshSchedule = schedule
	} else if !fs.Changed("refresh-schedule") && cfg.RefreshSchedule == "" && legacyRefreshInterval > 0 {
		schedule, err := intervalToCron(legacyRefreshInterval)
		if err != nil {
			return Config{}, fmt.Errorf("convert REFRESH_INTERVAL to REFRESH_SCHEDULE: %w", err)
		}
		cfg.RefreshSchedule = schedule
	}

	if err := cfg.normalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) normalize() error {
	c.PlaylistURL = strings.TrimSpace(c.PlaylistURL)
	c.DBPath = strings.TrimSpace(c.DBPath)
	c.LogDir = strings.TrimSpace(c.LogDir)
	c.HTTPAddr = strings.TrimSpace(c.HTTPAddr)
	c.LegacyHTTPAddr = strings.TrimSpace(c.LegacyHTTPAddr)
	c.UPnPAddr = strings.TrimSpace(c.UPnPAddr)
	c.FriendlyName = strings.TrimSpace(c.FriendlyName)
	c.DeviceID = strings.TrimSpace(c.DeviceID)
	c.DeviceAuth = strings.TrimSpace(c.DeviceAuth)
	c.RefreshSchedule = strings.TrimSpace(c.RefreshSchedule)
	c.FFmpegPath = strings.TrimSpace(c.FFmpegPath)
	c.FFprobePath = strings.TrimSpace(c.FFprobePath)
	c.FFmpegReconnectHTTPErrors = strings.TrimSpace(c.FFmpegReconnectHTTPErrors)
	c.StreamMode = strings.ToLower(strings.TrimSpace(c.StreamMode))
	c.StallPolicy = strings.ToLower(strings.TrimSpace(c.StallPolicy))
	c.RecoveryFillerMode = strings.ToLower(strings.TrimSpace(c.RecoveryFillerMode))
	if c.RecoveryFillerMode == "slate-av" {
		c.RecoveryFillerMode = "slate_av"
	}
	c.RecoveryFillerText = strings.TrimSpace(c.RecoveryFillerText)
	c.SubscriberSlowPolicy = strings.ToLower(strings.TrimSpace(c.SubscriberSlowPolicy))
	c.AutoPrioritizeWorkers = strings.ToLower(strings.TrimSpace(c.AutoPrioritizeWorkers))
	c.LogLevel = strings.ToLower(strings.TrimSpace(c.LogLevel))
	c.FFmpegSourceLogLevel = normalizeFFmpegSourceLogLevel(c.FFmpegSourceLogLevel)
	c.FFmpegSourceStderrLogLevel = normalizeFFmpegSourceStderrLogLevel(c.FFmpegSourceStderrLogLevel)

	normalizedTrustedProxies, err := normalizeTrustedProxyCIDRs(c.RateLimitTrustedProxies)
	if err != nil {
		return fmt.Errorf("rate-limit-trusted-proxies: %w", err)
	}
	c.RateLimitTrustedProxies = normalizedTrustedProxies

	if c.DBPath == "" {
		c.DBPath = "./hdhr-iptv.db"
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":5004"
	}
	if c.UPnPAddr == "" {
		c.UPnPAddr = ":1900"
	}
	if c.FriendlyName == "" {
		c.FriendlyName = "HDHR IPTV"
	}
	if c.FFmpegPath == "" {
		c.FFmpegPath = "ffmpeg"
	}
	if c.FFprobePath == "" {
		c.FFprobePath = "ffprobe"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogDir == "" {
		c.LogDir = defaultWorkingDirectory()
	}
	if c.RecoveryFillerMode == "" {
		c.RecoveryFillerMode = "slate_av"
	}
	if c.RecoveryFillerText == "" {
		c.RecoveryFillerText = defaultRecoveryFillerText
	}
	if c.AutoPrioritizeWorkers == "" {
		c.AutoPrioritizeWorkers = "2"
	}
	c.FFmpegStartupProbeSize = normalizeFFmpegStartupProbeSize(c.FFmpegStartupProbeSize)
	c.FFmpegStartupAnalyzeDuration = normalizeFFmpegStartupAnalyzeDuration(c.FFmpegStartupAnalyzeDuration)

	if c.TunerCount < 1 {
		return fmt.Errorf("tuner-count must be at least 1")
	}
	resolvedPlaylistSources, err := resolvePlaylistSources(
		c.PlaylistURL,
		c.TunerCount,
		c.playlistSourceSpecsRaw,
	)
	if err != nil {
		return err
	}
	c.PrimaryTunerCount = c.TunerCount
	c.PlaylistSources = resolvedPlaylistSources
	c.TunerCount = totalEnabledPlaylistSourceTuners(c.PlaylistSources)
	if c.TunerCount < 1 {
		return fmt.Errorf("at least one enabled playlist source is required")
	}
	if c.TraditionalGuideStart < 1 {
		return fmt.Errorf("traditional-guide-start must be at least 1")
	}
	if c.TraditionalGuideStart >= dynamicGuideStart {
		return fmt.Errorf("traditional-guide-start must be less than %d", dynamicGuideStart)
	}
	if c.UPnPNotifyInterval < 0 {
		return fmt.Errorf("upnp-notify-interval must be zero or positive")
	}
	if c.UPnPMaxAge <= 0 {
		return fmt.Errorf("upnp-max-age must be positive")
	}
	if c.UPnPContentDirectoryUpdateIDCacheTTL <= 0 {
		return fmt.Errorf("upnp-content-directory-update-id-cache-ttl must be positive")
	}
	if c.RefreshSchedule != "" {
		if err := validateCron(c.RefreshSchedule); err != nil {
			return fmt.Errorf("refresh-schedule is invalid: %w", err)
		}
	}
	if c.PlaylistSyncSourceConcurrency < 1 {
		return fmt.Errorf("playlist-sync-source-concurrency must be at least 1")
	}
	if c.PlaylistSyncSourceConcurrency > maxPlaylistSyncSourceConcurrency {
		return fmt.Errorf("playlist-sync-source-concurrency cannot exceed %d", maxPlaylistSyncSourceConcurrency)
	}
	if c.StartupTimeout <= 0 {
		return fmt.Errorf("startup-timeout must be positive")
	}
	if c.MinProbeBytes < 1 {
		return fmt.Errorf("min-probe-bytes must be at least 1")
	}
	if c.MaxFailovers < 0 {
		return fmt.Errorf("max-failovers must be zero or positive")
	}
	if c.FailoverTotalTimeout <= 0 {
		return fmt.Errorf("failover-total-timeout must be positive")
	}
	if c.UpstreamOverlimitCooldown < 0 {
		return fmt.Errorf("upstream-overlimit-cooldown must be zero or positive")
	}
	if c.FFmpegReconnectDelayMax < 0 {
		return fmt.Errorf("ffmpeg-reconnect-delay-max must be zero or positive")
	}
	if c.FFmpegReconnectMaxRetries < -1 {
		return fmt.Errorf("ffmpeg-reconnect-max-retries must be -1 or greater")
	}
	if c.FFmpegRWTimeout < 0 {
		return fmt.Errorf("ffmpeg-rw-timeout must be zero or positive")
	}
	if c.FFmpegInputBufferSize < 0 {
		return fmt.Errorf("ffmpeg-input-buffer-size must be zero or positive")
	}
	if c.FFmpegInputBufferSize > maxFFmpegInputBufferSize {
		return fmt.Errorf(
			"ffmpeg-input-buffer-size must be less than or equal to %d bytes (64 MiB)",
			maxFFmpegInputBufferSize,
		)
	}
	if c.FFmpegSourceStderrMaxLineBytes <= 0 {
		return fmt.Errorf("ffmpeg-source-stderr-max-line-bytes must be positive")
	}
	switch c.FFmpegSourceLogLevel {
	case "error", "warning", "info", "debug":
	default:
		return fmt.Errorf("invalid ffmpeg-source-log-level %q", c.FFmpegSourceLogLevel)
	}
	switch c.FFmpegSourceStderrLogLevel {
	case "debug", "info", "warn":
	default:
		return fmt.Errorf("invalid ffmpeg-source-stderr-log-level %q", c.FFmpegSourceStderrLogLevel)
	}
	if c.ProducerReadRate <= 0 {
		return fmt.Errorf("producer-readrate must be positive")
	}
	if c.ProducerReadRateCatchup <= 0 {
		return fmt.Errorf("producer-readrate-catchup must be positive")
	}
	if c.ProducerReadRateCatchup < c.ProducerReadRate {
		return fmt.Errorf("producer-readrate-catchup must be greater than or equal to producer-readrate")
	}
	if c.ProducerInitialBurst < 0 {
		return fmt.Errorf("producer-initial-burst must be zero or positive")
	}
	if c.BufferChunkBytes < 1 {
		return fmt.Errorf("buffer-chunk-bytes must be at least 1")
	}
	if c.BufferFlushInterval <= 0 {
		return fmt.Errorf("buffer-publish-flush-interval must be positive")
	}
	if c.StallDetect <= 0 {
		return fmt.Errorf("stall-detect must be positive")
	}
	if c.StallHardDeadline <= 0 {
		return fmt.Errorf("stall-hard-deadline must be positive")
	}
	switch c.StallPolicy {
	case "failover_source", "restart_same", "close_session":
	default:
		return fmt.Errorf("invalid stall-policy %q", c.StallPolicy)
	}
	if c.StallMaxFailovers < 0 {
		return fmt.Errorf("stall-max-failovers-per-stall must be zero or positive")
	}
	if c.CycleFailureMinHealth < 0 {
		return fmt.Errorf("cycle-failure-min-health must be zero or positive")
	}
	if c.RecoveryFillerInterval <= 0 {
		return fmt.Errorf("recovery-filler-interval must be positive")
	}
	switch c.RecoveryFillerMode {
	case "null", "psi", "slate_av":
	default:
		return fmt.Errorf("invalid recovery-filler-mode %q", c.RecoveryFillerMode)
	}
	if c.SubscriberJoinLag < 0 {
		return fmt.Errorf("subscriber-join-lag-bytes must be zero or positive")
	}
	switch c.SubscriberSlowPolicy {
	case "disconnect", "skip":
	default:
		return fmt.Errorf("invalid subscriber-slow-client-policy %q", c.SubscriberSlowPolicy)
	}
	if c.SubscriberMaxBlocked < 0 {
		return fmt.Errorf("subscriber-max-blocked-write must be zero or positive")
	}
	if c.SessionIdleTimeout < 0 {
		return fmt.Errorf("session-idle-timeout must be zero or positive")
	}
	if c.SessionDrainTimeout <= 0 {
		return fmt.Errorf("session-drain-timeout must be positive")
	}
	if c.SessionMaxSubscribers < 0 {
		return fmt.Errorf("session-max-subscribers must be zero or positive")
	}
	if c.SessionHistoryLimit < 0 {
		return fmt.Errorf("session-history-limit must be zero or positive")
	}
	if c.SessionSourceHistoryLimit < 0 {
		return fmt.Errorf("session-source-history-limit must be zero or positive")
	}
	if c.SessionSubscriberHistoryLimit < 0 {
		return fmt.Errorf("session-subscriber-history-limit must be zero or positive")
	}
	if c.SourceHealthDrainTimeout < 0 {
		return fmt.Errorf("source-health-drain-timeout must be zero or positive")
	}
	if c.PreemptSettleDelay < 0 {
		return fmt.Errorf("preempt-settle-delay must be zero or positive")
	}
	if c.AutoPrioritizeProbeTuneDelay < 0 {
		return fmt.Errorf("auto-prioritize-probe-tune-delay must be zero or positive")
	}
	normalizedWorkers, err := normalizeAutoPrioritizeWorkers(c.AutoPrioritizeWorkers)
	if err != nil {
		return fmt.Errorf("auto-prioritize-workers must be \"auto\" or a positive integer")
	}
	c.AutoPrioritizeWorkers = normalizedWorkers
	if c.ProbeInterval < 0 {
		return fmt.Errorf("probe-interval must be zero or positive")
	}
	if c.ProbeTimeout <= 0 {
		return fmt.Errorf("probe-timeout must be positive")
	}
	if c.ProbeInterval > 0 && c.ProbeTimeout > c.ProbeInterval {
		return fmt.Errorf("probe-timeout must be less than or equal to probe-interval when probing is enabled")
	}
	if c.AdminJSONBodyLimitBytes <= 0 {
		return fmt.Errorf("admin-json-body-limit-bytes must be positive")
	}
	if c.DVRLineupReloadTimeout <= 0 {
		return fmt.Errorf("dvr-lineup-reload-timeout must be positive")
	}
	if c.RequestTimeout < 0 {
		return fmt.Errorf("request-timeout must be zero or positive")
	}
	if c.RateLimitRPS < 0 {
		return fmt.Errorf("rate-limit-rps must be zero or positive")
	}
	if c.RateLimitRPS > 0 && c.RateLimitBurst < 1 {
		return fmt.Errorf("rate-limit-burst must be at least 1 when rate limiting is enabled")
	}
	if c.RateLimitMaxClients < 0 {
		return fmt.Errorf("rate-limit-max-clients must be zero or positive")
	}
	if c.TuneBackoffMaxTunes < 0 {
		return fmt.Errorf("tune-backoff-max-tunes must be zero or positive")
	}
	if c.TuneBackoffInterval < 0 {
		return fmt.Errorf("tune-backoff-interval must be zero or positive")
	}
	if c.TuneBackoffCooldown < 0 {
		return fmt.Errorf("tune-backoff-cooldown must be zero or positive")
	}
	if c.TuneBackoffMaxTunes > 0 {
		if c.TuneBackoffInterval <= 0 {
			return fmt.Errorf("tune-backoff-interval must be positive when tune-backoff-max-tunes is enabled")
		}
		if c.TuneBackoffCooldown <= 0 {
			return fmt.Errorf("tune-backoff-cooldown must be positive when tune-backoff-max-tunes is enabled")
		}
	}
	if c.CatalogSearchMaxTerms < 0 {
		return fmt.Errorf("catalog-search-max-terms must be zero or positive")
	}
	if c.CatalogSearchMaxTerms == 0 {
		c.CatalogSearchMaxTerms = defaultCatalogSearchMaxTerms
	}
	if c.CatalogSearchMaxTerms > maxCatalogSearchMaxTermsLimit {
		return fmt.Errorf("catalog-search-max-terms cannot exceed %d", maxCatalogSearchMaxTermsLimit)
	}
	if c.CatalogSearchMaxDisjuncts < 0 {
		return fmt.Errorf("catalog-search-max-disjuncts must be zero or positive")
	}
	if c.CatalogSearchMaxDisjuncts == 0 {
		c.CatalogSearchMaxDisjuncts = defaultCatalogSearchMaxDisjuncts
	}
	if c.CatalogSearchMaxDisjuncts > maxCatalogSearchMaxDisjunctsLimit {
		return fmt.Errorf("catalog-search-max-disjuncts cannot exceed %d", maxCatalogSearchMaxDisjunctsLimit)
	}
	if c.CatalogSearchMaxTermRunes < 0 {
		return fmt.Errorf("catalog-search-max-term-runes must be zero or positive")
	}
	if c.CatalogSearchMaxTermRunes == 0 {
		c.CatalogSearchMaxTermRunes = defaultCatalogSearchMaxTermRunes
	}
	if c.CatalogSearchMaxTermRunes > maxCatalogSearchMaxTermRunesLimit {
		return fmt.Errorf("catalog-search-max-term-runes cannot exceed %d", maxCatalogSearchMaxTermRunesLimit)
	}

	switch c.StreamMode {
	case streamModeDirect, streamModeFFmpegCopy, streamModeFFmpegTranscode:
	default:
		return fmt.Errorf("invalid stream-mode %q", c.StreamMode)
	}

	if c.DeviceID != "" {
		if len(c.DeviceID) != 8 {
			return fmt.Errorf("device-id must be 8 hex characters")
		}
		for _, ch := range c.DeviceID {
			if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') && !(ch >= 'A' && ch <= 'F') {
				return fmt.Errorf("device-id must be hex")
			}
		}
		c.DeviceID = strings.ToUpper(c.DeviceID)
	}

	if c.LogLevel != "trace" && c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("invalid log-level %q", c.LogLevel)
	}

	return nil
}

// DiscoveryAdvertisedTunerCount returns tuner count exposed on discovery endpoints.
func (c Config) DiscoveryAdvertisedTunerCount() int {
	return capDiscoveryAdvertisedTunerCount(c.TunerCount)
}

// DiscoveryTunerCountCapped reports whether discovery is capped below internal capacity.
func (c Config) DiscoveryTunerCountCapped() bool {
	return c.TunerCount > maxDiscoveryTunerAdvertisedCount
}

func normalizeFFmpegSourceLogLevel(v string) string {
	normalized := strings.ToLower(strings.TrimSpace(v))
	switch normalized {
	case "error":
		return "error"
	case "warning", "warn":
		return "warning"
	case "info":
		return "info"
	case "debug":
		return "debug"
	case "":
		return defaultFFmpegSourceLogLevel
	default:
		return normalized
	}
}

func normalizeFFmpegSourceStderrLogLevel(v string) string {
	normalized := strings.ToLower(strings.TrimSpace(v))
	switch normalized {
	case "debug":
		return "debug"
	case "warn", "warning":
		return "warn"
	case "", "info":
		return "info"
	default:
		return normalized
	}
}

func normalizeFFmpegStartupProbeSize(v int) int {
	if v <= 0 {
		return defaultFFmpegStartupProbeSizeBytes
	}
	if v < minFFmpegStartupProbeSizeBytes {
		return minFFmpegStartupProbeSizeBytes
	}
	return v
}

func normalizeFFmpegStartupAnalyzeDuration(v time.Duration) time.Duration {
	if v <= 0 {
		return defaultFFmpegStartupAnalyzeDuration
	}
	if v < minFFmpegStartupAnalyzeDuration {
		return minFFmpegStartupAnalyzeDuration
	}
	return v
}

func capDiscoveryAdvertisedTunerCount(total int) int {
	if total > maxDiscoveryTunerAdvertisedCount {
		return maxDiscoveryTunerAdvertisedCount
	}
	if total < 1 {
		return 1
	}
	return total
}

func splitPlaylistSourceSpecs(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	entries := strings.Split(raw, ";")
	out := make([]string, 0, len(entries))
	for i, entry := range entries {
		spec := strings.TrimSpace(entry)
		if spec == "" {
			return nil, fmt.Errorf("entry %d is empty", i+1)
		}
		out = append(out, spec)
	}
	return out, nil
}

func resolvePlaylistSources(primaryURL string, primaryTunerCount int, additionalSpecs []string) ([]PlaylistSourceConfig, error) {
	sources := make([]PlaylistSourceConfig, 0, 1+len(additionalSpecs))
	sources = append(sources, PlaylistSourceConfig{
		Name:        defaultPrimaryPlaylistSourceName,
		PlaylistURL: strings.TrimSpace(primaryURL),
		TunerCount:  primaryTunerCount,
		Enabled:     true,
	})

	for i, rawSpec := range additionalSpecs {
		spec, err := parsePlaylistSourceSpec(rawSpec, i+1)
		if err != nil {
			return nil, err
		}
		if spec.Name == "" {
			spec.Name = fmt.Sprintf("Source %d", i+2)
		}
		sources = append(sources, spec)
	}

	if err := validatePlaylistSourceUniqueness(sources); err != nil {
		return nil, err
	}
	return sources, nil
}

func parsePlaylistSourceSpec(rawSpec string, index int) (PlaylistSourceConfig, error) {
	spec := PlaylistSourceConfig{Enabled: true}
	rawSpec = strings.TrimSpace(rawSpec)
	if rawSpec == "" {
		return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: spec is required", index)
	}

	seen := make(map[string]struct{})
	parts := strings.Split(rawSpec, ",")
	for _, part := range parts {
		token := strings.TrimSpace(part)
		if token == "" {
			return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: malformed token in %q", index, rawSpec)
		}
		keyValue := strings.SplitN(token, "=", 2)
		if len(keyValue) != 2 {
			return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: expected key=value token %q", index, token)
		}
		key := strings.ToLower(strings.TrimSpace(keyValue[0]))
		value := strings.TrimSpace(keyValue[1])
		if key == "" {
			return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: key is required in token %q", index, token)
		}
		canonicalKey, ok := canonicalPlaylistSourceSpecKey(key)
		if !ok {
			return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: unknown key %q", index, key)
		}
		if _, ok := seen[canonicalKey]; ok {
			return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: duplicate key %q", index, canonicalKey)
		}
		seen[canonicalKey] = struct{}{}

		switch canonicalKey {
		case "playlist_url":
			if value == "" {
				return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: url is required", index)
			}
			spec.PlaylistURL = value
		case "tuner_count":
			tunerCount, err := strconv.Atoi(value)
			if err != nil {
				return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: parse tuners %q: %w", index, value, err)
			}
			if tunerCount < 1 {
				return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: tuners must be at least 1", index)
			}
			spec.TunerCount = tunerCount
		case "name":
			if value == "" {
				return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: name is required when provided", index)
			}
			spec.Name = value
		case "enabled":
			enabled, err := parsePlaylistSourceEnabled(value)
			if err != nil {
				return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: %w", index, err)
			}
			spec.Enabled = enabled
		}
	}

	if strings.TrimSpace(spec.PlaylistURL) == "" {
		return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: url is required", index)
	}
	if spec.TunerCount < 1 {
		return PlaylistSourceConfig{}, fmt.Errorf("playlist-source[%d]: tuners is required and must be at least 1", index)
	}

	spec.Name = strings.TrimSpace(spec.Name)
	spec.PlaylistURL = strings.TrimSpace(spec.PlaylistURL)
	return spec, nil
}

func canonicalPlaylistSourceSpecKey(key string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "url", "playlist_url":
		return "playlist_url", true
	case "tuners", "tuner_count":
		return "tuner_count", true
	case "name":
		return "name", true
	case "enabled":
		return "enabled", true
	default:
		return "", false
	}
}

func parsePlaylistSourceEnabled(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("enabled must be true/false")
	}
}

func validatePlaylistSourceUniqueness(sources []PlaylistSourceConfig) error {
	nameEntries := make(map[string]int, len(sources))
	urlEntries := make(map[string]int, len(sources))

	for i, source := range sources {
		entry := i + 1
		name := strings.TrimSpace(source.Name)
		if name == "" {
			return fmt.Errorf("playlist source entry %d: name is required", entry)
		}
		nameKey := playlist.CanonicalPlaylistSourceName(name)
		if prev, ok := nameEntries[nameKey]; ok {
			return fmt.Errorf("duplicate playlist source name %q between entries %d and %d", name, prev, entry)
		}
		nameEntries[nameKey] = entry

		playlistURL := strings.TrimSpace(source.PlaylistURL)
		if playlistURL == "" {
			continue
		}
		urlKey := playlist.CanonicalPlaylistSourceURL(playlistURL)
		if prev, ok := urlEntries[urlKey]; ok {
			return fmt.Errorf("duplicate playlist source URL %q between entries %d and %d", playlistURL, prev, entry)
		}
		urlEntries[urlKey] = entry
	}
	return nil
}

func totalEnabledPlaylistSourceTuners(sources []PlaylistSourceConfig) int {
	total := 0
	for _, source := range sources {
		if !source.Enabled || source.TunerCount <= 0 {
			continue
		}
		total += source.TunerCount
	}
	return total
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func defaultWorkingDirectory() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	wd = strings.TrimSpace(wd)
	if wd == "" {
		return "."
	}
	return wd
}

func getenvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return parsed
}

func getenvInt64(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return parsed
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return parsed
}

func normalizeAutoPrioritizeWorkers(raw string) (string, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "2", nil
	}
	if raw == "auto" {
		return "auto", nil
	}

	workers, err := strconv.Atoi(raw)
	if err != nil || workers < 1 {
		return "", fmt.Errorf("invalid workers value")
	}
	return strconv.Itoa(workers), nil
}

var scheduleParser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

func validateCron(spec string) error {
	_, err := scheduleParser.Parse(strings.TrimSpace(spec))
	return err
}

func intervalToCron(interval time.Duration) (string, error) {
	if interval <= 0 {
		return "", fmt.Errorf("interval must be positive")
	}
	if interval%time.Minute != 0 {
		return "", fmt.Errorf("interval %s is not representable as cron; use --refresh-schedule", interval)
	}

	minutes := int(interval / time.Minute)
	if minutes < 60 {
		if 60%minutes != 0 {
			return "", fmt.Errorf("interval %s is not representable as stable cron minutes; use --refresh-schedule", interval)
		}
		return fmt.Sprintf("*/%d * * * *", minutes), nil
	}

	if minutes%60 == 0 {
		hours := minutes / 60
		if hours < 24 {
			if 24%hours != 0 {
				return "", fmt.Errorf("interval %s is not representable as stable cron hours; use --refresh-schedule", interval)
			}
			return fmt.Sprintf("0 */%d * * *", hours), nil
		}
		if hours == 24 {
			return "0 0 * * *", nil
		}
	}

	return "", fmt.Errorf("interval %s is not representable as cron; use --refresh-schedule", interval)
}

func getenvCSV(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

func normalizeTrustedProxyCIDRs(entries []string) ([]string, error) {
	normalized := make([]string, 0, len(entries))
	for _, entry := range entries {
		trimmed := strings.TrimSpace(entry)
		if trimmed == "" {
			continue
		}
		if prefix, err := netip.ParsePrefix(trimmed); err == nil {
			normalized = append(normalized, prefix.Masked().String())
			continue
		}
		if addr, err := netip.ParseAddr(trimmed); err == nil {
			addr = addr.Unmap()
			normalized = append(normalized, netip.PrefixFrom(addr, addr.BitLen()).String())
			continue
		}
		return nil, fmt.Errorf("invalid CIDR/IP %q", trimmed)
	}
	return normalized, nil
}

func getenvFloat(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return parsed
}

func getenvBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return parsed
}
