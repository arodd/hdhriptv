package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/arodd/hdhriptv/internal/analyzer"
	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/config"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/hdhr"
	"github.com/arodd/hdhriptv/internal/hdhr/discovery"
	"github.com/arodd/hdhriptv/internal/hdhr/upnp"
	httpapi "github.com/arodd/hdhriptv/internal/http"
	httpmw "github.com/arodd/hdhriptv/internal/http/middleware"
	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/arodd/hdhriptv/internal/logging"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/reconcile"
	"github.com/arodd/hdhriptv/internal/scheduler"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
	"github.com/arodd/hdhriptv/internal/stream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	initialPlaylistSyncAttemptTimeout = 2 * time.Minute
	initialPlaylistSyncRetryBudget    = 3 * time.Minute
	initialPlaylistSyncRetryAttempts  = 4

	initialPlaylistSyncRetryBaseDelay = 1 * time.Second
	initialPlaylistSyncRetryMaxDelay  = 8 * time.Second

	initialPlaylistSyncReadyTimeout  = 10 * time.Second
	initialPlaylistSyncReadyInterval = 100 * time.Millisecond
)

var reInitialPlaylistSyncJellyfin5xx = regexp.MustCompile(`failed:\s*5\d\d\b`)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	logRuntime, err := logging.New(cfg.LogLevel, cfg.LogDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
		os.Exit(2)
	}
	defer func() {
		if closeErr := logRuntime.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "logger close error: %v\n", closeErr)
		}
	}()
	logger := logRuntime.Logger
	slog.SetDefault(logger)

	sqliteOpenStart := time.Now()
	store, err := sqlite.OpenWithOptions(cfg.DBPath, sqlite.SQLiteOptions{
		CatalogSearchLimits: sqlite.CatalogSearchLimits{
			MaxTerms:     cfg.CatalogSearchMaxTerms,
			MaxDisjuncts: cfg.CatalogSearchMaxDisjuncts,
			MaxTermRunes: cfg.CatalogSearchMaxTermRunes,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "store error: %v\n", err)
		os.Exit(1)
	}
	logger.Info(
		"startup phase complete",
		"phase", "sqlite_open_total",
		"duration", time.Since(sqliteOpenStart),
	)
	defer store.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	explicitIdentity := detectExplicitIdentitySettings(os.Args[1:])
	resolveIdentityStart := time.Now()
	if err := resolveIdentitySettings(ctx, store, &cfg, explicitIdentity); err != nil {
		fmt.Fprintf(os.Stderr, "identity settings error: %v\n", err)
		os.Exit(1)
	}
	logger.Info(
		"startup phase complete",
		"phase", "identity_settings_resolve",
		"duration", time.Since(resolveIdentityStart),
	)

	tunerPool := stream.NewPool(cfg.TunerCount)
	tunerPool.SetPreemptSettleDelay(cfg.PreemptSettleDelay)

	automationOverridesStart := time.Now()
	if err := applyAutomationCLIOverrides(ctx, store, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "automation settings error: %v\n", err)
		os.Exit(2)
	}
	logger.Info(
		"startup phase complete",
		"phase", "automation_overrides_sync",
		"duration", time.Since(automationOverridesStart),
	)

	channelsSvc := channels.NewService(store)
	dvrSvc, err := dvr.NewService(store, cfg.DeviceID, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dvr service error: %v\n", err)
		os.Exit(1)
	}
	manager := playlist.NewManager(nil)
	refresher := playlist.NewRefresher(manager, store, logger)

	reconcilerSvc, err := reconcile.New(store, channelsSvc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "playlist reconciler error: %v\n", err)
		os.Exit(1)
	}
	reconcilerSvc.SetLogger(logger)
	reconcilerSvc.SetDynamicRulePagedMode(cfg.ReconcileDynamicRulePaged)
	reconcilerSvc.SetDynamicRuleMatchLimit(channels.DynamicGuideBlockMaxLen)
	playlistSyncJob, err := jobs.NewPlaylistSyncJob(store, refresher, reconcilerSvc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "playlist sync job error: %v\n", err)
		os.Exit(1)
	}
	playlistSyncJob.SetPostSyncLineupReloader(dvrSvc)
	analyzerCfg, err := loadAnalyzerConfig(ctx, store, cfg.FFmpegPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "analyzer config error: %v\n", err)
		os.Exit(1)
	}
	streamAnalyzer, err := analyzer.NewFFmpegAnalyzer(analyzerCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "analyzer init error: %v\n", err)
		os.Exit(1)
	}
	autoWorkersMode := "auto"
	autoWorkersFixed := 0
	if strings.TrimSpace(cfg.AutoPrioritizeWorkers) != "auto" {
		parsedWorkers, parseErr := strconv.Atoi(strings.TrimSpace(cfg.AutoPrioritizeWorkers))
		if parseErr != nil || parsedWorkers < 1 {
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "auto-prioritize workers config error: %v\n", parseErr)
			} else {
				fmt.Fprintf(os.Stderr, "auto-prioritize workers config error: value must be at least 1\n")
			}
			os.Exit(2)
		}
		autoWorkersMode = "fixed"
		autoWorkersFixed = parsedWorkers
	}
	autoPrioritizeJob, err := jobs.NewAutoPrioritizeJob(
		store,
		channelsSvc,
		store,
		streamAnalyzer,
		jobs.AutoPrioritizeOptions{
			WorkerMode:     autoWorkersMode,
			FixedWorkers:   autoWorkersFixed,
			TunerCount:     cfg.TunerCount,
			TunerUsage:     tunerPool,
			ProbeTuneDelay: cfg.AutoPrioritizeProbeTuneDelay,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "auto-prioritize job error: %v\n", err)
		os.Exit(1)
	}

	jobRunner, err := jobs.NewRunner(store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "job runner error: %v\n", err)
		os.Exit(1)
	}
	jobRunner.SetLogger(logger)
	defer jobRunner.Close()

	schedulerSvc, err := scheduler.New(store, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scheduler error: %v\n", err)
		os.Exit(1)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(runCtx context.Context, _ string) error {
		_, startErr := jobRunner.Start(runCtx, jobs.JobPlaylistSync, jobs.TriggerSchedule, playlistSyncJob.Run)
		if errors.Is(startErr, jobs.ErrAlreadyRunning) {
			logger.Warn("scheduled playlist sync skipped because a run is already active")
			return nil
		}
		return startErr
	}); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler register playlist_sync error: %v\n", err)
		os.Exit(1)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(runCtx context.Context, _ string) error {
		_, startErr := jobRunner.Start(runCtx, jobs.JobAutoPrioritize, jobs.TriggerSchedule, autoPrioritizeJob.Run)
		if errors.Is(startErr, jobs.ErrAlreadyRunning) {
			logger.Warn("scheduled auto-prioritize skipped because a run is already active")
			return nil
		}
		return startErr
	}); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler register auto_prioritize error: %v\n", err)
		os.Exit(1)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobDVRLineupSync, func(runCtx context.Context, _ string) error {
		_, startErr := jobRunner.Start(runCtx, jobs.JobDVRLineupSync, jobs.TriggerSchedule, func(jobCtx context.Context, run *jobs.RunContext) error {
			result, syncErr := dvrSvc.Sync(jobCtx, dvr.SyncRequest{DryRun: false})
			if syncErr != nil {
				return syncErr
			}
			if run != nil {
				summary := fmt.Sprintf(
					"updated=%d cleared=%d unchanged=%d unresolved=%d warnings=%d",
					result.UpdatedCount,
					result.ClearedCount,
					result.UnchangedCount,
					result.UnresolvedCount,
					len(result.Warnings),
				)
				_ = run.SetSummary(jobCtx, summary)
			}
			logger.Info(
				"scheduled dvr lineup sync completed",
				"updated", result.UpdatedCount,
				"cleared", result.ClearedCount,
				"unchanged", result.UnchangedCount,
				"unresolved", result.UnresolvedCount,
				"warnings", len(result.Warnings),
			)
			return nil
		})
		if errors.Is(startErr, jobs.ErrAlreadyRunning) {
			logger.Warn("scheduled dvr lineup sync skipped because a run is already active")
			return nil
		}
		return startErr
	}); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler register dvr_lineup_sync error: %v\n", err)
		os.Exit(1)
	}

	dvrScheduleSyncStart := time.Now()
	if err := syncDVRScheduleSettings(ctx, store, dvrSvc); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler dvr sync settings error: %v\n", err)
		os.Exit(1)
	}
	logger.Info(
		"startup phase complete",
		"phase", "dvr_schedule_sync",
		"duration", time.Since(dvrScheduleSyncStart),
	)
	schedulerLoadStart := time.Now()
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler load error: %v\n", err)
		os.Exit(1)
	}
	logger.Info(
		"startup phase complete",
		"phase", "scheduler_load_from_settings",
		"duration", time.Since(schedulerLoadStart),
	)
	schedulerSvc.Start()
	defer func() {
		<-schedulerSvc.Stop().Done()
	}()

	adminHandler, err := httpapi.NewAdminHandler(store, channelsSvc, httpapi.AutomationDeps{
		Settings:  store,
		Scheduler: schedulerSvc,
		Runner:    jobRunner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   playlistSyncJob.Run,
			jobs.JobAutoPrioritize: autoPrioritizeJob.Run,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin handler error: %v\n", err)
		os.Exit(1)
	}
	adminHandler.SetLogger(logger)
	adminHandler.SetJSONBodyLimitBytes(cfg.AdminJSONBodyLimitBytes)
	adminHandler.SetDVRService(dvrSvc)
	adminHandler.SetDVRScheduler(schedulerSvc)

	hdhrHandler := hdhr.NewHandler(hdhr.Config{
		FriendlyName:                     cfg.FriendlyName,
		DeviceID:                         cfg.DeviceID,
		DeviceAuth:                       cfg.DeviceAuth,
		TunerCount:                       cfg.TunerCount,
		ContentDirectoryUpdateIDCacheTTL: cfg.UPnPContentDirectoryUpdateIDCacheTTL,
	}, channelsSvc)
	ffmpegCopyRegenerateTimestamps := cfg.FFmpegCopyRegenerateTimestamps
	streamHandler := stream.NewHandler(stream.Config{
		Mode:                            cfg.StreamMode,
		FFmpegPath:                      cfg.FFmpegPath,
		Logger:                          logger,
		StartupTimeout:                  cfg.StartupTimeout,
		StartupRandomAccessRecoveryOnly: cfg.StartupRandomAccessRecoveryOnly,
		MinProbeBytes:                   cfg.MinProbeBytes,
		MaxFailovers:                    cfg.MaxFailovers,
		FailoverTotalTimeout:            cfg.FailoverTotalTimeout,
		UpstreamOverlimitCooldown:       cfg.UpstreamOverlimitCooldown,
		FFmpegReconnectEnabled:          cfg.FFmpegReconnectEnabled,
		FFmpegReconnectDelayMax:         cfg.FFmpegReconnectDelayMax,
		FFmpegReconnectMaxRetries:       cfg.FFmpegReconnectMaxRetries,
		FFmpegReconnectHTTPErrors:       cfg.FFmpegReconnectHTTPErrors,
		FFmpegStartupProbeSize:          cfg.FFmpegStartupProbeSize,
		FFmpegStartupAnalyzeDelay:       cfg.FFmpegStartupAnalyzeDuration,
		FFmpegInputBufferSize:           cfg.FFmpegInputBufferSize,
		FFmpegDiscardCorrupt:            cfg.FFmpegDiscardCorrupt,
		FFmpegCopyRegenerateTimestamps:  &ffmpegCopyRegenerateTimestamps,
		ProducerReadRate:                cfg.ProducerReadRate,
		ProducerReadRateCatchup:         cfg.ProducerReadRateCatchup,
		ProducerInitialBurst:            cfg.ProducerInitialBurst,
		BufferChunkBytes:                cfg.BufferChunkBytes,
		BufferPublishFlushInterval:      cfg.BufferFlushInterval,
		BufferTSAlign188:                cfg.BufferTSAlign188,
		StallDetect:                     cfg.StallDetect,
		StallHardDeadline:               cfg.StallHardDeadline,
		StallPolicy:                     cfg.StallPolicy,
		StallMaxFailoversPerStall:       cfg.StallMaxFailovers,
		CycleFailureMinHealth:           cfg.CycleFailureMinHealth,
		RecoveryFillerEnabled:           cfg.RecoveryFillerEnabled,
		RecoveryFillerMode:              cfg.RecoveryFillerMode,
		RecoveryFillerInterval:          cfg.RecoveryFillerInterval,
		RecoveryFillerText:              cfg.RecoveryFillerText,
		RecoveryFillerEnableAudio:       cfg.RecoveryFillerEnableAudio,
		SubscriberJoinLagBytes:          cfg.SubscriberJoinLag,
		SubscriberSlowClientPolicy:      cfg.SubscriberSlowPolicy,
		SubscriberMaxBlockedWrite:       cfg.SubscriberMaxBlocked,
		SessionIdleTimeout:              cfg.SessionIdleTimeout,
		SessionDrainTimeout:             cfg.SessionDrainTimeout,
		SessionMaxSubscribers:           cfg.SessionMaxSubscribers,
		SessionHistoryLimit:             cfg.SessionHistoryLimit,
		SessionSourceHistoryLimit:       cfg.SessionSourceHistoryLimit,
		SessionSubscriberHistoryLimit:   cfg.SessionSubscriberHistoryLimit,
		SourceHealthDrainTimeout:        cfg.SourceHealthDrainTimeout,
		TuneBackoffMaxTunes:             cfg.TuneBackoffMaxTunes,
		TuneBackoffInterval:             cfg.TuneBackoffInterval,
		TuneBackoffCooldown:             cfg.TuneBackoffCooldown,
	}, tunerPool, channelsSvc)
	adminHandler.SetTunerStatusProvider(streamHandler)
	adminHandler.SetSourceHealthClearRuntime(streamHandler)

	sourceProber := stream.NewBackgroundProber(stream.ProberConfig{
		Mode:                           cfg.StreamMode,
		FFmpegPath:                     cfg.FFmpegPath,
		Logger:                         logger,
		ProducerReadRate:               cfg.ProducerReadRate,
		ProducerReadRateCatchup:        cfg.ProducerReadRateCatchup,
		ProducerInitialBurst:           cfg.ProducerInitialBurst,
		FFmpegReconnectEnabled:         cfg.FFmpegReconnectEnabled,
		FFmpegReconnectDelayMax:        cfg.FFmpegReconnectDelayMax,
		FFmpegReconnectMaxRetries:      cfg.FFmpegReconnectMaxRetries,
		FFmpegReconnectHTTPErrors:      cfg.FFmpegReconnectHTTPErrors,
		FFmpegStartupProbeSize:         cfg.FFmpegStartupProbeSize,
		FFmpegStartupAnalyzeDelay:      cfg.FFmpegStartupAnalyzeDuration,
		FFmpegInputBufferSize:          cfg.FFmpegInputBufferSize,
		FFmpegDiscardCorrupt:           cfg.FFmpegDiscardCorrupt,
		FFmpegCopyRegenerateTimestamps: &ffmpegCopyRegenerateTimestamps,
		MinProbeBytes:                  cfg.MinProbeBytes,
		ProbeInterval:                  cfg.ProbeInterval,
		ProbeTimeout:                   cfg.ProbeTimeout,
		TunerUsage:                     tunerPool,
		ProbeTuneDelay:                 cfg.AutoPrioritizeProbeTuneDelay,
	}, channelsSvc)

	mux := http.NewServeMux()
	adminHandler.RegisterRoutes(mux, cfg.AdminBasicAuth)
	hdhrHandler.RegisterRoutes(mux)

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.Handle("GET /auto/{guide}", streamHandler)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	if cfg.EnableMetrics {
		mux.Handle("GET /metrics", promhttp.Handler())
	}

	handler := http.Handler(mux)
	handler = httpmw.RateLimitByIPWithConfig(
		cfg.RateLimitRPS,
		cfg.RateLimitBurst,
		httpmw.RateLimitByIPConfig{
			IdleTTL:            10 * time.Minute,
			MaxClients:         cfg.RateLimitMaxClients,
			ExemptPathPrefixes: []string{"/healthz"},
			TrustedProxyCIDRs:  cfg.RateLimitTrustedProxies,
		},
	)(handler)
	handler = httpmw.Timeout(cfg.RequestTimeout, "/auto/")(handler)
	handler = debugRequestLogger(handler, cfg.HTTPRequestLogEnabled)

	discoveryServer, err := discovery.NewServer(discovery.Config{
		DeviceID:       cfg.DeviceID,
		DeviceAuth:     cfg.DeviceAuth,
		TunerCount:     cfg.TunerCount,
		HTTPAddr:       cfg.HTTPAddr,
		LegacyHTTPAddr: cfg.LegacyHTTPAddr,
		Logger:         logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "discovery server error: %v\n", err)
		os.Exit(1)
	}

	var upnpServer *upnp.Server
	if cfg.UPnPEnabled {
		upnpServer, err = upnp.NewServer(upnp.Config{
			ListenAddr:     cfg.UPnPAddr,
			DeviceID:       cfg.DeviceID,
			FriendlyName:   cfg.FriendlyName,
			HTTPAddr:       cfg.HTTPAddr,
			LegacyHTTPAddr: cfg.LegacyHTTPAddr,
			NotifyInterval: cfg.UPnPNotifyInterval,
			MaxAge:         cfg.UPnPMaxAge,
			Logger:         logger,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "upnp server error: %v\n", err)
			os.Exit(1)
		}
	}

	mainHTTPServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	httpServers := []*http.Server{mainHTTPServer}
	serverNames := []string{"http"}

	if cfg.LegacyHTTPAddr != "" && cfg.LegacyHTTPAddr != cfg.HTTPAddr {
		httpServers = append(httpServers, &http.Server{
			Addr:              cfg.LegacyHTTPAddr,
			Handler:           handler,
			ReadTimeout:       30 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second,
		})
		serverNames = append(serverNames, "http_legacy")
	}

	discoveryAddr := ""
	if addr := discoveryServer.Addr(); addr != nil {
		discoveryAddr = addr.String()
	}
	upnpAddr := ""
	if upnpServer != nil {
		if addr := upnpServer.Addr(); addr != nil {
			upnpAddr = addr.String()
		}
	}

	dbRuntimePragmas, err := store.RuntimePragmas(ctx)
	if err != nil {
		logger.Warn("failed to read active sqlite runtime pragmas", "error", err)
	}

	playlistSyncCron, _ := readSettingOrDefault(ctx, store, sqlite.SettingJobsPlaylistSyncCron, "")
	playlistSyncEnabled, _ := readSettingOrDefault(ctx, store, sqlite.SettingJobsPlaylistSyncEnabled, "false")
	autoPrioritizeCron, _ := readSettingOrDefault(ctx, store, sqlite.SettingJobsAutoPrioritizeCron, "")
	autoPrioritizeEnabled, _ := readSettingOrDefault(ctx, store, sqlite.SettingJobsAutoPrioritizeEnabled, "false")
	dvrLineupSyncCron, _ := readSettingOrDefault(ctx, store, sqlite.SettingJobsDVRLineupSyncCron, "")
	dvrLineupSyncEnabled, _ := readSettingOrDefault(ctx, store, sqlite.SettingJobsDVRLineupSyncEnabled, "false")
	playlistURLConfigured := hasPlaylistURLSetting(ctx, store)

	logger.Info("starting server",
		"http_addr", cfg.HTTPAddr,
		"legacy_http_addr", cfg.LegacyHTTPAddr,
		"log_dir", cfg.LogDir,
		"log_file", logRuntime.LogFilePath,
		"db_journal_mode", dbRuntimePragmas.JournalMode,
		"db_synchronous", dbRuntimePragmas.Synchronous,
		"db_busy_timeout_ms", dbRuntimePragmas.BusyTimeoutMS,
		"udp_discovery_addr", discoveryAddr,
		"upnp_enabled", cfg.UPnPEnabled,
		"upnp_addr", cfg.UPnPAddr,
		"upnp_runtime_addr", upnpAddr,
		"upnp_notify_interval", cfg.UPnPNotifyInterval.String(),
		"upnp_max_age", cfg.UPnPMaxAge.String(),
		"playlist_url_configured", playlistURLConfigured,
		"reconcile_dynamic_rule_paged", cfg.ReconcileDynamicRulePaged,
		"reconcile_dynamic_rule_match_limit", channels.DynamicGuideBlockMaxLen,
		"playlist_sync_enabled", strings.EqualFold(strings.TrimSpace(playlistSyncEnabled), "true"),
		"playlist_sync_cron", playlistSyncCron,
		"auto_prioritize_enabled", strings.EqualFold(strings.TrimSpace(autoPrioritizeEnabled), "true"),
		"auto_prioritize_cron", autoPrioritizeCron,
		"dvr_lineup_sync_enabled", strings.EqualFold(strings.TrimSpace(dvrLineupSyncEnabled), "true"),
		"dvr_lineup_sync_cron", dvrLineupSyncCron,
		"friendly_name", cfg.FriendlyName,
		"device_id", cfg.DeviceID,
		"tuner_count", cfg.TunerCount,
		"stream_mode", cfg.StreamMode,
		"startup_timeout", cfg.StartupTimeout.String(),
		"startup_random_access_recovery_only", cfg.StartupRandomAccessRecoveryOnly,
		"min_probe_bytes", cfg.MinProbeBytes,
		"max_failovers", cfg.MaxFailovers,
		"failover_total_timeout", cfg.FailoverTotalTimeout.String(),
		"upstream_overlimit_cooldown", cfg.UpstreamOverlimitCooldown.String(),
		"ffmpeg_reconnect_enabled", cfg.FFmpegReconnectEnabled,
		"ffmpeg_reconnect_delay_max", cfg.FFmpegReconnectDelayMax.String(),
		"ffmpeg_reconnect_max_retries", cfg.FFmpegReconnectMaxRetries,
		"ffmpeg_reconnect_http_errors", cfg.FFmpegReconnectHTTPErrors,
		"ffmpeg_startup_probesize_bytes", cfg.FFmpegStartupProbeSize,
		"ffmpeg_startup_analyzeduration", cfg.FFmpegStartupAnalyzeDuration.String(),
		"ffmpeg_input_buffer_size", cfg.FFmpegInputBufferSize,
		"ffmpeg_discard_corrupt", cfg.FFmpegDiscardCorrupt,
		"ffmpeg_copy_regenerate_timestamps", cfg.FFmpegCopyRegenerateTimestamps,
		"producer_readrate", cfg.ProducerReadRate,
		"producer_readrate_catchup", cfg.ProducerReadRateCatchup,
		"producer_initial_burst", cfg.ProducerInitialBurst,
		"buffer_chunk_bytes", cfg.BufferChunkBytes,
		"buffer_publish_flush_interval", cfg.BufferFlushInterval.String(),
		"buffer_ts_align_188", cfg.BufferTSAlign188,
		"stall_detect", cfg.StallDetect.String(),
		"stall_hard_deadline", cfg.StallHardDeadline.String(),
		"stall_policy", cfg.StallPolicy,
		"stall_max_failovers_per_stall", cfg.StallMaxFailovers,
		"cycle_failure_min_health", cfg.CycleFailureMinHealth.String(),
		"recovery_filler_enabled", cfg.RecoveryFillerEnabled,
		"recovery_filler_mode", cfg.RecoveryFillerMode,
		"recovery_filler_interval", cfg.RecoveryFillerInterval.String(),
		"recovery_transition_strategy", "default",
		"recovery_filler_text", cfg.RecoveryFillerText,
		"recovery_filler_enable_audio", cfg.RecoveryFillerEnableAudio,
		"subscriber_join_lag_bytes", cfg.SubscriberJoinLag,
		"subscriber_slow_client_policy", cfg.SubscriberSlowPolicy,
		"subscriber_max_blocked_write", cfg.SubscriberMaxBlocked.String(),
		"session_idle_timeout", cfg.SessionIdleTimeout.String(),
		"session_max_subscribers", cfg.SessionMaxSubscribers,
		"preempt_settle_delay", cfg.PreemptSettleDelay.String(),
		"auto_prioritize_probe_tune_delay", cfg.AutoPrioritizeProbeTuneDelay.String(),
		"auto_prioritize_workers", cfg.AutoPrioritizeWorkers,
		"probe_interval", cfg.ProbeInterval.String(),
		"probe_timeout", cfg.ProbeTimeout.String(),
		"admin_json_body_limit_bytes", cfg.AdminJSONBodyLimitBytes,
		"request_timeout", cfg.RequestTimeout.String(),
		"http_request_log_enabled", cfg.HTTPRequestLogEnabled,
		"rate_limit_rps", cfg.RateLimitRPS,
		"rate_limit_burst", cfg.RateLimitBurst,
		"rate_limit_max_clients", cfg.RateLimitMaxClients,
		"tune_backoff_max_tunes", cfg.TuneBackoffMaxTunes,
		"tune_backoff_interval", cfg.TuneBackoffInterval.String(),
		"tune_backoff_cooldown", cfg.TuneBackoffCooldown.String(),
		"metrics_enabled", cfg.EnableMetrics,
	)

	componentErrCh := make(chan error, len(httpServers)+2)
	var wg sync.WaitGroup

	for i := range httpServers {
		srv := httpServers[i]
		name := serverNames[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				componentErrCh <- fmt.Errorf("%s listener failed: %w", name, err)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := discoveryServer.Serve(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
			componentErrCh <- fmt.Errorf("udp discovery failed: %w", err)
		}
	}()

	if upnpServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := upnpServer.Serve(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
				componentErrCh <- fmt.Errorf("upnp discovery failed: %w", err)
			}
		}()
	}

	if sourceProber.Enabled() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// BackgroundProber.Run is ticker-only; shutdown ownership stays in
			// main via sourceProber.Close() below.
			sourceProber.Run(ctx)
		}()
	}

	if playlistURLConfigured {
		if err := runInitialPlaylistSyncAfterListenerStart(ctx, logger, cfg.HTTPAddr, func(syncCtx context.Context) error {
			return runAndWaitPlaylistSync(syncCtx, jobRunner, playlistSyncJob.Run)
		}); err != nil {
			logger.Error("initial playlist sync failed", "error", err)
		}
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-componentErrCh:
		logger.Error("runtime component failed", "error", runErr)
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	for _, srv := range httpServers {
		if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http shutdown failed", "addr", srv.Addr, "error", err)
			// Force-close active connections when graceful shutdown times out.
			srv.Close()
		}
	}
	if err := discoveryServer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		logger.Error("udp discovery shutdown failed", "error", err)
	}
	if upnpServer != nil {
		if err := upnpServer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("upnp discovery shutdown failed", "error", err)
		}
	}
	// Caller-owned prober shutdown: close and wait for queued probe session
	// close drains before stream handler/store teardown continues.
	sourceProber.Close()

	// Cancel all active stream sessions so they release tuner leases and stop
	// touching the database before store.Close() runs.
	if deadline, ok := shutdownCtx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		logger.Info(
			"stream session shutdown starting",
			"shutdown_budget_remaining", remaining,
		)
	}
	if err := streamHandler.CloseWithContext(shutdownCtx); err != nil {
		logger.Warn("stream session shutdown did not fully converge before deadline", "error", err)
	}

	// Wait for admin dynamic sync background workers to finish before
	// store.Close() runs.
	adminHandler.Close()

	wg.Wait()

	// jobRunner.Close() is deferred above and waits for in-flight FinishRun
	// persistence. store.Close() is deferred even earlier, so the ordering is:
	// scheduler stop -> jobRunner.Close() (waits for persistence) -> store.Close()

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", runErr)
		os.Exit(1)
	}

	logger.Info("server stopped")
}

type identityExplicitSettings struct {
	FriendlyName bool
	DeviceID     bool
	DeviceAuth   bool
}

func detectExplicitIdentitySettings(args []string) identityExplicitSettings {
	return identityExplicitSettings{
		FriendlyName: settingExplicitlyProvided(args, "--friendly-name", "FRIENDLY_NAME"),
		DeviceID:     settingExplicitlyProvided(args, "--device-id", "DEVICE_ID"),
		DeviceAuth:   settingExplicitlyProvided(args, "--device-auth", "DEVICE_AUTH"),
	}
}

func settingExplicitlyProvided(args []string, flagName, envKey string) bool {
	if flagPresent, flagNonEmpty := lookupFlagSetting(args, flagName); flagPresent {
		return flagNonEmpty
	}
	value, ok := os.LookupEnv(envKey)
	return ok && strings.TrimSpace(value) != ""
}

func lookupFlagSetting(args []string, flagName string) (present bool, nonEmpty bool) {
	if strings.TrimSpace(flagName) == "" {
		return false, false
	}
	prefix := flagName + "="
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if arg == flagName {
			present = true
			value := ""
			if i+1 < len(args) {
				value = args[i+1]
				i++
			}
			nonEmpty = strings.TrimSpace(value) != ""
			continue
		}
		if strings.HasPrefix(arg, prefix) {
			present = true
			nonEmpty = strings.TrimSpace(strings.TrimPrefix(arg, prefix)) != ""
		}
	}
	return present, nonEmpty
}

func resolveIdentitySettings(
	ctx context.Context,
	store *sqlite.Store,
	cfg *config.Config,
	explicit identityExplicitSettings,
) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	if cfg == nil {
		return fmt.Errorf("config is required")
	}

	friendlyName, err := readSettingOrDefault(ctx, store, sqlite.SettingIdentityFriendlyName, "")
	if err != nil {
		return fmt.Errorf("load persisted friendly name: %w", err)
	}
	if explicit.FriendlyName || strings.TrimSpace(friendlyName) == "" {
		friendlyName = strings.TrimSpace(cfg.FriendlyName)
	}
	if strings.TrimSpace(friendlyName) == "" {
		friendlyName = "HDHR IPTV"
	}

	deviceID, err := readSettingOrDefault(ctx, store, sqlite.SettingIdentityDeviceID, "")
	if err != nil {
		return fmt.Errorf("load persisted device id: %w", err)
	}
	deviceID = normalizeDeviceID(deviceID)
	if explicit.DeviceID || deviceID == "" {
		deviceID = normalizeDeviceID(cfg.DeviceID)
	}
	if deviceID == "" {
		generated, genErr := randomHexBytes(4)
		if genErr != nil {
			return fmt.Errorf("generate device id: %w", genErr)
		}
		deviceID = strings.ToUpper(generated)
	}

	deviceAuth, err := readSettingOrDefault(ctx, store, sqlite.SettingIdentityDeviceAuth, "")
	if err != nil {
		return fmt.Errorf("load persisted device auth: %w", err)
	}
	deviceAuth = strings.TrimSpace(deviceAuth)
	if explicit.DeviceAuth || deviceAuth == "" {
		deviceAuth = strings.TrimSpace(cfg.DeviceAuth)
	}
	if deviceAuth == "" {
		generated, genErr := randomHexBytes(16)
		if genErr != nil {
			return fmt.Errorf("generate device auth: %w", genErr)
		}
		deviceAuth = generated
	}

	cfg.FriendlyName = friendlyName
	cfg.DeviceID = deviceID
	cfg.DeviceAuth = deviceAuth

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingIdentityFriendlyName: friendlyName,
		sqlite.SettingIdentityDeviceID:     deviceID,
		sqlite.SettingIdentityDeviceAuth:   deviceAuth,
	}); err != nil {
		return fmt.Errorf("persist identity settings: %w", err)
	}

	return nil
}

func normalizeDeviceID(raw string) string {
	id := strings.ToUpper(strings.TrimSpace(raw))
	if len(id) != 8 {
		return ""
	}
	for _, ch := range id {
		isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
		if !isHex {
			return ""
		}
	}
	return id
}

func randomHexBytes(n int) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("byte length must be positive")
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func applyAutomationCLIOverrides(
	ctx context.Context,
	store *sqlite.Store,
	cfg config.Config,
) error {
	updates := map[string]string{}
	if playlistURL := strings.TrimSpace(cfg.PlaylistURL); playlistURL != "" {
		updates[sqlite.SettingPlaylistURL] = playlistURL
	}
	if schedule := strings.TrimSpace(cfg.RefreshSchedule); schedule != "" {
		updates[sqlite.SettingJobsPlaylistSyncCron] = schedule
		updates[sqlite.SettingJobsPlaylistSyncEnabled] = "true"
	}
	if len(updates) == 0 {
		return nil
	}
	return store.SetSettings(ctx, updates)
}

func syncDVRScheduleSettings(
	ctx context.Context,
	store *sqlite.Store,
	dvrSvc *dvr.Service,
) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	if dvrSvc == nil {
		return fmt.Errorf("dvr service is required")
	}

	state, err := dvrSvc.GetState(ctx)
	if err != nil {
		return fmt.Errorf("get dvr config state: %w", err)
	}
	cronSpec := strings.TrimSpace(state.Instance.SyncCron)
	if cronSpec == "" {
		cronSpec = "*/30 * * * *"
	}

	return store.SetSettings(ctx, map[string]string{
		sqlite.SettingJobsDVRLineupSyncEnabled: strconv.FormatBool(state.Instance.SyncEnabled),
		sqlite.SettingJobsDVRLineupSyncCron:    cronSpec,
	})
}

func runAndWaitPlaylistSync(
	ctx context.Context,
	runner *jobs.Runner,
	jobFn jobs.JobFunc,
) error {
	runID, err := runner.Start(ctx, jobs.JobPlaylistSync, jobs.TriggerManual, jobFn)
	if err != nil {
		if errors.Is(err, jobs.ErrAlreadyRunning) {
			return nil
		}
		return err
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			run, runErr := runner.GetRun(ctx, runID)
			if runErr != nil {
				return runErr
			}

			switch run.Status {
			case jobs.StatusRunning:
				continue
			case jobs.StatusSuccess:
				return nil
			case jobs.StatusError:
				if strings.TrimSpace(run.ErrorMessage) == "" {
					return fmt.Errorf("playlist sync run %d failed", runID)
				}
				return fmt.Errorf("playlist sync run %d failed: %s", runID, run.ErrorMessage)
			case jobs.StatusCanceled:
				return fmt.Errorf("playlist sync run %d was canceled", runID)
			default:
				return fmt.Errorf("playlist sync run %d ended with unknown status %q", runID, run.Status)
			}
		}
	}
}

func runInitialPlaylistSyncAfterListenerStart(
	ctx context.Context,
	logger *slog.Logger,
	httpAddr string,
	runSync func(context.Context) error,
) error {
	if runSync == nil {
		return fmt.Errorf("initial playlist sync runner is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	phaseStartedAt := time.Now()
	logger.Info("initial playlist sync phase", "initial_sync_phase", "scheduled_after_listener_start")

	readyCtx, readyCancel := context.WithTimeout(ctx, initialPlaylistSyncReadyTimeout)
	defer readyCancel()
	if err := waitForHTTPReadiness(readyCtx, httpAddr, initialPlaylistSyncReadyInterval); err != nil {
		logger.Error(
			"initial playlist sync phase",
			"initial_sync_phase", "failed",
			"duration", time.Since(phaseStartedAt),
			"error", err,
		)
		return err
	}

	budgetCtx, budgetCancel := context.WithTimeout(ctx, initialPlaylistSyncRetryBudget)
	defer budgetCancel()

	var lastErr error
	for attempt := 1; attempt <= initialPlaylistSyncRetryAttempts; attempt++ {
		if budgetCtx.Err() != nil {
			break
		}

		attemptCtx, attemptCancel := context.WithTimeout(budgetCtx, initialPlaylistSyncAttemptTimeout)
		err := runSync(attemptCtx)
		attemptCancel()
		if err == nil {
			logger.Info(
				"initial playlist sync phase",
				"initial_sync_phase", "completed",
				"attempt", attempt,
				"duration", time.Since(phaseStartedAt),
			)
			return nil
		}

		lastErr = err
		if !isTransientStartupJellyfinLineupReloadError(err) {
			break
		}
		if attempt >= initialPlaylistSyncRetryAttempts {
			break
		}

		backoff := initialPlaylistSyncRetryDelay(attempt)
		logger.Warn(
			"initial playlist sync transient jellyfin lineup reload failure; retrying",
			"attempt", attempt,
			"max_attempts", initialPlaylistSyncRetryAttempts,
			"backoff", backoff,
			"error", err,
		)
		timer := time.NewTimer(backoff)
		select {
		case <-budgetCtx.Done():
			timer.Stop()
			break
		case <-timer.C:
		}
	}

	if lastErr == nil {
		lastErr = context.DeadlineExceeded
	}
	logger.Error(
		"initial playlist sync phase",
		"initial_sync_phase", "failed",
		"duration", time.Since(phaseStartedAt),
		"error", lastErr,
	)
	return lastErr
}

func waitForHTTPReadiness(ctx context.Context, httpAddr string, pollInterval time.Duration) error {
	addr := strings.TrimSpace(httpAddr)
	if addr == "" {
		return fmt.Errorf("http listen addr is required")
	}

	probeURL, err := readinessProbeURL(addr)
	if err != nil {
		return err
	}
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}

	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
		if reqErr != nil {
			return fmt.Errorf("build readiness probe request: %w", reqErr)
		}

		resp, doErr := client.Do(req)
		if doErr == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			slog.Debug(
				"http listener readiness probe returned non-2xx; retrying",
				"probe_url", probeURL,
				"status_code", resp.StatusCode,
				"poll_interval", pollInterval,
			)
		} else {
			slog.Debug(
				"http listener readiness probe failed; retrying",
				"probe_url", probeURL,
				"poll_interval", pollInterval,
				"error", doErr,
			)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for http listener readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

type debugResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (w *debugResponseWriter) WriteHeader(code int) {
	if w.statusCode == 0 {
		w.statusCode = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *debugResponseWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytesWritten += int64(n)
	return n, err
}

func (w *debugResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *debugResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *debugResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *debugResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	readerFrom, ok := w.ResponseWriter.(io.ReaderFrom)
	if !ok {
		return io.Copy(struct{ io.Writer }{w}, src)
	}
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	n, err := readerFrom.ReadFrom(src)
	w.bytesWritten += n
	return n, err
}

func (w *debugResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func debugRequestLogger(next http.Handler, enabled bool) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}
	if !enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		observed := &debugResponseWriter{ResponseWriter: w}
		next.ServeHTTP(observed, r)

		statusCode := observed.statusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		slog.Info(
			"http request",
			"remote_addr", strings.TrimSpace(r.RemoteAddr),
			"method", r.Method,
			"host", strings.TrimSpace(r.Host),
			"path", r.URL.Path,
			"query", strings.TrimSpace(r.URL.RawQuery),
			"proto", r.Proto,
			"user_agent", strings.TrimSpace(r.UserAgent()),
			"accept", strings.TrimSpace(r.Header.Get("Accept")),
			"content_type", strings.TrimSpace(r.Header.Get("Content-Type")),
			"soap_action", strings.TrimSpace(r.Header.Get("SOAPAction")),
			"status_code", statusCode,
			"response_bytes", observed.bytesWritten,
			"duration", time.Since(startedAt),
		)
	})
}

func readinessProbeURL(httpAddr string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(httpAddr))
	if err != nil {
		return "", fmt.Errorf("invalid http listen addr %q: %w", httpAddr, err)
	}
	host = strings.TrimSpace(host)
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, strings.TrimSpace(port)) + "/healthz", nil
}

func initialPlaylistSyncRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := initialPlaylistSyncRetryBaseDelay << (attempt - 1)
	if delay > initialPlaylistSyncRetryMaxDelay {
		return initialPlaylistSyncRetryMaxDelay
	}
	return delay
}

func isTransientStartupJellyfinLineupReloadError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	if text == "" {
		return false
	}
	if !strings.Contains(text, "reload dvr lineup after playlist sync") {
		return false
	}
	if !strings.Contains(text, "provider jellyfin") {
		return false
	}

	if reInitialPlaylistSyncJellyfin5xx.MatchString(text) {
		return true
	}

	switch {
	case strings.Contains(text, "connection refused"),
		strings.Contains(text, "connection reset"),
		strings.Contains(text, "network is unreachable"),
		strings.Contains(text, "broken pipe"),
		strings.Contains(text, "deadline exceeded"),
		strings.Contains(text, "timed out"),
		strings.Contains(text, "timeout"),
		strings.Contains(text, "temporarily unavailable"),
		strings.Contains(text, "no route to host"),
		strings.Contains(text, "eof"):
		return true
	default:
		return false
	}
}

func hasPlaylistURLSetting(
	ctx context.Context,
	store *sqlite.Store,
) bool {
	value, err := store.GetSetting(ctx, sqlite.SettingPlaylistURL)
	if err != nil {
		return false
	}
	return strings.TrimSpace(value) != ""
}

func readSettingOrDefault(
	ctx context.Context,
	store *sqlite.Store,
	key,
	def string,
) (string, error) {
	value, err := store.GetSetting(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return def, nil
		}
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return def, nil
	}
	return value, nil
}

func loadAnalyzerConfig(
	ctx context.Context,
	store *sqlite.Store,
	ffmpegPath string,
) (analyzer.Config, error) {
	probeTimeoutMS, err := readIntSettingOrDefault(ctx, store, sqlite.SettingAnalyzerProbeTimeoutMS, 7000)
	if err != nil {
		return analyzer.Config{}, err
	}
	analyzeDurationUS, err := readInt64SettingOrDefault(ctx, store, sqlite.SettingAnalyzerAnalyzeDurationUS, 1_500_000)
	if err != nil {
		return analyzer.Config{}, err
	}
	probeSizeBytes, err := readInt64SettingOrDefault(ctx, store, sqlite.SettingAnalyzerProbeSizeBytes, 1_000_000)
	if err != nil {
		return analyzer.Config{}, err
	}
	bitrateMode, err := readSettingOrDefault(ctx, store, sqlite.SettingAnalyzerBitrateMode, analyzer.BitrateModeMetadataThenSample)
	if err != nil {
		return analyzer.Config{}, err
	}
	sampleSeconds, err := readIntSettingOrDefault(ctx, store, sqlite.SettingAnalyzerSampleSeconds, 3)
	if err != nil {
		return analyzer.Config{}, err
	}

	ffmpegPath = strings.TrimSpace(ffmpegPath)
	if ffmpegPath == "" {
		ffmpegPath = "ffmpeg"
	}

	return analyzer.Config{
		FFprobePath:       "ffprobe",
		FFmpegPath:        ffmpegPath,
		ProbeTimeout:      time.Duration(probeTimeoutMS) * time.Millisecond,
		AnalyzeDurationUS: analyzeDurationUS,
		ProbeSizeBytes:    probeSizeBytes,
		BitrateMode:       bitrateMode,
		SampleSeconds:     sampleSeconds,
	}, nil
}

func readIntSettingOrDefault(
	ctx context.Context,
	store *sqlite.Store,
	key string,
	def int,
) (int, error) {
	value, err := readSettingOrDefault(ctx, store, key, strconv.Itoa(def))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid integer setting %q=%q: %w", key, value, err)
	}
	return parsed, nil
}

func readInt64SettingOrDefault(
	ctx context.Context,
	store *sqlite.Store,
	key string,
	def int64,
) (int64, error) {
	value, err := readSettingOrDefault(ctx, store, key, strconv.FormatInt(def, 10))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer setting %q=%q: %w", key, value, err)
	}
	return parsed, nil
}
