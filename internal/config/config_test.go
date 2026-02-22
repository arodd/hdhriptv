package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaultStabilityProfile(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.StreamMode != "ffmpeg-copy" {
		t.Fatalf("StreamMode = %q, want ffmpeg-copy", cfg.StreamMode)
	}
	if !cfg.UPnPEnabled {
		t.Fatal("UPnPEnabled = false, want true")
	}
	if cfg.UPnPAddr != ":1900" {
		t.Fatalf("UPnPAddr = %q, want :1900", cfg.UPnPAddr)
	}
	if cfg.UPnPNotifyInterval.String() != "5m0s" {
		t.Fatalf("UPnPNotifyInterval = %s, want 5m0s", cfg.UPnPNotifyInterval)
	}
	if cfg.UPnPMaxAge.String() != "30m0s" {
		t.Fatalf("UPnPMaxAge = %s, want 30m0s", cfg.UPnPMaxAge)
	}
	if cfg.UPnPContentDirectoryUpdateIDCacheTTL != defaultUPnPContentDirectoryUpdateIDCacheTTL {
		t.Fatalf(
			"UPnPContentDirectoryUpdateIDCacheTTL = %s, want %s",
			cfg.UPnPContentDirectoryUpdateIDCacheTTL,
			defaultUPnPContentDirectoryUpdateIDCacheTTL,
		)
	}
	if cfg.FFmpegReconnectEnabled {
		t.Fatal("FFmpegReconnectEnabled = true, want false")
	}
	if cfg.FFmpegReconnectDelayMax.String() != "3s" {
		t.Fatalf("FFmpegReconnectDelayMax = %s, want 3s", cfg.FFmpegReconnectDelayMax)
	}
	if cfg.FFmpegReconnectMaxRetries != 1 {
		t.Fatalf("FFmpegReconnectMaxRetries = %d, want 1", cfg.FFmpegReconnectMaxRetries)
	}
	if cfg.FFmpegReconnectHTTPErrors != "" {
		t.Fatalf("FFmpegReconnectHTTPErrors = %q, want empty", cfg.FFmpegReconnectHTTPErrors)
	}
	if cfg.FFmpegStartupProbeSize != defaultFFmpegStartupProbeSizeBytes {
		t.Fatalf("FFmpegStartupProbeSize = %d, want %d", cfg.FFmpegStartupProbeSize, defaultFFmpegStartupProbeSizeBytes)
	}
	if cfg.FFmpegStartupAnalyzeDuration != defaultFFmpegStartupAnalyzeDuration {
		t.Fatalf("FFmpegStartupAnalyzeDuration = %s, want %s", cfg.FFmpegStartupAnalyzeDuration, defaultFFmpegStartupAnalyzeDuration)
	}
	if !cfg.FFmpegCopyRegenerateTimestamps {
		t.Fatal("FFmpegCopyRegenerateTimestamps = false, want true")
	}
	if cfg.PreemptSettleDelay.String() != "500ms" {
		t.Fatalf("PreemptSettleDelay = %s, want 500ms", cfg.PreemptSettleDelay)
	}
	if cfg.UpstreamOverlimitCooldown.String() != "3s" {
		t.Fatalf("UpstreamOverlimitCooldown = %s, want 3s", cfg.UpstreamOverlimitCooldown)
	}
	if cfg.StartupTimeout.String() != "6s" {
		t.Fatalf("StartupTimeout = %s, want 6s", cfg.StartupTimeout)
	}
	if cfg.ReconcileDynamicRulePaged {
		t.Fatal("ReconcileDynamicRulePaged = true, want false")
	}
	if !cfg.StartupRandomAccessRecoveryOnly {
		t.Fatal("StartupRandomAccessRecoveryOnly = false, want true")
	}
	if cfg.MinProbeBytes != 940 {
		t.Fatalf("MinProbeBytes = %d, want 940", cfg.MinProbeBytes)
	}
	if cfg.FailoverTotalTimeout.String() != "32s" {
		t.Fatalf("FailoverTotalTimeout = %s, want 32s", cfg.FailoverTotalTimeout)
	}
	if cfg.MaxFailovers != 3 {
		t.Fatalf("MaxFailovers = %d, want 3", cfg.MaxFailovers)
	}
	if cfg.StallDetect.String() != "4s" {
		t.Fatalf("StallDetect = %s, want 4s", cfg.StallDetect)
	}
	if cfg.StallHardDeadline.String() != "32s" {
		t.Fatalf("StallHardDeadline = %s, want 32s", cfg.StallHardDeadline)
	}
	if cfg.StallPolicy != "failover_source" {
		t.Fatalf("StallPolicy = %q, want failover_source", cfg.StallPolicy)
	}
	if cfg.StallMaxFailovers != 3 {
		t.Fatalf("StallMaxFailovers = %d, want 3", cfg.StallMaxFailovers)
	}
	if cfg.CycleFailureMinHealth.String() != "20s" {
		t.Fatalf("CycleFailureMinHealth = %s, want 20s", cfg.CycleFailureMinHealth)
	}
	if !cfg.RecoveryFillerEnabled {
		t.Fatal("RecoveryFillerEnabled = false, want true")
	}
	if cfg.RecoveryFillerMode != "slate_av" {
		t.Fatalf("RecoveryFillerMode = %q, want slate_av", cfg.RecoveryFillerMode)
	}
	if cfg.RecoveryFillerInterval.String() != "200ms" {
		t.Fatalf("RecoveryFillerInterval = %s, want 200ms", cfg.RecoveryFillerInterval)
	}
	if cfg.BufferFlushInterval.String() != "100ms" {
		t.Fatalf("BufferFlushInterval = %s, want 100ms", cfg.BufferFlushInterval)
	}
	if cfg.ProbeInterval.String() != "0s" {
		t.Fatalf("ProbeInterval = %s, want 0s", cfg.ProbeInterval)
	}
	if cfg.SessionIdleTimeout.String() != "5s" {
		t.Fatalf("SessionIdleTimeout = %s, want 5s", cfg.SessionIdleTimeout)
	}
	if cfg.SessionDrainTimeout.String() != "2s" {
		t.Fatalf("SessionDrainTimeout = %s, want 2s", cfg.SessionDrainTimeout)
	}
	if cfg.SessionHistoryLimit != 0 {
		t.Fatalf("SessionHistoryLimit = %d, want 0", cfg.SessionHistoryLimit)
	}
	if cfg.SessionSourceHistoryLimit != 0 {
		t.Fatalf("SessionSourceHistoryLimit = %d, want 0", cfg.SessionSourceHistoryLimit)
	}
	if cfg.SessionSubscriberHistoryLimit != 0 {
		t.Fatalf("SessionSubscriberHistoryLimit = %d, want 0", cfg.SessionSubscriberHistoryLimit)
	}
	if cfg.SourceHealthDrainTimeout != 0 {
		t.Fatalf("SourceHealthDrainTimeout = %s, want 0s", cfg.SourceHealthDrainTimeout)
	}
	if cfg.AutoPrioritizeProbeTuneDelay.String() != "1s" {
		t.Fatalf("AutoPrioritizeProbeTuneDelay = %s, want 1s", cfg.AutoPrioritizeProbeTuneDelay)
	}
	if cfg.SubscriberMaxBlocked.String() != "6s" {
		t.Fatalf("SubscriberMaxBlocked = %s, want 6s", cfg.SubscriberMaxBlocked)
	}
	if cfg.AutoPrioritizeWorkers != "2" {
		t.Fatalf("AutoPrioritizeWorkers = %q, want 2", cfg.AutoPrioritizeWorkers)
	}
	if cfg.TuneBackoffMaxTunes != 8 {
		t.Fatalf("TuneBackoffMaxTunes = %d, want 8", cfg.TuneBackoffMaxTunes)
	}
	if cfg.RateLimitMaxClients != 4096 {
		t.Fatalf("RateLimitMaxClients = %d, want 4096", cfg.RateLimitMaxClients)
	}
	if cfg.AdminJSONBodyLimitBytes != 1<<20 {
		t.Fatalf("AdminJSONBodyLimitBytes = %d, want %d", cfg.AdminJSONBodyLimitBytes, 1<<20)
	}
	if len(cfg.RateLimitTrustedProxies) != 0 {
		t.Fatalf("RateLimitTrustedProxies = %v, want empty", cfg.RateLimitTrustedProxies)
	}
	if cfg.TuneBackoffInterval.String() != "1m0s" {
		t.Fatalf("TuneBackoffInterval = %s, want 1m0s", cfg.TuneBackoffInterval)
	}
	if cfg.TuneBackoffCooldown.String() != "20s" {
		t.Fatalf("TuneBackoffCooldown = %s, want 20s", cfg.TuneBackoffCooldown)
	}
	if cfg.HTTPRequestLogEnabled {
		t.Fatal("HTTPRequestLogEnabled = true, want false")
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() error = %v", err)
	}
	if cfg.LogDir != wd {
		t.Fatalf("LogDir = %q, want %q", cfg.LogDir, wd)
	}
}

func TestConfigRedactedIncludesUPnPContentDirectoryUpdateIDCacheTTL(t *testing.T) {
	cfg := Config{
		UPnPContentDirectoryUpdateIDCacheTTL: 1500 * time.Millisecond,
	}

	redacted := cfg.Redacted()
	if redacted.UPnPContentDirectoryUpdateIDCacheTTL != "1.5s" {
		t.Fatalf(
			"Redacted().UPnPContentDirectoryUpdateIDCacheTTL = %q, want %q",
			redacted.UPnPContentDirectoryUpdateIDCacheTTL,
			"1.5s",
		)
	}
}

func TestLoadSessionDrainTimeoutFromEnvAndFlag(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("SESSION_DRAIN_TIMEOUT", "4s")

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatalf("Load() env error = %v", err)
	}
	if cfg.SessionDrainTimeout != 4*time.Second {
		t.Fatalf("SessionDrainTimeout = %s, want 4s from env", cfg.SessionDrainTimeout)
	}

	cfg, err = Load([]string{"--session-drain-timeout=750ms"})
	if err != nil {
		t.Fatalf("Load() flag override error = %v", err)
	}
	if cfg.SessionDrainTimeout != 750*time.Millisecond {
		t.Fatalf("SessionDrainTimeout = %s, want 750ms from flag", cfg.SessionDrainTimeout)
	}
}

func TestLoadSessionHistoryAndSourceHealthDrainFromEnvAndFlag(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("SESSION_HISTORY_LIMIT", "300")
	t.Setenv("SESSION_SOURCE_HISTORY_LIMIT", "128")
	t.Setenv("SESSION_SUBSCRIBER_HISTORY_LIMIT", "96")
	t.Setenv("SOURCE_HEALTH_DRAIN_TIMEOUT", "420ms")

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatalf("Load() env error = %v", err)
	}
	if cfg.SessionHistoryLimit != 300 {
		t.Fatalf("SessionHistoryLimit = %d, want 300 from env", cfg.SessionHistoryLimit)
	}
	if cfg.SessionSourceHistoryLimit != 128 {
		t.Fatalf("SessionSourceHistoryLimit = %d, want 128 from env", cfg.SessionSourceHistoryLimit)
	}
	if cfg.SessionSubscriberHistoryLimit != 96 {
		t.Fatalf("SessionSubscriberHistoryLimit = %d, want 96 from env", cfg.SessionSubscriberHistoryLimit)
	}
	if cfg.SourceHealthDrainTimeout != 420*time.Millisecond {
		t.Fatalf("SourceHealthDrainTimeout = %s, want 420ms from env", cfg.SourceHealthDrainTimeout)
	}

	cfg, err = Load([]string{
		"--session-history-limit=250",
		"--session-source-history-limit=64",
		"--session-subscriber-history-limit=32",
		"--source-health-drain-timeout=150ms",
	})
	if err != nil {
		t.Fatalf("Load() flag override error = %v", err)
	}
	if cfg.SessionHistoryLimit != 250 {
		t.Fatalf("SessionHistoryLimit = %d, want 250 from flag", cfg.SessionHistoryLimit)
	}
	if cfg.SessionSourceHistoryLimit != 64 {
		t.Fatalf("SessionSourceHistoryLimit = %d, want 64 from flag", cfg.SessionSourceHistoryLimit)
	}
	if cfg.SessionSubscriberHistoryLimit != 32 {
		t.Fatalf("SessionSubscriberHistoryLimit = %d, want 32 from flag", cfg.SessionSubscriberHistoryLimit)
	}
	if cfg.SourceHealthDrainTimeout != 150*time.Millisecond {
		t.Fatalf("SourceHealthDrainTimeout = %s, want 150ms from flag", cfg.SourceHealthDrainTimeout)
	}
}

func TestLoadAcceptsLogDirOverride(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{"--log-dir=/tmp/hdhriptv-logs"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.LogDir != "/tmp/hdhriptv-logs" {
		t.Fatalf("LogDir = %q, want /tmp/hdhriptv-logs", cfg.LogDir)
	}
}

func TestLoadHTTPRequestLogEnabledFromEnvAndFlag(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("HTTP_REQUEST_LOG_ENABLED", "true")

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatalf("Load() env error = %v", err)
	}
	if !cfg.HTTPRequestLogEnabled {
		t.Fatal("HTTPRequestLogEnabled = false, want true from env")
	}

	cfg, err = Load([]string{"--http-request-log-enabled=false"})
	if err != nil {
		t.Fatalf("Load() flag override error = %v", err)
	}
	if cfg.HTTPRequestLogEnabled {
		t.Fatal("HTTPRequestLogEnabled = true, want false from flag override")
	}
}

func TestLoadNormalizesFFmpegStartupDetectionFloors(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--ffmpeg-startup-probesize-bytes=16384",
		"--ffmpeg-startup-analyzeduration=100ms",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.FFmpegStartupProbeSize != minFFmpegStartupProbeSizeBytes {
		t.Fatalf("FFmpegStartupProbeSize = %d, want %d", cfg.FFmpegStartupProbeSize, minFFmpegStartupProbeSizeBytes)
	}
	if cfg.FFmpegStartupAnalyzeDuration != minFFmpegStartupAnalyzeDuration {
		t.Fatalf(
			"FFmpegStartupAnalyzeDuration = %s, want %s",
			cfg.FFmpegStartupAnalyzeDuration,
			minFFmpegStartupAnalyzeDuration,
		)
	}
}

func TestLoadAcceptsRefreshSchedule(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--refresh-schedule=*/15 * * * *",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RefreshSchedule != "*/15 * * * *" {
		t.Fatalf("RefreshSchedule = %q, want */15 * * * *", cfg.RefreshSchedule)
	}
}

func TestLoadAllowsIdentityValuesToBeResolvedLater(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DeviceID != "" {
		t.Fatalf("DeviceID = %q, want empty so main identity resolver can populate it", cfg.DeviceID)
	}
	if cfg.DeviceAuth != "" {
		t.Fatalf("DeviceAuth = %q, want empty so main identity resolver can populate it", cfg.DeviceAuth)
	}
}

func TestLoadNormalizesExplicitDeviceID(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{"--device-id=deadbeef"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DeviceID != "DEADBEEF" {
		t.Fatalf("DeviceID = %q, want DEADBEEF", cfg.DeviceID)
	}
}

func TestLoadRejectsInvalidDeviceID(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{"--device-id=bad-id"})
	if err == nil {
		t.Fatal("expected error for invalid device-id")
	}
}

func TestLoadRejectsInvalidRefreshSchedule(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--refresh-schedule=not-a-cron",
	})
	if err == nil {
		t.Fatal("expected error for invalid refresh-schedule")
	}
}

func TestLoadConvertsLegacyRefreshIntervalFlag(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--refresh-interval=30m",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RefreshSchedule != "*/30 * * * *" {
		t.Fatalf("RefreshSchedule = %q, want */30 * * * *", cfg.RefreshSchedule)
	}
}

func TestLoadReconcileDynamicRulePagedFromFlagAndEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("RECONCILE_DYNAMIC_RULE_PAGED", "true")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.ReconcileDynamicRulePaged {
		t.Fatal("ReconcileDynamicRulePaged = false, want true from env")
	}

	cfg, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--reconcile-dynamic-rule-paged=false",
	})
	if err != nil {
		t.Fatalf("Load(flag override) error = %v", err)
	}
	if cfg.ReconcileDynamicRulePaged {
		t.Fatal("ReconcileDynamicRulePaged = true, want false from flag override")
	}
}

func TestLoadConvertsLegacyRefreshIntervalEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("REFRESH_INTERVAL", "2h")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RefreshSchedule != "0 */2 * * *" {
		t.Fatalf("RefreshSchedule = %q, want 0 */2 * * *", cfg.RefreshSchedule)
	}
}

func TestLoadRejectsMixedRefreshScheduleAndIntervalFlags(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--refresh-schedule=*/15 * * * *",
		"--refresh-interval=30m",
	})
	if err == nil {
		t.Fatal("expected error when both refresh-schedule and refresh-interval are set")
	}
}

func TestLoadRejectsNegativeRateLimit(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--rate-limit-rps=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative rate-limit-rps")
	}
}

func TestLoadRejectsRateLimitBurstWhenEnabled(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--rate-limit-rps=5",
		"--rate-limit-burst=0",
	})
	if err == nil {
		t.Fatal("expected error for non-positive rate-limit-burst when rate limiting is enabled")
	}
}

func TestLoadRejectsNegativeRateLimitMaxClients(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--rate-limit-max-clients=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative rate-limit-max-clients")
	}
}

func TestLoadRateLimitTrustedProxiesFromFlag(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--rate-limit-trusted-proxies=10.0.0.0/8,203.0.113.4,2001:db8::/32",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := []string{"10.0.0.0/8", "203.0.113.4/32", "2001:db8::/32"}
	if len(cfg.RateLimitTrustedProxies) != len(want) {
		t.Fatalf("RateLimitTrustedProxies len = %d, want %d (%v)", len(cfg.RateLimitTrustedProxies), len(want), cfg.RateLimitTrustedProxies)
	}
	for i := range want {
		if cfg.RateLimitTrustedProxies[i] != want[i] {
			t.Fatalf("RateLimitTrustedProxies[%d] = %q, want %q", i, cfg.RateLimitTrustedProxies[i], want[i])
		}
	}
}

func TestLoadRateLimitTrustedProxiesFromEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("RATE_LIMIT_TRUSTED_PROXIES", "10.1.0.0/16, 198.51.100.10 ")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := []string{"10.1.0.0/16", "198.51.100.10/32"}
	if len(cfg.RateLimitTrustedProxies) != len(want) {
		t.Fatalf("RateLimitTrustedProxies len = %d, want %d (%v)", len(cfg.RateLimitTrustedProxies), len(want), cfg.RateLimitTrustedProxies)
	}
	for i := range want {
		if cfg.RateLimitTrustedProxies[i] != want[i] {
			t.Fatalf("RateLimitTrustedProxies[%d] = %q, want %q", i, cfg.RateLimitTrustedProxies[i], want[i])
		}
	}
}

func TestLoadRejectsInvalidRateLimitTrustedProxies(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--rate-limit-trusted-proxies=not-a-cidr",
	})
	if err == nil {
		t.Fatal("expected error for invalid rate-limit-trusted-proxies")
	}
}

func TestLoadRejectsNegativeRequestTimeout(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--request-timeout=-5s",
	})
	if err == nil {
		t.Fatal("expected error for negative request-timeout")
	}
}

func TestLoadAdminJSONBodyLimitOverride(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--admin-json-body-limit-bytes=2048",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AdminJSONBodyLimitBytes != 2048 {
		t.Fatalf("AdminJSONBodyLimitBytes = %d, want 2048", cfg.AdminJSONBodyLimitBytes)
	}
}

func TestLoadRejectsNonPositiveAdminJSONBodyLimit(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--admin-json-body-limit-bytes=0",
	})
	if err == nil {
		t.Fatal("expected error for non-positive admin-json-body-limit-bytes")
	}
}

func TestLoadAllowsRateLimitingDisabled(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--rate-limit-rps=0",
		"--rate-limit-burst=0",
		"--enable-metrics=true",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RateLimitRPS != 0 {
		t.Fatalf("RateLimitRPS = %v, want 0", cfg.RateLimitRPS)
	}
	if cfg.RateLimitBurst != 0 {
		t.Fatalf("RateLimitBurst = %d, want 0", cfg.RateLimitBurst)
	}
	if !cfg.EnableMetrics {
		t.Fatal("EnableMetrics = false, want true")
	}
}

func TestLoadTuneBackoffSettings(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-max-tunes=6",
		"--tune-backoff-interval=45s",
		"--tune-backoff-cooldown=30s",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TuneBackoffMaxTunes != 6 {
		t.Fatalf("TuneBackoffMaxTunes = %d, want 6", cfg.TuneBackoffMaxTunes)
	}
	if cfg.TuneBackoffInterval.String() != "45s" {
		t.Fatalf("TuneBackoffInterval = %s, want 45s", cfg.TuneBackoffInterval)
	}
	if cfg.TuneBackoffCooldown.String() != "30s" {
		t.Fatalf("TuneBackoffCooldown = %s, want 30s", cfg.TuneBackoffCooldown)
	}
}

func TestLoadAllowsTuneBackoffDisabled(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-max-tunes=0",
		"--tune-backoff-interval=0s",
		"--tune-backoff-cooldown=0s",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TuneBackoffMaxTunes != 0 {
		t.Fatalf("TuneBackoffMaxTunes = %d, want 0", cfg.TuneBackoffMaxTunes)
	}
}

func TestLoadRejectsInvalidFailoverSettings(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--min-probe-bytes=0",
	})
	if err == nil {
		t.Fatal("expected error for min-probe-bytes=0")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--max-failovers=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative max-failovers")
	}
}

func TestLoadRejectsProbeTimeoutGreaterThanInterval(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--probe-interval=5s",
		"--probe-timeout=10s",
	})
	if err == nil {
		t.Fatal("expected error when probe-timeout exceeds probe-interval")
	}
}

func TestLoadFailoverSettings(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--startup-timeout=4s",
		"--min-probe-bytes=940",
		"--max-failovers=2",
		"--failover-total-timeout=12s",
		"--upstream-overlimit-cooldown=3s",
		"--ffmpeg-reconnect-enabled=false",
		"--ffmpeg-reconnect-delay-max=1500ms",
		"--ffmpeg-reconnect-max-retries=4",
		"--ffmpeg-reconnect-http-errors=429,5xx",
		"--ffmpeg-copy-regenerate-timestamps=false",
		"--probe-interval=1m",
		"--probe-timeout=2s",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.StartupTimeout.String() != "4s" {
		t.Fatalf("StartupTimeout = %s, want 4s", cfg.StartupTimeout)
	}
	if cfg.MinProbeBytes != 940 {
		t.Fatalf("MinProbeBytes = %d, want 940", cfg.MinProbeBytes)
	}
	if cfg.MaxFailovers != 2 {
		t.Fatalf("MaxFailovers = %d, want 2", cfg.MaxFailovers)
	}
	if cfg.FailoverTotalTimeout.String() != "12s" {
		t.Fatalf("FailoverTotalTimeout = %s, want 12s", cfg.FailoverTotalTimeout)
	}
	if cfg.UpstreamOverlimitCooldown.String() != "3s" {
		t.Fatalf("UpstreamOverlimitCooldown = %s, want 3s", cfg.UpstreamOverlimitCooldown)
	}
	if cfg.FFmpegReconnectEnabled {
		t.Fatal("FFmpegReconnectEnabled = true, want false")
	}
	if cfg.FFmpegReconnectDelayMax.String() != "1.5s" {
		t.Fatalf("FFmpegReconnectDelayMax = %s, want 1.5s", cfg.FFmpegReconnectDelayMax)
	}
	if cfg.FFmpegReconnectMaxRetries != 4 {
		t.Fatalf("FFmpegReconnectMaxRetries = %d, want 4", cfg.FFmpegReconnectMaxRetries)
	}
	if cfg.FFmpegReconnectHTTPErrors != "429,5xx" {
		t.Fatalf("FFmpegReconnectHTTPErrors = %q, want 429,5xx", cfg.FFmpegReconnectHTTPErrors)
	}
	if cfg.FFmpegCopyRegenerateTimestamps {
		t.Fatal("FFmpegCopyRegenerateTimestamps = true, want false")
	}
	if cfg.ProducerReadRate != 1 {
		t.Fatalf("ProducerReadRate = %v, want 1", cfg.ProducerReadRate)
	}
	if cfg.BufferChunkBytes != 64*1024 {
		t.Fatalf("BufferChunkBytes = %d, want 65536", cfg.BufferChunkBytes)
	}
	if cfg.ProbeInterval.String() != "1m0s" {
		t.Fatalf("ProbeInterval = %s, want 1m0s", cfg.ProbeInterval)
	}
	if cfg.ProbeTimeout.String() != "2s" {
		t.Fatalf("ProbeTimeout = %s, want 2s", cfg.ProbeTimeout)
	}
}

func TestLoadAutoPrioritizeWorkersFixedValue(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--auto-prioritize-workers=3",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AutoPrioritizeWorkers != "3" {
		t.Fatalf("AutoPrioritizeWorkers = %q, want 3", cfg.AutoPrioritizeWorkers)
	}
}

func TestLoadRejectsInvalidAutoPrioritizeWorkers(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--auto-prioritize-workers=0",
	})
	if err == nil {
		t.Fatal("expected error for auto-prioritize-workers=0")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--auto-prioritize-workers=banana",
	})
	if err == nil {
		t.Fatal("expected error for non-numeric auto-prioritize-workers")
	}
}

func TestLoadCatalogSearchLimitsDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CatalogSearchMaxTerms != defaultCatalogSearchMaxTerms {
		t.Fatalf("CatalogSearchMaxTerms = %d, want %d", cfg.CatalogSearchMaxTerms, defaultCatalogSearchMaxTerms)
	}
	if cfg.CatalogSearchMaxDisjuncts != defaultCatalogSearchMaxDisjuncts {
		t.Fatalf("CatalogSearchMaxDisjuncts = %d, want %d", cfg.CatalogSearchMaxDisjuncts, defaultCatalogSearchMaxDisjuncts)
	}
	if cfg.CatalogSearchMaxTermRunes != defaultCatalogSearchMaxTermRunes {
		t.Fatalf("CatalogSearchMaxTermRunes = %d, want %d", cfg.CatalogSearchMaxTermRunes, defaultCatalogSearchMaxTermRunes)
	}
}

func TestLoadCatalogSearchLimitsFromEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CATALOG_SEARCH_MAX_TERMS", "20")
	t.Setenv("CATALOG_SEARCH_MAX_DISJUNCTS", "10")
	t.Setenv("CATALOG_SEARCH_MAX_TERM_RUNES", "90")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CatalogSearchMaxTerms != 20 {
		t.Fatalf("CatalogSearchMaxTerms = %d, want 20", cfg.CatalogSearchMaxTerms)
	}
	if cfg.CatalogSearchMaxDisjuncts != 10 {
		t.Fatalf("CatalogSearchMaxDisjuncts = %d, want 10", cfg.CatalogSearchMaxDisjuncts)
	}
	if cfg.CatalogSearchMaxTermRunes != 90 {
		t.Fatalf("CatalogSearchMaxTermRunes = %d, want 90", cfg.CatalogSearchMaxTermRunes)
	}
}

func TestLoadCatalogSearchLimitsFromEnvZeroUsesDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CATALOG_SEARCH_MAX_TERMS", "0")
	t.Setenv("CATALOG_SEARCH_MAX_DISJUNCTS", "0")
	t.Setenv("CATALOG_SEARCH_MAX_TERM_RUNES", "0")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CatalogSearchMaxTerms != defaultCatalogSearchMaxTerms {
		t.Fatalf("CatalogSearchMaxTerms = %d, want default %d", cfg.CatalogSearchMaxTerms, defaultCatalogSearchMaxTerms)
	}
	if cfg.CatalogSearchMaxDisjuncts != defaultCatalogSearchMaxDisjuncts {
		t.Fatalf(
			"CatalogSearchMaxDisjuncts = %d, want default %d",
			cfg.CatalogSearchMaxDisjuncts,
			defaultCatalogSearchMaxDisjuncts,
		)
	}
	if cfg.CatalogSearchMaxTermRunes != defaultCatalogSearchMaxTermRunes {
		t.Fatalf(
			"CatalogSearchMaxTermRunes = %d, want default %d",
			cfg.CatalogSearchMaxTermRunes,
			defaultCatalogSearchMaxTermRunes,
		)
	}
}

func TestLoadCatalogSearchLimitsFromFlags(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CATALOG_SEARCH_MAX_TERMS", "20")
	t.Setenv("CATALOG_SEARCH_MAX_DISJUNCTS", "10")
	t.Setenv("CATALOG_SEARCH_MAX_TERM_RUNES", "90")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-terms=24",
		"--catalog-search-max-disjuncts=12",
		"--catalog-search-max-term-runes=96",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CatalogSearchMaxTerms != 24 {
		t.Fatalf("CatalogSearchMaxTerms = %d, want 24", cfg.CatalogSearchMaxTerms)
	}
	if cfg.CatalogSearchMaxDisjuncts != 12 {
		t.Fatalf("CatalogSearchMaxDisjuncts = %d, want 12", cfg.CatalogSearchMaxDisjuncts)
	}
	if cfg.CatalogSearchMaxTermRunes != 96 {
		t.Fatalf("CatalogSearchMaxTermRunes = %d, want 96", cfg.CatalogSearchMaxTermRunes)
	}
}

func TestLoadCatalogSearchLimitsZeroUsesDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-terms=0",
		"--catalog-search-max-disjuncts=0",
		"--catalog-search-max-term-runes=0",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CatalogSearchMaxTerms != defaultCatalogSearchMaxTerms {
		t.Fatalf("CatalogSearchMaxTerms = %d, want default %d", cfg.CatalogSearchMaxTerms, defaultCatalogSearchMaxTerms)
	}
	if cfg.CatalogSearchMaxDisjuncts != defaultCatalogSearchMaxDisjuncts {
		t.Fatalf(
			"CatalogSearchMaxDisjuncts = %d, want default %d",
			cfg.CatalogSearchMaxDisjuncts,
			defaultCatalogSearchMaxDisjuncts,
		)
	}
	if cfg.CatalogSearchMaxTermRunes != defaultCatalogSearchMaxTermRunes {
		t.Fatalf(
			"CatalogSearchMaxTermRunes = %d, want default %d",
			cfg.CatalogSearchMaxTermRunes,
			defaultCatalogSearchMaxTermRunes,
		)
	}
}

func TestLoadRejectsInvalidCatalogSearchLimits(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-terms=-1",
	})
	if err == nil {
		t.Fatal("expected error for catalog-search-max-terms=-1")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-disjuncts=-1",
	})
	if err == nil {
		t.Fatal("expected error for catalog-search-max-disjuncts=-1")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-term-runes=-1",
	})
	if err == nil {
		t.Fatal("expected error for catalog-search-max-term-runes=-1")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-terms=257",
	})
	if err == nil {
		t.Fatal("expected error for catalog-search-max-terms above limit")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-disjuncts=129",
	})
	if err == nil {
		t.Fatal("expected error for catalog-search-max-disjuncts above limit")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--catalog-search-max-term-runes=257",
	})
	if err == nil {
		t.Fatal("expected error for catalog-search-max-term-runes above limit")
	}
}

func TestLoadRejectsInvalidCatalogSearchLimitsFromEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CATALOG_SEARCH_MAX_TERMS", "-1")

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err == nil {
		t.Fatal("expected error for CATALOG_SEARCH_MAX_TERMS=-1")
	}

	t.Setenv("CATALOG_SEARCH_MAX_TERMS", "12")
	t.Setenv("CATALOG_SEARCH_MAX_DISJUNCTS", "-1")

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err == nil {
		t.Fatal("expected error for CATALOG_SEARCH_MAX_DISJUNCTS=-1")
	}

	t.Setenv("CATALOG_SEARCH_MAX_DISJUNCTS", "6")
	t.Setenv("CATALOG_SEARCH_MAX_TERM_RUNES", "-1")

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err == nil {
		t.Fatal("expected error for CATALOG_SEARCH_MAX_TERM_RUNES=-1")
	}

	t.Setenv("CATALOG_SEARCH_MAX_TERM_RUNES", "64")
	t.Setenv("CATALOG_SEARCH_MAX_TERMS", "257")

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err == nil {
		t.Fatal("expected error for CATALOG_SEARCH_MAX_TERMS above max limit")
	}
}

func TestLoadSharedSessionSettings(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--producer-readrate=1.25",
		"--producer-initial-burst=2",
		"--startup-random-access-recovery-only=true",
		"--buffer-chunk-bytes=32768",
		"--buffer-publish-flush-interval=150ms",
		"--buffer-ts-align-188=true",
		"--stall-detect=2s",
		"--stall-hard-deadline=7s",
		"--stall-policy=restart_same",
		"--stall-max-failovers-per-stall=1",
		"--cycle-failure-min-health=3s",
		"--recovery-filler-enabled=true",
		"--recovery-filler-mode=slate_av",
		"--recovery-filler-interval=350ms",
		"--recovery-filler-text=Recovering now",
		"--recovery-filler-enable-audio=false",
		"--subscriber-join-lag-bytes=1048576",
		"--subscriber-slow-client-policy=skip",
		"--subscriber-max-blocked-write=1s",
		"--session-idle-timeout=1500ms",
		"--session-max-subscribers=12",
		"--session-history-limit=384",
		"--session-source-history-limit=144",
		"--session-subscriber-history-limit=128",
		"--source-health-drain-timeout=325ms",
		"--preempt-settle-delay=450ms",
		"--upstream-overlimit-cooldown=2s",
		"--auto-prioritize-probe-tune-delay=900ms",
		"--ffmpeg-copy-regenerate-timestamps=false",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ProducerReadRate != 1.25 {
		t.Fatalf("ProducerReadRate = %v, want 1.25", cfg.ProducerReadRate)
	}
	if cfg.ProducerInitialBurst != 2 {
		t.Fatalf("ProducerInitialBurst = %d, want 2", cfg.ProducerInitialBurst)
	}
	if !cfg.StartupRandomAccessRecoveryOnly {
		t.Fatal("StartupRandomAccessRecoveryOnly = false, want true")
	}
	if cfg.BufferChunkBytes != 32768 {
		t.Fatalf("BufferChunkBytes = %d, want 32768", cfg.BufferChunkBytes)
	}
	if cfg.BufferFlushInterval.String() != "150ms" {
		t.Fatalf("BufferFlushInterval = %s, want 150ms", cfg.BufferFlushInterval)
	}
	if !cfg.BufferTSAlign188 {
		t.Fatal("BufferTSAlign188 = false, want true")
	}
	if cfg.StallPolicy != "restart_same" {
		t.Fatalf("StallPolicy = %q, want restart_same", cfg.StallPolicy)
	}
	if cfg.CycleFailureMinHealth.String() != "3s" {
		t.Fatalf("CycleFailureMinHealth = %s, want 3s", cfg.CycleFailureMinHealth)
	}
	if !cfg.RecoveryFillerEnabled {
		t.Fatal("RecoveryFillerEnabled = false, want true")
	}
	if cfg.RecoveryFillerMode != "slate_av" {
		t.Fatalf("RecoveryFillerMode = %q, want slate_av", cfg.RecoveryFillerMode)
	}
	if cfg.RecoveryFillerInterval.String() != "350ms" {
		t.Fatalf("RecoveryFillerInterval = %s, want 350ms", cfg.RecoveryFillerInterval)
	}
	if cfg.RecoveryFillerText != "Recovering now" {
		t.Fatalf("RecoveryFillerText = %q, want %q", cfg.RecoveryFillerText, "Recovering now")
	}
	if cfg.RecoveryFillerEnableAudio {
		t.Fatal("RecoveryFillerEnableAudio = true, want false")
	}
	if cfg.SessionMaxSubscribers != 12 {
		t.Fatalf("SessionMaxSubscribers = %d, want 12", cfg.SessionMaxSubscribers)
	}
	if cfg.SessionHistoryLimit != 384 {
		t.Fatalf("SessionHistoryLimit = %d, want 384", cfg.SessionHistoryLimit)
	}
	if cfg.SessionSourceHistoryLimit != 144 {
		t.Fatalf("SessionSourceHistoryLimit = %d, want 144", cfg.SessionSourceHistoryLimit)
	}
	if cfg.SessionSubscriberHistoryLimit != 128 {
		t.Fatalf("SessionSubscriberHistoryLimit = %d, want 128", cfg.SessionSubscriberHistoryLimit)
	}
	if cfg.SourceHealthDrainTimeout != 325*time.Millisecond {
		t.Fatalf("SourceHealthDrainTimeout = %s, want 325ms", cfg.SourceHealthDrainTimeout)
	}
	if cfg.PreemptSettleDelay.String() != "450ms" {
		t.Fatalf("PreemptSettleDelay = %s, want 450ms", cfg.PreemptSettleDelay)
	}
	if cfg.UpstreamOverlimitCooldown.String() != "2s" {
		t.Fatalf("UpstreamOverlimitCooldown = %s, want 2s", cfg.UpstreamOverlimitCooldown)
	}
	if cfg.AutoPrioritizeProbeTuneDelay.String() != "900ms" {
		t.Fatalf("AutoPrioritizeProbeTuneDelay = %s, want 900ms", cfg.AutoPrioritizeProbeTuneDelay)
	}
	if cfg.FFmpegCopyRegenerateTimestamps {
		t.Fatal("FFmpegCopyRegenerateTimestamps = true, want false")
	}
}

func TestLoadFFmpegCopyRegenerateTimestampsFromEnvAndFlag(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FFMPEG_COPY_REGENERATE_TIMESTAMPS", "false")

	cfg, err := Load([]string{})
	if err != nil {
		t.Fatalf("Load() env error = %v", err)
	}
	if cfg.FFmpegCopyRegenerateTimestamps {
		t.Fatal("FFmpegCopyRegenerateTimestamps = true, want false from env")
	}

	cfg, err = Load([]string{"--ffmpeg-copy-regenerate-timestamps=true"})
	if err != nil {
		t.Fatalf("Load() flag override error = %v", err)
	}
	if !cfg.FFmpegCopyRegenerateTimestamps {
		t.Fatal("FFmpegCopyRegenerateTimestamps = false, want true from flag")
	}
}

func TestLoadRecoveryFillerEnvDefaultsAndOverrides(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("RECOVERY_FILLER_ENABLED", "false")
	t.Setenv("RECOVERY_FILLER_MODE", "slate_av")
	t.Setenv("RECOVERY_FILLER_INTERVAL", "520ms")
	t.Setenv("RECOVERY_FILLER_TEXT", "Recovering from env")
	t.Setenv("RECOVERY_FILLER_ENABLE_AUDIO", "false")
	t.Setenv("STARTUP_RANDOM_ACCESS_RECOVERY_ONLY", "true")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RecoveryFillerEnabled {
		t.Fatal("RecoveryFillerEnabled = true, want false from RECOVERY_FILLER_ENABLED")
	}
	if cfg.RecoveryFillerMode != "slate_av" {
		t.Fatalf("RecoveryFillerMode = %q, want slate_av from RECOVERY_FILLER_MODE", cfg.RecoveryFillerMode)
	}
	if cfg.RecoveryFillerInterval.String() != "520ms" {
		t.Fatalf("RecoveryFillerInterval = %s, want 520ms from RECOVERY_FILLER_INTERVAL", cfg.RecoveryFillerInterval)
	}
	if cfg.RecoveryFillerText != "Recovering from env" {
		t.Fatalf("RecoveryFillerText = %q, want %q", cfg.RecoveryFillerText, "Recovering from env")
	}
	if cfg.RecoveryFillerEnableAudio {
		t.Fatal("RecoveryFillerEnableAudio = true, want false")
	}
	if !cfg.StartupRandomAccessRecoveryOnly {
		t.Fatal("StartupRandomAccessRecoveryOnly = false, want true")
	}
}

func TestLoadRejectsInvalidSharedSessionSettings(t *testing.T) {
	clearConfigEnv(t)

	_, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--stall-policy=unknown",
	})
	if err == nil {
		t.Fatal("expected error for invalid stall-policy")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--subscriber-slow-client-policy=unknown",
	})
	if err == nil {
		t.Fatal("expected error for invalid subscriber-slow-client-policy")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--recovery-filler-mode=not-valid",
	})
	if err == nil {
		t.Fatal("expected error for invalid recovery-filler-mode")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--recovery-filler-interval=0s",
	})
	if err == nil {
		t.Fatal("expected error for non-positive recovery-filler-interval")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--preempt-settle-delay=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative preempt-settle-delay")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--session-drain-timeout=0s",
	})
	if err == nil {
		t.Fatal("expected error for non-positive session-drain-timeout")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--session-history-limit=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative session-history-limit")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--session-source-history-limit=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative session-source-history-limit")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--session-subscriber-history-limit=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative session-subscriber-history-limit")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--source-health-drain-timeout=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative source-health-drain-timeout")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--auto-prioritize-probe-tune-delay=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative auto-prioritize-probe-tune-delay")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upstream-overlimit-cooldown=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative upstream-overlimit-cooldown")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upnp-notify-interval=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative upnp-notify-interval")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upnp-max-age=0s",
	})
	if err == nil {
		t.Fatal("expected error for non-positive upnp-max-age")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upnp-content-directory-update-id-cache-ttl=0s",
	})
	if err == nil {
		t.Fatal("expected error for non-positive upnp-content-directory-update-id-cache-ttl")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upnp-content-directory-update-id-cache-ttl=-1s",
	})
	if err == nil {
		t.Fatal("expected error for negative upnp-content-directory-update-id-cache-ttl")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--ffmpeg-reconnect-delay-max=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative ffmpeg-reconnect-delay-max")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--ffmpeg-reconnect-max-retries=-2",
	})
	if err == nil {
		t.Fatal("expected error for ffmpeg-reconnect-max-retries below -1")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--cycle-failure-min-health=-1ms",
	})
	if err == nil {
		t.Fatal("expected error for negative cycle-failure-min-health")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-max-tunes=-1",
	})
	if err == nil {
		t.Fatal("expected error for negative tune-backoff-max-tunes")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-max-tunes=5",
		"--tune-backoff-interval=0s",
	})
	if err == nil {
		t.Fatal("expected error for non-positive tune-backoff-interval when tune backoff is enabled")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-max-tunes=5",
		"--tune-backoff-cooldown=0s",
	})
	if err == nil {
		t.Fatal("expected error for non-positive tune-backoff-cooldown when tune backoff is enabled")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-interval=-1s",
	})
	if err == nil {
		t.Fatal("expected error for negative tune-backoff-interval")
	}

	_, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--tune-backoff-cooldown=-1s",
	})
	if err == nil {
		t.Fatal("expected error for negative tune-backoff-cooldown")
	}
}

func TestLoadUPnPSettings(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upnp-enabled=true",
		"--upnp-addr=:1910",
		"--upnp-notify-interval=2m",
		"--upnp-max-age=45m",
		"--upnp-content-directory-update-id-cache-ttl=3s",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !cfg.UPnPEnabled {
		t.Fatal("UPnPEnabled = false, want true")
	}
	if cfg.UPnPAddr != ":1910" {
		t.Fatalf("UPnPAddr = %q, want :1910", cfg.UPnPAddr)
	}
	if cfg.UPnPNotifyInterval.String() != "2m0s" {
		t.Fatalf("UPnPNotifyInterval = %s, want 2m0s", cfg.UPnPNotifyInterval)
	}
	if cfg.UPnPMaxAge.String() != "45m0s" {
		t.Fatalf("UPnPMaxAge = %s, want 45m0s", cfg.UPnPMaxAge)
	}
	if cfg.UPnPContentDirectoryUpdateIDCacheTTL.String() != "3s" {
		t.Fatalf(
			"UPnPContentDirectoryUpdateIDCacheTTL = %s, want 3s",
			cfg.UPnPContentDirectoryUpdateIDCacheTTL,
		)
	}
}

func TestLoadUPnPContentDirectoryUpdateIDCacheTTLFromEnvAndFlag(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("UPNP_CONTENT_DIRECTORY_UPDATE_ID_CACHE_TTL", "1500ms")

	cfg, err := Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
	})
	if err != nil {
		t.Fatalf("Load() env error = %v", err)
	}
	if cfg.UPnPContentDirectoryUpdateIDCacheTTL != 1500*time.Millisecond {
		t.Fatalf(
			"UPnPContentDirectoryUpdateIDCacheTTL = %s, want 1.5s from env",
			cfg.UPnPContentDirectoryUpdateIDCacheTTL,
		)
	}

	cfg, err = Load([]string{
		"--device-id=1234ABCD",
		"--device-auth=test-token",
		"--upnp-content-directory-update-id-cache-ttl=4s",
	})
	if err != nil {
		t.Fatalf("Load() flag override error = %v", err)
	}
	if cfg.UPnPContentDirectoryUpdateIDCacheTTL != 4*time.Second {
		t.Fatalf(
			"UPnPContentDirectoryUpdateIDCacheTTL = %s, want 4s from flag",
			cfg.UPnPContentDirectoryUpdateIDCacheTTL,
		)
	}
}

func TestLoadRejectsRemovedLegacyFlags(t *testing.T) {
	clearConfigEnv(t)

	removedFlags := []string{
		"--immediate-open-enabled=true",
		"--heartbeat-enabled=true",
		"--heartbeat-mode=psi",
		"--heartbeat-interval=250ms",
		"--recovery-transition-mode=simple",
	}

	for _, removedFlag := range removedFlags {
		_, err := Load([]string{
			"--device-id=1234ABCD",
			"--device-auth=test-token",
			removedFlag,
		})
		if err == nil {
			t.Fatalf("expected removed flag %q to be rejected", removedFlag)
		}
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()

	keys := []string{
		"PLAYLIST_URL",
		"DB_PATH",
		"HTTP_ADDR",
		"HTTP_ADDR_LEGACY",
		"UPNP_ENABLED",
		"UPNP_ADDR",
		"UPNP_NOTIFY_INTERVAL",
		"UPNP_MAX_AGE",
		"UPNP_CONTENT_DIRECTORY_UPDATE_ID_CACHE_TTL",
		"TUNER_COUNT",
		"FRIENDLY_NAME",
		"DEVICE_ID",
		"DEVICE_AUTH",
		"REFRESH_SCHEDULE",
		"RECONCILE_DYNAMIC_RULE_PAGED",
		"REFRESH_INTERVAL",
		"FFMPEG_PATH",
		"STREAM_MODE",
		"STARTUP_TIMEOUT",
		"STARTUP_RANDOM_ACCESS_RECOVERY_ONLY",
		"MIN_PROBE_BYTES",
		"MAX_FAILOVERS",
		"FAILOVER_TOTAL_TIMEOUT",
		"UPSTREAM_OVERLIMIT_COOLDOWN",
		"FFMPEG_RECONNECT_ENABLED",
		"FFMPEG_RECONNECT_DELAY_MAX",
		"FFMPEG_RECONNECT_MAX_RETRIES",
		"FFMPEG_RECONNECT_HTTP_ERRORS",
		"FFMPEG_COPY_REGENERATE_TIMESTAMPS",
		"PRODUCER_READRATE",
		"PRODUCER_INITIAL_BURST",
		"BUFFER_CHUNK_BYTES",
		"BUFFER_PUBLISH_FLUSH_INTERVAL",
		"BUFFER_TS_ALIGN_188",
		"STALL_DETECT",
		"STALL_HARD_DEADLINE",
		"STALL_POLICY",
		"STALL_MAX_FAILOVERS_PER_STALL",
		"CYCLE_FAILURE_MIN_HEALTH",
		"RECOVERY_FILLER_ENABLED",
		"RECOVERY_FILLER_MODE",
		"RECOVERY_FILLER_INTERVAL",
		"RECOVERY_FILLER_TEXT",
		"RECOVERY_FILLER_ENABLE_AUDIO",
		"SUBSCRIBER_JOIN_LAG_BYTES",
		"SUBSCRIBER_SLOW_CLIENT_POLICY",
		"SUBSCRIBER_MAX_BLOCKED_WRITE",
		"SESSION_IDLE_TIMEOUT",
		"SESSION_DRAIN_TIMEOUT",
		"SESSION_MAX_SUBSCRIBERS",
		"SESSION_HISTORY_LIMIT",
		"SESSION_SOURCE_HISTORY_LIMIT",
		"SESSION_SUBSCRIBER_HISTORY_LIMIT",
		"SOURCE_HEALTH_DRAIN_TIMEOUT",
		"PREEMPT_SETTLE_DELAY",
		"AUTO_PRIORITIZE_PROBE_TUNE_DELAY",
		"AUTO_PRIORITIZE_WORKERS",
		"PROBE_INTERVAL",
		"PROBE_TIMEOUT",
		"ADMIN_AUTH",
		"ADMIN_JSON_BODY_LIMIT_BYTES",
		"REQUEST_TIMEOUT",
		"RATE_LIMIT_RPS",
		"RATE_LIMIT_BURST",
		"RATE_LIMIT_MAX_CLIENTS",
		"RATE_LIMIT_TRUSTED_PROXIES",
		"TUNE_BACKOFF_MAX_TUNES",
		"TUNE_BACKOFF_INTERVAL",
		"TUNE_BACKOFF_COOLDOWN",
		"CATALOG_SEARCH_MAX_TERMS",
		"CATALOG_SEARCH_MAX_DISJUNCTS",
		"CATALOG_SEARCH_MAX_TERM_RUNES",
		"ENABLE_METRICS",
		"HTTP_REQUEST_LOG_ENABLED",
		"LOG_DIR",
		"LOG_LEVEL",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
}
