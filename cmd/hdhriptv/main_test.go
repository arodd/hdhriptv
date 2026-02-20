package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/config"
	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
)

func TestResolveIdentitySettingsGenerateAndPersistWhenMissing(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	cfg := config.Config{}
	if err := resolveIdentitySettings(ctx, store, &cfg, identityExplicitSettings{}); err != nil {
		t.Fatalf("resolveIdentitySettings() error = %v", err)
	}

	if cfg.FriendlyName != "HDHR IPTV" {
		t.Fatalf("FriendlyName = %q, want HDHR IPTV", cfg.FriendlyName)
	}
	if !isValidDeviceID(cfg.DeviceID) {
		t.Fatalf("DeviceID = %q, want 8-char uppercase hex", cfg.DeviceID)
	}
	if !isHexString(cfg.DeviceAuth) || len(cfg.DeviceAuth) != 32 {
		t.Fatalf("DeviceAuth = %q, want 32-char hex token", cfg.DeviceAuth)
	}

	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityFriendlyName, cfg.FriendlyName)
	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityDeviceID, cfg.DeviceID)
	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityDeviceAuth, cfg.DeviceAuth)
}

func TestResolveIdentitySettingsReusePersistedWhenNotExplicit(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingIdentityFriendlyName: "Living Room",
		sqlite.SettingIdentityDeviceID:     "ABCDEF12",
		sqlite.SettingIdentityDeviceAuth:   "persisted-auth-token",
	}); err != nil {
		t.Fatalf("SetSettings(seed identity) error = %v", err)
	}

	cfg := config.Config{
		FriendlyName: "CLI Friendly",
		DeviceID:     "11112222",
		DeviceAuth:   "cli-auth",
	}
	if err := resolveIdentitySettings(ctx, store, &cfg, identityExplicitSettings{}); err != nil {
		t.Fatalf("resolveIdentitySettings() error = %v", err)
	}

	if cfg.FriendlyName != "Living Room" {
		t.Fatalf("FriendlyName = %q, want persisted value", cfg.FriendlyName)
	}
	if cfg.DeviceID != "ABCDEF12" {
		t.Fatalf("DeviceID = %q, want persisted value", cfg.DeviceID)
	}
	if cfg.DeviceAuth != "persisted-auth-token" {
		t.Fatalf("DeviceAuth = %q, want persisted value", cfg.DeviceAuth)
	}
}

func TestResolveIdentitySettingsExplicitOverrideUpdatesPersistence(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingIdentityFriendlyName: "Old Name",
		sqlite.SettingIdentityDeviceID:     "AAAABBBB",
		sqlite.SettingIdentityDeviceAuth:   "old-auth",
	}); err != nil {
		t.Fatalf("SetSettings(seed identity) error = %v", err)
	}

	cfg := config.Config{
		FriendlyName: "New Name",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "new-auth",
	}
	if err := resolveIdentitySettings(ctx, store, &cfg, identityExplicitSettings{
		FriendlyName: true,
		DeviceID:     true,
		DeviceAuth:   true,
	}); err != nil {
		t.Fatalf("resolveIdentitySettings() error = %v", err)
	}

	if cfg.FriendlyName != "New Name" {
		t.Fatalf("FriendlyName = %q, want New Name", cfg.FriendlyName)
	}
	if cfg.DeviceID != "1234ABCD" {
		t.Fatalf("DeviceID = %q, want 1234ABCD", cfg.DeviceID)
	}
	if cfg.DeviceAuth != "new-auth" {
		t.Fatalf("DeviceAuth = %q, want new-auth", cfg.DeviceAuth)
	}

	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityFriendlyName, "New Name")
	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityDeviceID, "1234ABCD")
	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityDeviceAuth, "new-auth")
}

func TestResolveIdentitySettingsRepairsInvalidPersistedDeviceID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingIdentityFriendlyName: "Family Room",
		sqlite.SettingIdentityDeviceID:     "bad-id",
		sqlite.SettingIdentityDeviceAuth:   "existing-auth",
	}); err != nil {
		t.Fatalf("SetSettings(seed identity) error = %v", err)
	}

	cfg := config.Config{
		FriendlyName: "Ignored",
		DeviceID:     "DEADBEEF",
		DeviceAuth:   "ignored-auth",
	}
	if err := resolveIdentitySettings(ctx, store, &cfg, identityExplicitSettings{}); err != nil {
		t.Fatalf("resolveIdentitySettings() error = %v", err)
	}

	if cfg.DeviceID != "DEADBEEF" {
		t.Fatalf("DeviceID = %q, want DEADBEEF", cfg.DeviceID)
	}
	assertSettingEquals(t, ctx, store, sqlite.SettingIdentityDeviceID, "DEADBEEF")
}

func TestSettingExplicitlyProvidedPrefersFlagOverEnv(t *testing.T) {
	t.Setenv("DEVICE_ID", "ABCDEF12")

	if !settingExplicitlyProvided([]string{}, "--device-id", "DEVICE_ID") {
		t.Fatal("settingExplicitlyProvided(no flag, env set) = false, want true")
	}
	if settingExplicitlyProvided([]string{"--device-id", ""}, "--device-id", "DEVICE_ID") {
		t.Fatal("settingExplicitlyProvided(empty flag override) = true, want false")
	}
	if !settingExplicitlyProvided([]string{"--device-id=9999AAAA"}, "--device-id", "DEVICE_ID") {
		t.Fatal("settingExplicitlyProvided(non-empty flag) = false, want true")
	}
}

func TestRunAndWaitPlaylistSyncTimeoutCancelsUnderlyingRun(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingPlaylistURL: "http://example.com/playlist.m3u",
	}); err != nil {
		t.Fatalf("SetSettings(playlist url) error = %v", err)
	}

	runner, err := jobs.NewRunner(store)
	if err != nil {
		t.Fatalf("jobs.NewRunner() error = %v", err)
	}
	defer runner.Close()

	jobCanceled := make(chan error, 1)
	jobFn := func(jobCtx context.Context, _ *jobs.RunContext) error {
		<-jobCtx.Done()
		jobCanceled <- jobCtx.Err()
		return jobCtx.Err()
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()

	waitErr := runAndWaitPlaylistSync(waitCtx, runner, jobFn)
	if !errors.Is(waitErr, context.DeadlineExceeded) && !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("runAndWaitPlaylistSync() error = %v, want context timeout/canceled", waitErr)
	}

	select {
	case canceledErr := <-jobCanceled:
		if !errors.Is(canceledErr, context.Canceled) && !errors.Is(canceledErr, context.DeadlineExceeded) {
			t.Fatalf("job cancellation error = %v, want canceled/deadline", canceledErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for playlist job function cancellation")
	}

	runs, err := runner.ListRuns(context.Background(), jobs.JobPlaylistSync, 1, 0)
	if err != nil {
		t.Fatalf("runner.ListRuns() error = %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("len(runs) = %d, want 1", len(runs))
	}

	run := waitForJobRunDone(t, runner, runs[0].RunID)
	if run.Status != jobs.StatusCanceled {
		t.Fatalf("run status = %q, want %q", run.Status, jobs.StatusCanceled)
	}
}

func startupSyncJellyfinReloadError(detail string) error {
	return errors.New("reload dvr lineup after playlist sync: reload lineup for provider jellyfin: " + strings.TrimSpace(detail))
}

func TestRunInitialPlaylistSyncAfterListenerStartWaitsForReadiness(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	runCalls := 0
	err := runInitialPlaylistSyncAfterListenerStart(
		ctx,
		nil,
		"127.0.0.1:1",
		func(context.Context) error {
			runCalls++
			return nil
		},
	)
	if err == nil {
		t.Fatal("runInitialPlaylistSyncAfterListenerStart() error = nil, want non-nil readiness failure")
	}
	if runCalls != 0 {
		t.Fatalf("runCalls = %d, want 0 when listener readiness was never reached", runCalls)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartReadinessFailureLogsDuration(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runCalls := 0

	err := runInitialPlaylistSyncAfterListenerStart(
		context.Background(),
		logger,
		"invalid",
		func(context.Context) error {
			runCalls++
			return nil
		},
	)
	if err == nil {
		t.Fatal("runInitialPlaylistSyncAfterListenerStart() error = nil, want readiness error")
	}
	if runCalls != 0 {
		t.Fatalf("runCalls = %d, want 0 on readiness failure", runCalls)
	}

	out := logs.String()
	if !strings.Contains(out, "initial_sync_phase=failed") {
		t.Fatalf("logs missing failed phase event: %s", out)
	}
	if !strings.Contains(out, "duration=") {
		t.Fatalf("logs missing duration field on failed phase event: %s", out)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartRetriesTransientJellyfinFailure(t *testing.T) {
	t.Parallel()

	httpAddr := startTestHealthzServer(t)
	attempts := 0
	transientErr := startupSyncJellyfinReloadError(
		`reload device lineup "8F07FDC6": jellyfin tuner-host refresh trigger failed: POST /LiveTv/TunerHosts failed: 500 Internal Server Error: connect: connection refused`,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := runInitialPlaylistSyncAfterListenerStart(
		ctx,
		nil,
		httpAddr,
		func(context.Context) error {
			attempts++
			if attempts == 1 {
				return transientErr
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runInitialPlaylistSyncAfterListenerStart() error = %v, want nil after transient retry", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (single retry)", attempts)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartCompletedLogIncludesAttemptAndDuration(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	httpAddr := startTestHealthzServer(t)

	err := runInitialPlaylistSyncAfterListenerStart(
		context.Background(),
		logger,
		httpAddr,
		func(context.Context) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runInitialPlaylistSyncAfterListenerStart() error = %v, want nil", err)
	}

	out := logs.String()
	if !strings.Contains(out, "initial_sync_phase=completed") {
		t.Fatalf("logs missing completed phase event: %s", out)
	}
	if !strings.Contains(out, "attempt=1") {
		t.Fatalf("logs missing attempt field on completed phase event: %s", out)
	}
	if !strings.Contains(out, "duration=") {
		t.Fatalf("logs missing duration field on completed phase event: %s", out)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartRejectsNilRunner(t *testing.T) {
	t.Parallel()

	err := runInitialPlaylistSyncAfterListenerStart(context.Background(), nil, "127.0.0.1:5004", nil)
	if err == nil {
		t.Fatal("runInitialPlaylistSyncAfterListenerStart() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "initial playlist sync runner is required") {
		t.Fatalf("runInitialPlaylistSyncAfterListenerStart() error = %v, want missing runner error", err)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartStopsOnNonTransientError(t *testing.T) {
	t.Parallel()

	httpAddr := startTestHealthzServer(t)
	attempts := 0
	nonTransientErr := startupSyncJellyfinReloadError(
		`POST /LiveTv/TunerHosts failed: 401 Unauthorized`,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runInitialPlaylistSyncAfterListenerStart(
		ctx,
		nil,
		httpAddr,
		func(context.Context) error {
			attempts++
			return nonTransientErr
		},
	)
	if !errors.Is(err, nonTransientErr) {
		t.Fatalf("runInitialPlaylistSyncAfterListenerStart() error = %v, want %v", err, nonTransientErr)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 for non-transient failure", attempts)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartReturnsLastErrorAfterRetryExhaustion(t *testing.T) {
	httpAddr := startTestHealthzServer(t)
	retryErrs := []error{
		startupSyncJellyfinReloadError(`POST /LiveTv/TunerHosts failed: 500 Internal Server Error`),
		startupSyncJellyfinReloadError(`dial tcp: connection reset by peer`),
		startupSyncJellyfinReloadError(`service temporarily unavailable`),
		startupSyncJellyfinReloadError(`request timeout while waiting for response`),
	}
	attempts := 0

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := runInitialPlaylistSyncAfterListenerStart(
		ctx,
		nil,
		httpAddr,
		func(context.Context) error {
			idx := attempts
			attempts++
			if idx >= len(retryErrs) {
				return retryErrs[len(retryErrs)-1]
			}
			return retryErrs[idx]
		},
	)
	wantErr := retryErrs[len(retryErrs)-1]
	if !errors.Is(err, wantErr) {
		t.Fatalf("runInitialPlaylistSyncAfterListenerStart() error = %v, want %v", err, wantErr)
	}
	if attempts != initialPlaylistSyncRetryAttempts {
		t.Fatalf("attempts = %d, want %d (retry exhaustion)", attempts, initialPlaylistSyncRetryAttempts)
	}
}

func TestRunInitialPlaylistSyncAfterListenerStartStopsBackoffWhenBudgetCanceled(t *testing.T) {
	httpAddr := startTestHealthzServer(t)
	attempts := 0
	transientErr := startupSyncJellyfinReloadError(`dial tcp: connect: connection refused`)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := runInitialPlaylistSyncAfterListenerStart(
		ctx,
		nil,
		httpAddr,
		func(context.Context) error {
			attempts++
			return transientErr
		},
	)
	elapsed := time.Since(started)

	if !errors.Is(err, transientErr) {
		t.Fatalf("runInitialPlaylistSyncAfterListenerStart() error = %v, want last transient error %v", err, transientErr)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 when retry budget is canceled during backoff", attempts)
	}
	if elapsed >= initialPlaylistSyncRetryBaseDelay {
		t.Fatalf("elapsed = %s, want < %s when backoff is canceled by context", elapsed, initialPlaylistSyncRetryBaseDelay)
	}
}

func TestWaitForHTTPReadinessRejectsEmptyAddr(t *testing.T) {
	t.Parallel()

	err := waitForHTTPReadiness(context.Background(), " \t ", 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitForHTTPReadiness() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "http listen addr is required") {
		t.Fatalf("waitForHTTPReadiness() error = %v, want missing addr error", err)
	}
}

func TestWaitForHTTPReadinessRejectsInvalidAddr(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waitForHTTPReadiness(ctx, "invalid", 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitForHTTPReadiness() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "invalid http listen addr") {
		t.Fatalf("waitForHTTPReadiness() error = %v, want invalid addr error", err)
	}
}

func TestWaitForHTTPReadinessCanceledContext(t *testing.T) {
	t.Parallel()

	httpAddr := startTestHealthzServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForHTTPReadiness(ctx, httpAddr, 10*time.Millisecond)
	if err == nil {
		t.Fatal("waitForHTTPReadiness() error = nil, want canceled context error")
	}
	if !strings.Contains(err.Error(), "wait for http listener readiness") {
		t.Fatalf("waitForHTTPReadiness() error = %v, want readiness context prefix", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForHTTPReadiness() error = %v, want context canceled", err)
	}
}

func TestWaitForHTTPReadinessConnectionRefusedRetriesUntilContextDeadline(t *testing.T) {
	t.Parallel()

	httpAddr := closedLoopbackAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := waitForHTTPReadiness(ctx, httpAddr, 5*time.Millisecond)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("waitForHTTPReadiness() error = nil, want deadline error after retries")
	}
	if !strings.Contains(err.Error(), "wait for http listener readiness") {
		t.Fatalf("waitForHTTPReadiness() error = %v, want readiness context prefix", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForHTTPReadiness() error = %v, want context deadline exceeded", err)
	}
	if elapsed < 90*time.Millisecond {
		t.Fatalf("waitForHTTPReadiness() elapsed = %s, want >= 90ms to show retry loop", elapsed)
	}
}

func TestWaitForHTTPReadinessRetriesAfterNon2xx(t *testing.T) {
	t.Parallel()

	httpAddr, probeCount := startTestHealthzSequenceServer(t, func(call int) int {
		if call == 1 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := waitForHTTPReadiness(ctx, httpAddr, 5*time.Millisecond); err != nil {
		t.Fatalf("waitForHTTPReadiness() error = %v, want nil after retry", err)
	}
	if got := probeCount.Load(); got < 2 {
		t.Fatalf("probe count = %d, want >= 2 (non-2xx then recovery)", got)
	}
}

func TestWaitForHTTPReadinessDefaultsPollIntervalWhenNonPositive(t *testing.T) {
	t.Parallel()

	httpAddr, probeCount := startTestHealthzSequenceServer(t, func(call int) int {
		if call == 1 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := time.Now()
	if err := waitForHTTPReadiness(ctx, httpAddr, 0); err != nil {
		t.Fatalf("waitForHTTPReadiness() error = %v, want nil after retry", err)
	}
	elapsed := time.Since(started)
	if got := probeCount.Load(); got < 2 {
		t.Fatalf("probe count = %d, want >= 2 with default poll interval", got)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("waitForHTTPReadiness() elapsed = %s, want >= 80ms to reflect default poll interval", elapsed)
	}
}

func TestWaitForHTTPReadinessLogsDebugProbeFailures(t *testing.T) {
	// Do not run t.Parallel(): mutates global slog default.
	var logs bytes.Buffer
	origDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() {
		slog.SetDefault(origDefault)
	})

	httpAddr := closedLoopbackAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	err := waitForHTTPReadiness(ctx, httpAddr, 5*time.Millisecond)
	if err == nil {
		t.Fatal("waitForHTTPReadiness() error = nil, want deadline error after probe failures")
	}

	out := logs.String()
	if !strings.Contains(out, "http listener readiness probe failed; retrying") {
		t.Fatalf("logs missing readiness probe failure debug message: %s", out)
	}
	if !strings.Contains(out, "poll_interval=5ms") {
		t.Fatalf("logs missing poll_interval debug field: %s", out)
	}
	if !strings.Contains(out, "probe_url=http://") {
		t.Fatalf("logs missing probe_url debug field: %s", out)
	}
}

func TestReadinessProbeURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		httpAddr string
		wantURL  string
		wantErr  bool
	}{
		{
			name:     "wildcard_ipv4_maps_to_loopback",
			httpAddr: "0.0.0.0:5004",
			wantURL:  "http://127.0.0.1:5004/healthz",
		},
		{
			name:     "wildcard_ipv6_maps_to_loopback",
			httpAddr: "[::]:5004",
			wantURL:  "http://127.0.0.1:5004/healthz",
		},
		{
			name:     "empty_host_maps_to_loopback",
			httpAddr: ":5004",
			wantURL:  "http://127.0.0.1:5004/healthz",
		},
		{
			name:     "explicit_ipv6_loopback_preserved",
			httpAddr: "[::1]:5004",
			wantURL:  "http://[::1]:5004/healthz",
		},
		{
			name:     "explicit_host_preserved",
			httpAddr: "192.168.1.2:5004",
			wantURL:  "http://192.168.1.2:5004/healthz",
		},
		{
			name:     "invalid_addr",
			httpAddr: "invalid",
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotURL, err := readinessProbeURL(tc.httpAddr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("readinessProbeURL(%q) error = nil, want non-nil", tc.httpAddr)
				}
				return
			}

			if err != nil {
				t.Fatalf("readinessProbeURL(%q) error = %v, want nil", tc.httpAddr, err)
			}
			if gotURL != tc.wantURL {
				t.Fatalf("readinessProbeURL(%q) = %q, want %q", tc.httpAddr, gotURL, tc.wantURL)
			}
		})
	}
}

func TestIsTransientStartupJellyfinLineupReloadError(t *testing.T) {
	t.Parallel()

	jellyfinErr := func(detail string) error {
		return errors.New("reload dvr lineup after playlist sync: reload lineup for provider jellyfin: " + detail)
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil_error",
			err:  nil,
			want: false,
		},
		{
			name: "jellyfin_500",
			err: errors.New(
				`playlist sync run 1 failed: reload dvr lineup after playlist sync: reload lineup for provider jellyfin: POST /LiveTv/TunerHosts failed: 500 Internal Server Error`,
			),
			want: true,
		},
		{
			name: "jellyfin_connection_refused",
			err:  jellyfinErr("connect: connection refused"),
			want: true,
		},
		{
			name: "jellyfin_connection_reset",
			err:  jellyfinErr("write tcp: connection reset by peer"),
			want: true,
		},
		{
			name: "jellyfin_network_unreachable",
			err:  jellyfinErr("dial tcp: connect: network is unreachable"),
			want: true,
		},
		{
			name: "jellyfin_broken_pipe",
			err:  jellyfinErr("write tcp 172.20.0.5:5004->172.20.0.8:8096: write: broken pipe"),
			want: true,
		},
		{
			name: "jellyfin_deadline_exceeded",
			err:  jellyfinErr("context deadline exceeded"),
			want: true,
		},
		{
			name: "jellyfin_timed_out",
			err:  jellyfinErr("i/o timed out"),
			want: true,
		},
		{
			name: "jellyfin_timeout",
			err:  jellyfinErr("request timeout while waiting for response"),
			want: true,
		},
		{
			name: "jellyfin_temporarily_unavailable",
			err:  jellyfinErr("service temporarily unavailable"),
			want: true,
		},
		{
			name: "jellyfin_no_route_to_host",
			err:  jellyfinErr("dial tcp: no route to host"),
			want: true,
		},
		{
			name: "jellyfin_eof",
			err:  jellyfinErr("EOF"),
			want: true,
		},
		{
			name: "jellyfin_non_transient_401",
			err: errors.New(
				`reload dvr lineup after playlist sync: reload lineup for provider jellyfin: POST /LiveTv/TunerHosts failed: 401 Unauthorized`,
			),
			want: false,
		},
		{
			name: "other_provider_500",
			err: errors.New(
				`reload dvr lineup after playlist sync: reload lineup for provider channels: PUT /dvr/lineups/USA failed: 500 Internal Server Error`,
			),
			want: false,
		},
		{
			name: "unrelated_error",
			err:  errors.New("refresh playlist: not configured"),
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isTransientStartupJellyfinLineupReloadError(tc.err)
			if got != tc.want {
				t.Fatalf("isTransientStartupJellyfinLineupReloadError() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitialPlaylistSyncRetryDelayCapsAtMax(t *testing.T) {
	t.Parallel()

	if got, want := initialPlaylistSyncRetryDelay(1), 1*time.Second; got != want {
		t.Fatalf("initialPlaylistSyncRetryDelay(1) = %s, want %s", got, want)
	}
	if got, want := initialPlaylistSyncRetryDelay(4), 8*time.Second; got != want {
		t.Fatalf("initialPlaylistSyncRetryDelay(4) = %s, want %s", got, want)
	}
	if got, want := initialPlaylistSyncRetryDelay(7), 8*time.Second; got != want {
		t.Fatalf("initialPlaylistSyncRetryDelay(7) = %s, want %s", got, want)
	}
}

func startTestHealthzSequenceServer(
	t *testing.T,
	statusForCall func(call int) int,
) (string, *atomic.Int32) {
	t.Helper()

	if statusForCall == nil {
		statusForCall = func(int) int { return http.StatusOK }
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	var callCount atomic.Int32
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/healthz" {
				http.NotFound(w, r)
				return
			}
			status := statusForCall(int(callCount.Add(1)))
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}),
	}

	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
		<-done
	})

	return listener.Addr().String(), &callCount
}

func closedLoopbackAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
	return addr
}

func openTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "hdhr-iptv-test.db")
	store, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open(%q) error = %v", dbPath, err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func waitForJobRunDone(t *testing.T, runner *jobs.Runner, runID int64) jobs.Run {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := runner.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("runner.GetRun(%d) error = %v", runID, err)
		}
		if run.Status != jobs.StatusRunning {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %d did not finish before timeout", runID)
	return jobs.Run{}
}

func startTestHealthzServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/healthz" {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}),
	}

	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
		<-done
	})

	return listener.Addr().String()
}

func assertSettingEquals(t *testing.T, ctx context.Context, store *sqlite.Store, key, want string) {
	t.Helper()
	got, err := store.GetSetting(ctx, key)
	if err != nil {
		t.Fatalf("GetSetting(%q) error = %v", key, err)
	}
	if got != want {
		t.Fatalf("GetSetting(%q) = %q, want %q", key, got, want)
	}
}

func isValidDeviceID(value string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	if len(value) != 8 {
		return false
	}
	for _, ch := range value {
		isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

func isHexString(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, ch := range value {
		isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}
