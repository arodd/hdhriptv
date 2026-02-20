package scheduler

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

type memorySettingsStore struct {
	mu     sync.Mutex
	values map[string]string
}

func newMemorySettingsStore(values map[string]string) *memorySettingsStore {
	cloned := make(map[string]string, len(values))
	for k, v := range values {
		cloned[k] = v
	}
	return &memorySettingsStore{values: cloned}
}

func (m *memorySettingsStore) GetSetting(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[key]
	if !ok {
		return "", sql.ErrNoRows
	}
	return value, nil
}

func (m *memorySettingsStore) SetSettings(_ context.Context, values map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, value := range values {
		m.values[key] = value
	}
	return nil
}

func TestSchedulerLoadAndUpdate(t *testing.T) {
	store := newMemorySettingsStore(map[string]string{
		settingJobsTimezone:         "America/Chicago",
		settingPlaylistSyncEnabled:  "true",
		settingPlaylistSyncCron:     "*/10 * * * *",
		settingAutoPrioritizeEnable: "false",
		settingAutoPrioritizeCron:   "30 3 * * *",
		settingDVRLineupSyncEnable:  "false",
		settingDVRLineupSyncCron:    "*/30 * * * *",
	})

	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobDVRLineupSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(dvr_lineup_sync) error = %v", err)
	}

	if err := svc.LoadFromSettings(context.Background()); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	svc.Start()
	defer func() { <-svc.Stop().Done() }()

	schedules, err := svc.ListSchedules(context.Background())
	if err != nil {
		t.Fatalf("ListSchedules() error = %v", err)
	}
	if len(schedules) != 3 {
		t.Fatalf("len(schedules) = %d, want 3", len(schedules))
	}

	var playlistSchedule JobSchedule
	for _, schedule := range schedules {
		if schedule.JobName == jobs.JobPlaylistSync {
			playlistSchedule = schedule
		}
	}
	if !playlistSchedule.Enabled {
		t.Fatal("playlist sync schedule should be enabled")
	}
	if playlistSchedule.NextRun.IsZero() {
		t.Fatal("playlist sync next_run should be populated")
	}

	if err := svc.UpdateJobSchedule(context.Background(), jobs.JobAutoPrioritize, true, "*/15 * * * *"); err != nil {
		t.Fatalf("UpdateJobSchedule(auto_prioritize) error = %v", err)
	}
	next, ok := svc.NextRun(jobs.JobAutoPrioritize)
	if !ok {
		t.Fatal("NextRun(auto_prioritize) expected configured entry")
	}
	if next.IsZero() {
		t.Fatal("NextRun(auto_prioritize) must not be zero")
	}

	if err := svc.UpdateJobSchedule(context.Background(), jobs.JobAutoPrioritize, true, "not a cron"); err == nil {
		t.Fatal("UpdateJobSchedule(invalid cron) expected error")
	}
}

func TestSchedulerValidateCronFormats(t *testing.T) {
	store := newMemorySettingsStore(nil)
	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := svc.ValidateCron("*/30 * * * *"); err != nil {
		t.Fatalf("ValidateCron(5-field) error = %v", err)
	}
	if err := svc.ValidateCron("*/10 * * * * *"); err != nil {
		t.Fatalf("ValidateCron(6-field optional seconds) error = %v", err)
	}
	if err := svc.ValidateCron(""); err == nil {
		t.Fatal("ValidateCron(blank) expected error")
	}
	if err := svc.ValidateCron("hello world"); err == nil {
		t.Fatal("ValidateCron(invalid) expected error")
	}
}

func TestSchedulerTimezoneFallbackToUTC(t *testing.T) {
	store := newMemorySettingsStore(map[string]string{
		settingJobsTimezone:         "Not/A_Real_Timezone",
		settingPlaylistSyncEnabled:  "false",
		settingPlaylistSyncCron:     "*/30 * * * *",
		settingAutoPrioritizeEnable: "false",
		settingAutoPrioritizeCron:   "30 3 * * *",
		settingDVRLineupSyncEnable:  "false",
		settingDVRLineupSyncCron:    "*/30 * * * *",
	})

	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobDVRLineupSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}

	if err := svc.LoadFromSettings(context.Background()); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	if svc.Timezone() != "UTC" {
		t.Fatalf("Timezone() = %q, want UTC", svc.Timezone())
	}
}

func TestSchedulerCanFireSecondGranularityJobs(t *testing.T) {
	store := newMemorySettingsStore(map[string]string{
		settingJobsTimezone:         "UTC",
		settingPlaylistSyncEnabled:  "true",
		settingPlaylistSyncCron:     "*/1 * * * * *",
		settingAutoPrioritizeEnable: "false",
		settingAutoPrioritizeCron:   "30 3 * * *",
		settingDVRLineupSyncEnable:  "false",
		settingDVRLineupSyncCron:    "*/30 * * * *",
	})

	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	triggered := make(chan struct{}, 1)
	if err := svc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error {
		select {
		case triggered <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobDVRLineupSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}

	if err := svc.LoadFromSettings(context.Background()); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	svc.Start()
	defer func() { <-svc.Stop().Done() }()

	select {
	case <-triggered:
	case <-time.After(2200 * time.Millisecond):
		t.Fatal("expected scheduler callback to fire within 2.2s")
	}
}

func TestSchedulerStopCancelsCallbackContext(t *testing.T) {
	store := newMemorySettingsStore(map[string]string{
		settingJobsTimezone:         "UTC",
		settingPlaylistSyncEnabled:  "true",
		settingPlaylistSyncCron:     "*/1 * * * * *",
		settingAutoPrioritizeEnable: "false",
		settingAutoPrioritizeCron:   "30 3 * * *",
		settingDVRLineupSyncEnable:  "false",
		settingDVRLineupSyncCron:    "*/30 * * * *",
	})

	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	if err := svc.RegisterJob(jobs.JobPlaylistSync, func(ctx context.Context, _ string) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case canceled <- struct{}{}:
		default:
		}
		return ctx.Err()
	}); err != nil {
		t.Fatalf("RegisterJob() error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobDVRLineupSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(dvr_lineup_sync) error = %v", err)
	}

	if err := svc.LoadFromSettings(context.Background()); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	svc.Start()

	select {
	case <-started:
	case <-time.After(2200 * time.Millisecond):
		t.Fatal("expected scheduler callback to fire within 2.2s")
	}

	stopCtx := svc.Stop()
	<-stopCtx.Done()

	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected scheduler callback context to be canceled on Stop()")
	}
}

func TestSchedulerCallbackContextRemainsCanceledAfterStop(t *testing.T) {
	store := newMemorySettingsStore(nil)
	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	svc.Start()
	stopCtx := svc.Stop()
	<-stopCtx.Done()

	ctx := svc.callbackContext()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("callbackContext() should be canceled after Stop()")
	}

	ctx = svc.callbackContext()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("callbackContext() should remain canceled after Stop()")
	}
}

func TestSchedulerLoadFromSettingsWaitsForRunningCallbacksBeforeStartingReplacement(t *testing.T) {
	store := newMemorySettingsStore(map[string]string{
		settingJobsTimezone:         "UTC",
		settingPlaylistSyncEnabled:  "true",
		settingPlaylistSyncCron:     "*/1 * * * * *",
		settingAutoPrioritizeEnable: "false",
		settingAutoPrioritizeCron:   "30 3 * * *",
		settingDVRLineupSyncEnable:  "false",
		settingDVRLineupSyncCron:    "*/30 * * * *",
	})

	svc, err := New(store, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-svc.Stop().Done()
	})

	var invocationCount atomic.Int32
	invocations := make(chan int32, 8)
	if err := svc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error {
		count := invocationCount.Add(1)
		select {
		case invocations <- count:
		default:
		}
		<-release
		return nil
	}); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := svc.RegisterJob(jobs.JobDVRLineupSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(dvr_lineup_sync) error = %v", err)
	}

	if err := svc.LoadFromSettings(context.Background()); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	svc.Start()

	select {
	case n := <-invocations:
		if n != 1 {
			t.Fatalf("first callback invocation = %d, want 1", n)
		}
	case <-time.After(2200 * time.Millisecond):
		t.Fatal("expected first callback invocation within 2.2s")
	}

	reloadDone := make(chan error, 1)
	go func() {
		reloadDone <- svc.LoadFromSettings(context.Background())
	}()

	select {
	case n := <-invocations:
		t.Fatalf("unexpected callback invocation %d during reload; replacement engine started before old callbacks drained", n)
	case <-time.After(3200 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })

	select {
	case err := <-reloadDone:
		if err != nil {
			t.Fatalf("LoadFromSettings() during reload error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected LoadFromSettings() to complete after callback drain")
	}
}
