package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/analyzer"
	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/stream"
)

type fakeAutoSettings struct {
	values map[string]string
}

func (f *fakeAutoSettings) GetSetting(_ context.Context, key string) (string, error) {
	if f == nil || f.values == nil {
		return "", sql.ErrNoRows
	}
	value, ok := f.values[key]
	if !ok {
		return "", sql.ErrNoRows
	}
	return value, nil
}

type fakeAutoChannels struct {
	channels []channels.Channel
	sources  map[int64][]channels.Source

	mu                           sync.Mutex
	reorders                     map[int64][]int64
	reorderErrByChannel          map[int64]error
	listSourcesErrByChannel      map[int64]error
	profileUpdates               map[int64][]channels.SourceProfileUpdate
	profileErr                   error
	listSourcesCalls             int
	listSourcesByChannelIDsCalls int
	listSourcesByChannelIDsErr   error
}

func (f *fakeAutoChannels) ListEnabled(_ context.Context) ([]channels.Channel, error) {
	out := make([]channels.Channel, 0, len(f.channels))
	for _, ch := range f.channels {
		if !ch.Enabled {
			continue
		}
		out = append(out, ch)
	}
	return out, nil
}

func (f *fakeAutoChannels) ListSources(_ context.Context, channelID int64, _ bool) ([]channels.Source, error) {
	f.mu.Lock()
	f.listSourcesCalls++
	listErr := error(nil)
	if f.listSourcesErrByChannel != nil {
		listErr = f.listSourcesErrByChannel[channelID]
	}
	f.mu.Unlock()
	if listErr != nil {
		return nil, listErr
	}

	sources, ok := f.sources[channelID]
	if !ok {
		return nil, channels.ErrChannelNotFound
	}
	out := make([]channels.Source, len(sources))
	copy(out, sources)
	return out, nil
}

func (f *fakeAutoChannels) ListSourcesByChannelIDs(_ context.Context, channelIDs []int64, _ bool) (map[int64][]channels.Source, error) {
	f.mu.Lock()
	f.listSourcesByChannelIDsCalls++
	err := f.listSourcesByChannelIDsErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}

	out := make(map[int64][]channels.Source, len(channelIDs))
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		sources, ok := f.sources[channelID]
		if !ok {
			return nil, channels.ErrChannelNotFound
		}
		copied := make([]channels.Source, len(sources))
		copy(copied, sources)
		out[channelID] = copied
	}
	return out, nil
}

func (f *fakeAutoChannels) ReorderSources(_ context.Context, channelID int64, sourceIDs []int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reorderErrByChannel != nil {
		if err := f.reorderErrByChannel[channelID]; err != nil {
			return err
		}
	}

	if f.reorders == nil {
		f.reorders = make(map[int64][]int64)
	}
	f.reorders[channelID] = append([]int64(nil), sourceIDs...)
	return nil
}

func (f *fakeAutoChannels) UpdateSourceProfile(_ context.Context, sourceID int64, profile channels.SourceProfileUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.profileErr != nil {
		return f.profileErr
	}
	if f.profileUpdates == nil {
		f.profileUpdates = make(map[int64][]channels.SourceProfileUpdate)
	}
	f.profileUpdates[sourceID] = append(f.profileUpdates[sourceID], profile)
	return nil
}

type fakeMetricsStore struct {
	mu      sync.Mutex
	metrics map[string]StreamMetric
	upserts map[string]int
}

func (f *fakeMetricsStore) GetStreamMetric(_ context.Context, itemKey string) (StreamMetric, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	metric, ok := f.metrics[itemKey]
	if !ok {
		return StreamMetric{}, sql.ErrNoRows
	}
	return metric, nil
}

func (f *fakeMetricsStore) UpsertStreamMetric(_ context.Context, metric StreamMetric) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.metrics == nil {
		f.metrics = make(map[string]StreamMetric)
	}
	if f.upserts == nil {
		f.upserts = make(map[string]int)
	}
	f.metrics[metric.ItemKey] = metric
	f.upserts[metric.ItemKey]++
	return nil
}

type fakeAnalyzer struct {
	mu      sync.Mutex
	results map[string]analyzer.Metrics
	errs    map[string]error
	calls   map[string]int
}

func (f *fakeAnalyzer) Analyze(_ context.Context, streamURL string) (analyzer.Metrics, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.calls == nil {
		f.calls = make(map[string]int)
	}
	f.calls[streamURL]++

	if err, ok := f.errs[streamURL]; ok {
		return analyzer.Metrics{}, err
	}
	result, ok := f.results[streamURL]
	if !ok {
		return analyzer.Metrics{}, fmt.Errorf("unexpected stream URL %q", streamURL)
	}
	return result, nil
}

type timedAnalyzer struct {
	mu    sync.Mutex
	calls []time.Time
}

func (a *timedAnalyzer) Analyze(_ context.Context, _ string) (analyzer.Metrics, error) {
	a.mu.Lock()
	a.calls = append(a.calls, time.Now().UTC())
	a.mu.Unlock()
	return analyzer.Metrics{
		Width:      1280,
		Height:     720,
		FPS:        30,
		BitrateBPS: 2_000_000,
	}, nil
}

func (a *timedAnalyzer) callTimes() []time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]time.Time, len(a.calls))
	copy(out, a.calls)
	return out
}

type scriptedProbeResult struct {
	metrics analyzer.Metrics
	err     error
}

type scriptedAnalyzer struct {
	mu        sync.Mutex
	results   map[string][]scriptedProbeResult
	callOrder []string
	callTimes []time.Time
}

func (a *scriptedAnalyzer) Analyze(_ context.Context, streamURL string) (analyzer.Metrics, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.callOrder = append(a.callOrder, streamURL)
	a.callTimes = append(a.callTimes, time.Now().UTC())

	queued := a.results[streamURL]
	if len(queued) == 0 {
		return analyzer.Metrics{}, fmt.Errorf("unexpected stream URL %q", streamURL)
	}
	next := queued[0]
	a.results[streamURL] = queued[1:]
	return next.metrics, next.err
}

func (a *scriptedAnalyzer) calls() ([]string, []time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	order := make([]string, len(a.callOrder))
	copy(order, a.callOrder)
	times := make([]time.Time, len(a.callTimes))
	copy(times, a.callTimes)
	return order, times
}

type blockingAnalyzer struct {
	started chan struct{}
	once    sync.Once
}

func (b *blockingAnalyzer) Analyze(ctx context.Context, _ string) (analyzer.Metrics, error) {
	if b != nil && b.started != nil {
		b.once.Do(func() {
			close(b.started)
		})
	}
	<-ctx.Done()
	return analyzer.Metrics{}, ctx.Err()
}

type fakeTunerUsage struct {
	inUse int
}

func (f fakeTunerUsage) InUseCount() int {
	return f.inUse
}

func (f fakeTunerUsage) AcquireProbe(_ context.Context, _ string, _ context.CancelCauseFunc) (*stream.Lease, error) {
	return &stream.Lease{}, nil
}

func (f fakeTunerUsage) AcquireProbeForSource(_ context.Context, _ int64, _ string, _ context.CancelCauseFunc) (*stream.Lease, error) {
	return &stream.Lease{}, nil
}

type scriptedTunerUsage struct {
	inUse int

	mu                  sync.Mutex
	acquireErrs         []error
	acquireErrsBySource map[int64][]error
	acquireCalls        int
	callsBySource       map[int64]int
	capacityBySource    map[int64]int
	inUseBySource       map[int64]int
}

func (s *scriptedTunerUsage) InUseCount() int {
	if s == nil {
		return 0
	}
	return s.inUse
}

func (s *scriptedTunerUsage) AcquireProbe(ctx context.Context, label string, cancel context.CancelCauseFunc) (*stream.Lease, error) {
	return s.AcquireProbeForSource(ctx, 0, label, cancel)
}

func (s *scriptedTunerUsage) AcquireProbeForSource(_ context.Context, sourceID int64, _ string, _ context.CancelCauseFunc) (*stream.Lease, error) {
	if s == nil {
		return &stream.Lease{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireCalls++
	if s.callsBySource == nil {
		s.callsBySource = make(map[int64]int)
	}
	s.callsBySource[sourceID]++
	if len(s.acquireErrsBySource[sourceID]) > 0 {
		err := s.acquireErrsBySource[sourceID][0]
		s.acquireErrsBySource[sourceID] = s.acquireErrsBySource[sourceID][1:]
		if err != nil {
			return nil, err
		}
	}
	if len(s.acquireErrs) > 0 {
		err := s.acquireErrs[0]
		s.acquireErrs = s.acquireErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &stream.Lease{}, nil
}

func (s *scriptedTunerUsage) CapacityForSource(sourceID int64) int {
	if s == nil {
		return -1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.capacityBySource) == 0 {
		return -1
	}
	return s.capacityBySource[sourceID]
}

func (s *scriptedTunerUsage) InUseCountForSource(sourceID int64) int {
	if s == nil {
		return -1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.inUseBySource) == 0 {
		return -1
	}
	return s.inUseBySource[sourceID]
}

func (s *scriptedTunerUsage) Calls() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireCalls
}

func (s *scriptedTunerUsage) CallsForSource(sourceID int64) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callsBySource[sourceID]
}

func TestAutoPrioritizeJobRunReordersByScore(t *testing.T) {
	now := time.Now().UTC()

	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0, LastOKAt: 10},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1, LastOKAt: 20},
				{SourceID: 13, ChannelID: 1, ItemKey: "src:c", StreamURL: "http://example.com/c", Enabled: false, PriorityIndex: 2, LastOKAt: 30},
			},
		},
	}

	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:a": {
			ItemKey:    "src:a",
			AnalyzedAt: now.Add(-1 * time.Hour).Unix(),
			Width:      1920,
			Height:     1080,
			FPS:        30,
			BitrateBPS: 2_000_000,
		},
	}}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/b": {
			Width:      1280,
			Height:     720,
			FPS:        60,
			BitrateBPS: 5_000_000,
		},
	}}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)

	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if run.ProgressCur != 1 || run.ProgressMax != 1 {
		t.Fatalf("run progress = %d/%d, want 1/1", run.ProgressCur, run.ProgressMax)
	}
	if !strings.Contains(run.Summary, "analyzed=1") || !strings.Contains(run.Summary, "cache_hits=1") {
		t.Fatalf("run summary = %q, expected analyzed/cache counts", run.Summary)
	}

	reordered := channelsStore.reorders[1]
	want := []int64{12, 11, 13}
	if len(reordered) != len(want) {
		t.Fatalf("reorder length = %d, want %d", len(reordered), len(want))
	}
	for i := range want {
		if reordered[i] != want[i] {
			t.Fatalf("reorder[%d] = %d, want %d", i, reordered[i], want[i])
		}
	}

	metricsStore.mu.Lock()
	defer metricsStore.mu.Unlock()
	if metricsStore.upserts["src:b"] == 0 {
		t.Fatal("expected src:b metrics to be upserted after analysis")
	}
}

func TestAutoPrioritizeJobRunLoadsSourcesInBulk(t *testing.T) {
	now := time.Now().UTC()
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{
			{ChannelID: 1, Enabled: true},
			{ChannelID: 2, Enabled: true},
		},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
			2: {
				{SourceID: 21, ChannelID: 2, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 0},
			},
		},
	}

	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:a": {
			ItemKey:    "src:a",
			AnalyzedAt: now.Unix(),
			Width:      1280,
			Height:     720,
			FPS:        30,
			BitrateBPS: 2_000_000,
		},
		"src:b": {
			ItemKey:    "src:b",
			AnalyzedAt: now.Unix(),
			Width:      1280,
			Height:     720,
			FPS:        30,
			BitrateBPS: 2_000_000,
		},
	}}
	analyzerFake := &fakeAnalyzer{}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	channelsStore.mu.Lock()
	listSourcesCalls := channelsStore.listSourcesCalls
	listSourcesByChannelIDsCalls := channelsStore.listSourcesByChannelIDsCalls
	channelsStore.mu.Unlock()

	if listSourcesCalls != 0 {
		t.Fatalf("ListSources call count = %d, want 0 (bulk setup path only)", listSourcesCalls)
	}
	if listSourcesByChannelIDsCalls != 1 {
		t.Fatalf("ListSourcesByChannelIDs call count = %d, want 1", listSourcesByChannelIDsCalls)
	}
}

func TestAutoPrioritizeJobRunSkipsReorderMutationDrift(t *testing.T) {
	now := time.Now().UTC()
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{
			{ChannelID: 1, Enabled: true},
			{ChannelID: 2, Enabled: true},
		},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
			},
			2: {
				{SourceID: 21, ChannelID: 2, ItemKey: "src:c", StreamURL: "http://example.com/c", Enabled: true, PriorityIndex: 0},
				{SourceID: 22, ChannelID: 2, ItemKey: "src:d", StreamURL: "http://example.com/d", Enabled: true, PriorityIndex: 1},
			},
		},
		reorderErrByChannel: map[int64]error{
			1: fmt.Errorf("%w: source_ids count mismatch: got 1, want 2", channels.ErrSourceOrderDrift),
		},
	}

	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:a": {ItemKey: "src:a", AnalyzedAt: now.Unix(), Width: 640, Height: 360, FPS: 30, BitrateBPS: 800_000},
		"src:b": {ItemKey: "src:b", AnalyzedAt: now.Unix(), Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 5_000_000},
		"src:c": {ItemKey: "src:c", AnalyzedAt: now.Unix(), Width: 640, Height: 360, FPS: 30, BitrateBPS: 900_000},
		"src:d": {ItemKey: "src:d", AnalyzedAt: now.Unix(), Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 4_500_000},
	}}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		&fakeAnalyzer{},
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if !strings.Contains(run.Summary, "reordered=1") {
		t.Fatalf("run summary = %q, expected one successful reorder", run.Summary)
	}
	if !strings.Contains(run.Summary, "skipped_channels=1") {
		t.Fatalf("run summary = %q, expected one skipped channel", run.Summary)
	}
	skipBuckets := ParseAutoPrioritizeSkipReasonBuckets(run.Summary)
	if skipBuckets[autoPrioritizeSkipReasonReorderSourceSetDrift] != 1 {
		t.Fatalf("skip_reason_buckets[%q] = %d, want 1", autoPrioritizeSkipReasonReorderSourceSetDrift, skipBuckets[autoPrioritizeSkipReasonReorderSourceSetDrift])
	}

	reordered := channelsStore.reorders[2]
	want := []int64{22, 21}
	if len(reordered) != len(want) {
		t.Fatalf("channel 2 reorder len = %d, want %d", len(reordered), len(want))
	}
	for i := range want {
		if reordered[i] != want[i] {
			t.Fatalf("channel 2 reorder[%d] = %d, want %d", i, reordered[i], want[i])
		}
	}
	if _, ok := channelsStore.reorders[1]; ok {
		t.Fatalf("channel 1 reorder unexpectedly persisted despite drift skip: %v", channelsStore.reorders[1])
	}
}

func TestAutoPrioritizeJobRunSkipsMissingChannelsDuringSourceLoad(t *testing.T) {
	now := time.Now().UTC()
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{
			{ChannelID: 1, Enabled: true},
			{ChannelID: 2, Enabled: true},
		},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
			},
		},
	}
	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:a": {ItemKey: "src:a", AnalyzedAt: now.Unix(), Width: 640, Height: 360, FPS: 30, BitrateBPS: 800_000},
		"src:b": {ItemKey: "src:b", AnalyzedAt: now.Unix(), Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 5_000_000},
	}}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		&fakeAnalyzer{},
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if !strings.Contains(run.Summary, "reordered=1") {
		t.Fatalf("run summary = %q, expected unaffected channel reorder", run.Summary)
	}
	if !strings.Contains(run.Summary, "skipped_channels=1") {
		t.Fatalf("run summary = %q, expected one skipped channel", run.Summary)
	}
	skipBuckets := ParseAutoPrioritizeSkipReasonBuckets(run.Summary)
	if skipBuckets[autoPrioritizeSkipReasonSourceLoadChannelNotFound] != 1 {
		t.Fatalf("skip_reason_buckets[%q] = %d, want 1", autoPrioritizeSkipReasonSourceLoadChannelNotFound, skipBuckets[autoPrioritizeSkipReasonSourceLoadChannelNotFound])
	}

	reordered := channelsStore.reorders[1]
	want := []int64{12, 11}
	if len(reordered) != len(want) {
		t.Fatalf("channel 1 reorder len = %d, want %d", len(reordered), len(want))
	}
	for i := range want {
		if reordered[i] != want[i] {
			t.Fatalf("channel 1 reorder[%d] = %d, want %d", i, reordered[i], want[i])
		}
	}
	if _, ok := channelsStore.reorders[2]; ok {
		t.Fatalf("channel 2 reorder unexpectedly persisted: %v", channelsStore.reorders[2])
	}

	channelsStore.mu.Lock()
	listSourcesCalls := channelsStore.listSourcesCalls
	listSourcesByChannelIDsCalls := channelsStore.listSourcesByChannelIDsCalls
	channelsStore.mu.Unlock()
	if listSourcesByChannelIDsCalls != 1 {
		t.Fatalf("ListSourcesByChannelIDs calls = %d, want 1 (bulk attempt)", listSourcesByChannelIDsCalls)
	}
	if listSourcesCalls != 2 {
		t.Fatalf("ListSources calls = %d, want 2 (per-channel best-effort fallback)", listSourcesCalls)
	}
}

func TestAutoPrioritizeJobRunStillFailsOnUnexpectedStoreErrors(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
		listSourcesByChannelIDsErr: fmt.Errorf("database unavailable"),
	}
	metricsStore := &fakeMetricsStore{}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		&fakeAnalyzer{},
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "list channel sources") {
		t.Fatalf("run error = %q, want list channel sources failure", run.ErrorMessage)
	}
	if !strings.Contains(run.ErrorMessage, "database unavailable") {
		t.Fatalf("run error = %q, want underlying store failure", run.ErrorMessage)
	}

	channelsStore.mu.Lock()
	listSourcesCalls := channelsStore.listSourcesCalls
	channelsStore.mu.Unlock()
	if listSourcesCalls != 0 {
		t.Fatalf("ListSources calls = %d, want 0 (no drift fallback on unexpected errors)", listSourcesCalls)
	}
}

func TestAutoPrioritizeJobThrottlesProgressPersistence(t *testing.T) {
	now := time.Now().UTC()
	const channelCount = 40

	channelsList := make([]channels.Channel, 0, channelCount)
	sourcesByChannel := make(map[int64][]channels.Source, channelCount)
	metricsByItem := make(map[string]StreamMetric, channelCount)
	for i := 1; i <= channelCount; i++ {
		channelID := int64(i)
		itemKey := fmt.Sprintf("src:%d", i)
		channelsList = append(channelsList, channels.Channel{
			ChannelID: channelID,
			Enabled:   true,
		})
		sourcesByChannel[channelID] = []channels.Source{
			{
				SourceID:      int64(i + 1000),
				ChannelID:     channelID,
				ItemKey:       itemKey,
				StreamURL:     fmt.Sprintf("http://example.com/%d", i),
				Enabled:       true,
				PriorityIndex: 0,
			},
		}
		metricsByItem[itemKey] = StreamMetric{
			ItemKey:    itemKey,
			AnalyzedAt: now.Unix(),
			Width:      1280,
			Height:     720,
			FPS:        30,
			BitrateBPS: 2_000_000,
		}
	}

	channelsStore := &fakeAutoChannels{
		channels: channelsList,
		sources:  sourcesByChannel,
	}
	metricsStore := &fakeMetricsStore{metrics: metricsByItem}
	analyzerFake := &fakeAnalyzer{}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if run.ProgressCur != channelCount || run.ProgressMax != channelCount {
		t.Fatalf("run progress = %d/%d, want %d/%d", run.ProgressCur, run.ProgressMax, channelCount, channelCount)
	}
	if !strings.Contains(run.Summary, "channels=40") || !strings.Contains(run.Summary, "cache_hits=40") {
		t.Fatalf("run summary = %q, expected final channel/cache counts", run.Summary)
	}

	progressWrites := store.progressWrites(runID)
	if progressWrites >= channelCount {
		t.Fatalf("UpdateRunProgress writes = %d, want less than channel count %d", progressWrites, channelCount)
	}
	if progressWrites > 12 {
		t.Fatalf("UpdateRunProgress writes = %d, expected throttled write volume", progressWrites)
	}
}

func TestAutoPrioritizeJobPersistsFreshProfilesForAnalyzedItems(t *testing.T) {
	now := time.Now().UTC()
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{
			{ChannelID: 1, Enabled: true},
			{ChannelID: 2, Enabled: true},
		},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:shared", StreamURL: "http://example.com/shared", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:cached", StreamURL: "http://example.com/cached", Enabled: true, PriorityIndex: 1},
			},
			2: {
				{SourceID: 21, ChannelID: 2, ItemKey: "src:shared", StreamURL: "http://example.com/shared-disabled", Enabled: false, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:cached": {
			ItemKey:    "src:cached",
			AnalyzedAt: now.Add(-2 * time.Minute).Unix(),
			Width:      1280,
			Height:     720,
			FPS:        30,
			BitrateBPS: 2_500_000,
			VideoCodec: "h264",
			AudioCodec: "aac",
		},
	}}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/shared": {
			Width:      1920,
			Height:     1080,
			FPS:        59.94,
			VideoCodec: "h264",
			AudioCodec: "aac",
			BitrateBPS: 5_500_000,
		},
	}}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	channelsStore.mu.Lock()
	updatesPrimary := channelsStore.profileUpdates[11]
	updatesSharedDisabled := channelsStore.profileUpdates[21]
	updatesCached := channelsStore.profileUpdates[12]
	channelsStore.mu.Unlock()

	if len(updatesPrimary) != 1 {
		t.Fatalf("profile updates for source 11 = %d, want 1", len(updatesPrimary))
	}
	if len(updatesSharedDisabled) != 1 {
		t.Fatalf("profile updates for source 21 = %d, want 1", len(updatesSharedDisabled))
	}
	if len(updatesCached) != 0 {
		t.Fatalf("profile updates for cached-only source 12 = %d, want 0", len(updatesCached))
	}

	update := updatesPrimary[0]
	if update.LastProbeAt.IsZero() {
		t.Fatal("primary profile update LastProbeAt = zero, want probe timestamp")
	}
	if update.Width != 1920 || update.Height != 1080 {
		t.Fatalf("primary profile resolution = %dx%d, want 1920x1080", update.Width, update.Height)
	}
	if update.VideoCodec != "h264" || update.AudioCodec != "aac" {
		t.Fatalf("primary profile codecs = (%q,%q), want (h264,aac)", update.VideoCodec, update.AudioCodec)
	}
	if update.BitrateBPS != 5_500_000 {
		t.Fatalf("primary profile bitrate = %d, want 5500000", update.BitrateBPS)
	}
}

func TestAutoPrioritizeJobCacheHitOnlyDoesNotPersistSourceProfiles(t *testing.T) {
	now := time.Now().UTC()
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:a": {
			ItemKey:    "src:a",
			AnalyzedAt: now.Add(-2 * time.Minute).Unix(),
			Width:      1920,
			Height:     1080,
			FPS:        60,
			BitrateBPS: 5_000_000,
			VideoCodec: "h264",
			AudioCodec: "aac",
		},
	}}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{}}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	channelsStore.mu.Lock()
	updateCount := len(channelsStore.profileUpdates)
	channelsStore.mu.Unlock()
	if updateCount != 0 {
		t.Fatalf("profile update source count = %d, want 0 on cache-hit-only run", updateCount)
	}

	analyzerFake.mu.Lock()
	callCount := analyzerFake.calls["http://example.com/a"]
	analyzerFake.mu.Unlock()
	if callCount != 0 {
		t.Fatalf("analyzer call count = %d, want 0 when cache is fresh", callCount)
	}
}

func TestAutoPrioritizeJobFailedProbeDoesNotPersistSourceProfiles(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{
		errs: map[string]error{
			"http://example.com/a": fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 404 Not Found {  }"),
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	channelsStore.mu.Lock()
	updateCount := len(channelsStore.profileUpdates)
	channelsStore.mu.Unlock()
	if updateCount != 0 {
		t.Fatalf("profile update source count = %d, want 0 when probe fails", updateCount)
	}
}

func TestAutoPrioritizeJobSummaryIncludesErrorBuckets(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
				{SourceID: 13, ChannelID: 1, ItemKey: "src:c", StreamURL: "http://example.com/c", Enabled: true, PriorityIndex: 2},
			},
		},
	}

	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{
		errs: map[string]error{
			"http://example.com/a": fmt.Errorf("decode ffprobe JSON: invalid character 'h' looking for beginning of value"),
			"http://example.com/b": fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 429 Too Many Requests {  }"),
			"http://example.com/c": fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 404 Not Found {  }"),
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			HTTP429Backoff: time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	if !strings.Contains(run.Summary, "analysis_error_buckets=") {
		t.Fatalf("run summary = %q, expected analysis_error_buckets marker", run.Summary)
	}
	buckets := ParseAnalysisErrorBuckets(run.Summary)
	if buckets["decode_ffprobe_json"] != 1 {
		t.Fatalf("decode_ffprobe_json bucket = %d, want 1", buckets["decode_ffprobe_json"])
	}
	if buckets["http_429"] != 1 {
		t.Fatalf("http_429 bucket = %d, want 1", buckets["http_429"])
	}
	if buckets["http_404"] != 1 {
		t.Fatalf("http_404 bucket = %d, want 1", buckets["http_404"])
	}

	metricsStore.mu.Lock()
	defer metricsStore.mu.Unlock()
	if len(metricsStore.metrics) != 3 {
		t.Fatalf("cached metrics entries = %d, want 3 when all probes fail", len(metricsStore.metrics))
	}
	for itemKey, metric := range metricsStore.metrics {
		if strings.TrimSpace(metric.Error) == "" {
			t.Fatalf("cached metric %q error = %q, want persisted failure text", itemKey, metric.Error)
		}
	}
}

func TestAutoPrioritizeJobBacksOffAndRetriesSameStreamOnHTTP429(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
			},
		},
	}

	metricsStore := &fakeMetricsStore{}
	analyzerFake := &scriptedAnalyzer{
		results: map[string][]scriptedProbeResult{
			"http://example.com/a": {
				{
					err: fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 429 Too Many Requests {  }"),
				},
				{
					metrics: analyzer.Metrics{
						Width:      1920,
						Height:     1080,
						FPS:        30,
						BitrateBPS: 4_000_000,
					},
				},
			},
			"http://example.com/b": {
				{
					metrics: analyzer.Metrics{
						Width:      1280,
						Height:     720,
						FPS:        30,
						BitrateBPS: 2_000_000,
					},
				},
			},
		},
	}

	backoff := 120 * time.Millisecond
	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			TunerCount:     1,
			HTTP429Backoff: backoff,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if strings.Contains(run.Summary, "analysis_errors=1") || strings.Contains(run.Summary, "http_429") {
		t.Fatalf("run summary = %q, expected transient 429 to be retried successfully", run.Summary)
	}

	callOrder, callTimes := analyzerFake.calls()
	wantOrder := []string{
		"http://example.com/a",
		"http://example.com/a",
		"http://example.com/b",
	}
	if len(callOrder) != len(wantOrder) {
		t.Fatalf("len(callOrder) = %d, want %d", len(callOrder), len(wantOrder))
	}
	for i := range wantOrder {
		if callOrder[i] != wantOrder[i] {
			t.Fatalf("callOrder[%d] = %q, want %q", i, callOrder[i], wantOrder[i])
		}
	}
	if len(callTimes) != len(wantOrder) {
		t.Fatalf("len(callTimes) = %d, want %d", len(callTimes), len(wantOrder))
	}
	if delta := callTimes[1].Sub(callTimes[0]); delta < backoff-20*time.Millisecond {
		t.Fatalf("retry spacing after 429 = %s, want at least %s", delta, backoff-20*time.Millisecond)
	}

	metricsStore.mu.Lock()
	defer metricsStore.mu.Unlock()
	if metricA, ok := metricsStore.metrics["src:a"]; !ok {
		t.Fatal("metric for src:a missing")
	} else if strings.TrimSpace(metricA.Error) != "" {
		t.Fatalf("metric src:a error = %q, want empty after successful retry", metricA.Error)
	}
}

func TestAutoPrioritizeJobCachesHTTP404FailureAndSkipsReprobeWithinRetryWindow(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}

	metricsStore := &fakeMetricsStore{}
	analyzerFake := &scriptedAnalyzer{
		results: map[string][]scriptedProbeResult{
			"http://example.com/a": {
				{
					err: fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 404 Not Found {  }"),
				},
			},
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			ErrorRetry: 30 * time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start(run1) error = %v", err)
	}
	run1 := waitForRunDone(t, runner, runID)
	if run1.Status != StatusSuccess {
		t.Fatalf("run1 status = %q, want %q", run1.Status, StatusSuccess)
	}
	if !strings.Contains(run1.Summary, "http_404") {
		t.Fatalf("run1 summary = %q, want http_404 bucket", run1.Summary)
	}

	metricsStore.mu.Lock()
	metric, ok := metricsStore.metrics["src:a"]
	upserts := metricsStore.upserts["src:a"]
	metricsStore.mu.Unlock()
	if !ok {
		t.Fatal("run1 expected failed metric cache row for src:a")
	}
	if strings.TrimSpace(metric.Error) == "" {
		t.Fatalf("run1 cached metric error = %q, want persisted failure text", metric.Error)
	}
	if upserts != 1 {
		t.Fatalf("run1 upserts for src:a = %d, want 1", upserts)
	}

	runID, err = runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start(run2) error = %v", err)
	}
	run2 := waitForRunDone(t, runner, runID)
	if run2.Status != StatusSuccess {
		t.Fatalf("run2 status = %q, want %q", run2.Status, StatusSuccess)
	}
	if !strings.Contains(run2.Summary, "analyzed=0") || !strings.Contains(run2.Summary, "cache_hits=1") {
		t.Fatalf("run2 summary = %q, want cached-error reuse markers", run2.Summary)
	}

	callOrder, _ := analyzerFake.calls()
	if len(callOrder) != 1 {
		t.Fatalf("analyzer call count = %d, want 1 across two runs when retry window is active", len(callOrder))
	}
}

func TestAutoPrioritizeJobCachesTerminalHTTP429AndReprobesAfterRetryWindow(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}

	metricsStore := &fakeMetricsStore{}
	analyzerFake := &scriptedAnalyzer{
		results: map[string][]scriptedProbeResult{
			"http://example.com/a": {
				{
					err: fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 429 Too Many Requests {  }"),
				},
				{
					err: fmt.Errorf("ffprobe failed: exit status 1: stream: Server returned 429 Too Many Requests {  }"),
				},
				{
					metrics: analyzer.Metrics{
						Width:      1920,
						Height:     1080,
						FPS:        30,
						BitrateBPS: 4_000_000,
					},
				},
			},
		},
	}

	errorRetry := 30 * time.Minute
	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			ErrorRetry:     errorRetry,
			HTTP429Backoff: time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start(run1) error = %v", err)
	}
	run1 := waitForRunDone(t, runner, runID)
	if run1.Status != StatusSuccess {
		t.Fatalf("run1 status = %q, want %q", run1.Status, StatusSuccess)
	}
	if !strings.Contains(run1.Summary, "http_429") {
		t.Fatalf("run1 summary = %q, want http_429 bucket", run1.Summary)
	}

	metricsStore.mu.Lock()
	metricAfterRun1, ok := metricsStore.metrics["src:a"]
	upsertsAfterRun1 := metricsStore.upserts["src:a"]
	metricsStore.mu.Unlock()
	if !ok {
		t.Fatal("run1 expected failed metric cache row for src:a")
	}
	if strings.TrimSpace(metricAfterRun1.Error) == "" {
		t.Fatalf("run1 cached metric error = %q, want persisted failure text", metricAfterRun1.Error)
	}
	if upsertsAfterRun1 != 1 {
		t.Fatalf("run1 upserts for src:a = %d, want 1", upsertsAfterRun1)
	}

	runID, err = runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start(run2) error = %v", err)
	}
	run2 := waitForRunDone(t, runner, runID)
	if run2.Status != StatusSuccess {
		t.Fatalf("run2 status = %q, want %q", run2.Status, StatusSuccess)
	}
	if !strings.Contains(run2.Summary, "analyzed=0") || !strings.Contains(run2.Summary, "cache_hits=1") {
		t.Fatalf("run2 summary = %q, want cached-error reuse markers", run2.Summary)
	}

	callOrder, _ := analyzerFake.calls()
	if len(callOrder) != 2 {
		t.Fatalf("analyzer call count after run2 = %d, want 2 (run1 + in-run retry only)", len(callOrder))
	}

	metricsStore.mu.Lock()
	stale := metricsStore.metrics["src:a"]
	stale.AnalyzedAt = time.Now().UTC().Add(-(errorRetry + time.Minute)).Unix()
	metricsStore.metrics["src:a"] = stale
	metricsStore.mu.Unlock()

	runID, err = runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start(run3) error = %v", err)
	}
	run3 := waitForRunDone(t, runner, runID)
	if run3.Status != StatusSuccess {
		t.Fatalf("run3 status = %q, want %q", run3.Status, StatusSuccess)
	}
	if !strings.Contains(run3.Summary, "analyzed=1") {
		t.Fatalf("run3 summary = %q, want stale-error re-probe", run3.Summary)
	}

	callOrder, _ = analyzerFake.calls()
	if len(callOrder) != 3 {
		t.Fatalf("analyzer call count after run3 = %d, want 3 (run3 re-probe success)", len(callOrder))
	}

	metricsStore.mu.Lock()
	metricFinal, ok := metricsStore.metrics["src:a"]
	upsertsFinal := metricsStore.upserts["src:a"]
	metricsStore.mu.Unlock()
	if !ok {
		t.Fatal("run3 expected cached metric for src:a")
	}
	if strings.TrimSpace(metricFinal.Error) != "" {
		t.Fatalf("run3 cached metric error = %q, want empty after recovery", metricFinal.Error)
	}
	if upsertsFinal != 2 {
		t.Fatalf("total upserts for src:a = %d, want 2 (failed run1 + recovered run3)", upsertsFinal)
	}
}

func TestAutoPrioritizeJobHonorsFreshCachedErrorMetric(t *testing.T) {
	now := time.Now().UTC()
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 1, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{metrics: map[string]StreamMetric{
		"src:a": {
			ItemKey:    "src:a",
			AnalyzedAt: now.Add(-10 * time.Minute).Unix(),
			Error:      "temporary error",
		},
	}}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/a": {Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 2_000_000},
	}}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{SuccessFreshness: 24 * time.Hour, ErrorRetry: 30 * time.Minute},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if !strings.Contains(run.Summary, "analyzed=0") || !strings.Contains(run.Summary, "cache_hits=1") {
		t.Fatalf("run summary = %q, want cached-error reuse markers", run.Summary)
	}

	analyzerFake.mu.Lock()
	calls := analyzerFake.calls["http://example.com/a"]
	analyzerFake.mu.Unlock()
	if calls != 0 {
		t.Fatalf("analyzer calls = %d, want 0 when cached error is still within retry window", calls)
	}

	metricsStore.mu.Lock()
	metric := metricsStore.metrics["src:a"]
	upserts := metricsStore.upserts["src:a"]
	metricsStore.mu.Unlock()
	if strings.TrimSpace(metric.Error) == "" {
		t.Fatalf("cached metric error = %q, want existing error to remain until retry window expires", metric.Error)
	}
	if upserts != 0 {
		t.Fatalf("upserts for src:a = %d, want 0 when no re-probe occurs", upserts)
	}
}

func TestOrderSourcesByScoreTieBreakers(t *testing.T) {
	sources := []channels.Source{
		{SourceID: 1, ItemKey: "src:a", Enabled: true, LastOKAt: 10},
		{SourceID: 2, ItemKey: "src:b", Enabled: true, LastOKAt: 20},
	}
	metrics := map[string]StreamMetric{
		"src:a": {ItemKey: "src:a", Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 2_000_000},
		"src:b": {ItemKey: "src:b", Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 2_000_000},
	}

	ordered, changed := orderSourcesByScore(sources, metrics)
	if !changed {
		t.Fatal("expected tie-breaker on last_ok_at to change ordering")
	}
	if len(ordered) != 2 || ordered[0] != 2 || ordered[1] != 1 {
		t.Fatalf("ordered = %v, want [2 1]", ordered)
	}
}

func TestOrderSourcesByScoreAppliesFailureHealthPenalty(t *testing.T) {
	nowUnix := time.Now().UTC().Unix()
	sources := []channels.Source{
		{
			SourceID:      1,
			ItemKey:       "src:a",
			Enabled:       true,
			FailCount:     3,
			LastFailAt:    nowUnix - 60,
			LastOKAt:      nowUnix - 3600,
			CooldownUntil: nowUnix + 120,
		},
		{
			SourceID:   2,
			ItemKey:    "src:b",
			Enabled:    true,
			LastOKAt:   nowUnix - 30,
			FailCount:  0,
			LastFailAt: 0,
		},
	}
	metrics := map[string]StreamMetric{
		"src:a": {ItemKey: "src:a", Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 6_000_000},
		"src:b": {ItemKey: "src:b", Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 5_200_000},
	}

	ordered, changed := orderSourcesByScore(sources, metrics)
	if !changed {
		t.Fatal("expected health penalty to reorder unstable source behind stable alternative")
	}
	if len(ordered) != 2 || ordered[0] != 2 || ordered[1] != 1 {
		t.Fatalf("ordered = %v, want [2 1]", ordered)
	}
}

func TestOrderSourcesByScoreDoesNotPenalizeRecoveredSource(t *testing.T) {
	nowUnix := time.Now().UTC().Unix()
	sources := []channels.Source{
		{
			SourceID:      1,
			ItemKey:       "src:a",
			Enabled:       true,
			FailCount:     0,
			LastFailAt:    nowUnix - 60,
			LastOKAt:      nowUnix - 10,
			CooldownUntil: 0,
		},
		{
			SourceID:   2,
			ItemKey:    "src:b",
			Enabled:    true,
			LastOKAt:   nowUnix - 30,
			FailCount:  0,
			LastFailAt: 0,
		},
	}
	metrics := map[string]StreamMetric{
		"src:a": {ItemKey: "src:a", Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 6_000_000},
		"src:b": {ItemKey: "src:b", Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 5_200_000},
	}

	ordered, changed := orderSourcesByScore(sources, metrics)
	if changed {
		t.Fatal("expected recovered higher-quality source to stay first")
	}
	if len(ordered) != 2 || ordered[0] != 1 || ordered[1] != 2 {
		t.Fatalf("ordered = %v, want [1 2]", ordered)
	}
}

func TestOrderSourcesByScoreRepeatedFailuresStayBottomAcrossRuns(t *testing.T) {
	nowUnix := time.Now().UTC().Unix()
	sources := []channels.Source{
		{
			SourceID:      1,
			ItemKey:       "src:a",
			Enabled:       true,
			FailCount:     2,
			LastFailAt:    nowUnix - int64((10 * 24 * time.Hour).Seconds()),
			LastOKAt:      nowUnix - int64((11 * 24 * time.Hour).Seconds()),
			CooldownUntil: 0,
		},
		{
			SourceID:      2,
			ItemKey:       "src:b",
			Enabled:       true,
			FailCount:     0,
			LastFailAt:    0,
			LastOKAt:      nowUnix - 120,
			CooldownUntil: 0,
		},
	}
	metrics := map[string]StreamMetric{
		"src:a": {ItemKey: "src:a", Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 8_000_000},
		"src:b": {ItemKey: "src:b", Width: 1920, Height: 1080, FPS: 60, BitrateBPS: 6_500_000},
	}

	ordered, changed := orderSourcesByScore(sources, metrics)
	if !changed {
		t.Fatal("expected repeated-failure source to be forced below healthy source")
	}
	if len(ordered) != 2 || ordered[0] != 2 || ordered[1] != 1 {
		t.Fatalf("ordered = %v, want [2 1]", ordered)
	}
}

func TestAutoPrioritizeJobRetriesWhenInitialProbeAvailabilitySnapshotIsZero(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/a": {Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 3_000_000},
	}}
	tunerUsage := &scriptedTunerUsage{
		inUse: 2,
		acquireErrs: []error{
			stream.ErrNoTunersAvailable,
			stream.ErrNoTunersAvailable,
			stream.ErrNoTunersAvailable,
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			TunerCount: 2,
			TunerUsage: tunerUsage,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if !strings.Contains(run.Summary, "analysis_errors=1") {
		t.Fatalf("run summary = %q, want one probe-slot-unavailable analysis error", run.Summary)
	}
	buckets := ParseAnalysisErrorBuckets(run.Summary)
	if buckets["probe_slot_unavailable"] != 1 {
		t.Fatalf("probe_slot_unavailable bucket = %d, want 1", buckets["probe_slot_unavailable"])
	}
	if got := tunerUsage.Calls(); got != maxProbeSlotAcquireAttempts {
		t.Fatalf("AcquireProbe call count = %d, want %d bounded retries", got, maxProbeSlotAcquireAttempts)
	}

	analyzerFake.mu.Lock()
	defer analyzerFake.mu.Unlock()
	if analyzerFake.calls["http://example.com/a"] != 0 {
		t.Fatalf("analyzer call count = %d, want 0 on persistent contention", analyzerFake.calls["http://example.com/a"])
	}
}

func TestAutoPrioritizeJobRetriesTransientProbeSlotContention(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/a": {Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 3_000_000},
	}}
	tunerUsage := &scriptedTunerUsage{
		acquireErrs: []error{
			stream.ErrNoTunersAvailable,
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			TunerCount: 1,
			TunerUsage: tunerUsage,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if strings.Contains(run.Summary, "probe_slot_unavailable") || strings.Contains(run.Summary, "analysis_errors=1") {
		t.Fatalf("run summary = %q, expected transient contention retry to complete without analysis errors", run.Summary)
	}
	if tunerUsage.Calls() != 2 {
		t.Fatalf("AcquireProbe call count = %d, want 2 (retry after transient contention)", tunerUsage.Calls())
	}

	analyzerFake.mu.Lock()
	calls := analyzerFake.calls["http://example.com/a"]
	analyzerFake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("analyzer calls = %d, want 1 after successful retry", calls)
	}
}

func TestAutoPrioritizeJobProbeSlotContentionBecomesPerTaskError(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/b": {Width: 1280, Height: 720, FPS: 30, BitrateBPS: 2_500_000},
	}}
	tunerUsage := &scriptedTunerUsage{
		acquireErrs: []error{
			stream.ErrNoTunersAvailable,
			stream.ErrNoTunersAvailable,
			stream.ErrNoTunersAvailable,
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			WorkerMode:   "fixed",
			FixedWorkers: 1,
			TunerCount:   1,
			TunerUsage:   tunerUsage,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if !strings.Contains(run.Summary, "analysis_errors=1") {
		t.Fatalf("run summary = %q, expected one contention analysis error", run.Summary)
	}
	buckets := ParseAnalysisErrorBuckets(run.Summary)
	if buckets["probe_slot_unavailable"] != 1 {
		t.Fatalf("probe_slot_unavailable bucket = %d, want 1", buckets["probe_slot_unavailable"])
	}
	if tunerUsage.Calls() != maxProbeSlotAcquireAttempts+1 {
		t.Fatalf("AcquireProbe call count = %d, want %d", tunerUsage.Calls(), maxProbeSlotAcquireAttempts+1)
	}

	analyzerFake.mu.Lock()
	callsA := analyzerFake.calls["http://example.com/a"]
	callsB := analyzerFake.calls["http://example.com/b"]
	analyzerFake.mu.Unlock()
	if callsA != 0 {
		t.Fatalf("analyzer calls for source A = %d, want 0 on persistent contention", callsA)
	}
	if callsB != 1 {
		t.Fatalf("analyzer calls for source B = %d, want 1 for continued run progress", callsB)
	}
}

func TestAutoPrioritizeJobRespectsPerSourceProbeAvailability(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{
					SourceID:         11,
					ChannelID:        1,
					ItemKey:          "src:primary:a",
					StreamURL:        "http://example.com/a",
					Enabled:          true,
					PriorityIndex:    0,
					PlaylistSourceID: 1,
				},
				{
					SourceID:         12,
					ChannelID:        1,
					ItemKey:          "src:backup:b",
					StreamURL:        "http://example.com/b",
					Enabled:          true,
					PriorityIndex:    1,
					PlaylistSourceID: 2,
				},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/a": {Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 3_000_000},
	}}
	tunerUsage := &scriptedTunerUsage{
		inUse: 0,
		capacityBySource: map[int64]int{
			1: 1,
			2: 1,
		},
		inUseBySource: map[int64]int{
			1: 0,
			2: 1,
		},
		acquireErrsBySource: map[int64][]error{
			2: {
				stream.ErrNoTunersAvailable,
				stream.ErrNoTunersAvailable,
				stream.ErrNoTunersAvailable,
			},
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			WorkerMode:   "fixed",
			FixedWorkers: 2,
			TunerCount:   2,
			TunerUsage:   tunerUsage,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if !strings.Contains(run.Summary, "analysis_errors=1") {
		t.Fatalf("run summary = %q, want one per-source probe availability error", run.Summary)
	}
	buckets := ParseAnalysisErrorBuckets(run.Summary)
	if buckets["probe_slot_unavailable"] != 1 {
		t.Fatalf("probe_slot_unavailable bucket = %d, want 1", buckets["probe_slot_unavailable"])
	}
	if got := tunerUsage.CallsForSource(1); got != 1 {
		t.Fatalf("AcquireProbeForSource calls for source 1 = %d, want 1", got)
	}
	if got := tunerUsage.CallsForSource(2); got != maxProbeSlotAcquireAttempts {
		t.Fatalf("AcquireProbeForSource calls for source 2 = %d, want %d bounded retries under source contention", got, maxProbeSlotAcquireAttempts)
	}

	analyzerFake.mu.Lock()
	callsA := analyzerFake.calls["http://example.com/a"]
	callsB := analyzerFake.calls["http://example.com/b"]
	analyzerFake.mu.Unlock()
	if callsA != 1 {
		t.Fatalf("analyzer calls for source A = %d, want 1", callsA)
	}
	if callsB != 0 {
		t.Fatalf("analyzer calls for source B = %d, want 0 when source pool unavailable", callsB)
	}
}

func TestAutoPrioritizeJobUsesUnscopedSourceIDWithoutPrimaryCoercion(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{
					SourceID:         11,
					ChannelID:        1,
					ItemKey:          "src:invalid:source",
					StreamURL:        "http://example.com/invalid.ts",
					Enabled:          true,
					PriorityIndex:    0,
					PlaylistSourceID: 0,
				},
				{
					SourceID:         12,
					ChannelID:        1,
					ItemKey:          "src:valid:source",
					StreamURL:        "http://example.com/valid.ts",
					Enabled:          true,
					PriorityIndex:    1,
					PlaylistSourceID: 2,
				},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/invalid.ts": {Width: 960, Height: 540, FPS: 30, BitrateBPS: 1_000_000},
		"http://example.com/valid.ts":   {Width: 1280, Height: 720, FPS: 30, BitrateBPS: 2_000_000},
	}}
	tunerUsage := &scriptedTunerUsage{}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			WorkerMode:   "fixed",
			FixedWorkers: 1,
			TunerCount:   1,
			TunerUsage:   tunerUsage,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if got := tunerUsage.CallsForSource(0); got != 1 {
		t.Fatalf("AcquireProbeForSource calls for source 0 = %d, want 1 (legacy/unscoped source)", got)
	}
	if got := tunerUsage.CallsForSource(1); got != 0 {
		t.Fatalf("AcquireProbeForSource calls for source 1 = %d, want 0 (no primary coercion)", got)
	}
	if got := tunerUsage.CallsForSource(2); got != 1 {
		t.Fatalf("AcquireProbeForSource calls for source 2 = %d, want 1", got)
	}
}

func TestAutoPrioritizeJobFailsOnUnexpectedProbeAcquireError(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{results: map[string]analyzer.Metrics{
		"http://example.com/a": {Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 3_000_000},
	}}
	tunerUsage := &scriptedTunerUsage{
		acquireErrs: []error{
			fmt.Errorf("probe backend offline"),
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			TunerCount: 1,
			TunerUsage: tunerUsage,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "acquire probe slot") {
		t.Fatalf("error message = %q, want acquire probe slot fatal error", run.ErrorMessage)
	}

	analyzerFake.mu.Lock()
	calls := analyzerFake.calls["http://example.com/a"]
	analyzerFake.mu.Unlock()
	if calls != 0 {
		t.Fatalf("analyzer call count = %d, want 0 on unexpected acquire failure", calls)
	}
}

func TestResolveWorkersUsesTunerAvailability(t *testing.T) {
	job := &AutoPrioritizeJob{
		opts: AutoPrioritizeOptions{
			TunerCount: 4,
			TunerUsage: fakeTunerUsage{inUse: 1},
		},
	}
	if workers := job.resolveWorkers(); workers != 3 {
		t.Fatalf("workers = %d, want 3", workers)
	}

	job.opts.TunerUsage = fakeTunerUsage{inUse: 5}
	if workers := job.resolveWorkers(); workers != 0 {
		t.Fatalf("workers = %d, want 0", workers)
	}
}

func TestResolveWorkersUsesDefaultWhenTunersNotConfigured(t *testing.T) {
	job := &AutoPrioritizeJob{
		opts: AutoPrioritizeOptions{
			DefaultWorkers: 6,
		},
	}

	if workers := job.resolveWorkers(); workers != 6 {
		t.Fatalf("workers = %d, want 6 from default workers", workers)
	}

	job.opts.DefaultWorkers = 128
	if workers := job.resolveWorkers(); workers != 64 {
		t.Fatalf("workers = %d, want 64 max cap", workers)
	}
}

func TestResolveWorkersUsesFixedModeAndCapsToAvailableTuners(t *testing.T) {
	job := &AutoPrioritizeJob{
		opts: AutoPrioritizeOptions{
			WorkerMode:   "fixed",
			FixedWorkers: 6,
			TunerCount:   4,
			TunerUsage:   fakeTunerUsage{inUse: 1},
		},
	}

	if workers := job.resolveWorkers(); workers != 3 {
		t.Fatalf("workers = %d, want 3 (fixed capped by available tuners)", workers)
	}

	job.opts.TunerUsage = fakeTunerUsage{inUse: 5}
	if workers := job.resolveWorkers(); workers != 0 {
		t.Fatalf("workers = %d, want 0 when no tuner slots are available", workers)
	}
}

func TestNewAutoPrioritizeJobRejectsInvalidFixedWorkers(t *testing.T) {
	_, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		&fakeAutoChannels{},
		&fakeMetricsStore{},
		&fakeAnalyzer{},
		AutoPrioritizeOptions{
			WorkerMode:   "fixed",
			FixedWorkers: 0,
		},
	)
	if err == nil {
		t.Fatal("expected error for fixed worker mode with zero workers")
	}
}

func TestSelectAnalysisSourcesRespectsScope(t *testing.T) {
	sources := []channels.Source{
		{SourceID: 1, Enabled: true},
		{SourceID: 2, Enabled: false},
		{SourceID: 3, Enabled: true},
		{SourceID: 4, Enabled: true},
	}

	enabledOnly := selectAnalysisSources(sources, true, 0)
	if len(enabledOnly) != 3 {
		t.Fatalf("len(enabledOnly) = %d, want 3", len(enabledOnly))
	}
	if enabledOnly[0].SourceID != 1 || enabledOnly[1].SourceID != 3 || enabledOnly[2].SourceID != 4 {
		t.Fatalf("enabledOnly order = %v, want [1 3 4]", []int64{enabledOnly[0].SourceID, enabledOnly[1].SourceID, enabledOnly[2].SourceID})
	}

	allTop2 := selectAnalysisSources(sources, false, 2)
	if len(allTop2) != 2 {
		t.Fatalf("len(allTop2) = %d, want 2", len(allTop2))
	}
	if allTop2[0].SourceID != 1 || allTop2[1].SourceID != 2 {
		t.Fatalf("allTop2 order = %v, want [1 2]", []int64{allTop2[0].SourceID, allTop2[1].SourceID})
	}
}

func TestAutoPrioritizeJobTopNPerChannelLimitsAnalysis(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
				{SourceID: 13, ChannelID: 1, ItemKey: "src:c", StreamURL: "http://example.com/c", Enabled: true, PriorityIndex: 2},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &fakeAnalyzer{
		results: map[string]analyzer.Metrics{
			"http://example.com/a": {Width: 1920, Height: 1080, FPS: 30, BitrateBPS: 4_000_000},
			"http://example.com/b": {Width: 1280, Height: 720, FPS: 30, BitrateBPS: 2_000_000},
			"http://example.com/c": {Width: 854, Height: 480, FPS: 30, BitrateBPS: 1_000_000},
		},
	}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{values: map[string]string{
			settingAutoPrioritizeEnabledOnly:    "true",
			settingAutoPrioritizeTopNPerChannel: "1",
		}},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	analyzerFake.mu.Lock()
	callsA := analyzerFake.calls["http://example.com/a"]
	callsB := analyzerFake.calls["http://example.com/b"]
	callsC := analyzerFake.calls["http://example.com/c"]
	analyzerFake.mu.Unlock()

	if callsA != 1 {
		t.Fatalf("calls for source A = %d, want 1", callsA)
	}
	if callsB != 0 || callsC != 0 {
		t.Fatalf("calls for top-N skipped sources = (%d,%d), want (0,0)", callsB, callsC)
	}

	if !strings.Contains(run.Summary, "top_n_per_channel=1") || !strings.Contains(run.Summary, "limited_channels=1") {
		t.Fatalf("run summary = %q, expected top-N scope markers", run.Summary)
	}
}

func TestAutoPrioritizeJobFailsWhenProbeIsPreemptedByClient(t *testing.T) {
	pool := stream.NewPool(1)
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	blocker := &blockingAnalyzer{started: make(chan struct{})}

	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		blocker,
		AutoPrioritizeOptions{
			TunerCount: 1,
			TunerUsage: pool,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-blocker.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for probe analyzer to start")
	}

	clientLease, err := pool.AcquireClient(context.Background(), "200", "192.168.1.99:5000")
	if err != nil {
		t.Fatalf("AcquireClient() after probe start error = %v", err)
	}
	clientLease.Release()

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(strings.ToLower(run.ErrorMessage), "preempt") {
		t.Fatalf("error message = %q, want preemption failure", run.ErrorMessage)
	}
}

func TestAutoPrioritizeJobAppliesProbeTuneDelayBetweenAnalyses(t *testing.T) {
	channelsStore := &fakeAutoChannels{
		channels: []channels.Channel{{ChannelID: 1, Enabled: true}},
		sources: map[int64][]channels.Source{
			1: {
				{SourceID: 11, ChannelID: 1, ItemKey: "src:a", StreamURL: "http://example.com/a", Enabled: true, PriorityIndex: 0},
				{SourceID: 12, ChannelID: 1, ItemKey: "src:b", StreamURL: "http://example.com/b", Enabled: true, PriorityIndex: 1},
			},
		},
	}
	metricsStore := &fakeMetricsStore{}
	analyzerFake := &timedAnalyzer{}

	delay := 120 * time.Millisecond
	job, err := NewAutoPrioritizeJob(
		&fakeAutoSettings{},
		channelsStore,
		metricsStore,
		analyzerFake,
		AutoPrioritizeOptions{
			TunerCount:     1,
			TunerUsage:     fakeTunerUsage{inUse: 0},
			ProbeTuneDelay: delay,
		},
	)
	if err != nil {
		t.Fatalf("NewAutoPrioritizeJob() error = %v", err)
	}

	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}

	callTimes := analyzerFake.callTimes()
	if len(callTimes) != 2 {
		t.Fatalf("len(callTimes) = %d, want 2", len(callTimes))
	}
	delta := callTimes[1].Sub(callTimes[0])
	if delta < delay-20*time.Millisecond {
		t.Fatalf("analysis call spacing = %s, want at least %s", delta, delay-20*time.Millisecond)
	}
}
