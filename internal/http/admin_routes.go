package httpapi

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/http/middleware"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
	"github.com/arodd/hdhriptv/internal/stream"
	"github.com/arodd/hdhriptv/internal/ui"
)

const defaultAdminJSONBodyLimitBytes int64 = 1 << 20
const defaultDynamicSyncTimeout = 30 * time.Second
const defaultDVRSyncTimeout = 2 * time.Minute
const defaultDVRMutationTimeout = 30 * time.Second
const defaultAutomationMutationTimeout = 30 * time.Second
const defaultDVRLineupReloadDebounce = 60 * time.Second
const defaultDVRLineupReloadMaxWait = 5 * time.Minute
const defaultDVRLineupReloadTimeout = 30 * time.Second
const defaultResolveClientHostLookupTimeout = 2 * time.Second
const defaultResolveClientHostsTotalTimeout = 8 * time.Second
const defaultResolveClientHostCacheTTL = 2 * time.Minute
const defaultResolveClientHostNegativeCacheTTL = 15 * time.Second
const defaultResolveClientHostCacheSweepInterval = 30 * time.Second
const defaultResolveClientHostCacheMaxEntries = 4096
const defaultGroupsListLimit = 200
const maxGroupsListLimit = 1000
const defaultChannelsListLimit = 200
const maxChannelsListLimit = 1000
const defaultDynamicChannelQueriesListLimit = 200
const maxDynamicChannelQueriesListLimit = 1000
const defaultChannelSourcesListLimit = 200
const maxChannelSourcesListLimit = 2000
const defaultDuplicateSuggestionsLimit = 100
const maxDuplicateSuggestionsLimit = 500
const defaultDVRChannelMappingsListLimit = 200
const maxDVRChannelMappingsListLimit = 1000

// CatalogStore captures read APIs required by the admin HTTP layer.
type CatalogStore interface {
	GetGroups(ctx context.Context) ([]playlist.Group, error)
	ListGroupsPaged(ctx context.Context, limit, offset int, includeCounts bool) ([]playlist.Group, int, error)
	ListItems(ctx context.Context, q playlist.Query) ([]playlist.Item, int, error)
	ListCatalogItems(ctx context.Context, q playlist.Query) ([]playlist.Item, int, error)
	CatalogSearchWarningForQuery(search string, searchRegex bool) (sqlite.CatalogSearchWarning, error)
	ListActiveItemKeysByCatalogFilter(ctx context.Context, groupNames []string, searchQuery string, searchRegex bool) ([]string, error)
}

type catalogSearchWarningProvider interface {
	CatalogSearchWarningForQuery(search string, searchRegex bool) (sqlite.CatalogSearchWarning, error)
}

type catalogChannelResponse struct {
	channels.Channel
	SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
}

// ChannelsService captures channel/source operations needed by admin routes.
type ChannelsService interface {
	Create(ctx context.Context, itemKey, guideName, channelKey string, dynamicRule *channels.DynamicSourceRule) (channels.Channel, error)
	Delete(ctx context.Context, channelID int64) error
	List(ctx context.Context) ([]channels.Channel, error)
	ListPaged(ctx context.Context, limit, offset int) ([]channels.Channel, int, error)
	Reorder(ctx context.Context, channelIDs []int64) error
	Update(ctx context.Context, channelID int64, guideName *string, enabled *bool, dynamicRule *channels.DynamicSourceRule) (channels.Channel, error)
	SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (channels.DynamicSourceSyncResult, error)
	ListDynamicChannelQueries(ctx context.Context) ([]channels.DynamicChannelQuery, error)
	ListDynamicChannelQueriesPaged(ctx context.Context, limit, offset int) ([]channels.DynamicChannelQuery, int, error)
	GetDynamicChannelQuery(ctx context.Context, queryID int64) (channels.DynamicChannelQuery, error)
	CreateDynamicChannelQuery(ctx context.Context, create channels.DynamicChannelQueryCreate) (channels.DynamicChannelQuery, error)
	UpdateDynamicChannelQuery(ctx context.Context, queryID int64, update channels.DynamicChannelQueryUpdate) (channels.DynamicChannelQuery, error)
	DeleteDynamicChannelQuery(ctx context.Context, queryID int64) error
	ListDynamicGeneratedChannelsPaged(ctx context.Context, queryID int64, limit, offset int) ([]channels.Channel, int, error)
	ReorderDynamicGeneratedChannels(ctx context.Context, queryID int64, channelIDs []int64) error
	SyncDynamicChannelBlocks(ctx context.Context) (channels.DynamicChannelSyncResult, error)

	ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error)
	ListSourcesPaged(ctx context.Context, channelID int64, enabledOnly bool, limit, offset int) ([]channels.Source, int, error)
	AddSource(ctx context.Context, channelID int64, itemKey string, allowCrossChannel bool) (channels.Source, error)
	DeleteSource(ctx context.Context, channelID, sourceID int64) error
	ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error
	UpdateSource(ctx context.Context, channelID, sourceID int64, enabled *bool) (channels.Source, error)
	ClearSourceHealth(ctx context.Context, channelID int64) (int64, error)
	ClearAllSourceHealth(ctx context.Context) (int64, error)

	DuplicateSuggestions(ctx context.Context, minItems int, searchQuery string, limit, offset int) ([]channels.DuplicateGroup, int, error)
}

// TunerStatusProvider captures runtime tuner/session status.
type TunerStatusProvider interface {
	TunerStatusSnapshot() stream.TunerStatusSnapshot
}

// TunerRecoveryTrigger optionally enables admin-triggered shared-session
// recovery requests from the tuner status UI/API.
type TunerRecoveryTrigger interface {
	TriggerSessionRecovery(channelID int64, reason string) error
}

// SourceHealthClearRuntime clears runtime health caches/persistence state after
// source-health mutations.
type SourceHealthClearRuntime interface {
	ClearSourceHealth(channelID int64) error
	ClearAllSourceHealth() error
}

// DVRService captures DVR integration operations exposed to admin APIs.
type DVRService interface {
	GetState(ctx context.Context) (dvr.ConfigState, error)
	UpdateConfig(ctx context.Context, instance dvr.InstanceConfig) (dvr.ConfigState, error)
	TestConnection(ctx context.Context) (dvr.TestResult, error)
	ListLineups(ctx context.Context, refresh bool) ([]dvr.DVRLineup, error)
	Sync(ctx context.Context, req dvr.SyncRequest) (dvr.SyncResult, error)
	ReverseSync(ctx context.Context, req dvr.ReverseSyncRequest) (dvr.ReverseSyncResult, error)
	ReverseSyncChannel(ctx context.Context, channelID int64, req dvr.ReverseSyncRequest) (dvr.ReverseSyncResult, error)
	ListChannelMappings(ctx context.Context, enabledOnly bool, includeDynamic bool) ([]dvr.ChannelMapping, error)
	ListChannelMappingsPaged(ctx context.Context, enabledOnly bool, includeDynamic bool, limit, offset int) ([]dvr.ChannelMapping, int, error)
	GetChannelMapping(ctx context.Context, channelID int64) (dvr.ChannelMapping, error)
	UpdateChannelMapping(ctx context.Context, channelID int64, update dvr.ChannelMappingUpdate) (dvr.ChannelMapping, error)
	ReloadLineup(ctx context.Context) error
}

// DVRScheduler captures scheduler operations needed by DVR configuration handlers.
type DVRScheduler interface {
	ValidateCron(spec string) error
	UpdateJobSchedule(ctx context.Context, jobName string, enabled bool, cronSpec string) error
}

// AdminHandler serves admin UI and admin JSON APIs.
type AdminHandler struct {
	catalog                             CatalogStore
	channels                            ChannelsService
	templates                           *template.Template
	automation                          *AutomationDeps
	tunerStatus                         TunerStatusProvider
	lookupAddr                          func(ctx context.Context, addr string) ([]string, error)
	sourceHealthClearRuntime            SourceHealthClearRuntime
	dvr                                 DVRService
	dvrScheduler                        DVRScheduler
	logger                              *slog.Logger
	adminJSONBodyLimitBytes             int64
	dynamicSyncTimeout                  time.Duration
	dynamicBlockSyncTimeout             time.Duration
	dvrSyncTimeout                      time.Duration
	resolveClientHostsTimeout           time.Duration
	resolveClientHostCacheTTL           time.Duration
	resolveClientHostCacheNegativeTTL   time.Duration
	resolveClientHostCacheNow           func() time.Time
	resolveClientHostCacheSweepInterval time.Duration
	resolveClientHostCacheMaxEntries    int
	resolveClientHostCacheMu            sync.Mutex
	resolveClientHostCache              map[string]resolveClientHostCacheEntry
	resolveClientHostCacheLastSweep     time.Time
	resolveClientHostCacheExpiryHeap    resolveClientHostCacheExpiryMinHeap
	adminConfigMutationMu               sync.Mutex
	dynamicSyncMu                       sync.Mutex
	dynamicSyncStates                   map[int64]*dynamicChannelSyncState
	dynamicBlockSyncMu                  sync.Mutex
	dynamicBlockSyncState               dynamicBlockSyncState
	dvrLineupReloadMu                   sync.Mutex
	dvrLineupReloadState                dvrLineupReloadState
	dvrLineupReloadTimeout              time.Duration
	dvrLineupReloadDebounce             time.Duration
	dvrLineupReloadMaxWait              time.Duration

	closeOnce sync.Once
	closeCh   chan struct{} // closed on Close() to signal background workers
	workerWg  sync.WaitGroup
}

type dynamicChannelSyncRequest struct {
	channel   channels.Channel
	shouldRun bool
	version   uint64
}

type dynamicChannelSyncState struct {
	running       bool
	pending       dynamicChannelSyncRequest
	hasPending    bool
	latestVersion uint64
	runCancel     context.CancelFunc
}

type dynamicBlockSyncState struct {
	running       bool
	hasPending    bool
	latestVersion uint64
	runCancel     context.CancelFunc
}

type dvrLineupReloadState struct {
	running        bool
	pending        bool
	timerSeq       uint64
	timerDueAt     time.Time
	timerCancel    chan struct{}
	firstQueuedAt  time.Time
	lastQueuedAt   time.Time
	coalescedCount int
	reasonCounts   map[string]int
}

type dvrLineupReloadBatch struct {
	reasonSummary   string
	coalescedCount  int
	firstQueuedAt   time.Time
	lastQueuedAt    time.Time
	queuedDuration  time.Duration
	queueWindowSpan time.Duration
}

type resolveClientHostCacheEntry struct {
	host      string
	expiresAt time.Time
}

type resolveClientHostCacheExpiryHeapItem struct {
	ip        string
	expiresAt time.Time
}

type resolveClientHostCacheExpiryMinHeap []resolveClientHostCacheExpiryHeapItem

func (h resolveClientHostCacheExpiryMinHeap) Len() int {
	return len(h)
}

func (h resolveClientHostCacheExpiryMinHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h resolveClientHostCacheExpiryMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *resolveClientHostCacheExpiryMinHeap) Push(x any) {
	*h = append(*h, x.(resolveClientHostCacheExpiryHeapItem))
}

func (h *resolveClientHostCacheExpiryMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type resolveClientHostResolutionStats struct {
	cacheHits          int
	cacheNegativeHits  int
	cacheMisses        int
	memoizedHits       int
	memoizedEmptyHits  int
	lookupCalls        int
	lookupErrors       int
	lookupEmptyResults int
}

func (s resolveClientHostResolutionStats) cacheHitRate() float64 {
	total := s.cacheHits + s.cacheNegativeHits + s.cacheMisses
	if total == 0 {
		return 0
	}
	return float64(s.cacheHits+s.cacheNegativeHits) / float64(total)
}

func NewAdminHandler(catalog CatalogStore, channelsSvc ChannelsService, automation ...AutomationDeps) (*AdminHandler, error) {
	tmpls, err := ui.ParseTemplates()
	if err != nil {
		return nil, fmt.Errorf("parse ui templates: %w", err)
	}

	var auto *AutomationDeps
	if len(automation) > 0 {
		deps := automation[0]
		if err := deps.validate(); err != nil {
			return nil, err
		}
		auto = &deps
	}

	return &AdminHandler{
		catalog:                             catalog,
		channels:                            channelsSvc,
		templates:                           tmpls,
		automation:                          auto,
		lookupAddr:                          net.DefaultResolver.LookupAddr,
		logger:                              slog.Default(),
		adminJSONBodyLimitBytes:             defaultAdminJSONBodyLimitBytes,
		dynamicSyncTimeout:                  defaultDynamicSyncTimeout,
		dynamicBlockSyncTimeout:             defaultDynamicSyncTimeout,
		dvrSyncTimeout:                      defaultDVRSyncTimeout,
		resolveClientHostsTimeout:           defaultResolveClientHostsTotalTimeout,
		resolveClientHostCacheTTL:           defaultResolveClientHostCacheTTL,
		resolveClientHostCacheNegativeTTL:   defaultResolveClientHostNegativeCacheTTL,
		resolveClientHostCacheNow:           time.Now,
		resolveClientHostCacheSweepInterval: defaultResolveClientHostCacheSweepInterval,
		resolveClientHostCacheMaxEntries:    defaultResolveClientHostCacheMaxEntries,
		resolveClientHostCache:              make(map[string]resolveClientHostCacheEntry),
		dynamicSyncStates:                   make(map[int64]*dynamicChannelSyncState),
		dvrLineupReloadTimeout:              defaultDVRLineupReloadTimeout,
		dvrLineupReloadDebounce:             defaultDVRLineupReloadDebounce,
		dvrLineupReloadMaxWait:              defaultDVRLineupReloadMaxWait,
		closeCh:                             make(chan struct{}),
	}, nil
}

func (h *AdminHandler) SetTunerStatusProvider(provider TunerStatusProvider) {
	if h == nil {
		return
	}
	h.tunerStatus = provider
}

func (h *AdminHandler) SetSourceHealthClearRuntime(provider SourceHealthClearRuntime) {
	if h == nil {
		return
	}
	h.sourceHealthClearRuntime = provider
}

func (h *AdminHandler) SetDVRService(service DVRService) {
	if h == nil {
		return
	}
	h.dvr = service
}

func (h *AdminHandler) SetDVRScheduler(scheduler DVRScheduler) {
	if h == nil {
		return
	}
	h.dvrScheduler = scheduler
}

func (h *AdminHandler) SetLogger(logger *slog.Logger) {
	if h == nil {
		return
	}
	if logger == nil {
		h.logger = slog.Default()
		return
	}
	h.logger = logger
}

// Close cancels all in-flight dynamic sync workers and waits for them to
// finish. It is safe to call multiple times.
func (h *AdminHandler) Close() {
	if h == nil {
		return
	}
	h.closeOnce.Do(func() {
		close(h.closeCh)
	})
	// Acquire enqueue mutexes to establish a happens-before barrier with any
	// in-flight enqueue that might call workerWg.Add(1). After these
	// lock/unlock pairs, no new Add(1) can occur because enqueue methods check
	// closeCh under their respective mutex.
	h.dynamicSyncMu.Lock()
	h.dynamicSyncMu.Unlock()
	h.dynamicBlockSyncMu.Lock()
	h.dynamicBlockSyncMu.Unlock()
	h.dvrLineupReloadMu.Lock()
	if h.dvrLineupReloadState.timerCancel != nil {
		close(h.dvrLineupReloadState.timerCancel)
		h.dvrLineupReloadState.timerCancel = nil
		h.dvrLineupReloadState.timerDueAt = time.Time{}
	}
	h.dvrLineupReloadState.pending = false
	h.dvrLineupReloadState.firstQueuedAt = time.Time{}
	h.dvrLineupReloadState.lastQueuedAt = time.Time{}
	h.dvrLineupReloadState.coalescedCount = 0
	h.dvrLineupReloadState.reasonCounts = nil
	h.dvrLineupReloadMu.Unlock()
	h.resolveClientHostCacheMu.Lock()
	h.resolveClientHostCache = nil
	h.resolveClientHostCacheLastSweep = time.Time{}
	h.resolveClientHostCacheExpiryHeap = nil
	h.resolveClientHostCacheMu.Unlock()

	h.workerWg.Wait()
}

// workerContext returns a context derived from closeCh so that background
// workers are canceled when Close() is called.
func (h *AdminHandler) workerContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-h.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (h *AdminHandler) RegisterRoutes(mux *http.ServeMux, adminAuth string) {
	auth := middleware.BasicAuth(adminAuth)

	mux.Handle("GET /ui/", auth(http.HandlerFunc(h.handleUIRoot)))
	mux.Handle("GET /ui/catalog", auth(http.HandlerFunc(h.handleUICatalog)))
	mux.Handle("GET /ui/channels", auth(http.HandlerFunc(h.handleUIChannels)))
	mux.Handle("GET /ui/channels/{channelID}", auth(http.HandlerFunc(h.handleUIChannelDetail)))
	mux.Handle("GET /ui/dynamic-channels/{queryID}", auth(http.HandlerFunc(h.handleUIDynamicChannelDetail)))
	mux.Handle("GET /ui/merge", auth(http.HandlerFunc(h.handleUIMerge)))
	mux.Handle("GET /ui/tuners", auth(http.HandlerFunc(h.handleUITuners)))
	if h.automation != nil {
		mux.Handle("GET /ui/automation", auth(http.HandlerFunc(h.handleUIAutomation)))
	}
	if h.dvr != nil {
		mux.Handle("GET /ui/dvr", auth(http.HandlerFunc(h.handleUIDVR)))
	}

	mux.Handle("GET /api/groups", auth(http.HandlerFunc(h.handleGroups)))
	mux.Handle("GET /api/items", auth(http.HandlerFunc(h.handleItems)))

	mux.Handle("GET /api/channels", auth(http.HandlerFunc(h.handleChannels)))
	mux.Handle("POST /api/channels", auth(http.HandlerFunc(h.handleCreateChannel)))
	mux.Handle("PATCH /api/channels/reorder", auth(http.HandlerFunc(h.handleReorderChannels)))
	mux.Handle("PATCH /api/channels/{channelID}", auth(http.HandlerFunc(h.handleUpdateChannel)))
	mux.Handle("DELETE /api/channels/{channelID}", auth(http.HandlerFunc(h.handleDeleteChannel)))
	mux.Handle("GET /api/dynamic-channels", auth(http.HandlerFunc(h.handleDynamicChannelQueries)))
	mux.Handle("POST /api/dynamic-channels", auth(http.HandlerFunc(h.handleCreateDynamicChannelQuery)))
	mux.Handle("GET /api/dynamic-channels/{queryID}", auth(http.HandlerFunc(h.handleGetDynamicChannelQuery)))
	mux.Handle("PATCH /api/dynamic-channels/{queryID}", auth(http.HandlerFunc(h.handleUpdateDynamicChannelQuery)))
	mux.Handle("DELETE /api/dynamic-channels/{queryID}", auth(http.HandlerFunc(h.handleDeleteDynamicChannelQuery)))
	mux.Handle("GET /api/dynamic-channels/{queryID}/channels", auth(http.HandlerFunc(h.handleDynamicGeneratedChannels)))
	mux.Handle("PATCH /api/dynamic-channels/{queryID}/channels/reorder", auth(http.HandlerFunc(h.handleReorderDynamicGeneratedChannels)))

	mux.Handle("GET /api/channels/{channelID}/sources", auth(http.HandlerFunc(h.handleSources)))
	mux.Handle("POST /api/channels/{channelID}/sources", auth(http.HandlerFunc(h.handleAddSource)))
	mux.Handle("POST /api/channels/{channelID}/sources/health/clear", auth(http.HandlerFunc(h.handleClearSourceHealth)))
	mux.Handle("PATCH /api/channels/{channelID}/sources/reorder", auth(http.HandlerFunc(h.handleReorderSources)))
	mux.Handle("PATCH /api/channels/{channelID}/sources/{sourceID}", auth(http.HandlerFunc(h.handleUpdateSource)))
	mux.Handle("DELETE /api/channels/{channelID}/sources/{sourceID}", auth(http.HandlerFunc(h.handleDeleteSource)))
	mux.Handle("POST /api/channels/sources/health/clear", auth(http.HandlerFunc(h.handleClearAllSourceHealth)))

	mux.Handle("GET /api/suggestions/duplicates", auth(http.HandlerFunc(h.handleDuplicateSuggestions)))
	mux.Handle("GET /api/admin/tuners", auth(http.HandlerFunc(h.handleTunerStatus)))
	mux.Handle("POST /api/admin/tuners/recovery", auth(http.HandlerFunc(h.handleTriggerTunerRecovery)))
	if h.dvr != nil {
		mux.Handle("GET /api/admin/dvr", auth(http.HandlerFunc(h.handleGetDVR)))
		mux.Handle("PUT /api/admin/dvr", auth(http.HandlerFunc(h.handlePutDVR)))
		mux.Handle("POST /api/admin/dvr/test", auth(http.HandlerFunc(h.handleTestDVR)))
		mux.Handle("GET /api/admin/dvr/lineups", auth(http.HandlerFunc(h.handleDVRLineups)))
		mux.Handle("POST /api/admin/dvr/sync", auth(http.HandlerFunc(h.handleDVRSync)))
		mux.Handle("POST /api/admin/dvr/reverse-sync", auth(http.HandlerFunc(h.handleDVRReverseSync)))
		mux.Handle("GET /api/channels/dvr", auth(http.HandlerFunc(h.handleListChannelDVRMappings)))
		mux.Handle("GET /api/channels/{channelID}/dvr", auth(http.HandlerFunc(h.handleGetChannelDVRMapping)))
		mux.Handle("PUT /api/channels/{channelID}/dvr", auth(http.HandlerFunc(h.handlePutChannelDVRMapping)))
		mux.Handle("POST /api/channels/{channelID}/dvr/reverse-sync", auth(http.HandlerFunc(h.handleChannelDVRReverseSync)))
	}
	if h.automation != nil {
		mux.Handle("GET /api/admin/automation", auth(http.HandlerFunc(h.handleGetAutomation)))
		mux.Handle("PUT /api/admin/automation", auth(http.HandlerFunc(h.handlePutAutomation)))
		mux.Handle("POST /api/admin/jobs/playlist-sync/run", auth(http.HandlerFunc(h.handleRunPlaylistSync)))
		mux.Handle("POST /api/admin/jobs/auto-prioritize/run", auth(http.HandlerFunc(h.handleRunAutoPrioritize)))
		mux.Handle("POST /api/admin/jobs/auto-prioritize/cache/clear", auth(http.HandlerFunc(h.handleClearAutoPrioritizeCache)))
		mux.Handle("GET /api/admin/jobs/{runID}", auth(http.HandlerFunc(h.handleGetJobRun)))
		mux.Handle("GET /api/admin/jobs", auth(http.HandlerFunc(h.handleListJobRuns)))
	}
}

func (h *AdminHandler) SetJSONBodyLimitBytes(limit int64) {
	if h == nil {
		return
	}
	if limit < 1 {
		limit = defaultAdminJSONBodyLimitBytes
	}
	h.adminJSONBodyLimitBytes = limit
}

func (h *AdminHandler) SetDVRLineupReloadTimeout(timeout time.Duration) {
	if h == nil {
		return
	}
	if timeout <= 0 {
		timeout = defaultDVRLineupReloadTimeout
	}
	h.dvrLineupReloadTimeout = timeout
}

func (h *AdminHandler) dvrSyncRequestTimeout() time.Duration {
	if h == nil || h.dvrSyncTimeout <= 0 {
		return defaultDVRSyncTimeout
	}
	return h.dvrSyncTimeout
}

func (h *AdminHandler) detachedDVRContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := h.dvrSyncRequestTimeout()
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

func (h *AdminHandler) detachedDVRMutationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), defaultDVRMutationTimeout)
}

func (h *AdminHandler) detachedAutomationMutationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), defaultAutomationMutationTimeout)
}

func (h *AdminHandler) handleUIRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/catalog", http.StatusFound)
}

func (h *AdminHandler) handleUICatalog(w http.ResponseWriter, _ *http.Request) {
	h.renderTemplate(w, "catalog.html", map[string]any{"Title": "HDHR IPTV Catalog"})
}

func (h *AdminHandler) handleUIChannels(w http.ResponseWriter, _ *http.Request) {
	h.renderTemplate(w, "channels.html", map[string]any{"Title": "HDHR IPTV Channels"})
}

func (h *AdminHandler) handleUIChannelDetail(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}
	h.renderTemplate(w, "channel_detail.html", map[string]any{
		"Title":     "HDHR IPTV Channel Detail",
		"ChannelID": channelID,
	})
}

func (h *AdminHandler) handleUIDynamicChannelDetail(w http.ResponseWriter, r *http.Request) {
	queryID, err := parsePathInt64(r, "queryID")
	if err != nil {
		http.Error(w, "invalid query id", http.StatusBadRequest)
		return
	}
	h.renderTemplate(w, "dynamic_channel_detail.html", map[string]any{
		"Title":   "HDHR IPTV Dynamic Channel Block",
		"QueryID": queryID,
	})
}

func (h *AdminHandler) handleUIMerge(w http.ResponseWriter, _ *http.Request) {
	h.renderTemplate(w, "merge.html", map[string]any{"Title": "HDHR IPTV Merge Suggestions"})
}

func (h *AdminHandler) handleUITuners(w http.ResponseWriter, _ *http.Request) {
	h.renderTemplate(w, "tuners.html", map[string]any{"Title": "HDHR IPTV Tuner Status"})
}

func (h *AdminHandler) handleUIAutomation(w http.ResponseWriter, _ *http.Request) {
	h.renderTemplate(w, "automation.html", map[string]any{"Title": "HDHR IPTV Automation"})
}

func (h *AdminHandler) handleUIDVR(w http.ResponseWriter, _ *http.Request) {
	h.renderTemplate(w, "dvr.html", map[string]any{"Title": "HDHR IPTV DVR Sync"})
}

func (h *AdminHandler) renderTemplate(w http.ResponseWriter, name string, data map[string]any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, fmt.Sprintf("render template: %v", err), http.StatusInternalServerError)
	}
}

func (h *AdminHandler) handleGroups(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePaginationQuery(r, defaultGroupsListLimit, maxGroupsListLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	includeCounts, err := parseOptionalBoolQueryParam(r, "include_counts", true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	groups, total, err := h.catalog.ListGroupsPaged(r.Context(), limit, offset, includeCounts)
	if err != nil {
		http.Error(w, fmt.Sprintf("get groups: %v", err), http.StatusInternalServerError)
		return
	}

	type groupResponse struct {
		Name  string `json:"name"`
		Count int    `json:"count,omitempty"`
	}
	out := make([]groupResponse, 0, len(groups))
	for _, group := range groups {
		entry := groupResponse{Name: group.Name}
		if includeCounts {
			entry.Count = group.Count
		}
		out = append(out, entry)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"groups":         out,
		"total":          total,
		"limit":          limit,
		"offset":         offset,
		"include_counts": includeCounts,
	})
}

func (h *AdminHandler) handleItems(w http.ResponseWriter, r *http.Request) {
	groupNames := parseCatalogGroupFilters(r)
	searchRegex, err := parseOptionalBoolQueryParam(r, "q_regex", false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	q := playlist.Query{
		Group:       firstCatalogGroupName(groupNames),
		GroupNames:  groupNames,
		Search:      strings.TrimSpace(r.URL.Query().Get("q")),
		SearchRegex: searchRegex,
		Limit:       parseInt(r.URL.Query().Get("limit"), 100),
		Offset:      parseInt(r.URL.Query().Get("offset"), 0),
	}
	if q.Limit > 1000 {
		q.Limit = 1000
	}
	if q.Limit < 1 {
		q.Limit = 100
	}
	if q.Offset < 0 {
		q.Offset = 0
	}

	items, total, err := h.catalog.ListCatalogItems(r.Context(), q)
	if err != nil {
		if isInputError(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("list items: %v", err), http.StatusInternalServerError)
		return
	}

	type itemResponse struct {
		ItemKey    string `json:"item_key"`
		ChannelKey string `json:"channel_key,omitempty"`
		Name       string `json:"name"`
		Group      string `json:"group"`
		StreamURL  string `json:"stream_url,omitempty"`
		Logo       string `json:"logo,omitempty"`
		Active     bool   `json:"active"`
	}

	out := make([]itemResponse, 0, len(items))
	for _, item := range items {
		out = append(out, itemResponse{
			ItemKey:    item.ItemKey,
			ChannelKey: item.ChannelKey,
			Name:       item.Name,
			Group:      item.Group,
			StreamURL:  item.StreamURL,
			Logo:       item.TVGLogo,
			Active:     item.Active,
		})
	}

	var searchWarning *sqlite.CatalogSearchWarning
	if strings.TrimSpace(q.Search) != "" {
		searchWarning = h.catalogSearchWarning(q.Search, q.SearchRegex)
	}

	type itemsResponse struct {
		Items         []itemResponse               `json:"items"`
		Total         int                          `json:"total"`
		Limit         int                          `json:"limit"`
		Offset        int                          `json:"offset"`
		QRegex        bool                         `json:"q_regex"`
		SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
	}

	writeJSON(w, http.StatusOK, itemsResponse{
		Items:         out,
		Total:         total,
		Limit:         q.Limit,
		Offset:        q.Offset,
		QRegex:        q.SearchRegex,
		SearchWarning: searchWarning,
	})
}

func parseCatalogGroupFilters(r *http.Request) []string {
	if r == nil || r.URL == nil {
		return nil
	}
	values := r.URL.Query()
	if len(values) == 0 {
		return nil
	}

	raw := make([]string, 0, len(values["group"])+len(values["group_names"]))
	raw = append(raw, values["group"]...)
	raw = append(raw, values["group_names"]...)

	parts := make([]string, 0, len(raw))
	for _, token := range raw {
		if strings.TrimSpace(token) == "" {
			continue
		}
		for _, chunk := range strings.Split(token, ",") {
			chunk = strings.TrimSpace(chunk)
			if chunk == "" {
				continue
			}
			parts = append(parts, chunk)
		}
	}

	return channels.NormalizeGroupNames("", parts)
}

func firstCatalogGroupName(groupNames []string) string {
	return channels.GroupNameAlias(groupNames)
}

func (h *AdminHandler) handleChannels(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePaginationQuery(r, defaultChannelsListLimit, maxChannelsListLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	items, total, err := h.channels.ListPaged(r.Context(), limit, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("list channels: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channels": items,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

func (h *AdminHandler) handleCreateChannel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ItemKey     string                      `json:"item_key"`
		GuideName   string                      `json:"guide_name"`
		ChannelKey  string                      `json:"channel_key"`
		DynamicRule *channels.DynamicSourceRule `json:"dynamic_rule"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	channel, err := h.channels.Create(r.Context(), req.ItemKey, req.GuideName, req.ChannelKey, req.DynamicRule)
	if err != nil {
		writeChannelsError(w, err, "create channel")
		return
	}
	h.scheduleDynamicChannelSync(r, channel)
	var searchWarning *sqlite.CatalogSearchWarning
	if req.DynamicRule != nil {
		searchWarning = h.catalogSearchWarning(channel.DynamicRule.SearchQuery, channel.DynamicRule.SearchRegex)
	}

	response := catalogChannelResponse{
		Channel:       channel,
		SearchWarning: searchWarning,
	}
	h.logAdminMutation(
		r,
		"admin channel created",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"guide_name", channel.GuideName,
		"channel_key", channel.ChannelKey,
		"item_key", strings.TrimSpace(req.ItemKey),
		"dynamic_enabled", channel.DynamicRule.Enabled,
		"dynamic_group", channel.DynamicRule.GroupName,
		"dynamic_query", channel.DynamicRule.SearchQuery,
		"dynamic_query_regex", channel.DynamicRule.SearchRegex,
	)
	if searchWarning != nil && searchWarning.Truncated {
		h.logAdminMutation(
			r,
			"admin channel created with truncated dynamic search",
			"channel_id", channel.ChannelID,
			"search_warning", searchWarning,
		)
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *AdminHandler) handleUpdateChannel(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	var req struct {
		GuideName   *string                     `json:"guide_name"`
		Enabled     *bool                       `json:"enabled"`
		DynamicRule *channels.DynamicSourceRule `json:"dynamic_rule"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	channel, err := h.channels.Update(r.Context(), channelID, req.GuideName, req.Enabled, req.DynamicRule)
	if err != nil {
		writeChannelsError(w, err, "update channel")
		return
	}
	if req.Enabled != nil && !*req.Enabled {
		h.triggerChannelRecoveryForActiveChannelMutation(r, channelID, "admin_channel_disabled")
	}
	h.scheduleDynamicChannelSync(r, channel)

	h.logAdminMutation(
		r,
		"admin channel updated",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"guide_name", channel.GuideName,
		"enabled", channel.Enabled,
		"dynamic_enabled", channel.DynamicRule.Enabled,
		"dynamic_group", channel.DynamicRule.GroupName,
		"dynamic_query", channel.DynamicRule.SearchQuery,
		"dynamic_query_regex", channel.DynamicRule.SearchRegex,
	)
	var searchWarning *sqlite.CatalogSearchWarning
	if req.DynamicRule != nil {
		searchWarning = h.catalogSearchWarning(channel.DynamicRule.SearchQuery, channel.DynamicRule.SearchRegex)
	}
	response := catalogChannelResponse{
		Channel:       channel,
		SearchWarning: searchWarning,
	}
	if searchWarning != nil && searchWarning.Truncated {
		h.logAdminMutation(
			r,
			"admin channel updated with truncated dynamic search",
			"channel_id", channel.ChannelID,
			"search_warning", searchWarning,
		)
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *AdminHandler) scheduleDynamicChannelSync(r *http.Request, channel channels.Channel) {
	if h == nil || r == nil {
		return
	}
	shouldRun := shouldRunDynamicChannelSync(channel)
	queuedWhileRunning, canceledRunning := h.enqueueDynamicChannelSync(channel, shouldRun)
	if !shouldRun {
		if canceledRunning {
			h.logAdminMutation(
				r,
				"admin dynamic channel immediate sync canceled",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"dynamic_enabled", channel.DynamicRule.Enabled,
				"group", channel.DynamicRule.GroupName,
				"query", channel.DynamicRule.SearchQuery,
				"query_regex", channel.DynamicRule.SearchRegex,
				"queued_while_running", queuedWhileRunning,
				"canceled_running", canceledRunning,
			)
		}
		return
	}

	h.logAdminMutation(
		r,
		"admin dynamic channel immediate sync queued",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"group", channel.DynamicRule.GroupName,
		"query", channel.DynamicRule.SearchQuery,
		"query_regex", channel.DynamicRule.SearchRegex,
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
}

func shouldRunDynamicChannelSync(channel channels.Channel) bool {
	if !channel.DynamicRule.Enabled {
		return false
	}
	return strings.TrimSpace(channel.DynamicRule.SearchQuery) != ""
}

func (h *AdminHandler) enqueueDynamicChannelSync(channel channels.Channel, shouldRun bool) (bool, bool) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	select {
	case <-h.closeCh:
		return false, false
	default:
	}

	if h.dynamicSyncStates == nil {
		h.dynamicSyncStates = make(map[int64]*dynamicChannelSyncState)
	}

	state, ok := h.dynamicSyncStates[channel.ChannelID]
	if !ok {
		if !shouldRun {
			return false, false
		}
		state = &dynamicChannelSyncState{}
		h.dynamicSyncStates[channel.ChannelID] = state
	}
	state.latestVersion++
	request := dynamicChannelSyncRequest{
		channel:   channel,
		shouldRun: shouldRun,
		version:   state.latestVersion,
	}

	if state.running {
		state.pending = request
		state.hasPending = true
		canceledRunning := false
		// Superseded enabled updates should preempt stale in-flight lookup work.
		if state.runCancel != nil {
			state.runCancel()
			canceledRunning = true
		}
		return true, canceledRunning
	}

	if !shouldRun {
		delete(h.dynamicSyncStates, channel.ChannelID)
		return false, false
	}

	state.running = true
	h.workerWg.Add(1)
	go func() {
		defer h.workerWg.Done()
		h.runDynamicChannelSyncLoop(request)
	}()
	return false, false
}

func (h *AdminHandler) runDynamicChannelSyncLoop(request dynamicChannelSyncRequest) {
	current := request
	for {
		h.runDynamicChannelSyncOnce(current)

		next, ok := h.consumePendingDynamicChannelSync(current.channel.ChannelID)
		if !ok {
			return
		}
		current = next
	}
}

func (h *AdminHandler) runDynamicChannelSyncOnce(request dynamicChannelSyncRequest) {
	if !request.shouldRun {
		return
	}

	channel := request.channel
	logger := h.loggerOrDefault()
	startedAt := time.Now()
	timeout := h.dynamicSyncTimeout
	if timeout <= 0 {
		timeout = defaultDynamicSyncTimeout
	}

	logger.Info(
		"admin dynamic channel immediate sync started",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"group", channel.DynamicRule.GroupName,
		"query", channel.DynamicRule.SearchQuery,
		"query_regex", channel.DynamicRule.SearchRegex,
		"timeout", timeout.String(),
		"sync_version", request.version,
	)

	ctx, cancelWorker := h.workerContext()
	ctx, cancelRun := context.WithCancel(ctx)
	h.setDynamicChannelSyncRunCancel(channel.ChannelID, cancelRun)
	defer func() {
		cancelRun()
		cancelWorker()
		h.clearDynamicChannelSyncRunCancel(channel.ChannelID)
	}()

	stale, latestVersion := h.isDynamicChannelSyncRunStale(channel.ChannelID, request.version)
	if stale {
		logger.Info(
			"admin dynamic channel immediate sync skipped stale run",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"matched_items", 0,
			"sync_version", request.version,
			"latest_version", latestVersion,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"stale_stage", "before_lookup",
		)
		return
	}

	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	itemKeys, err := h.catalog.ListActiveItemKeysByCatalogFilter(
		ctx,
		channel.DynamicRule.GroupNames,
		channel.DynamicRule.SearchQuery,
		channel.DynamicRule.SearchRegex,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			cancelReason, latestVersion := h.dynamicChannelSyncCancelReason(channel.ChannelID, request.version)
			logger.Info(
				"admin dynamic channel immediate sync canceled",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"group", channel.DynamicRule.GroupName,
				"query", channel.DynamicRule.SearchQuery,
				"query_regex", channel.DynamicRule.SearchRegex,
				"matched_items", 0,
				"duration_ms", time.Since(startedAt).Milliseconds(),
				"sync_version", request.version,
				"latest_version", latestVersion,
				"cancel_reason", cancelReason,
			)
			return
		}
		logger.Warn(
			"admin dynamic channel immediate sync failed",
			"stage", "list_active_item_keys",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"query_regex", channel.DynamicRule.SearchRegex,
			"matched_items", 0,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"sync_version", request.version,
			"error", err,
		)
		return
	}

	stale, latestVersion = h.isDynamicChannelSyncRunStale(channel.ChannelID, request.version)
	if stale {
		logger.Info(
			"admin dynamic channel immediate sync skipped stale run",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"query_regex", channel.DynamicRule.SearchRegex,
			"matched_items", len(itemKeys),
			"sync_version", request.version,
			"latest_version", latestVersion,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"stale_stage", "after_lookup",
		)
		return
	}

	result, err := h.channels.SyncDynamicSources(ctx, channel.ChannelID, itemKeys)
	if err != nil {
		logger.Warn(
			"admin dynamic channel immediate sync failed",
			"stage", "sync_dynamic_sources",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"query_regex", channel.DynamicRule.SearchRegex,
			"matched_items", len(itemKeys),
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"sync_version", request.version,
			"error", err,
		)
		return
	}

	logger.Info(
		"admin dynamic channel immediate sync completed",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"group", channel.DynamicRule.GroupName,
		"query", channel.DynamicRule.SearchQuery,
		"query_regex", channel.DynamicRule.SearchRegex,
		"matched_items", len(itemKeys),
		"added_sources", result.Added,
		"removed_sources", result.Removed,
		"retained_sources", result.Retained,
		"duration_ms", time.Since(startedAt).Milliseconds(),
		"sync_version", request.version,
	)
}

func (h *AdminHandler) consumePendingDynamicChannelSync(channelID int64) (dynamicChannelSyncRequest, bool) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return dynamicChannelSyncRequest{}, false
	}
	state.runCancel = nil

	if state.hasPending {
		next := state.pending
		state.pending = dynamicChannelSyncRequest{}
		state.hasPending = false
		if !next.shouldRun {
			delete(h.dynamicSyncStates, channelID)
			return dynamicChannelSyncRequest{}, false
		}
		return next, true
	}

	delete(h.dynamicSyncStates, channelID)
	return dynamicChannelSyncRequest{}, false
}

func (h *AdminHandler) setDynamicChannelSyncRunCancel(channelID int64, cancel context.CancelFunc) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return
	}
	state.runCancel = cancel
}

func (h *AdminHandler) clearDynamicChannelSyncRunCancel(channelID int64) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return
	}
	state.runCancel = nil
}

func (h *AdminHandler) isDynamicChannelSyncRunStale(channelID int64, version uint64) (bool, uint64) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return true, 0
	}
	return state.latestVersion != version, state.latestVersion
}

func (h *AdminHandler) dynamicChannelSyncCancelReason(channelID int64, version uint64) (string, uint64) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return "state_removed", 0
	}
	if state.latestVersion != version {
		if state.hasPending && !state.pending.shouldRun {
			return "disabled_or_deleted", state.latestVersion
		}
		return "superseded", state.latestVersion
	}
	return "canceled", state.latestVersion
}

func (h *AdminHandler) cancelDynamicChannelSyncForDelete(channelID int64) (bool, bool, bool) {
	if h == nil {
		return false, false, false
	}

	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return false, false, false
	}

	state.latestVersion++
	state.pending = dynamicChannelSyncRequest{
		channel:   channels.Channel{ChannelID: channelID},
		shouldRun: false,
		version:   state.latestVersion,
	}
	state.hasPending = true

	canceledRunning := false
	if state.runCancel != nil {
		state.runCancel()
		state.runCancel = nil
		canceledRunning = true
	}

	wasRunning := state.running
	if !wasRunning {
		delete(h.dynamicSyncStates, channelID)
	}

	return true, wasRunning, canceledRunning
}

func (h *AdminHandler) handleReorderChannels(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChannelIDs []int64 `json:"channel_ids"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	if err := h.channels.Reorder(r.Context(), req.ChannelIDs); err != nil {
		writeChannelsError(w, err, "reorder channels")
		return
	}
	h.logAdminMutation(
		r,
		"admin channels reordered",
		"channel_count", len(req.ChannelIDs),
		"channel_ids", req.ChannelIDs,
	)
	h.enqueueDVRLineupReload(
		"channels_reorder",
		"channel_count", len(req.ChannelIDs),
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) handleDeleteChannel(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	if err := h.channels.Delete(r.Context(), channelID); err != nil {
		writeChannelsError(w, err, "delete channel")
		return
	}
	// Delete occurs before recovery trigger intentionally: recovery is driven
	// by in-memory active session state only and does not re-read store rows.
	h.triggerChannelRecoveryForActiveChannelMutation(r, channelID, "admin_channel_deleted")
	dynamicSyncStateFound, dynamicSyncRunning, dynamicSyncCanceled := h.cancelDynamicChannelSyncForDelete(channelID)

	h.logAdminMutation(
		r,
		"admin channel deleted",
		"channel_id", channelID,
		"dynamic_sync_state_found", dynamicSyncStateFound,
		"dynamic_sync_running", dynamicSyncRunning,
		"dynamic_sync_canceled", dynamicSyncCanceled,
	)
	w.WriteHeader(http.StatusNoContent)
}

type dynamicChannelQueryResponse struct {
	QueryID       int64                        `json:"query_id"`
	Enabled       bool                         `json:"enabled"`
	Name          string                       `json:"name"`
	GroupName     string                       `json:"group_name"`
	GroupNames    []string                     `json:"group_names,omitempty"`
	SearchQuery   string                       `json:"search_query"`
	SearchRegex   bool                         `json:"search_regex,omitempty"`
	OrderIndex    int                          `json:"order_index"`
	BlockStart    int                          `json:"block_start"`
	BlockEnd      int                          `json:"block_end"`
	Generated     int                          `json:"generated_count"`
	TruncatedBy   int                          `json:"truncated_by"`
	CreatedAt     int64                        `json:"created_at"`
	UpdatedAt     int64                        `json:"updated_at"`
	SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
}

func toDynamicChannelQueryResponse(query channels.DynamicChannelQuery, searchWarning *sqlite.CatalogSearchWarning) dynamicChannelQueryResponse {
	blockStart, err := channels.DynamicGuideBlockStart(query.OrderIndex)
	if err != nil {
		blockStart = 0
	}
	blockEnd := 0
	if blockStart > 0 {
		blockEnd = blockStart + channels.DynamicGuideBlockSize - 1
	}
	return dynamicChannelQueryResponse{
		QueryID:       query.QueryID,
		Enabled:       query.Enabled,
		Name:          query.Name,
		GroupName:     query.GroupName,
		GroupNames:    query.GroupNames,
		SearchQuery:   query.SearchQuery,
		SearchRegex:   query.SearchRegex,
		OrderIndex:    query.OrderIndex,
		BlockStart:    blockStart,
		BlockEnd:      blockEnd,
		Generated:     query.LastCount,
		TruncatedBy:   query.TruncatedBy,
		CreatedAt:     query.CreatedAt,
		UpdatedAt:     query.UpdatedAt,
		SearchWarning: searchWarning,
	}
}

func (h *AdminHandler) handleDynamicChannelQueries(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePaginationQuery(r, defaultDynamicChannelQueriesListLimit, maxDynamicChannelQueriesListLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queries, total, err := h.channels.ListDynamicChannelQueriesPaged(r.Context(), limit, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("list dynamic channel queries: %v", err), http.StatusInternalServerError)
		return
	}

	out := make([]dynamicChannelQueryResponse, 0, len(queries))
	for _, query := range queries {
		searchWarning := h.catalogSearchWarning(query.SearchQuery, query.SearchRegex)
		out = append(out, toDynamicChannelQueryResponse(query, searchWarning))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queries": out,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func (h *AdminHandler) handleGetDynamicChannelQuery(w http.ResponseWriter, r *http.Request) {
	queryID, err := parsePathInt64(r, "queryID")
	if err != nil {
		http.Error(w, "invalid query id", http.StatusBadRequest)
		return
	}

	query, err := h.channels.GetDynamicChannelQuery(r.Context(), queryID)
	if err != nil {
		writeChannelsError(w, err, "get dynamic channel query")
		return
	}
	searchWarning := h.catalogSearchWarning(query.SearchQuery, query.SearchRegex)
	writeJSON(w, http.StatusOK, toDynamicChannelQueryResponse(query, searchWarning))
}

func (h *AdminHandler) handleCreateDynamicChannelQuery(w http.ResponseWriter, r *http.Request) {
	var req channels.DynamicChannelQueryCreate
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	query, err := h.channels.CreateDynamicChannelQuery(r.Context(), req)
	if err != nil {
		writeChannelsError(w, err, "create dynamic channel query")
		return
	}
	searchWarning := h.catalogSearchWarning(query.SearchQuery, query.SearchRegex)
	queuedWhileRunning, canceledRunning := h.scheduleDynamicBlockSync(r, "create")
	h.logAdminMutation(
		r,
		"admin dynamic channel query created",
		"query_id", query.QueryID,
		"enabled", query.Enabled,
		"name", query.Name,
		"group", query.GroupName,
		"groups", query.GroupNames,
		"search_query", query.SearchQuery,
		"search_regex", query.SearchRegex,
		"order_index", query.OrderIndex,
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
	if searchWarning != nil && searchWarning.Truncated {
		h.logAdminMutation(
			r,
			"admin dynamic channel query created with truncated dynamic search",
			"query_id", query.QueryID,
			"search_warning", searchWarning,
		)
	}
	writeJSON(w, http.StatusOK, toDynamicChannelQueryResponse(query, searchWarning))
}

func (h *AdminHandler) handleUpdateDynamicChannelQuery(w http.ResponseWriter, r *http.Request) {
	queryID, err := parsePathInt64(r, "queryID")
	if err != nil {
		http.Error(w, "invalid query id", http.StatusBadRequest)
		return
	}

	var req channels.DynamicChannelQueryUpdate
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	query, err := h.channels.UpdateDynamicChannelQuery(r.Context(), queryID, req)
	if err != nil {
		writeChannelsError(w, err, "update dynamic channel query")
		return
	}
	searchWarning := h.catalogSearchWarning(query.SearchQuery, query.SearchRegex)
	queuedWhileRunning, canceledRunning := h.scheduleDynamicBlockSync(r, "update")
	h.logAdminMutation(
		r,
		"admin dynamic channel query updated",
		"query_id", query.QueryID,
		"enabled", query.Enabled,
		"name", query.Name,
		"group", query.GroupName,
		"groups", query.GroupNames,
		"search_query", query.SearchQuery,
		"search_regex", query.SearchRegex,
		"order_index", query.OrderIndex,
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
	if searchWarning != nil && searchWarning.Truncated {
		h.logAdminMutation(
			r,
			"admin dynamic channel query updated with truncated dynamic search",
			"query_id", query.QueryID,
			"search_warning", searchWarning,
		)
	}
	writeJSON(w, http.StatusOK, toDynamicChannelQueryResponse(query, searchWarning))
}

func (h *AdminHandler) catalogSearchWarning(search string, searchRegex bool) *sqlite.CatalogSearchWarning {
	if h == nil || h.catalog == nil {
		return nil
	}

	provider, ok := h.catalog.(catalogSearchWarningProvider)
	if !ok {
		return nil
	}

	warning, err := provider.CatalogSearchWarningForQuery(search, searchRegex)
	if err != nil {
		h.loggerOrDefault().Warn(
			"catalog search warning check failed",
			"search", search,
			"search_regex", searchRegex,
			"error", err,
		)
		return nil
	}
	return &warning
}

func (h *AdminHandler) handleDeleteDynamicChannelQuery(w http.ResponseWriter, r *http.Request) {
	queryID, err := parsePathInt64(r, "queryID")
	if err != nil {
		http.Error(w, "invalid query id", http.StatusBadRequest)
		return
	}

	if err := h.channels.DeleteDynamicChannelQuery(r.Context(), queryID); err != nil {
		writeChannelsError(w, err, "delete dynamic channel query")
		return
	}
	queuedWhileRunning, canceledRunning := h.scheduleDynamicBlockSync(r, "delete")
	h.logAdminMutation(
		r,
		"admin dynamic channel query deleted",
		"query_id", queryID,
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) handleDynamicGeneratedChannels(w http.ResponseWriter, r *http.Request) {
	queryID, err := parsePathInt64(r, "queryID")
	if err != nil {
		http.Error(w, "invalid query id", http.StatusBadRequest)
		return
	}

	limit, offset, err := parsePaginationQuery(r, defaultChannelsListLimit, maxChannelsListLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rows, total, err := h.channels.ListDynamicGeneratedChannelsPaged(r.Context(), queryID, limit, offset)
	if err != nil {
		writeChannelsError(w, err, "list dynamic generated channels")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channels": rows,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

func (h *AdminHandler) handleReorderDynamicGeneratedChannels(w http.ResponseWriter, r *http.Request) {
	queryID, err := parsePathInt64(r, "queryID")
	if err != nil {
		http.Error(w, "invalid query id", http.StatusBadRequest)
		return
	}

	var req struct {
		ChannelIDs []int64 `json:"channel_ids"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	if err := h.channels.ReorderDynamicGeneratedChannels(r.Context(), queryID, req.ChannelIDs); err != nil {
		writeChannelsError(w, err, "reorder dynamic generated channels")
		return
	}
	h.logAdminMutation(
		r,
		"admin dynamic generated channels reordered",
		"query_id", queryID,
		"channel_count", len(req.ChannelIDs),
		"channel_ids", req.ChannelIDs,
	)
	h.enqueueDVRLineupReload(
		"dynamic_generated_reorder",
		"query_id", queryID,
		"channel_count", len(req.ChannelIDs),
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) scheduleDynamicBlockSync(r *http.Request, reason string) (bool, bool) {
	if h == nil || r == nil {
		return false, false
	}
	queuedWhileRunning, canceledRunning := h.enqueueDynamicBlockSync()
	h.logAdminMutation(
		r,
		"admin dynamic block sync queued",
		"reason", strings.TrimSpace(reason),
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
	return queuedWhileRunning, canceledRunning
}

func (h *AdminHandler) enqueueDynamicBlockSync() (bool, bool) {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()

	select {
	case <-h.closeCh:
		return false, false
	default:
	}

	h.dynamicBlockSyncState.latestVersion++
	currentVersion := h.dynamicBlockSyncState.latestVersion
	if h.dynamicBlockSyncState.running {
		h.dynamicBlockSyncState.hasPending = true
		canceledRunning := false
		if h.dynamicBlockSyncState.runCancel != nil {
			h.dynamicBlockSyncState.runCancel()
			canceledRunning = true
		}
		return true, canceledRunning
	}

	h.dynamicBlockSyncState.running = true
	h.workerWg.Add(1)
	go func() {
		defer h.workerWg.Done()
		h.runDynamicBlockSyncLoop(currentVersion)
	}()
	return false, false
}

func (h *AdminHandler) runDynamicBlockSyncLoop(version uint64) {
	currentVersion := version
	for {
		h.runDynamicBlockSyncOnce(currentVersion)
		nextVersion, ok := h.consumePendingDynamicBlockSync()
		if !ok {
			return
		}
		currentVersion = nextVersion
	}
}

func (h *AdminHandler) runDynamicBlockSyncOnce(version uint64) {
	logger := h.loggerOrDefault()
	timeout := h.dynamicBlockSyncTimeout
	if timeout <= 0 {
		timeout = defaultDynamicSyncTimeout
	}

	startedAt := time.Now()
	logger.Info(
		"admin dynamic block immediate sync started",
		"sync_version", version,
		"timeout", timeout.String(),
	)

	ctx, cancelWorker := h.workerContext()
	ctx, cancelRun := context.WithCancel(ctx)
	h.setDynamicBlockSyncRunCancel(cancelRun)
	defer func() {
		cancelRun()
		cancelWorker()
		h.clearDynamicBlockSyncRunCancel()
	}()

	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	result, err := h.channels.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Info(
				"admin dynamic block immediate sync canceled",
				"sync_version", version,
				"duration_ms", time.Since(startedAt).Milliseconds(),
				"error", err,
			)
			return
		}
		logger.Warn(
			"admin dynamic block immediate sync failed",
			"sync_version", version,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"error", err,
		)
		return
	}

	logger.Info(
		"admin dynamic block immediate sync completed",
		"sync_version", version,
		"duration_ms", time.Since(startedAt).Milliseconds(),
		"queries_processed", result.QueriesProcessed,
		"queries_enabled", result.QueriesEnabled,
		"channels_added", result.ChannelsAdded,
		"channels_updated", result.ChannelsUpdated,
		"channels_retained", result.ChannelsRetained,
		"channels_removed", result.ChannelsRemoved,
		"truncated", result.TruncatedCount,
	)

	changed := result.ChannelsAdded+result.ChannelsUpdated+result.ChannelsRemoved > 0
	if changed {
		h.enqueueDVRLineupReload(
			"dynamic_block_sync",
			"sync_version", version,
			"channels_added", result.ChannelsAdded,
			"channels_updated", result.ChannelsUpdated,
			"channels_removed", result.ChannelsRemoved,
		)
	}
}

func (h *AdminHandler) enqueueDVRLineupReload(reason string, attrs ...any) {
	if h == nil || h.dvr == nil {
		return
	}

	now := time.Now()
	normalizedReason := normalizeDVRLineupReloadReason(reason)

	var (
		prevDueAt          time.Time
		dueAt              time.Time
		coalescedCount     int
		queuedWhileRunning bool
		scheduled          bool
	)

	h.dvrLineupReloadMu.Lock()
	select {
	case <-h.closeCh:
		h.dvrLineupReloadMu.Unlock()
		return
	default:
	}

	h.recordDVRLineupReloadEnqueueLocked(normalizedReason, now)
	state := &h.dvrLineupReloadState
	coalescedCount = state.coalescedCount
	if state.running {
		state.pending = true
		queuedWhileRunning = true
	} else {
		prevDueAt, dueAt = h.rescheduleDVRLineupReloadLocked(now)
		scheduled = true
	}
	h.dvrLineupReloadMu.Unlock()

	logFields := []any{
		"reason", normalizedReason,
		"coalesced_count", coalescedCount,
		"debounce", h.dvrLineupReloadDebounceDuration().String(),
		"max_wait", h.dvrLineupReloadMaxWaitDuration().String(),
		"queued_while_running", queuedWhileRunning,
	}
	if scheduled {
		dueIn := time.Until(dueAt).Milliseconds()
		if dueIn < 0 {
			dueIn = 0
		}
		logFields = append(logFields,
			"due_in_ms", dueIn,
			"due_at", dueAt.UTC().Format(time.RFC3339Nano),
		)
		if !prevDueAt.IsZero() {
			logFields = append(logFields, "previous_due_at", prevDueAt.UTC().Format(time.RFC3339Nano))
		}
	}
	logFields = append(logFields, attrs...)
	h.loggerOrDefault().Info("admin dvr lineup reload queued", logFields...)
}

func (h *AdminHandler) recordDVRLineupReloadEnqueueLocked(reason string, now time.Time) {
	state := &h.dvrLineupReloadState
	if state.firstQueuedAt.IsZero() {
		state.firstQueuedAt = now
	}
	state.lastQueuedAt = now
	state.coalescedCount++
	if state.reasonCounts == nil {
		state.reasonCounts = make(map[string]int)
	}
	state.reasonCounts[reason]++
}

func (h *AdminHandler) rescheduleDVRLineupReloadLocked(now time.Time) (time.Time, time.Time) {
	state := &h.dvrLineupReloadState
	if state.firstQueuedAt.IsZero() {
		state.firstQueuedAt = now
	}

	debounce := h.dvrLineupReloadDebounceDuration()
	maxWait := h.dvrLineupReloadMaxWaitDuration()
	dueAt := now.Add(debounce)
	maxDueAt := state.firstQueuedAt.Add(maxWait)
	if dueAt.After(maxDueAt) {
		dueAt = maxDueAt
	}

	prevDueAt := state.timerDueAt
	if state.timerCancel != nil {
		close(state.timerCancel)
	}
	state.timerSeq++
	seq := state.timerSeq
	state.timerDueAt = dueAt
	cancelCh := make(chan struct{})
	state.timerCancel = cancelCh

	delay := dueAt.Sub(now)
	if delay < 0 {
		delay = 0
	}

	h.workerWg.Add(1)
	go h.waitForDVRLineupReloadTimer(seq, delay, cancelCh)

	return prevDueAt, dueAt
}

func (h *AdminHandler) waitForDVRLineupReloadTimer(seq uint64, delay time.Duration, cancelCh <-chan struct{}) {
	defer h.workerWg.Done()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		h.fireQueuedDVRLineupReload(seq)
	case <-cancelCh:
	case <-h.closeCh:
	}
}

func (h *AdminHandler) fireQueuedDVRLineupReload(seq uint64) {
	batch, ok := h.beginQueuedDVRLineupReloadRun(seq)
	if !ok {
		return
	}

	h.runDVRLineupReloadOnce(batch)
	h.finalizeDVRLineupReloadRun()
}

func (h *AdminHandler) beginQueuedDVRLineupReloadRun(seq uint64) (dvrLineupReloadBatch, bool) {
	now := time.Now()

	h.dvrLineupReloadMu.Lock()
	defer h.dvrLineupReloadMu.Unlock()

	state := &h.dvrLineupReloadState
	if state.timerSeq != seq || state.timerCancel == nil {
		return dvrLineupReloadBatch{}, false
	}

	state.timerCancel = nil
	state.timerDueAt = time.Time{}
	if state.running || state.firstQueuedAt.IsZero() || state.coalescedCount == 0 {
		return dvrLineupReloadBatch{}, false
	}

	state.running = true
	batch := dvrLineupReloadBatch{
		reasonSummary:  formatDVRLineupReloadReasons(state.reasonCounts),
		coalescedCount: state.coalescedCount,
		firstQueuedAt:  state.firstQueuedAt,
		lastQueuedAt:   state.lastQueuedAt,
	}
	if !batch.firstQueuedAt.IsZero() {
		batch.queuedDuration = now.Sub(batch.firstQueuedAt)
		if batch.queuedDuration < 0 {
			batch.queuedDuration = 0
		}
	}
	if !batch.firstQueuedAt.IsZero() && !batch.lastQueuedAt.IsZero() {
		batch.queueWindowSpan = batch.lastQueuedAt.Sub(batch.firstQueuedAt)
		if batch.queueWindowSpan < 0 {
			batch.queueWindowSpan = 0
		}
	}

	state.firstQueuedAt = time.Time{}
	state.lastQueuedAt = time.Time{}
	state.coalescedCount = 0
	state.reasonCounts = nil

	return batch, true
}

func (h *AdminHandler) runDVRLineupReloadOnce(batch dvrLineupReloadBatch) {
	if h == nil || h.dvr == nil {
		return
	}

	timeout := h.dvrLineupReloadTimeoutDuration()

	logger := h.loggerOrDefault()
	logFields := []any{
		"reason_summary", batch.reasonSummary,
		"coalesced_count", batch.coalescedCount,
		"queued_duration_ms", batch.queuedDuration.Milliseconds(),
		"queue_window_ms", batch.queueWindowSpan.Milliseconds(),
		"timeout", timeout.String(),
	}

	logger.Info("admin dvr lineup reload started", logFields...)

	ctx, cancelWorker := h.workerContext()
	defer cancelWorker()
	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	startedAt := time.Now()
	if err := h.dvr.ReloadLineup(ctx); err != nil {
		errorFields := append(append([]any{}, logFields...), "duration_ms", time.Since(startedAt).Milliseconds(), "error", err)
		if errors.Is(err, context.Canceled) {
			logger.Info("admin dvr lineup reload canceled", errorFields...)
			return
		}
		logger.Warn("admin dvr lineup reload failed", errorFields...)
		return
	}

	completedFields := append(append([]any{}, logFields...), "duration_ms", time.Since(startedAt).Milliseconds())
	logger.Info("admin dvr lineup reload completed", completedFields...)
}

func (h *AdminHandler) finalizeDVRLineupReloadRun() {
	if h == nil {
		return
	}

	now := time.Now()
	var (
		scheduled      bool
		coalescedCount int
		prevDueAt      time.Time
		dueAt          time.Time
	)

	h.dvrLineupReloadMu.Lock()
	state := &h.dvrLineupReloadState
	state.running = false

	select {
	case <-h.closeCh:
		state.pending = false
		state.firstQueuedAt = time.Time{}
		state.lastQueuedAt = time.Time{}
		state.coalescedCount = 0
		state.reasonCounts = nil
		h.dvrLineupReloadMu.Unlock()
		return
	default:
	}

	if state.pending && !state.firstQueuedAt.IsZero() {
		state.pending = false
		coalescedCount = state.coalescedCount
		prevDueAt, dueAt = h.rescheduleDVRLineupReloadLocked(now)
		scheduled = true
	} else {
		state.pending = false
	}
	h.dvrLineupReloadMu.Unlock()

	if !scheduled {
		return
	}

	dueIn := time.Until(dueAt).Milliseconds()
	if dueIn < 0 {
		dueIn = 0
	}
	logFields := []any{
		"coalesced_count", coalescedCount,
		"due_in_ms", dueIn,
		"due_at", dueAt.UTC().Format(time.RFC3339Nano),
	}
	if !prevDueAt.IsZero() {
		logFields = append(logFields, "previous_due_at", prevDueAt.UTC().Format(time.RFC3339Nano))
	}
	h.loggerOrDefault().Info("admin dvr lineup reload follow-up queued", logFields...)
}

func (h *AdminHandler) dvrLineupReloadDebounceDuration() time.Duration {
	if h == nil || h.dvrLineupReloadDebounce <= 0 {
		return defaultDVRLineupReloadDebounce
	}
	return h.dvrLineupReloadDebounce
}

func (h *AdminHandler) dvrLineupReloadTimeoutDuration() time.Duration {
	if h == nil || h.dvrLineupReloadTimeout <= 0 {
		return defaultDVRLineupReloadTimeout
	}
	return h.dvrLineupReloadTimeout
}

func (h *AdminHandler) dvrLineupReloadMaxWaitDuration() time.Duration {
	if h == nil || h.dvrLineupReloadMaxWait <= 0 {
		return defaultDVRLineupReloadMaxWait
	}
	return h.dvrLineupReloadMaxWait
}

func normalizeDVRLineupReloadReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "unspecified"
	}
	return reason
}

func formatDVRLineupReloadReasons(reasonCounts map[string]int) string {
	if len(reasonCounts) == 0 {
		return "unspecified(1)"
	}
	reasons := make([]string, 0, len(reasonCounts))
	for reason := range reasonCounts {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)

	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, fmt.Sprintf("%s(%d)", reason, reasonCounts[reason]))
	}
	return strings.Join(parts, ",")
}

func (h *AdminHandler) consumePendingDynamicBlockSync() (uint64, bool) {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()

	h.dynamicBlockSyncState.runCancel = nil
	if h.dynamicBlockSyncState.hasPending {
		h.dynamicBlockSyncState.hasPending = false
		return h.dynamicBlockSyncState.latestVersion, true
	}

	h.dynamicBlockSyncState.running = false
	return 0, false
}

func (h *AdminHandler) setDynamicBlockSyncRunCancel(cancel context.CancelFunc) {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()
	h.dynamicBlockSyncState.runCancel = cancel
}

func (h *AdminHandler) clearDynamicBlockSyncRunCancel() {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()
	h.dynamicBlockSyncState.runCancel = nil
}

func (h *AdminHandler) handleSources(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	limit, offset, err := parsePaginationQuery(r, defaultChannelSourcesListLimit, maxChannelSourcesListLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sources, total, err := h.channels.ListSourcesPaged(r.Context(), channelID, false, limit, offset)
	if err != nil {
		writeChannelsError(w, err, "list sources")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sources": sources,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func (h *AdminHandler) handleAddSource(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	var req struct {
		ItemKey           string `json:"item_key"`
		AllowCrossChannel bool   `json:"allow_cross_channel"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	source, err := h.channels.AddSource(r.Context(), channelID, req.ItemKey, req.AllowCrossChannel)
	if err != nil {
		writeChannelsError(w, err, "add source")
		return
	}

	h.logAdminMutation(
		r,
		"admin source added",
		"channel_id", channelID,
		"source_id", source.SourceID,
		"item_key", source.ItemKey,
		"allow_cross_channel", req.AllowCrossChannel,
	)
	writeJSON(w, http.StatusOK, source)
}

func (h *AdminHandler) handleReorderSources(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	var req struct {
		SourceIDs []int64 `json:"source_ids"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	if err := h.channels.ReorderSources(r.Context(), channelID, req.SourceIDs); err != nil {
		writeChannelsError(w, err, "reorder sources")
		return
	}
	h.logAdminMutation(
		r,
		"admin sources reordered",
		"channel_id", channelID,
		"source_count", len(req.SourceIDs),
		"source_ids", req.SourceIDs,
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) handleUpdateSource(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}
	sourceID, err := parsePathInt64(r, "sourceID")
	if err != nil {
		http.Error(w, "invalid source id", http.StatusBadRequest)
		return
	}

	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	source, err := h.channels.UpdateSource(r.Context(), channelID, sourceID, req.Enabled)
	if err != nil {
		writeChannelsError(w, err, "update source")
		return
	}
	if req.Enabled != nil && !*req.Enabled {
		h.triggerSourceRecoveryForActiveSourceMutation(r, channelID, sourceID, "admin_source_disabled")
	}

	h.logAdminMutation(
		r,
		"admin source updated",
		"channel_id", channelID,
		"source_id", source.SourceID,
		"enabled", source.Enabled,
	)
	writeJSON(w, http.StatusOK, source)
}

func (h *AdminHandler) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}
	sourceID, err := parsePathInt64(r, "sourceID")
	if err != nil {
		http.Error(w, "invalid source id", http.StatusBadRequest)
		return
	}

	if err := h.channels.DeleteSource(r.Context(), channelID, sourceID); err != nil {
		writeChannelsError(w, err, "delete source")
		return
	}
	// Delete occurs before recovery trigger intentionally: recovery is driven
	// by in-memory active session state only and does not re-read store rows.
	h.triggerSourceRecoveryForActiveSourceMutation(r, channelID, sourceID, "admin_source_deleted")

	h.logAdminMutation(
		r,
		"admin source deleted",
		"channel_id", channelID,
		"source_id", sourceID,
	)
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) triggerSourceRecoveryForActiveSourceMutation(r *http.Request, channelID, sourceID int64, reason string) {
	if h == nil || r == nil || channelID <= 0 {
		return
	}
	if h.tunerStatus == nil {
		return
	}
	trigger, ok := h.tunerStatus.(TunerRecoveryTrigger)
	if !ok {
		return
	}
	if !h.isActiveSourceForChannel(channelID, sourceID) {
		return
	}

	recoveryReason := strings.TrimSpace(reason)
	if recoveryReason == "" {
		recoveryReason = "admin_source_mutation"
	}

	if err := trigger.TriggerSessionRecovery(channelID, recoveryReason); err != nil {
		switch {
		case errors.Is(err, stream.ErrSessionNotFound), errors.Is(err, stream.ErrSessionRecoveryAlreadyPending):
			h.logAdminMutation(
				r,
				"admin source mutation recovery skipped",
				"channel_id", channelID,
				"source_id", sourceID,
				"recovery_reason", recoveryReason,
				"skip_reason", err.Error(),
			)
			return
		case isInputError(err):
			h.logAdminMutation(
				r,
				"admin source mutation recovery request rejected",
				"channel_id", channelID,
				"source_id", sourceID,
				"recovery_reason", recoveryReason,
				"error", err.Error(),
			)
			return
		default:
			h.logAdminMutation(
				r,
				"admin source mutation recovery request failed",
				"channel_id", channelID,
				"source_id", sourceID,
				"recovery_reason", recoveryReason,
				"error", err.Error(),
			)
			return
		}
	}

	h.logAdminMutation(
		r,
		"admin source mutation recovery requested",
		"channel_id", channelID,
		"source_id", sourceID,
		"recovery_reason", recoveryReason,
	)
}

func (h *AdminHandler) triggerChannelRecoveryForActiveChannelMutation(r *http.Request, channelID int64, reason string) {
	if h == nil || r == nil || channelID <= 0 {
		return
	}
	if h.tunerStatus == nil {
		return
	}
	trigger, ok := h.tunerStatus.(TunerRecoveryTrigger)
	if !ok {
		return
	}
	if !h.isActiveSourceForChannel(channelID, 0) {
		return
	}

	recoveryReason := strings.TrimSpace(reason)
	if recoveryReason == "" {
		recoveryReason = "admin_channel_mutation"
	}

	if err := trigger.TriggerSessionRecovery(channelID, recoveryReason); err != nil {
		switch {
		case errors.Is(err, stream.ErrSessionNotFound), errors.Is(err, stream.ErrSessionRecoveryAlreadyPending):
			h.logAdminMutation(
				r,
				"admin channel mutation recovery skipped",
				"channel_id", channelID,
				"recovery_reason", recoveryReason,
				"skip_reason", err.Error(),
			)
			return
		case isInputError(err):
			h.logAdminMutation(
				r,
				"admin channel mutation recovery request rejected",
				"channel_id", channelID,
				"recovery_reason", recoveryReason,
				"error", err.Error(),
			)
			return
		default:
			h.logAdminMutation(
				r,
				"admin channel mutation recovery request failed",
				"channel_id", channelID,
				"recovery_reason", recoveryReason,
				"error", err.Error(),
			)
			return
		}
	}

	h.logAdminMutation(
		r,
		"admin channel mutation recovery requested",
		"channel_id", channelID,
		"recovery_reason", recoveryReason,
	)
}

func (h *AdminHandler) isActiveSourceForChannel(channelID, sourceID int64) bool {
	if h == nil || channelID <= 0 || h.tunerStatus == nil {
		return false
	}
	for _, tuner := range h.tunerStatus.TunerStatusSnapshot().Tuners {
		if tuner.ChannelID != channelID {
			continue
		}
		if sourceID <= 0 || tuner.SourceID == sourceID {
			return true
		}
	}
	return false
}

func (h *AdminHandler) handleClearSourceHealth(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	cleared, err := h.channels.ClearSourceHealth(r.Context(), channelID)
	if err != nil {
		writeChannelsError(w, err, "clear source health")
		return
	}
	if h.sourceHealthClearRuntime != nil {
		if err := h.sourceHealthClearRuntime.ClearSourceHealth(channelID); err != nil {
			h.logAdminMutation(
				r,
				"admin source health clear runtime state update failed",
				"channel_id", channelID,
				"error", err,
			)
		}
	}
	h.logAdminMutation(
		r,
		"admin source health cleared",
		"channel_id", channelID,
		"cleared", cleared,
	)
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}

func (h *AdminHandler) handleClearAllSourceHealth(w http.ResponseWriter, r *http.Request) {
	cleared, err := h.channels.ClearAllSourceHealth(r.Context())
	if err != nil {
		writeChannelsError(w, err, "clear source health")
		return
	}
	if h.sourceHealthClearRuntime != nil {
		if err := h.sourceHealthClearRuntime.ClearAllSourceHealth(); err != nil {
			h.logAdminMutation(
				r,
				"admin all source health clear runtime state update failed",
				"cleared", cleared,
				"error", err,
			)
		}
	}
	h.logAdminMutation(
		r,
		"admin all source health cleared",
		"cleared", cleared,
	)
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}

func (h *AdminHandler) handleDuplicateSuggestions(w http.ResponseWriter, r *http.Request) {
	min := parseInt(r.URL.Query().Get("min"), 2)
	if min < 2 {
		min = 2
	}
	if min > 100 {
		min = 100
	}
	limit, offset, err := parsePaginationQuery(r, defaultDuplicateSuggestionsLimit, maxDuplicateSuggestionsLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	searchQuery := strings.TrimSpace(r.URL.Query().Get("q"))
	if searchQuery == "" {
		// Backward-compatible fallback for older clients.
		searchQuery = strings.TrimSpace(r.URL.Query().Get("tvg_id"))
	}

	groups, total, err := h.channels.DuplicateSuggestions(r.Context(), min, searchQuery, limit, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("list duplicate suggestions: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"min":    min,
		"q":      searchQuery,
		"groups": groups,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *AdminHandler) handleTunerStatus(w http.ResponseWriter, r *http.Request) {
	if h.tunerStatus == nil {
		http.Error(w, "tuner status is not configured", http.StatusServiceUnavailable)
		return
	}

	resolveIP, err := parseOptionalBoolQueryParam(r, "resolve_ip", false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	snapshot := h.tunerStatus.TunerStatusSnapshot()
	if resolveIP {
		resolveCtx, cancel := context.WithTimeout(r.Context(), h.resolveClientHostsTimeoutBudget())
		var resolveStats resolveClientHostResolutionStats
		snapshot, resolveStats = h.resolveClientHostsInSnapshot(resolveCtx, snapshot)
		cancel()
		if h.logger != nil && h.logger.Enabled(r.Context(), slog.LevelDebug) {
			h.logger.Debug(
				"admin tuner resolve_ip completed",
				"cache_hits", resolveStats.cacheHits,
				"cache_negative_hits", resolveStats.cacheNegativeHits,
				"cache_misses", resolveStats.cacheMisses,
				"cache_hit_rate", resolveStats.cacheHitRate(),
				"memoized_hits", resolveStats.memoizedHits,
				"memoized_empty_hits", resolveStats.memoizedEmptyHits,
				"lookup_calls", resolveStats.lookupCalls,
				"lookup_errors", resolveStats.lookupErrors,
				"lookup_empty_results", resolveStats.lookupEmptyResults,
			)
		}
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *AdminHandler) handleTriggerTunerRecovery(w http.ResponseWriter, r *http.Request) {
	if h.tunerStatus == nil {
		http.Error(w, "tuner status is not configured", http.StatusServiceUnavailable)
		return
	}
	trigger, ok := h.tunerStatus.(TunerRecoveryTrigger)
	if !ok {
		http.Error(w, "tuner recovery is not supported", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		ChannelID int64  `json:"channel_id"`
		Reason    string `json:"reason"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	if req.ChannelID <= 0 {
		http.Error(w, "channel_id must be greater than zero", http.StatusBadRequest)
		return
	}

	recoveryReason := strings.TrimSpace(req.Reason)
	if recoveryReason == "" {
		recoveryReason = "ui_manual_trigger"
	}
	if err := trigger.TriggerSessionRecovery(req.ChannelID, recoveryReason); err != nil {
		switch {
		case errors.Is(err, stream.ErrSessionNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, stream.ErrSessionRecoveryAlreadyPending):
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if isInputError(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("trigger tuner recovery: %v", err), http.StatusInternalServerError)
		return
	}

	h.logAdminMutation(
		r,
		"admin tuner recovery triggered",
		"channel_id", req.ChannelID,
		"recovery_reason", recoveryReason,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id":      req.ChannelID,
		"recovery_reason": recoveryReason,
		"accepted":        true,
	})
}

func writeChannelsError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, channels.ErrItemNotFound), errors.Is(err, channels.ErrChannelNotFound), errors.Is(err, channels.ErrSourceNotFound), errors.Is(err, channels.ErrDynamicQueryNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, channels.ErrAssociationMismatch):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if strings.Contains(strings.ToLower(err.Error()), "range exhausted") {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	if isInputError(err) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Error(w, fmt.Sprintf("%s: %v", action, err), http.StatusInternalServerError)
}

func isInputError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return false
	}

	inputFragments := []string{
		"required",
		"must be",
		"invalid",
		"at least one field",
		"count mismatch",
		"count exceeds",
		"contains duplicate",
		"missing id",
	}
	for _, fragment := range inputFragments {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func parsePathInt64(r *http.Request, field string) (int64, error) {
	value := strings.TrimSpace(r.PathValue(field))
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid %s", field)
	}
	return id, nil
}

func (h *AdminHandler) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if r == nil {
		return fmt.Errorf("request is required")
	}
	if w == nil {
		return fmt.Errorf("response writer is required")
	}
	limit := defaultAdminJSONBodyLimitBytes
	if h != nil && h.adminJSONBodyLimitBytes > 0 {
		limit = h.adminJSONBodyLimitBytes
	}
	body := http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("invalid json: trailing data")
	}
	return nil
}

func writeJSONDecodeError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		http.Error(
			w,
			fmt.Sprintf("request body too large: limit %d bytes", maxBytesErr.Limit),
			http.StatusRequestEntityTooLarge,
		)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func parseInt(value string, def int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return def
	}
	out, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	return out
}

func parsePaginationQuery(r *http.Request, defaultLimit, maxLimit int) (int, int, error) {
	if defaultLimit < 1 {
		defaultLimit = 1
	}

	limit, err := parsePaginationIntParam(r, "limit", defaultLimit)
	if err != nil {
		return 0, 0, err
	}
	if limit < 1 {
		limit = defaultLimit
	}
	if maxLimit > 0 && limit > maxLimit {
		limit = maxLimit
	}

	offset, err := parsePaginationIntParam(r, "offset", 0)
	if err != nil {
		return 0, 0, err
	}

	return limit, offset, nil
}

func parsePaginationIntParam(r *http.Request, name string, defaultValue int) (int, error) {
	if r == nil {
		return 0, fmt.Errorf("request is required")
	}
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return defaultValue, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s must be zero or greater", name)
	}
	return parsed, nil
}

func parseOptionalBoolQueryParam(r *http.Request, name string, defaultValue bool) (bool, error) {
	if r == nil {
		return false, fmt.Errorf("request is required")
	}
	value := strings.ToLower(strings.TrimSpace(r.URL.Query().Get(name)))
	if value == "" {
		return defaultValue, nil
	}

	switch value {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean", name)
	}
}

func (h *AdminHandler) resolveClientHostsTimeoutBudget() time.Duration {
	if h == nil || h.resolveClientHostsTimeout <= 0 {
		return defaultResolveClientHostsTotalTimeout
	}
	return h.resolveClientHostsTimeout
}

func (h *AdminHandler) resolveClientHostsInSnapshot(
	ctx context.Context,
	snapshot stream.TunerStatusSnapshot,
) (stream.TunerStatusSnapshot, resolveClientHostResolutionStats) {
	if h == nil {
		return snapshot, resolveClientHostResolutionStats{}
	}
	return resolveClientHostsInSnapshotWithCache(
		ctx,
		snapshot,
		h.lookupAddr,
		h.loadCachedResolvedClientHost,
		h.storeCachedResolvedClientHost,
	)
}

func (h *AdminHandler) loadCachedResolvedClientHost(ip string) (string, bool) {
	if h == nil || ip == "" || h.resolveClientHostCacheTTL <= 0 {
		return "", false
	}

	now := h.resolveClientHostCacheNow()

	h.resolveClientHostCacheMu.Lock()
	defer h.resolveClientHostCacheMu.Unlock()
	h.sweepExpiredResolvedClientHostEntriesLocked(now, false)

	entry, ok := h.resolveClientHostCache[ip]
	if !ok {
		return "", false
	}
	if !entry.expiresAt.After(now) {
		delete(h.resolveClientHostCache, ip)
		return "", false
	}
	return entry.host, true
}

func (h *AdminHandler) storeCachedResolvedClientHost(ip, host string) {
	if h == nil || ip == "" {
		return
	}

	ttl := h.resolveClientHostCacheTTL
	if host == "" {
		ttl = h.resolveClientHostCacheNegativeTTL
	}
	if ttl <= 0 {
		return
	}
	now := h.resolveClientHostCacheNow()
	expiresAt := now.Add(ttl)

	h.resolveClientHostCacheMu.Lock()
	defer h.resolveClientHostCacheMu.Unlock()
	if h.resolveClientHostCache == nil {
		h.resolveClientHostCache = make(map[string]resolveClientHostCacheEntry)
	}
	h.sweepExpiredResolvedClientHostEntriesLocked(now, false)
	h.evictResolvedClientHostCacheToCapacityLocked(ip, now)
	h.resolveClientHostCache[ip] = resolveClientHostCacheEntry{host: host, expiresAt: expiresAt}
	heap.Push(
		&h.resolveClientHostCacheExpiryHeap,
		resolveClientHostCacheExpiryHeapItem{ip: ip, expiresAt: expiresAt},
	)
	h.compactResolvedClientHostCacheExpiryHeapLocked()
}

func (h *AdminHandler) sweepExpiredResolvedClientHostEntriesLocked(now time.Time, force bool) {
	if h == nil || h.resolveClientHostCache == nil {
		return
	}
	if !force {
		interval := h.resolveClientHostCacheSweepInterval
		if interval > 0 &&
			!h.resolveClientHostCacheLastSweep.IsZero() &&
			now.Sub(h.resolveClientHostCacheLastSweep) < interval {
			return
		}
	}
	for cachedIP, entry := range h.resolveClientHostCache {
		if !entry.expiresAt.After(now) {
			delete(h.resolveClientHostCache, cachedIP)
		}
	}
	h.resolveClientHostCacheLastSweep = now
}

func (h *AdminHandler) evictResolvedClientHostCacheToCapacityLocked(ip string, now time.Time) {
	if h == nil || h.resolveClientHostCache == nil {
		return
	}
	maxEntries := h.resolveClientHostCacheMaxEntries
	if maxEntries <= 0 {
		return
	}
	if _, exists := h.resolveClientHostCache[ip]; exists {
		return
	}
	if len(h.resolveClientHostCache) < maxEntries {
		return
	}
	h.sweepExpiredResolvedClientHostEntriesLocked(now, true)
	if len(h.resolveClientHostCache) < maxEntries {
		return
	}

	for h.resolveClientHostCacheExpiryHeap.Len() > 0 {
		oldest := heap.Pop(&h.resolveClientHostCacheExpiryHeap).(resolveClientHostCacheExpiryHeapItem)
		entry, ok := h.resolveClientHostCache[oldest.ip]
		if !ok || !entry.expiresAt.Equal(oldest.expiresAt) {
			continue
		}
		delete(h.resolveClientHostCache, oldest.ip)
		h.compactResolvedClientHostCacheExpiryHeapLocked()
		return
	}

	// Fallback for empty/stale heap state; this keeps eviction resilient even
	// if the heap was intentionally compacted or reset.
	var oldestIP string
	var oldestExpiry time.Time
	for cachedIP, entry := range h.resolveClientHostCache {
		if oldestIP == "" || entry.expiresAt.Before(oldestExpiry) {
			oldestIP = cachedIP
			oldestExpiry = entry.expiresAt
		}
	}
	if oldestIP != "" {
		delete(h.resolveClientHostCache, oldestIP)
	}
	h.compactResolvedClientHostCacheExpiryHeapLocked()
}

func (h *AdminHandler) compactResolvedClientHostCacheExpiryHeapLocked() {
	if h == nil {
		return
	}
	cacheLen := len(h.resolveClientHostCache)
	if cacheLen == 0 {
		h.resolveClientHostCacheExpiryHeap = nil
		return
	}
	// Avoid unbounded stale-heap growth when hot IPs are refreshed repeatedly.
	const staleHeapMultiplier = 4
	if len(h.resolveClientHostCacheExpiryHeap) <= cacheLen*staleHeapMultiplier {
		return
	}
	rebuilt := make(resolveClientHostCacheExpiryMinHeap, 0, cacheLen)
	for cachedIP, entry := range h.resolveClientHostCache {
		rebuilt = append(rebuilt, resolveClientHostCacheExpiryHeapItem{
			ip:        cachedIP,
			expiresAt: entry.expiresAt,
		})
	}
	heap.Init(&rebuilt)
	h.resolveClientHostCacheExpiryHeap = rebuilt
}

func resolveClientHostsInSnapshotWithCache(
	ctx context.Context,
	snapshot stream.TunerStatusSnapshot,
	lookupAddr func(ctx context.Context, addr string) ([]string, error),
	loadCachedHost func(ip string) (string, bool),
	storeCachedHost func(ip, host string),
) (stream.TunerStatusSnapshot, resolveClientHostResolutionStats) {
	stats := resolveClientHostResolutionStats{}
	if lookupAddr == nil {
		return snapshot, stats
	}

	if len(snapshot.ClientStreams) == 0 && len(snapshot.SessionHistory) == 0 {
		return snapshot, stats
	}

	// Clone slices so we do not mutate shared backing arrays from providers.
	if len(snapshot.ClientStreams) > 0 {
		snapshot.ClientStreams = append([]stream.ClientStreamStatus(nil), snapshot.ClientStreams...)
	}
	if len(snapshot.SessionHistory) > 0 {
		snapshot.SessionHistory = append([]stream.SharedSessionHistory(nil), snapshot.SessionHistory...)
		for i := range snapshot.SessionHistory {
			if len(snapshot.SessionHistory[i].Subscribers) == 0 {
				continue
			}
			snapshot.SessionHistory[i].Subscribers = append(
				[]stream.SharedSessionSubscriberHistory(nil),
				snapshot.SessionHistory[i].Subscribers...,
			)
		}
	}

	resolvedByIP := make(map[string]string, len(snapshot.ClientStreams))
	for i := range snapshot.ClientStreams {
		if ctx != nil && ctx.Err() != nil {
			return snapshot, stats
		}
		if host := resolveClientHost(
			ctx,
			snapshot.ClientStreams[i].ClientAddr,
			resolvedByIP,
			lookupAddr,
			loadCachedHost,
			storeCachedHost,
			&stats,
		); host != "" {
			snapshot.ClientStreams[i].ClientHost = host
		}
	}
	for i := range snapshot.SessionHistory {
		if ctx != nil && ctx.Err() != nil {
			return snapshot, stats
		}
		for j := range snapshot.SessionHistory[i].Subscribers {
			if ctx != nil && ctx.Err() != nil {
				return snapshot, stats
			}
			if host := resolveClientHost(
				ctx,
				snapshot.SessionHistory[i].Subscribers[j].ClientAddr,
				resolvedByIP,
				lookupAddr,
				loadCachedHost,
				storeCachedHost,
				&stats,
			); host != "" {
				snapshot.SessionHistory[i].Subscribers[j].ClientHost = host
			}
		}
	}

	return snapshot, stats
}

func resolveClientHost(
	ctx context.Context,
	clientAddr string,
	resolvedByIP map[string]string,
	lookupAddr func(ctx context.Context, addr string) ([]string, error),
	loadCachedHost func(ip string) (string, bool),
	storeCachedHost func(ip, host string),
	stats *resolveClientHostResolutionStats,
) string {
	if ctx == nil {
		ctx = context.Background()
	}

	ip, ok := parseClientAddressIP(clientAddr)
	if !ok {
		return ""
	}
	if resolved, seen := resolvedByIP[ip]; seen {
		if stats != nil {
			stats.memoizedHits++
			if resolved == "" {
				stats.memoizedEmptyHits++
			}
		}
		return resolved
	}
	if loadCachedHost != nil {
		if cachedHost, ok := loadCachedHost(ip); ok {
			resolvedByIP[ip] = cachedHost
			if stats != nil {
				if cachedHost == "" {
					stats.cacheNegativeHits++
				} else {
					stats.cacheHits++
				}
			}
			return cachedHost
		}
		if stats != nil {
			stats.cacheMisses++
		}
	}
	if stats != nil {
		stats.lookupCalls++
	}

	lookupCtx, cancel := context.WithTimeout(ctx, defaultResolveClientHostLookupTimeout)
	defer cancel()

	names, err := lookupAddr(lookupCtx, ip)
	if err != nil {
		if stats != nil {
			stats.lookupErrors++
		}
		resolvedByIP[ip] = ""
		if storeCachedHost != nil {
			storeCachedHost(ip, "")
		}
		return ""
	}
	host := firstResolvedHostname(names)
	if stats != nil && host == "" {
		stats.lookupEmptyResults++
	}
	resolvedByIP[ip] = host
	if storeCachedHost != nil {
		storeCachedHost(ip, host)
	}
	return host
}

func parseClientAddressIP(clientAddr string) (string, bool) {
	addr := strings.TrimSpace(clientAddr)
	if addr == "" {
		return "", false
	}

	host := addr
	if splitHost, _, err := net.SplitHostPort(addr); err == nil {
		host = splitHost
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if zoneSep := strings.Index(host, "%"); zoneSep >= 0 {
		host = host[:zoneSep]
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

func firstResolvedHostname(names []string) string {
	for _, name := range names {
		trimmed := strings.TrimSuffix(strings.TrimSpace(name), ".")
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func (h *AdminHandler) loggerOrDefault() *slog.Logger {
	if h == nil || h.logger == nil {
		return slog.Default()
	}
	return h.logger
}

func (h *AdminHandler) logAdminMutation(r *http.Request, event string, fields ...any) {
	logger := h.loggerOrDefault()
	sanitizedFields := sanitizeAdminMutationFields(fields)
	args := make([]any, 0, len(fields)+6)
	args = append(
		args,
		"remote_addr", strings.TrimSpace(r.RemoteAddr),
		"method", r.Method,
		"path", r.URL.Path,
	)
	args = append(args, sanitizedFields...)
	logger.Info(event, args...)
}

func sanitizeAdminMutationFields(fields []any) []any {
	if len(fields) == 0 {
		return nil
	}

	sanitized := append([]any(nil), fields...)
	for i := 0; i+1 < len(sanitized); i += 2 {
		key, ok := sanitized[i].(string)
		if !ok || !isAdminMutationURLField(key) {
			continue
		}
		raw, ok := sanitized[i+1].(string)
		if !ok {
			continue
		}
		sanitized[i+1] = stream.SanitizeURLForLog(raw)
	}
	return sanitized
}

func isAdminMutationURLField(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return key == "url" || strings.HasSuffix(key, "_url")
}
