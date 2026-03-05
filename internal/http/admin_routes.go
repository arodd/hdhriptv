package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/http/middleware"
	"github.com/arodd/hdhriptv/internal/logging"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
	"github.com/arodd/hdhriptv/internal/stream"
	"github.com/arodd/hdhriptv/internal/ui"
	appversion "github.com/arodd/hdhriptv/internal/version"
)

const defaultAdminJSONBodyLimitBytes int64 = 1 << 20
const defaultDynamicSyncTimeout = 30 * time.Second
const defaultDVRSyncTimeout = 2 * time.Minute
const defaultDVRMutationTimeout = 30 * time.Second
const defaultAutomationMutationTimeout = 30 * time.Second
const defaultDVRLineupReloadDebounce = 20 * time.Second
const defaultDVRLineupReloadMaxWait = 5 * time.Minute
const defaultDVRLineupReloadTimeout = 30 * time.Second
const defaultResolveClientHostLookupTimeout = 2 * time.Second
const defaultResolveClientHostsTotalTimeout = 8 * time.Second
const defaultResolveClientHostSummaryLogInterval = 30 * time.Minute
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
const immediateDynamicSyncCatalogPageSize = 512

// CatalogStore captures read APIs required by the admin HTTP layer.
type CatalogStore interface {
	GetGroups(ctx context.Context) ([]playlist.Group, error)
	ListGroupsPaged(ctx context.Context, limit, offset int, includeCounts bool) ([]playlist.Group, int, error)
	ListItems(ctx context.Context, q playlist.Query) ([]playlist.Item, int, error)
	ListCatalogItems(ctx context.Context, q playlist.Query) ([]playlist.Item, int, error)
	CatalogSearchWarningForQuery(search string, searchRegex bool) (sqlite.CatalogSearchWarning, error)
	ListActiveItemKeysByCatalogFilter(ctx context.Context, groupNames []string, searchQuery string, searchRegex bool) ([]string, error)
}

type sourceScopedCatalogStore interface {
	ListActiveItemKeysByCatalogFilterBySourceIDs(ctx context.Context, sourceIDs []int64, groupNames []string, searchQuery string, searchRegex bool) ([]string, error)
}

type sourceScopedCatalogGroupsPager interface {
	ListGroupsPagedBySourceIDs(ctx context.Context, sourceIDs []int64, limit, offset int, includeCounts bool) ([]playlist.Group, int, error)
}

type catalogSearchWarningProvider interface {
	CatalogSearchWarningForQuery(search string, searchRegex bool) (sqlite.CatalogSearchWarning, error)
}

type sourceScopedDynamicChannelBlocksCapability interface {
	SupportsSourceScopedDynamicChannelBlocks() bool
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

// PlaylistSourceRuntime applies playlist-source mutations to in-memory stream
// runtime state (for example virtual tuner pools) without process restart.
type PlaylistSourceRuntime interface {
	ReloadPlaylistSources(ctx context.Context) error
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
	playlistSourceRuntime               PlaylistSourceRuntime
	dvr                                 DVRService
	dvrScheduler                        DVRScheduler
	uiVersion                           string
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
	resolveClientHostSummaryTracker     *logging.PeriodicStatsWindow[resolveClientHostResolutionStats]
	resolveClientHostSummaryNow         func() time.Time
	resolveClientHostSummaryLogInterval time.Duration
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
		uiVersion:                           appversion.Current().Version,
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
		resolveClientHostSummaryTracker: logging.NewPeriodicStatsWindow(
			false,
			func(dst *resolveClientHostResolutionStats, sample resolveClientHostResolutionStats) {
				if dst == nil {
					return
				}
				dst.add(sample)
			},
		),
		resolveClientHostSummaryNow:         time.Now,
		resolveClientHostSummaryLogInterval: defaultResolveClientHostSummaryLogInterval,
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

func (h *AdminHandler) SetPlaylistSourceRuntime(runtime PlaylistSourceRuntime) {
	if h == nil {
		return
	}
	h.playlistSourceRuntime = runtime
}

func (h *AdminHandler) SetDVRService(service DVRService) {
	if h == nil {
		return
	}
	h.dvr = service
}

func (h *AdminHandler) SetUIVersion(version string) {
	if h == nil {
		return
	}
	h.uiVersion = strings.TrimSpace(version)
	if h.uiVersion == "" {
		h.uiVersion = appversion.Current().Version
	}
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
	if h.resolveClientHostSummaryTracker != nil {
		h.resolveClientHostSummaryTracker.Reset()
	}

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
		mux.Handle("GET /api/admin/playlist-sources", auth(http.HandlerFunc(h.handleListPlaylistSources)))
		mux.Handle("POST /api/admin/playlist-sources", auth(http.HandlerFunc(h.handleCreatePlaylistSource)))
		mux.Handle("GET /api/admin/playlist-sources/{sourceID}", auth(http.HandlerFunc(h.handleGetPlaylistSource)))
		mux.Handle("PUT /api/admin/playlist-sources/{sourceID}", auth(http.HandlerFunc(h.handleUpdatePlaylistSource)))
		mux.Handle("DELETE /api/admin/playlist-sources/{sourceID}", auth(http.HandlerFunc(h.handleDeletePlaylistSource)))
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
	payload := make(map[string]any, len(data)+1)
	for key, value := range data {
		payload[key] = value
	}
	if _, ok := payload["AppTitle"]; !ok {
		payload["AppTitle"] = h.uiBrandTitle()
	}
	if _, ok := payload["NavShowDVR"]; !ok {
		payload["NavShowDVR"] = h != nil && h.dvr != nil
	}
	if _, ok := payload["NavShowAutomation"]; !ok {
		payload["NavShowAutomation"] = h != nil && h.automation != nil
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.templates.ExecuteTemplate(w, name, payload); err != nil {
		http.Error(w, fmt.Sprintf("render template: %v", err), http.StatusInternalServerError)
	}
}

func (h *AdminHandler) uiVersionLabel() string {
	if h == nil {
		return appversion.Current().Version
	}
	value := strings.TrimSpace(h.uiVersion)
	if value == "" {
		return appversion.Current().Version
	}
	return value
}

func (h *AdminHandler) uiBrandTitle() string {
	return fmt.Sprintf("HDHR IPTV Admin - %s", h.uiVersionLabel())
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
