# Admin Web UI Components

The admin UI is a set of server-rendered HTML pages that provide full management of HDHR IPTV's channels, sources, tuners, DVR integration, and automation jobs. All pages are protected by HTTP Basic Auth and served under the `/ui/` prefix.

## Technology Stack

| Layer | Choice |
|---|---|
| Templating | Go `html/template` with `embed.FS` |
| Asset delivery | Templates embedded at compile time via `//go:embed templates/*.html` (`internal/ui/templates.go`) |
| CSS | Inline `<style>` blocks per page; CSS custom properties for a shared palette (`--brand`, `--border`, `--muted`, etc.) |
| JavaScript | Vanilla JS in inline `<script>` blocks; no framework, no build step, no bundler |
| HTTP client | `fetch()` with JSON Content-Type headers |
| State management | Per-page `const state = { ... }` object, mutated in place, re-rendered by calling render functions |
| Persistence | `localStorage` for the DVR mapping visibility toggle on the Channels page |
| Real-time updates | `setInterval` polling (3s on Tuners, 2s on Automation when jobs are running) |

There are no WebSocket connections, no shared CSS/JS files, and no third-party dependencies.

## Template Architecture

**Source:** `internal/ui/templates.go` — a small Go file that embeds templates via `embed.FS`.

```go
//go:embed templates/*.html
var templateFS embed.FS

func ParseTemplates() (*template.Template, error) {
    return template.ParseFS(templateFS, "templates/*.html")
}
```

All 9 templates are parsed into a single `*template.Template` at server startup. Each page is a **standalone, self-contained HTML file** — there is no shared layout, no partials, and no template inheritance. The navigation bar (`<header class="topbar">`) is duplicated in every template with the appropriate `class="active"` link set per page.

Template data is minimal. The Go handler passes a `map[string]any` with:
- `Title` — page `<title>` (every page)
- `ChannelID` — numeric channel ID (channel_detail.html only)
- `QueryID` — numeric dynamic query ID (dynamic_channel_detail.html only)

All other data is loaded client-side via `fetch()` calls to `/api/*` endpoints.

**Files:**

| Template file | Route | Handler |
|---|---|---|
| *(none — redirect only)* | `GET /ui/` | `handleUIRoot` — 302 redirect to `/ui/catalog` |
| `catalog.html` | `GET /ui/catalog` | `handleUICatalog` |
| `channels.html` | `GET /ui/channels` | `handleUIChannels` |
| `channel_detail.html` | `GET /ui/channels/{channelID}` | `handleUIChannelDetail` |
| `dynamic_channel_detail.html` | `GET /ui/dynamic-channels/{queryID}` | `handleUIDynamicChannelDetail` |
| `merge.html` | `GET /ui/merge` | `handleUIMerge` |
| `tuners.html` | `GET /ui/tuners` | `handleUITuners` |
| `dvr.html` | `GET /ui/dvr` | `handleUIDVR` (conditional) |
| `automation.html` | `GET /ui/automation` | `handleUIAutomation` (conditional) |

`index.html` exists in the embedded template FS but is **not rendered by any current route handler**. `GET /ui/` is handled by `handleUIRoot`, which issues a bare `http.Redirect` to `/ui/catalog` (302 Found) without executing any template.

Route registration happens in `admin_routes.go:259-274`.

## Page-by-Page Guide

### Catalog (`/ui/catalog`)

**Template:** `catalog.html` — Two-column grid layout (groups sidebar + catalog panel).

**Purpose:** Browse the imported IPTV playlist catalog, search/filter items, and create channels or add sources from catalog entries.

**Layout:**
- **Left sidebar** — "Groups" panel listing all playlist groups with item counts. Clicking a group toggles it as a multi-select filter (rendered as removable chips above the item list). An "All groups" button clears the filter.
- **Main panel** — "Catalog" panel with search bar, catalog group chip picker, and paginated item list.

**Key interactions:**
- **Search** — Text input with optional "Regex mode" checkbox. Token mode supports `|`/`OR` disjuncts and `-term`/`!term` exclusions. Regex mode treats the full query as a case-insensitive regex pattern.
- **Multi-group filtering** — A `<datalist>`-backed text input lets users add group filter chips. The sidebar group buttons also toggle chip selection.
- **Dual operating modes:**
  - *Channel-create mode* (default, no target channel selected) — each catalog item row shows a "Create Channel" button that calls `POST /api/channels`.
  - *Source-add mode* (target channel selected in dropdown) — each row shows "Add Source" that calls `POST /api/channels/{id}/sources` with `allow_cross_channel: true`. Per-row inline feedback elements display success/error messages.
- **Create Dynamic Channel** — A "Create Dynamic Channel From Filters" button builds a `dynamic_rule` from the current search query and group filters, then creates a channel via `POST /api/channels`.

**API endpoints used:**
- `GET /api/groups` — paginated via `loadPagedCollection()`
- `GET /api/items?group=...&q=...&q_regex=...&limit=...&offset=...` — server-side pagination
- `GET /api/channels` — loaded in full to populate target channel dropdown
- `POST /api/channels` — create channel
- `POST /api/channels/{id}/sources` — add source to target channel

---

### Channels (`/ui/channels`)

**Template:** `channels.html` — Single-column layout with three panel sections.

**Purpose:** Manage the full channel lineup: traditional channels (guide numbers 100–9999), dynamic channel blocks (10000+), and DVR mappings.

**Sections:**

1. **Traditional Channels (100–9999)** — Create form (item_key + guide name), DVR toolbar (discover lineups, save default, sync, reverse-sync, toggle mapping visibility), and a note that DVR mappings apply only to traditional channels.

2. **Published Traditional Channels** — Rendered list of channel cards, each showing:
   - Title: guide number + guide name
   - Metadata: channel_key, position, order index, enabled/disabled, dynamic rule status, source counts (total/enabled/dynamic/manual)
   - Actions: Details link, Top/Up/Down/Bottom/Move To.../Rename/Enable-Disable/Delete buttons
   - DVR Mapping section (collapsible): Inline editor with lineup ID, lineup channel number, station ref, callsign hint fields plus Save/Clear/Pull from DVR buttons

3. **Dynamic Channel Blocks (10000+)** — Create form with name, multi-select group picker, search query, regex toggle, enabled checkbox. Lists existing blocks showing block name, range (e.g. 10000–10999), order index, mode, groups, search query, generated count, and truncation info. Actions: Block Details link, Quick Edit, Enable/Disable, Delete.

**DVR mapping visibility** is toggled via a "Show DVR Mappings" button and persisted in `localStorage` under key `ui.channels.showDVRMappings`. CSS classes on `#channel-list` control visibility of `.mapping` elements.

**Key patterns:**
- `loadPagedCollection()` fetches all channels and dynamic queries across multiple pages
- Reorder uses optimistic local array mutation during request flight, sends `PATCH /api/channels/reorder` with full ID list, then refetches canonical channel rows (`loadTraditionalChannels()`) so persisted guide-number renumbering is reflected immediately after success
- `reorderBusy` flag prevents concurrent reorder requests

**API endpoints used:**
- `GET /api/channels` — paginated
- `POST /api/channels` — create
- `PATCH /api/channels/{id}` — update (rename, enable/disable)
- `DELETE /api/channels/{id}` — delete
- `PATCH /api/channels/reorder` — reorder
- `GET /api/dynamic-channels` — list blocks
- `POST /api/dynamic-channels` — create block
- `PATCH /api/dynamic-channels/{id}` — update block
- `DELETE /api/dynamic-channels/{id}` — delete block
- `GET /api/admin/dvr` — DVR state
- `PUT /api/admin/dvr` — save default lineup
- `GET /api/admin/dvr/lineups?refresh=1` — discover lineups
- `POST /api/admin/dvr/sync` — sync lineup (with `dry_run` option)
- `POST /api/admin/dvr/reverse-sync` — pull associations from DVR
- `GET /api/channels/dvr?enabled_only=0&include_dynamic=0` — list DVR mappings
- `PUT /api/channels/{id}/dvr` — save channel DVR mapping
- `POST /api/channels/{id}/dvr/reverse-sync` — pull single channel mapping

---

### Channel Detail (`/ui/channels/{channelID}`)

**Template:** `channel_detail.html` — Single-column, three panels.

**Purpose:** Deep management of a single channel: edit metadata, configure dynamic source rules, and manage individual sources.

**Sections:**

1. **Channel Details** — Displays channel metadata (ID, guide number, guide name, mode). Edit form with:
   - Guide name input
   - Enabled checkbox
   - Dynamic source rule editor: enable toggle, multi-select group picker with `<datalist>` autocomplete, search query input, regex mode toggle, and a live hint showing the current rule summary
   - Save Channel / Clear Health + Cooldowns / Reload buttons

2. **Add Source** — Simple form with item_key input and "Allow cross-channel attach" checkbox.

3. **Sources** — Ordered list of source cards showing:
   - Source ID, priority index, item_key
   - Full metadata: tvg_name, association type, enabled/disabled, success/fail counts, timestamps (last_ok, last_fail, cooldown_until, last_probe), stream profile (resolution, FPS, video/audio codec, bitrate)
   - Stream URL
   - Actions: Move Up/Down, Enable/Disable, Remove

**Template data:** `ChannelID` is injected as a Go template variable (`{{.ChannelID}}`), used in JS to scope all API calls.

**API endpoints used:**
- `GET /api/channels` — find channel by ID (no direct get-by-ID endpoint; filters from full list)
- `PATCH /api/channels/{id}` — update channel
- `GET /api/channels/{id}/sources` — paginated source list
- `POST /api/channels/{id}/sources` — add source (with cross-channel retry flow)
- `PATCH /api/channels/{id}/sources/reorder` — reorder sources
- `PATCH /api/channels/{id}/sources/{sourceID}` — enable/disable source
- `DELETE /api/channels/{id}/sources/{sourceID}` — remove source
- `POST /api/channels/{id}/sources/health/clear` — clear health stats
- `GET /api/groups?include_counts=0` — catalog groups for dynamic rule autocomplete

---

### Dynamic Channel Detail (`/ui/dynamic-channels/{queryID}`)

**Template:** `dynamic_channel_detail.html` — Single-column, two panels.

**Purpose:** Configure a dynamic channel block query and manage ordering of generated channels within the block.

**Sections:**

1. **Dynamic Block Configuration** — Metadata display (query ID, guide number range, order index, enabled/disabled, mode, groups, generated count, truncation). Edit form with name, multi-select group picker, search query, regex toggle, enabled checkbox. Save Block / Reload / Delete Block / Back to Channels.

2. **Generated Channels (Block Sorting)** — Lists channels materialized from catalog matches. Each row shows guide number, guide name, channel_key, dynamic_item_key, position. Reorder with Top/Up/Down/Bottom/Move To... buttons.

**Template data:** `QueryID` injected as `{{.QueryID}}`.

**API endpoints used:**
- `GET /api/dynamic-channels/{id}` — get block config
- `PATCH /api/dynamic-channels/{id}` — update block
- `DELETE /api/dynamic-channels/{id}` — delete block (redirects to `/ui/channels`)
- `GET /api/dynamic-channels/{id}/channels` — paginated generated channels
- `PATCH /api/dynamic-channels/{id}/channels/reorder` — reorder generated channels
- `GET /api/groups?include_counts=0` — catalog groups for autocomplete

---

### Merge Suggestions (`/ui/merge`)

**Template:** `merge.html` — Single-column, two panels.

**Purpose:** Identify duplicate playlist items that share a `channel_key` and create channels from them in bulk.

**Sections:**

1. **Duplicate Suggestions** — Filter form with minimum items per group (2–100) and channel_key/tvg-id search input.

2. **Groups** — Paginated list of duplicate groups. Each group card shows:
   - Header: channel_key and item count
   - Actions: "Create From First" (creates channel from first item) and "Create + Add All Sources" (creates channel from first item, attaches remaining items as sources)
   - Table: Name, Group, tvg-id, item_key columns with per-item "Create Channel" button

**API endpoints used:**
- `GET /api/suggestions/duplicates?min=...&q=...&limit=...&offset=...` — server-side paginated
- `POST /api/channels` — create channel
- `POST /api/channels/{id}/sources` — attach additional sources

---

### Tuners (`/ui/tuners`)

**Template:** `tuners.html` — The most complex page. Multi-section dashboard layout.

**Purpose:** Real-time monitoring of tuner sessions, streaming health, client connections, and session history.

**Sections:**

1. **Diagnostics Command Bar** (sticky) — Refresh Now button, auto-refresh toggle (3-second polling via `setInterval`), search field for filtering across all panels, quick filter chip buttons (All/Active/Recovering/Errors/Idle), sort controls for active and history panels.

2. **KPI Cards** (7-column grid) — Summary metrics with severity-based coloring:
   - Configured Tuners, Tuners In Use, Tuners Idle, Client Streams, Recovering Sessions, Reselect Alert Sessions, Max Same-source Reselect
   - Each card has: label, value, severity badge (ok/warn/danger), and help tooltip
   - Card border/background changes based on severity (`severity-ok`, `severity-warn`, `severity-danger` CSS classes)

3. **Active Sessions** (full-width) — Table with columns: Tuner, State (with colored tags), Channel, Source, Throughput, Subscribers, Last Push, Actions (Trigger Recovery button). Rows are selectable — clicking a row populates the "Selected Active Session" detail aside.

4. **Client Streams** (8/12 grid) — Grouped by session using collapsible `<details>` elements. Each group shows subscriber count and a table of client connections. The panel header includes a **Resolve IP** checkbox that opt-in enables reverse-DNS hostname display for both live client rows and history subscriber rows.

5. **Selected Active Session** (4/12 grid aside) — Detail card grid showing full metrics for the clicked session row.

6. **Shared Session History** (full-width) — Master-detail layout:
   - Left pane: Filterable/searchable list of historical sessions with status/errors/recovery filters. Each item shows channel, source, status tag, timestamps, and terminal reason.
   - Right pane: Tabbed detail view (Summary/Sources/Clients/Alerts tabs) for the selected history entry with full session lifecycle data.

**Key patterns:**
- Auto-refresh polls `GET /api/admin/tuners` every 3 seconds when enabled (checkbox defaults to checked)
- When **Resolve IP** is checked, status polling appends `resolve_ip=1` so the backend resolves client hosts and the UI renders host + address together.
- Trigger Recovery button sends `POST /api/admin/tuners/recovery` with `channel_id` and `reason: "ui_manual_trigger"`
- Complex client-side filtering, sorting, and search across all panels using the shared `uiState.searchQuery`
- `formatDurationMs()`, `formatBitrate()`, `formatFPS()` utility functions for human-readable display

**API endpoints used:**
- `GET /api/admin/tuners` — full tuner status snapshot (active sessions, history, alerts, summary); supports optional `resolve_ip` boolean query for reverse-DNS host enrichment
- `POST /api/admin/tuners/recovery` — trigger session recovery

---

### DVR (`/ui/dvr`)

**Template:** `dvr.html` — Single-column, four panels.

**Purpose:** Configure DVR provider connections, discover lineups, set sync behavior, and run sync operations.

**Sections:**

1. **DVR Connection** — Form grid with:
   - Primary provider selector (Channels DVR only; sync/mapping provider)
   - Active providers for post-sync reload (checkboxes)
   - Channels DVR base URL, Jellyfin base URL, HDHR Device ID (read-only), Default Lineup ID
   - Jellyfin-specific panel: API token (write-only password field), tuner host ID override, token status display
   - Save Configuration / Test Connection buttons

   Migration note:
   The `Channels DVR` active-provider checkbox is URL-gated by backend
   normalization. If `channels_base_url` is blank/invalid, save/reload drops
   `channels` from `active_providers`; set a valid Channels base URL first when
   enabling post-sync reload fan-out.

2. **Lineups** — Lineup discovery: select dropdown for discovered lineups, Discover Lineups / Use Selected as Default buttons.

3. **Sync Behavior and Schedule** — Sync mode selector (configured_only / mirror_device), cron expression input, enable scheduled sync checkbox, pre-sync refresh checkbox. Dry-run Preview / Sync All Channels Now / Pull Associations from DVR action buttons.

4. **Sync Results** — Key-value card grid showing sync statistics (updated/cleared counts), warnings in a `<pre>` block, and patch preview JSON.

**API endpoints used:**
- `GET /api/admin/dvr` — get current config state
- `PUT /api/admin/dvr` — save config
- `POST /api/admin/dvr/test` — test connection
- `GET /api/admin/dvr/lineups?refresh=1` — discover lineups
- `POST /api/admin/dvr/sync` — run sync (with `dry_run` flag)
- `POST /api/admin/dvr/reverse-sync` — pull associations from DVR

---

### Automation (`/ui/automation`)

**Template:** `automation.html` — Single-column, three panels.

**Purpose:** Configure and run automated jobs: playlist sync and auto-prioritize.

**Sections:**

1. **Automation Settings** — Two-column form grid with:
   - Playlist URL, Scheduler Timezone
   - Playlist Sync: cron expression + enabled checkbox
   - Auto-prioritize: cron expression + enabled checkbox
   - Analyzer settings: probe timeout (ms), analyze duration (us), probe size (bytes), bitrate mode (metadata/sample/metadata_then_sample), sample seconds, enabled-only checkbox, top-N per channel
   - Actions: Save Settings, Reload, Run Playlist Sync Now, Run Auto-prioritize Now, Clear All Source Health + Cooldowns, Clear Auto-prioritize Cache

2. **Current Schedule State** — 3x2 metadata grid showing next run, last run, and last status for both playlist sync and auto-prioritize jobs.

3. **Recent Job Runs** — Table with columns: Run ID, Job, Status, Started, Finished, Progress, Summary/Error. When a job has `status: "running"`, a 2-second polling interval starts automatically and stops when no running jobs remain.

**API endpoints used:**
- `GET /api/admin/automation` — current config and schedule state
- `PUT /api/admin/automation` — save settings
- `POST /api/admin/jobs/playlist-sync/run` — trigger playlist sync
- `POST /api/admin/jobs/auto-prioritize/run` — trigger auto-prioritize
- `POST /api/admin/jobs/auto-prioritize/cache/clear` — clear analyzer cache
- `POST /api/channels/sources/health/clear` — clear all source health
- `GET /api/admin/jobs?limit=25` — list recent job runs

## Client-Side Patterns

### `fetch()` wrapper

Most pages define a local `api()` helper (`catalog`, `channels`, `channel_detail`, `dynamic_channel_detail`, `merge`, `dvr`, `automation`, and legacy `index`):

```js
async function api(path, options = {}) {
    const resp = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options,
    });
    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `request failed (${resp.status})`);
    }
    if (resp.status === 204) return null;
    return resp.json();
}
```

The helper is duplicated across templates (no shared JS file). `tuners.html` is the exception: it uses dedicated `fetchStatus()` / `triggerManualRecovery()` helpers instead of `api()`, but follows the same `resp.ok` + response-body error-text pattern.

### `loadPagedCollection()`

Used on the Catalog, Channels, Channel Detail, and Dynamic Channel Detail pages to auto-paginate through an API endpoint that returns `{ [key]: [...], total, limit, offset }`:

```js
async function loadPagedCollection(path, key, pageLimit = 200) {
    const items = [];
    let offset = 0;
    let total = -1;
    while (true) {
        const data = await api(`${path}?limit=${pageLimit}&offset=${offset}`);
        const page = data?.[key] || [];
        items.push(...page);
        if (page.length === 0) break;
        offset += (data?.limit || page.length);
        if (total >= 0 && offset >= total) break;
    }
    return items;
}
```

This accumulates all pages into a single in-memory array.

### DOM Manipulation

Rendering is primarily imperative (`document.createElement()` + `.textContent` + `.appendChild()`), but several templates also use `.innerHTML` for row/detail rendering (not only Tuners/Automation). Tuners escapes interpolated text via `escapeHTML()` before HTML insertion; other pages mostly inject server-provided/admin-entered values directly into generated markup. Lists are commonly re-rendered by clearing `container.innerHTML = ""` and rebuilding.

### `localStorage`

Used only on the Channels page to persist DVR mapping visibility preference:

```js
const dvrMappingVisibilityStorageKey = "ui.channels.showDVRMappings";
window.localStorage.getItem(dvrMappingVisibilityStorageKey);
window.localStorage.setItem(dvrMappingVisibilityStorageKey, "true" | "false");
```

### Polling

- **Tuners page:** `setInterval` at 3 seconds, controlled by an "Auto-refresh" checkbox (checked by default). Polls `GET /api/admin/tuners`.
- **Automation page:** `setInterval` at 2 seconds, started when a job run has `status: "running"`, stopped when no running jobs remain. Polls both `GET /api/admin/automation` and `GET /api/admin/jobs?limit=25`.

### Status Notifications

Every page exposes a `showStatus(...)` function that shows a timed banner (2.5–4.5 seconds depending on the page) with info/error styling. Signatures differ by page (boolean error flag vs. string kind), and banners auto-hide via `setTimeout`.

## Data Flow Summary

| Page | Load pattern | Mutation pattern |
|---|---|---|
| Catalog | Groups + Channels loaded in full; items paginated server-side | `POST /api/channels`, `POST /api/channels/{id}/sources` |
| Channels | Channels + dynamic queries loaded in full via `loadPagedCollection()`; DVR mappings loaded separately | `POST/PATCH/DELETE /api/channels/*`, `POST/PATCH/DELETE /api/dynamic-channels/*`, `PUT /api/channels/{id}/dvr` |
| Channel Detail | Channel found from full list; sources paginated | `PATCH /api/channels/{id}`, source CRUD via `/api/channels/{id}/sources/*` |
| Dynamic Channel Detail | Single query fetched by ID; generated channels paginated | `PATCH/DELETE /api/dynamic-channels/{id}`, reorder via `PATCH /api/dynamic-channels/{id}/channels/reorder` |
| Merge Suggestions | Server-side paginated duplicate groups | `POST /api/channels`, `POST /api/channels/{id}/sources` |
| Tuners | Single snapshot endpoint, polled | `POST /api/admin/tuners/recovery` |
| DVR | Config state loaded once; lineups discovered on demand | `PUT /api/admin/dvr`, `POST /api/admin/dvr/sync`, `POST /api/admin/dvr/reverse-sync` |
| Automation | Config + job runs loaded; polled when running | `PUT /api/admin/automation`, `POST /api/admin/jobs/*/run` |

## Conditional Features

Two pages are conditionally registered based on service availability at startup (`admin_routes.go:269-274`):

```go
if h.automation != nil {
    mux.Handle("GET /ui/automation", auth(http.HandlerFunc(h.handleUIAutomation)))
}
if h.dvr != nil {
    mux.Handle("GET /ui/dvr", auth(http.HandlerFunc(h.handleUIDVR)))
}
```

- **DVR page** — Only served when `AdminHandler.dvr` is non-nil (a `DVRService` was injected via `SetDVRService()`). The corresponding DVR API routes under `/api/admin/dvr/*` and `/api/channels/*/dvr` are also conditionally registered.
- **Automation page** — Only served when `AdminHandler.automation` is non-nil (an `AutomationDeps` was passed to `NewAdminHandler()`). The automation API routes under `/api/admin/automation`, `/api/admin/jobs/*` are also conditionally registered.

The navigation bar in every template **always includes links to all pages** (Catalog, Channels, Merge, Tuners, DVR, Automation) regardless of whether the DVR and Automation routes are actually registered. Visiting an unregistered route returns a 404.

On the Channels page, DVR-related UI elements (lineup discovery, sync buttons, per-channel mapping editor) gracefully degrade when the DVR API returns errors — `state.dvrAvailable` is set to `false` and DVR controls are disabled with an "unavailable" message.

## API Error / Status Handling Matrix

Most pages use a local `api()` wrapper; `tuners.html` uses dedicated `fetch()` helpers. Across all pages, non-2xx responses are surfaced as generic `Error` values using response-body text or a fallback `request failed (${resp.status})` message. **No template branches on specific HTTP status codes** (401, 404, 429, 500).

| Page | Catch mechanism | Display method | Auto-hide | Inline feedback | Notes |
|---|---|---|---|---|---|
| Catalog | `.catch(handleError)` | `showStatus(msg, true)` | 3 s | Per-item `showItemFeedback()` with 3 s clear | Item-level success/error badges during source-add mode |
| Channels | `.catch(handleError)` | `showStatus(msg, true)` | 3.5 s | None | `withBusy()` disables trigger button during request |
| Channel Detail | `try/catch` | `showStatus(msg, true)` | 3 s | `window.confirm()` for cross-channel attach | Parses error text for "metadata does not match" to offer retry |
| Dynamic Channel Detail | `.catch(handleError)` | `showStatus(msg, true)` | 3.5 s | None | — |
| Merge | `.catch(handleError)` | `showStatus(msg, true)` | 3 s | None | — |
| Tuners | `try/catch` | `showStatus(msg, true)` | 4.5 s | None | Polling failures are surfaced for the latest request; stale request failures are ignored |
| DVR | `try/catch` + `.catch()` | `showStatus(msg, "error")` | 4.5 s | Inline test-result element | Client-side validation throws before API call |
| Automation | `try/catch` | `showStatus(msg, "error")` | 4.5 s | None | Polling failures logged silently, not displayed |

`showStatus()` has two interface variants across the codebase:
- **Boolean form** (`showStatus(msg, isError)`) — Catalog, Channels, Channel Detail, Dynamic Channel Detail, Merge, Tuners, Index.
- **String form** (`showStatus(msg, kind)`) where kind is `"info"` | `"error"` | `"ok"` — DVR, Automation.

## State Ownership Map

| State | Storage | Lifecycle |
|---|---|---|
| DVR mapping visibility toggle | `localStorage` (`ui.channels.showDVRMappings`) | Persists across sessions; read with try/catch fallback to `false` |
| Channel list / dynamic queries | In-memory `state` object | Lost on page refresh; reloaded from API |
| Catalog search query & group filters | In-memory `state.q`, `state.catalogGroups` | Lost on refresh |
| Tuner auto-refresh toggle | In-memory checkbox state | Defaults to on (`checked`) on each load |
| Tuner search query & filters | In-memory `uiState.searchQuery`, `uiState.quickFilter` | Lost on refresh |
| DVR config / lineups / sync results | In-memory `state` object | Lost on refresh; reloaded from `GET /api/admin/dvr` |
| Automation form fields | Direct DOM element values (no `state` object) | Lost on refresh; reloaded from `GET /api/admin/automation` |
| Polling timer IDs | In-memory (`pollTimer`, `setInterval` ID) | Cleared on page unload |
| Channel Detail sources | In-memory `state.sources` | Lost on refresh |
| Selected history entry (Tuners) | In-memory selection | Lost on refresh |

## Maintainability Notes

**Duplicated code candidates for shared extraction:**

- **`api(path, options)`** — Near-identical fetch wrapper duplicated in most templates (all except Tuners). Could be extracted to a shared JS file.
- **`loadPagedCollection(path, key, pageLimit)`** — Exact duplicate in Catalog, Channels, Channel Detail, and Dynamic Channel Detail. Each copy is ~12 lines.
- **`showStatus()`** — Present in all templates with two incompatible signatures (boolean vs. string kind). Unifying the interface and extracting it would reduce drift.
- **`handleError(err)`** — Near-identical `console.error` + `showStatus` call in most templates.
- **`withBusy(button, work)`** — Identical in Channels and DVR; useful pattern that other pages re-implement inline.
- **`normalizeGroupNameList()` / `normalizeGroupNameValue()`** — Duplicated in Catalog, Channels, and Channel Detail.
- **Navigation bar (`<header class="topbar">`)** — Copy-pasted in all 9 templates with only the `class="active"` link differing. A Go partial template or `{{block}}` would eliminate the duplication.

**No shared JS or CSS files exist today.** Each template is fully self-contained. Extracting the above utilities into a single `/ui/static/common.js` (served via `embed.FS`) and converting the nav bar to a Go template partial would reduce total template LOC and prevent the implementations from silently diverging.
