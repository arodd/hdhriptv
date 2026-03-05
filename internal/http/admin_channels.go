package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
	"github.com/arodd/hdhriptv/internal/stream"
)

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

	sourceIDs, err := parseSourceIDFilters(r, "source_ids")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var (
		groups []playlist.Group
		total  int
	)
	if len(sourceIDs) > 0 {
		sourceScopedCatalog, ok := h.catalog.(sourceScopedCatalogGroupsPager)
		if !ok {
			writeUnsupportedSourceScopedOperationError(
				w,
				"groups_list",
				"source_ids filtering for /api/groups requires source-scoped group backend support",
			)
			return
		}
		groups, total, err = sourceScopedCatalog.ListGroupsPagedBySourceIDs(r.Context(), sourceIDs, limit, offset, includeCounts)
	} else {
		groups, total, err = h.catalog.ListGroupsPaged(r.Context(), limit, offset, includeCounts)
	}
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
	sourceIDs, err := parseSourceIDFilters(r, "source_ids")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	searchRegex, err := parseOptionalBoolQueryParam(r, "q_regex", false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	q := playlist.Query{
		SourceIDs:   sourceIDs,
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
		ItemKey            string `json:"item_key"`
		ChannelKey         string `json:"channel_key,omitempty"`
		Name               string `json:"name"`
		Group              string `json:"group"`
		StreamURL          string `json:"stream_url,omitempty"`
		Logo               string `json:"logo,omitempty"`
		PlaylistSourceID   int64  `json:"playlist_source_id,omitempty"`
		PlaylistSourceName string `json:"playlist_source_name,omitempty"`
		Active             bool   `json:"active"`
	}

	out := make([]itemResponse, 0, len(items))
	for _, item := range items {
		out = append(out, itemResponse{
			ItemKey:            item.ItemKey,
			ChannelKey:         item.ChannelKey,
			Name:               item.Name,
			Group:              item.Group,
			StreamURL:          item.StreamURL,
			Logo:               item.TVGLogo,
			PlaylistSourceID:   item.PlaylistSourceID,
			PlaylistSourceName: item.PlaylistSourceName,
			Active:             item.Active,
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

func parseSourceIDFilters(r *http.Request, param string) ([]int64, error) {
	if r == nil || r.URL == nil {
		return []int64{}, nil
	}
	if strings.TrimSpace(param) == "" {
		param = "source_ids"
	}
	values := r.URL.Query()
	if len(values) == 0 {
		return []int64{}, nil
	}

	rawValues := values[param]
	if len(rawValues) == 0 {
		return []int64{}, nil
	}

	parts := make([]int64, 0, len(rawValues))
	for _, token := range rawValues {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		for _, chunk := range strings.Split(token, ",") {
			chunk = strings.TrimSpace(chunk)
			if chunk == "" {
				continue
			}
			value, err := strconv.ParseInt(chunk, 10, 64)
			if err != nil || value <= 0 {
				return nil, fmt.Errorf("%s must contain only positive integers", param)
			}
			parts = append(parts, value)
		}
	}

	return channels.NormalizeSourceIDs(parts), nil
}

func dynamicRuleUsesSourceScope(rule *channels.DynamicSourceRule) bool {
	if rule == nil {
		return false
	}
	return len(channels.NormalizeSourceIDs(rule.SourceIDs)) > 0
}

func dynamicQueryCreateUsesSourceScope(req channels.DynamicChannelQueryCreate) bool {
	return len(channels.NormalizeSourceIDs(req.SourceIDs)) > 0
}

func dynamicQueryUpdateUsesSourceScope(req channels.DynamicChannelQueryUpdate) bool {
	if req.SourceIDs == nil {
		return false
	}
	return len(channels.NormalizeSourceIDs(*req.SourceIDs)) > 0
}

func (h *AdminHandler) supportsSourceScopedCatalogFiltering() bool {
	if h == nil || h.catalog == nil {
		return false
	}
	_, ok := h.catalog.(sourceScopedCatalogStore)
	return ok
}

func (h *AdminHandler) supportsSourceScopedDynamicChannelBlocks() bool {
	if h == nil || h.channels == nil {
		return false
	}
	capability, ok := h.channels.(sourceScopedDynamicChannelBlocksCapability)
	if !ok {
		return false
	}
	ok = capability.SupportsSourceScopedDynamicChannelBlocks()
	return ok
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
	if dynamicRuleUsesSourceScope(req.DynamicRule) && !h.supportsSourceScopedCatalogFiltering() {
		writeUnsupportedSourceScopedOperationError(
			w,
			"channel_dynamic_rule_create",
			"dynamic_rule.source_ids requires source-scoped catalog filtering support",
		)
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
		"dynamic_source_ids", channel.DynamicRule.SourceIDs,
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
	if dynamicRuleUsesSourceScope(req.DynamicRule) && !h.supportsSourceScopedCatalogFiltering() {
		writeUnsupportedSourceScopedOperationError(
			w,
			"channel_dynamic_rule_update",
			"dynamic_rule.source_ids requires source-scoped catalog filtering support",
		)
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
		"dynamic_source_ids", channel.DynamicRule.SourceIDs,
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
	SourceIDs     []int64                      `json:"source_ids"`
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
		SourceIDs:     query.SourceIDs,
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
	if dynamicQueryCreateUsesSourceScope(req) && !h.supportsSourceScopedDynamicChannelBlocks() {
		writeUnsupportedSourceScopedOperationError(
			w,
			"dynamic_channel_query_create",
			"source_ids filtering for dynamic channel queries requires source-scoped dynamic block backend support",
		)
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
		"source_ids", query.SourceIDs,
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
	if dynamicQueryUpdateUsesSourceScope(req) && !h.supportsSourceScopedDynamicChannelBlocks() {
		writeUnsupportedSourceScopedOperationError(
			w,
			"dynamic_channel_query_update",
			"source_ids filtering for dynamic channel queries requires source-scoped dynamic block backend support",
		)
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
		"source_ids", query.SourceIDs,
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

func writeChannelsError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, channels.ErrItemNotFound), errors.Is(err, channels.ErrChannelNotFound), errors.Is(err, channels.ErrSourceNotFound), errors.Is(err, channels.ErrDynamicQueryNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, channels.ErrAssociationMismatch):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case errors.Is(err, channels.ErrSourceOrderDrift):
		http.Error(w, err.Error(), http.StatusBadRequest)
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

func writeUnsupportedSourceScopedOperationError(w http.ResponseWriter, operation, detail string) {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = "source-scoped operation is not supported by the configured backend"
	}
	writeJSON(w, http.StatusNotImplemented, map[string]any{
		"error":     "source_scoped_operation_unsupported",
		"message":   "source-scoped operation is not supported by the configured backend",
		"operation": strings.TrimSpace(operation),
		"parameter": "source_ids",
		"detail":    detail,
	})
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
		"must contain",
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
