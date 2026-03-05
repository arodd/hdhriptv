package channels

import "fmt"

const (
	ChannelClassTraditional      = "traditional"
	ChannelClassDynamicGenerated = "dynamic_generated"
)

const (
	TraditionalGuideStart = 100
	TraditionalGuideEnd   = 9999

	DynamicGuideStart       = 10000
	DynamicGuideBlockSize   = 1000
	DynamicGuideBlockMaxLen = 1000
)

const dynamicChannelOrderBase = 1000000

// DynamicChannelQuery stores one dynamic block query configuration.
type DynamicChannelQuery struct {
	QueryID     int64    `json:"query_id"`
	Enabled     bool     `json:"enabled"`
	Name        string   `json:"name"`
	GroupName   string   `json:"group_name"`
	GroupNames  []string `json:"group_names,omitempty"`
	SourceIDs   []int64  `json:"source_ids"`
	SearchQuery string   `json:"search_query"`
	SearchRegex bool     `json:"search_regex,omitempty"`
	// NextSlotCursor tracks where new dynamic matches should be placed next.
	NextSlotCursor int   `json:"next_slot_cursor"`
	OrderIndex     int   `json:"order_index"`
	CreatedAt      int64 `json:"created_at"`
	UpdatedAt      int64 `json:"updated_at"`
	LastCount      int   `json:"last_count"`
	TruncatedBy    int   `json:"truncated_by"`
}

// DynamicChannelQueryCreate carries create-time fields for one dynamic query block.
type DynamicChannelQueryCreate struct {
	Enabled     *bool    `json:"enabled,omitempty"`
	Name        string   `json:"name"`
	GroupName   string   `json:"group_name"`
	GroupNames  []string `json:"group_names,omitempty"`
	SourceIDs   []int64  `json:"source_ids"`
	SearchQuery string   `json:"search_query"`
	SearchRegex bool     `json:"search_regex,omitempty"`
}

// DynamicChannelQueryUpdate carries mutable fields for one dynamic query block.
type DynamicChannelQueryUpdate struct {
	Enabled     *bool     `json:"enabled,omitempty"`
	Name        *string   `json:"name,omitempty"`
	GroupName   *string   `json:"group_name,omitempty"`
	GroupNames  *[]string `json:"group_names,omitempty"`
	SourceIDs   *[]int64  `json:"source_ids,omitempty"`
	SearchQuery *string   `json:"search_query,omitempty"`
	SearchRegex *bool     `json:"search_regex,omitempty"`
}

// DynamicChannelSyncResult summarizes one full dynamic block materialization run.
type DynamicChannelSyncResult struct {
	QueriesProcessed int `json:"queries_processed"`
	QueriesEnabled   int `json:"queries_enabled"`
	ChannelsAdded    int `json:"channels_added"`
	ChannelsUpdated  int `json:"channels_updated"`
	ChannelsRetained int `json:"channels_retained"`
	ChannelsRemoved  int `json:"channels_removed"`
	TruncatedCount   int `json:"truncated_count"`
}

func DynamicGuideBlockStart(orderIndex int) (int, error) {
	if orderIndex < 0 {
		return 0, fmt.Errorf("order_index must be zero or greater")
	}
	blockStart := int64(DynamicGuideStart) + (int64(orderIndex) * int64(DynamicGuideBlockSize))
	maxGuide := blockStart + int64(DynamicGuideBlockMaxLen-1)
	if blockStart > int64(maxSignedInt()) || maxGuide > int64(maxSignedInt()) {
		return 0, fmt.Errorf("dynamic guide range exhausted")
	}
	return int(blockStart), nil
}

func DynamicGuideNumber(orderIndex, position int) (int, error) {
	if position < 0 || position >= DynamicGuideBlockMaxLen {
		return 0, fmt.Errorf("position must be between 0 and %d", DynamicGuideBlockMaxLen-1)
	}
	blockStart, err := DynamicGuideBlockStart(orderIndex)
	if err != nil {
		return 0, err
	}
	return blockStart + position, nil
}

func DynamicChannelOrderIndex(orderIndex, position int) (int, error) {
	if orderIndex < 0 {
		return 0, fmt.Errorf("order_index must be zero or greater")
	}
	if position < 0 || position >= DynamicGuideBlockMaxLen {
		return 0, fmt.Errorf("position must be between 0 and %d", DynamicGuideBlockMaxLen-1)
	}
	order := int64(dynamicChannelOrderBase) + (int64(orderIndex) * int64(DynamicGuideBlockSize)) + int64(position)
	if order > int64(maxSignedInt()) {
		return 0, fmt.Errorf("dynamic channel order range exhausted")
	}
	return int(order), nil
}

func maxSignedInt() int {
	return int(^uint(0) >> 1)
}
