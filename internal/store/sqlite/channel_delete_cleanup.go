package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/arodd/hdhriptv/internal/channels"
)

func deleteChannelChildRowsTx(ctx context.Context, tx *sql.Tx, channelID int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM channel_sources WHERE channel_id = ?`, channelID); err != nil {
		return fmt.Errorf("delete channel sources for channel %d: %w", channelID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM published_channel_dvr_map WHERE channel_id = ?`, channelID); err != nil {
		return fmt.Errorf("delete dvr mappings for channel %d: %w", channelID, err)
	}
	return nil
}

func deleteDynamicGeneratedChannelChildRowsTx(ctx context.Context, tx *sql.Tx, queryID int64) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM channel_sources
		 WHERE channel_id IN (
			 SELECT channel_id
			 FROM published_channels
			 WHERE channel_class = ?
			   AND dynamic_query_id = ?
		 )`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	); err != nil {
		return fmt.Errorf("delete channel sources for dynamic query %d generated channels: %w", queryID, err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channel_dvr_map
		 WHERE channel_id IN (
			 SELECT channel_id
			 FROM published_channels
			 WHERE channel_class = ?
			   AND dynamic_query_id = ?
		 )`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	); err != nil {
		return fmt.Errorf("delete dvr mappings for dynamic query %d generated channels: %w", queryID, err)
	}
	return nil
}

func runOrphanChannelChildRowPruneSafetyCheckTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM channel_sources
		 WHERE NOT EXISTS (
			 SELECT 1
			 FROM published_channels pc
			 WHERE pc.channel_id = channel_sources.channel_id
		 )`,
	); err != nil {
		return fmt.Errorf("prune orphan channel_sources rows: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channel_dvr_map
		 WHERE NOT EXISTS (
			 SELECT 1
			 FROM published_channels pc
			 WHERE pc.channel_id = published_channel_dvr_map.channel_id
		 )`,
	); err != nil {
		return fmt.Errorf("prune orphan published_channel_dvr_map rows: %w", err)
	}
	return nil
}
