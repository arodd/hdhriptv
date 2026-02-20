package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
	modernsqlite "modernc.org/sqlite"
)

type sqliteDiagOptions struct {
	Phase     string
	ItemIndex int
	ItemTotal int
	ItemKey   string
	DBPath    string
	TraceRing *sqliteIOERRTraceRing
}

func sqliteDiagSuffix(ctx context.Context, db *sql.DB, err error, opts sqliteDiagOptions) string {
	parts := make([]string, 0, 16)

	if phase := strings.TrimSpace(opts.Phase); phase != "" {
		parts = append(parts, fmt.Sprintf("phase=%q", phase))
	}
	if opts.ItemIndex > 0 {
		parts = append(parts, fmt.Sprintf("item_index=%d", opts.ItemIndex))
	}
	if opts.ItemTotal > 0 {
		parts = append(parts, fmt.Sprintf("item_total=%d", opts.ItemTotal))
	}
	if itemKey := strings.TrimSpace(opts.ItemKey); itemKey != "" {
		parts = append(parts, fmt.Sprintf("item_key=%q", itemKey))
	}

	if runMeta, ok := jobs.RunMetadataFromContext(ctx); ok {
		parts = append(
			parts,
			fmt.Sprintf("run_id=%d", runMeta.RunID),
			fmt.Sprintf("job_name=%q", runMeta.JobName),
			fmt.Sprintf("triggered_by=%q", runMeta.TriggeredBy),
		)
	}

	code, codeName, ok := sqliteErrorDetails(err)
	if ok {
		parts = append(
			parts,
			fmt.Sprintf("sqlite_code=%d", code),
			fmt.Sprintf("sqlite_code_name=%q", codeName),
		)

		if db != nil {
			stats := db.Stats()
			waitDuration := stats.WaitDuration.Round(time.Millisecond)
			parts = append(
				parts,
				fmt.Sprintf("db_open=%d", stats.OpenConnections),
				fmt.Sprintf("db_in_use=%d", stats.InUse),
				fmt.Sprintf("db_idle=%d", stats.Idle),
				fmt.Sprintf("db_wait_count=%d", stats.WaitCount),
				fmt.Sprintf("db_wait_duration=%s", waitDuration.String()),
			)
		}

		if isSQLiteIOErrCode(code) {
			diagDecision := maybeEmitSQLiteIOERRDiagnosticBundle(ctx, db, opts, code, codeName)
			traceDecision := maybeEmitSQLiteIOERRTraceDump(ctx, opts.TraceRing, code, codeName)
			parts = append(
				parts,
				fmt.Sprintf("ioerr_diag_emitted=%t", diagDecision.Emitted),
				fmt.Sprintf("ioerr_diag_reason=%q", diagDecision.Reason),
				fmt.Sprintf("ioerr_trace_emitted=%t", traceDecision.Emitted),
				fmt.Sprintf("ioerr_trace_reason=%q", traceDecision.Reason),
				fmt.Sprintf("ioerr_trace_entries=%d", traceDecision.EntryCount),
			)
		}
	}

	if len(parts) == 0 {
		return ""
	}
	return " [" + strings.Join(parts, " ") + "]"
}

func sqliteErrorDetails(err error) (code int, codeName string, ok bool) {
	var sqliteErr *modernsqlite.Error
	if !errors.As(err, &sqliteErr) {
		return 0, "", false
	}

	code = sqliteErr.Code()
	codeName = strings.TrimSpace(modernsqlite.ErrorCodeString[code])
	if codeName == "" {
		codeName = fmt.Sprintf("SQLITE_CODE_%d", code)
	}
	return code, codeName, true
}
