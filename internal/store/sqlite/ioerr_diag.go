package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

const (
	sqliteBaseErrorCodeMask = 0xFF
	sqliteIOErrBaseCode     = 10

	sqliteIOERRDiagEventPrefix      = "sqlite_ioerr_diag_bundle"
	sqliteIOERRDiagCheckpointEnvVar = "HDHRIPTV_SQLITE_IOERR_CHECKPOINT_PROBE"

	sqliteIOERRDiagCaptureTimeout = 1500 * time.Millisecond

	sqliteIOERRFallbackBackoffBase = 30 * time.Second
	sqliteIOERRFallbackBackoffMax  = 15 * time.Minute
	sqliteIOERRRunCacheLimit       = 4096
)

var globalSQLiteIOERRDiagGuard = newIOERRDiagGuard(
	sqliteIOERRFallbackBackoffBase,
	sqliteIOERRFallbackBackoffMax,
	sqliteIOERRRunCacheLimit,
)

type ioerrDiagDecision struct {
	Emitted bool
	Reason  string
}

type ioerrDiagGuard struct {
	mu sync.Mutex

	runSeen      map[int64]struct{}
	runOrder     []int64
	runCacheSize int

	fallbackBaseBackoff time.Duration
	fallbackMaxBackoff  time.Duration
	fallbackBackoff     time.Duration
	fallbackNextAllowed time.Time
	fallbackLastEmitted time.Time
}

func newIOERRDiagGuard(fallbackBaseBackoff, fallbackMaxBackoff time.Duration, runCacheSize int) *ioerrDiagGuard {
	if fallbackBaseBackoff <= 0 {
		fallbackBaseBackoff = sqliteIOERRFallbackBackoffBase
	}
	if fallbackMaxBackoff <= 0 || fallbackMaxBackoff < fallbackBaseBackoff {
		fallbackMaxBackoff = fallbackBaseBackoff
	}
	if runCacheSize <= 0 {
		runCacheSize = sqliteIOERRRunCacheLimit
	}
	return &ioerrDiagGuard{
		runSeen:             make(map[int64]struct{}, runCacheSize),
		runOrder:            make([]int64, 0, runCacheSize),
		runCacheSize:        runCacheSize,
		fallbackBaseBackoff: fallbackBaseBackoff,
		fallbackMaxBackoff:  fallbackMaxBackoff,
		fallbackBackoff:     fallbackBaseBackoff,
		fallbackNextAllowed: time.Time{},
		fallbackLastEmitted: time.Time{},
	}
}

func (g *ioerrDiagGuard) allow(runID int64) (bool, string) {
	return g.allowAt(runID, time.Now().UTC())
}

func (g *ioerrDiagGuard) allowAt(runID int64, now time.Time) (bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if runID > 0 {
		if _, seen := g.runSeen[runID]; seen {
			return false, "run_id_already_captured"
		}
		g.runSeen[runID] = struct{}{}
		g.runOrder = append(g.runOrder, runID)
		for len(g.runOrder) > g.runCacheSize {
			dropRunID := g.runOrder[0]
			g.runOrder = g.runOrder[1:]
			delete(g.runSeen, dropRunID)
		}
		return true, "run_id_first_ioerr"
	}

	if !g.fallbackNextAllowed.IsZero() && now.Before(g.fallbackNextAllowed) {
		return false, fmt.Sprintf(
			"fallback_rate_limited_until=%s",
			g.fallbackNextAllowed.UTC().Format(time.RFC3339),
		)
	}

	// Reset backoff after a long quiet window so intermittent failures don't stay at max.
	if !g.fallbackLastEmitted.IsZero() && now.Sub(g.fallbackLastEmitted) > g.fallbackMaxBackoff {
		g.fallbackBackoff = g.fallbackBaseBackoff
	}
	currentBackoff := g.fallbackBackoff
	if currentBackoff <= 0 {
		currentBackoff = g.fallbackBaseBackoff
	}

	g.fallbackLastEmitted = now
	g.fallbackNextAllowed = now.Add(currentBackoff)

	nextBackoff := currentBackoff * 2
	if nextBackoff > g.fallbackMaxBackoff {
		nextBackoff = g.fallbackMaxBackoff
	}
	g.fallbackBackoff = nextBackoff

	return true, fmt.Sprintf("fallback_backoff=%s", currentBackoff.String())
}

func isSQLiteIOErrCode(code int) bool {
	if code <= 0 {
		return false
	}
	return code&sqliteBaseErrorCodeMask == sqliteIOErrBaseCode
}

type sqliteIOERRFileStat struct {
	Path      string
	Exists    bool
	SizeBytes int64
	Mode      string
	ModTime   string
	Owner     string
	Error     string
}

type sqliteIOERRCheckpointResult struct {
	Enabled      bool
	Attempted    bool
	Busy         int
	LogFrames    int
	Checkpointed int
	Error        string
}

type sqliteIOERRDiagBundle struct {
	DecisionReason string

	Phase     string
	ItemIndex int
	ItemTotal int
	ItemKey   string

	RunID       int64
	JobName     string
	TriggeredBy string

	SQLiteCode     int
	SQLiteCodeName string

	DBOpen         int
	DBInUse        int
	DBIdle         int
	DBWaitCount    int64
	DBWaitDuration string

	PragmaBusyTimeoutMS int
	PragmaSynchronous   string
	PragmaJournalMode   string

	DBPath      string
	DBFileStats sqliteIOERRFileStat
	WALFile     sqliteIOERRFileStat
	SHMFile     sqliteIOERRFileStat

	Checkpoint sqliteIOERRCheckpointResult

	CollectionErrors []string
}

func maybeEmitSQLiteIOERRDiagnosticBundle(
	ctx context.Context,
	db *sql.DB,
	opts sqliteDiagOptions,
	sqliteCode int,
	sqliteCodeName string,
) ioerrDiagDecision {
	return maybeEmitSQLiteIOERRDiagnosticBundleWithGuard(
		ctx,
		db,
		opts,
		sqliteCode,
		sqliteCodeName,
		globalSQLiteIOERRDiagGuard,
	)
}

func maybeEmitSQLiteIOERRDiagnosticBundleWithGuard(
	ctx context.Context,
	db *sql.DB,
	opts sqliteDiagOptions,
	sqliteCode int,
	sqliteCodeName string,
	guard *ioerrDiagGuard,
) ioerrDiagDecision {
	if guard == nil {
		guard = globalSQLiteIOERRDiagGuard
	}

	runID := int64(0)
	if runMeta, ok := jobs.RunMetadataFromContext(ctx); ok {
		runID = runMeta.RunID
	}

	allowed, reason := guard.allow(runID)
	if !allowed {
		return ioerrDiagDecision{Emitted: false, Reason: reason}
	}

	bundle := collectSQLiteIOERRDiagnosticBundle(ctx, db, opts, sqliteCode, sqliteCodeName)
	bundle.DecisionReason = reason
	logSQLiteIOERRDiagnosticBundle(bundle)
	return ioerrDiagDecision{Emitted: true, Reason: reason}
}

func collectSQLiteIOERRDiagnosticBundle(
	ctx context.Context,
	db *sql.DB,
	opts sqliteDiagOptions,
	sqliteCode int,
	sqliteCodeName string,
) sqliteIOERRDiagBundle {
	bundle := sqliteIOERRDiagBundle{
		Phase:          strings.TrimSpace(opts.Phase),
		ItemIndex:      opts.ItemIndex,
		ItemTotal:      opts.ItemTotal,
		ItemKey:        strings.TrimSpace(opts.ItemKey),
		SQLiteCode:     sqliteCode,
		SQLiteCodeName: strings.TrimSpace(sqliteCodeName),
		DBPath:         strings.TrimSpace(opts.DBPath),
	}

	if runMeta, ok := jobs.RunMetadataFromContext(ctx); ok {
		bundle.RunID = runMeta.RunID
		bundle.JobName = runMeta.JobName
		bundle.TriggeredBy = runMeta.TriggeredBy
	}

	if db != nil {
		stats := db.Stats()
		bundle.DBOpen = stats.OpenConnections
		bundle.DBInUse = stats.InUse
		bundle.DBIdle = stats.Idle
		bundle.DBWaitCount = int64(stats.WaitCount)
		bundle.DBWaitDuration = stats.WaitDuration.Round(time.Millisecond).String()
	}

	diagCtx, cancel := context.WithTimeout(context.Background(), sqliteIOERRDiagCaptureTimeout)
	defer cancel()

	appendCollectionError := func(format string, args ...any) {
		bundle.CollectionErrors = append(bundle.CollectionErrors, fmt.Sprintf(format, args...))
	}

	if db != nil {
		busyTimeout, err := queryPragmaInt(diagCtx, db, "busy_timeout")
		if err != nil {
			appendCollectionError("pragma busy_timeout: %v", err)
		} else {
			bundle.PragmaBusyTimeoutMS = busyTimeout
		}

		synchronousCode, err := queryPragmaInt(diagCtx, db, "synchronous")
		if err != nil {
			appendCollectionError("pragma synchronous: %v", err)
		} else {
			bundle.PragmaSynchronous = synchronousPragmaName(synchronousCode)
		}

		journalMode, err := queryPragmaText(diagCtx, db, "journal_mode")
		if err != nil {
			appendCollectionError("pragma journal_mode: %v", err)
		} else {
			bundle.PragmaJournalMode = strings.ToLower(strings.TrimSpace(journalMode))
		}

		if bundle.DBPath == "" {
			path, err := sqliteMainDBPath(diagCtx, db)
			if err != nil {
				appendCollectionError("resolve database path: %v", err)
			} else {
				bundle.DBPath = path
			}
		}
	}

	bundle.DBFileStats = collectSQLiteFileStat(bundle.DBPath)
	if bundle.DBPath != "" {
		bundle.WALFile = collectSQLiteFileStat(bundle.DBPath + "-wal")
		bundle.SHMFile = collectSQLiteFileStat(bundle.DBPath + "-shm")
	}

	bundle.Checkpoint.Enabled = sqliteIOERRCheckpointProbeEnabled()
	if bundle.Checkpoint.Enabled {
		bundle.Checkpoint.Attempted = true
		if db == nil {
			bundle.Checkpoint.Error = "sqlite db is unavailable"
		} else {
			busy, logFrames, checkpointed, err := probeSQLiteWALCheckpoint(diagCtx, db)
			if err != nil {
				bundle.Checkpoint.Error = err.Error()
				appendCollectionError("wal_checkpoint(PASSIVE): %v", err)
			} else {
				bundle.Checkpoint.Busy = busy
				bundle.Checkpoint.LogFrames = logFrames
				bundle.Checkpoint.Checkpointed = checkpointed
			}
		}
	}

	return bundle
}

func logSQLiteIOERRDiagnosticBundle(bundle sqliteIOERRDiagBundle) {
	attrs := make([]slog.Attr, 0, 64)
	attrs = append(attrs,
		slog.String("decision_reason", bundle.DecisionReason),
		slog.String("phase", bundle.Phase),
		slog.Int("item_index", bundle.ItemIndex),
		slog.Int("item_total", bundle.ItemTotal),
		slog.String("item_key", bundle.ItemKey),
		slog.Int("sqlite_code", bundle.SQLiteCode),
		slog.String("sqlite_code_name", bundle.SQLiteCodeName),
		slog.Int("db_open", bundle.DBOpen),
		slog.Int("db_in_use", bundle.DBInUse),
		slog.Int("db_idle", bundle.DBIdle),
		slog.Int64("db_wait_count", bundle.DBWaitCount),
		slog.String("db_wait_duration", bundle.DBWaitDuration),
		slog.Int("pragma_busy_timeout_ms", bundle.PragmaBusyTimeoutMS),
		slog.String("pragma_synchronous", bundle.PragmaSynchronous),
		slog.String("pragma_journal_mode", bundle.PragmaJournalMode),
		slog.String("db_path", bundle.DBPath),
	)

	if bundle.RunID > 0 {
		attrs = append(
			attrs,
			slog.Int64("run_id", bundle.RunID),
			slog.String("job_name", bundle.JobName),
			slog.String("triggered_by", bundle.TriggeredBy),
		)
	}

	attrs = appendFileStatAttrs(attrs, "db_file", bundle.DBFileStats)
	attrs = appendFileStatAttrs(attrs, "wal_file", bundle.WALFile)
	attrs = appendFileStatAttrs(attrs, "shm_file", bundle.SHMFile)

	attrs = append(
		attrs,
		slog.Bool("checkpoint_enabled", bundle.Checkpoint.Enabled),
		slog.Bool("checkpoint_attempted", bundle.Checkpoint.Attempted),
		slog.Int("checkpoint_busy", bundle.Checkpoint.Busy),
		slog.Int("checkpoint_log_frames", bundle.Checkpoint.LogFrames),
		slog.Int("checkpoint_checkpointed", bundle.Checkpoint.Checkpointed),
		slog.String("checkpoint_error", bundle.Checkpoint.Error),
	)
	if len(bundle.CollectionErrors) > 0 {
		attrs = append(attrs, slog.String("collection_errors", strings.Join(bundle.CollectionErrors, "; ")))
	}

	slog.LogAttrs(context.Background(), slog.LevelError, sqliteIOERRDiagEventPrefix, attrs...)
}

func appendFileStatAttrs(attrs []slog.Attr, prefix string, stat sqliteIOERRFileStat) []slog.Attr {
	attrs = append(
		attrs,
		slog.String(prefix+"_path", stat.Path),
		slog.Bool(prefix+"_exists", stat.Exists),
		slog.Int64(prefix+"_size_bytes", stat.SizeBytes),
		slog.String(prefix+"_mode", stat.Mode),
		slog.String(prefix+"_mod_time", stat.ModTime),
		slog.String(prefix+"_owner", stat.Owner),
		slog.String(prefix+"_error", stat.Error),
	)
	return attrs
}

func sqliteIOERRCheckpointProbeEnabled() bool {
	value := strings.TrimSpace(os.Getenv(sqliteIOERRDiagCheckpointEnvVar))
	if value == "" {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}
	return enabled
}

func probeSQLiteWALCheckpoint(ctx context.Context, db *sql.DB) (busy int, logFrames int, checkpointed int, err error) {
	if db == nil {
		return 0, 0, 0, fmt.Errorf("sqlite db is required")
	}
	if err := db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		return 0, 0, 0, fmt.Errorf("query PRAGMA wal_checkpoint(PASSIVE): %w", err)
	}
	return busy, logFrames, checkpointed, nil
}

func sqliteMainDBPath(ctx context.Context, db *sql.DB) (string, error) {
	if db == nil {
		return "", fmt.Errorf("sqlite db is required")
	}

	rows, err := db.QueryContext(ctx, `PRAGMA database_list`)
	if err != nil {
		return "", fmt.Errorf("query PRAGMA database_list: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			seq  int
			name string
			file string
		)
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return "", fmt.Errorf("scan PRAGMA database_list row: %w", err)
		}
		if strings.EqualFold(strings.TrimSpace(name), "main") {
			return strings.TrimSpace(file), nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate PRAGMA database_list rows: %w", err)
	}
	return "", fmt.Errorf("main database entry not found")
}

func collectSQLiteFileStat(path string) sqliteIOERRFileStat {
	path = strings.TrimSpace(path)
	stat := sqliteIOERRFileStat{Path: path}
	if path == "" {
		stat.Error = "path unavailable"
		return stat
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return stat
		}
		stat.Error = err.Error()
		return stat
	}

	stat.Exists = true
	stat.SizeBytes = info.Size()
	stat.Mode = info.Mode().String()
	stat.ModTime = info.ModTime().UTC().Format(time.RFC3339Nano)
	stat.Owner = sqliteFileOwner(info)
	return stat
}

func sqliteFileOwner(info os.FileInfo) string {
	if info == nil || info.Sys() == nil {
		return ""
	}

	sysValue := reflect.ValueOf(info.Sys())
	if !sysValue.IsValid() {
		return ""
	}
	if sysValue.Kind() == reflect.Pointer {
		if sysValue.IsNil() {
			return ""
		}
		sysValue = sysValue.Elem()
	}

	uid, uidOK := reflectIntField(sysValue, "Uid")
	gid, gidOK := reflectIntField(sysValue, "Gid")
	if !uidOK && !gidOK {
		return ""
	}

	if !uidOK {
		return fmt.Sprintf("?:%d", gid)
	}
	if !gidOK {
		return fmt.Sprintf("%d:?", uid)
	}
	return fmt.Sprintf("%d:%d", uid, gid)
}

func reflectIntField(v reflect.Value, fieldName string) (int64, bool) {
	if !v.IsValid() {
		return 0, false
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return 0, false
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int64(field.Uint()), true
	default:
		return 0, false
	}
}
