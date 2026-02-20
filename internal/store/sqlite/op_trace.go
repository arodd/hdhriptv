package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

const (
	sqliteIOERRTraceEventPrefix = "sqlite_ioerr_trace_dump"

	sqliteIOERRTraceEnabledEnvVar      = "HDHRIPTV_SQLITE_IOERR_TRACE_ENABLED"
	sqliteIOERRTraceRingSizeEnvVar     = "HDHRIPTV_SQLITE_IOERR_TRACE_RING_SIZE"
	sqliteIOERRTraceDumpLimitEnvVar    = "HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_LIMIT"
	sqliteIOERRTraceDumpIntervalEnvVar = "HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_INTERVAL"

	defaultSQLiteIOERRTraceRingSize     = 256
	defaultSQLiteIOERRTraceDumpLimit    = 64
	defaultSQLiteIOERRTraceDumpInterval = 30 * time.Second

	minSQLiteIOERRTraceRingSize  = 16
	maxSQLiteIOERRTraceRingSize  = 16384
	minSQLiteIOERRTraceDumpLimit = 1
)

type sqliteIOERRTraceConfig struct {
	Enabled      bool
	RingSize     int
	DumpLimit    int
	DumpInterval time.Duration
}

func loadSQLiteIOERRTraceConfigFromEnv() sqliteIOERRTraceConfig {
	cfg := sqliteIOERRTraceConfig{
		Enabled:      parseSQLiteIOERRTraceBoolEnv(sqliteIOERRTraceEnabledEnvVar, false),
		RingSize:     parseSQLiteIOERRTraceBoundedIntEnv(sqliteIOERRTraceRingSizeEnvVar, defaultSQLiteIOERRTraceRingSize, minSQLiteIOERRTraceRingSize, maxSQLiteIOERRTraceRingSize),
		DumpLimit:    parseSQLiteIOERRTraceBoundedIntEnv(sqliteIOERRTraceDumpLimitEnvVar, defaultSQLiteIOERRTraceDumpLimit, minSQLiteIOERRTraceDumpLimit, maxSQLiteIOERRTraceRingSize),
		DumpInterval: parseSQLiteIOERRTraceDurationEnv(sqliteIOERRTraceDumpIntervalEnvVar, defaultSQLiteIOERRTraceDumpInterval),
	}
	if cfg.DumpLimit > cfg.RingSize {
		cfg.DumpLimit = cfg.RingSize
	}
	return cfg
}

func parseSQLiteIOERRTraceBoolEnv(key string, def bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return def
	}
	return parsed
}

func parseSQLiteIOERRTraceBoundedIntEnv(key string, def, min, max int) int {
	if def < min {
		def = min
	}
	if max > 0 && def > max {
		def = max
	}

	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	if parsed < min {
		return min
	}
	if max > 0 && parsed > max {
		return max
	}
	return parsed
}

func parseSQLiteIOERRTraceDurationEnv(key string, def time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return def
	}
	if parsed < 0 {
		return 0
	}
	return parsed
}

type sqliteIOERRTraceEntry struct {
	TimestampUnixNano int64
	DurationMicros    int64
	Operation         string
	Phase             string
	ErrorClass        string
	SQLiteCode        int
	SQLiteCodeName    string
}

type sqliteIOERRTraceDumpDecision struct {
	Emitted    bool
	Reason     string
	EntryCount int
}

type sqliteIOERRTraceRing struct {
	mu sync.Mutex

	entries []sqliteIOERRTraceEntry
	next    int
	count   int

	dumpLimit     int
	dumpInterval  time.Duration
	nextDumpAfter time.Time
}

func newSQLiteIOERRTraceRing(cfg sqliteIOERRTraceConfig) *sqliteIOERRTraceRing {
	if !cfg.Enabled {
		return nil
	}

	if cfg.RingSize < minSQLiteIOERRTraceRingSize {
		cfg.RingSize = minSQLiteIOERRTraceRingSize
	}
	if cfg.RingSize > maxSQLiteIOERRTraceRingSize {
		cfg.RingSize = maxSQLiteIOERRTraceRingSize
	}
	if cfg.DumpLimit < minSQLiteIOERRTraceDumpLimit {
		cfg.DumpLimit = minSQLiteIOERRTraceDumpLimit
	}
	if cfg.DumpLimit > cfg.RingSize {
		cfg.DumpLimit = cfg.RingSize
	}
	if cfg.DumpInterval < 0 {
		cfg.DumpInterval = 0
	}

	return &sqliteIOERRTraceRing{
		entries:       make([]sqliteIOERRTraceEntry, cfg.RingSize),
		dumpLimit:     cfg.DumpLimit,
		dumpInterval:  cfg.DumpInterval,
		nextDumpAfter: time.Time{},
	}
}

type sqliteIOERRTraceToken struct {
	ring          *sqliteIOERRTraceRing
	operation     string
	phase         string
	startUnixNano int64
}

func (s *Store) beginSQLiteIOERRTrace(operation, phase string) sqliteIOERRTraceToken {
	if s == nil || s.dbIOERRTrace == nil {
		return sqliteIOERRTraceToken{}
	}
	return sqliteIOERRTraceToken{
		ring:          s.dbIOERRTrace,
		operation:     operation,
		phase:         phase,
		startUnixNano: time.Now().UnixNano(),
	}
}

func (s *Store) endSQLiteIOERRTrace(token sqliteIOERRTraceToken, err error) {
	if token.ring == nil {
		return
	}
	token.ring.record(token.operation, token.phase, token.startUnixNano, err)
}

func (r *sqliteIOERRTraceRing) record(operation, phase string, startUnixNano int64, err error) {
	if r == nil {
		return
	}

	completedUnixNano := time.Now().UnixNano()
	durationMicros := int64(0)
	if startUnixNano > 0 && completedUnixNano >= startUnixNano {
		durationMicros = (completedUnixNano - startUnixNano) / int64(time.Microsecond)
	}

	if operation == "" {
		operation = "unknown_op"
	}
	if phase == "" {
		phase = "unknown_phase"
	}

	errorClass, sqliteCode, sqliteCodeName := classifySQLiteTraceError(err)
	entry := sqliteIOERRTraceEntry{
		TimestampUnixNano: completedUnixNano,
		DurationMicros:    durationMicros,
		Operation:         operation,
		Phase:             phase,
		ErrorClass:        errorClass,
		SQLiteCode:        sqliteCode,
		SQLiteCodeName:    sqliteCodeName,
	}

	r.mu.Lock()
	r.entries[r.next] = entry
	r.next++
	if r.next >= len(r.entries) {
		r.next = 0
	}
	if r.count < len(r.entries) {
		r.count++
	}
	r.mu.Unlock()
}

func classifySQLiteTraceError(err error) (errorClass string, sqliteCode int, sqliteCodeName string) {
	if err == nil {
		return "ok", 0, ""
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled", 0, ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline", 0, ""
	}
	if code, codeName, ok := sqliteErrorDetails(err); ok {
		return "sqlite", code, codeName
	}
	return "other", 0, ""
}

func maybeEmitSQLiteIOERRTraceDump(
	ctx context.Context,
	trace *sqliteIOERRTraceRing,
	sqliteCode int,
	sqliteCodeName string,
) sqliteIOERRTraceDumpDecision {
	if trace == nil {
		return sqliteIOERRTraceDumpDecision{Emitted: false, Reason: "trace_disabled", EntryCount: 0}
	}

	entries, reason, allowed := trace.snapshotForDumpAt(time.Now().UTC())
	if !allowed {
		return sqliteIOERRTraceDumpDecision{Emitted: false, Reason: reason, EntryCount: 0}
	}

	logSQLiteIOERRTraceDump(ctx, entries, reason, sqliteCode, sqliteCodeName)
	return sqliteIOERRTraceDumpDecision{Emitted: true, Reason: reason, EntryCount: len(entries)}
}

func (r *sqliteIOERRTraceRing) snapshotForDumpAt(now time.Time) ([]sqliteIOERRTraceEntry, string, bool) {
	if r == nil {
		return nil, "trace_disabled", false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count <= 0 {
		return nil, "trace_empty", false
	}
	if !r.nextDumpAfter.IsZero() && now.Before(r.nextDumpAfter) {
		return nil, fmt.Sprintf("trace_rate_limited_until=%s", r.nextDumpAfter.UTC().Format(time.RFC3339)), false
	}

	if r.dumpInterval > 0 {
		r.nextDumpAfter = now.Add(r.dumpInterval)
	}

	limit := r.dumpLimit
	if limit <= 0 || limit > r.count {
		limit = r.count
	}

	start := r.next - r.count
	if start < 0 {
		start += len(r.entries)
	}
	skip := r.count - limit
	start = (start + skip) % len(r.entries)

	out := make([]sqliteIOERRTraceEntry, 0, limit)
	for i := 0; i < limit; i++ {
		index := start + i
		if index >= len(r.entries) {
			index -= len(r.entries)
		}
		out = append(out, r.entries[index])
	}

	return out, fmt.Sprintf("trace_dump_limit=%d", limit), true
}

func logSQLiteIOERRTraceDump(
	ctx context.Context,
	entries []sqliteIOERRTraceEntry,
	reason string,
	sqliteCode int,
	sqliteCodeName string,
) {
	if len(entries) == 0 {
		return
	}

	attrs := make([]slog.Attr, 0, 12)
	attrs = append(
		attrs,
		slog.String("decision_reason", reason),
		slog.Int("entry_count", len(entries)),
		slog.Int("sqlite_code", sqliteCode),
		slog.String("sqlite_code_name", strings.TrimSpace(sqliteCodeName)),
		slog.String("entries", formatSQLiteIOERRTraceEntries(entries)),
	)
	if runMeta, ok := jobs.RunMetadataFromContext(ctx); ok {
		attrs = append(
			attrs,
			slog.Int64("run_id", runMeta.RunID),
			slog.String("job_name", runMeta.JobName),
			slog.String("triggered_by", runMeta.TriggeredBy),
		)
	}

	slog.LogAttrs(context.Background(), slog.LevelError, sqliteIOERRTraceEventPrefix, attrs...)
}

func formatSQLiteIOERRTraceEntries(entries []sqliteIOERRTraceEntry) string {
	if len(entries) == 0 {
		return ""
	}

	var b strings.Builder
	for i := range entries {
		if i > 0 {
			b.WriteString(" | ")
		}
		entry := entries[i]
		b.WriteString("ts=")
		b.WriteString(time.Unix(0, entry.TimestampUnixNano).UTC().Format(time.RFC3339Nano))
		b.WriteString(" op=")
		b.WriteString(entry.Operation)
		b.WriteString(" phase=")
		b.WriteString(entry.Phase)
		b.WriteString(" dur_us=")
		b.WriteString(strconv.FormatInt(entry.DurationMicros, 10))
		b.WriteString(" err=")
		b.WriteString(entry.ErrorClass)
		if entry.SQLiteCode > 0 {
			b.WriteString(" code=")
			b.WriteString(strconv.Itoa(entry.SQLiteCode))
			if entry.SQLiteCodeName != "" {
				b.WriteString(" code_name=")
				b.WriteString(entry.SQLiteCodeName)
			}
		}
	}
	return b.String()
}
