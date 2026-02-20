package jobs

import "testing"

func TestParseAnalysisErrorBuckets(t *testing.T) {
	summary := "channels=19 analyzed=103 cache_hits=0 reordered=12 analysis_errors=93 analysis_error_buckets=http_429:75,decode_ffprobe_json:13,http_404:5 enabled_only=true top_n_per_channel=0 limited_channels=0"
	got := ParseAnalysisErrorBuckets(summary)
	if len(got) != 3 {
		t.Fatalf("len(ParseAnalysisErrorBuckets) = %d, want 3", len(got))
	}
	if got["http_429"] != 75 {
		t.Fatalf("http_429 bucket = %d, want 75", got["http_429"])
	}
	if got["decode_ffprobe_json"] != 13 {
		t.Fatalf("decode_ffprobe_json bucket = %d, want 13", got["decode_ffprobe_json"])
	}
	if got["http_404"] != 5 {
		t.Fatalf("http_404 bucket = %d, want 5", got["http_404"])
	}
}

func TestParseAnalysisErrorBucketsReturnsNilWhenMissing(t *testing.T) {
	if got := ParseAnalysisErrorBuckets("channels=1 analyzed=0 analysis_errors=0"); got != nil {
		t.Fatalf("ParseAnalysisErrorBuckets(missing) = %#v, want nil", got)
	}
	if got := ParseAnalysisErrorBuckets("analysis_error_buckets=none"); got != nil {
		t.Fatalf("ParseAnalysisErrorBuckets(none) = %#v, want nil", got)
	}
}

func TestParseAutoPrioritizeSkipReasonBuckets(t *testing.T) {
	summary := "channels=19 analyzed=103 cache_hits=0 reordered=12 skipped_channels=3 analysis_errors=0 analysis_error_buckets=none skip_reason_buckets=source_load_channel_not_found:2,reorder_source_set_drift:1 enabled_only=true top_n_per_channel=0 limited_channels=0"
	got := ParseAutoPrioritizeSkipReasonBuckets(summary)
	if len(got) != 2 {
		t.Fatalf("len(ParseAutoPrioritizeSkipReasonBuckets) = %d, want 2", len(got))
	}
	if got["source_load_channel_not_found"] != 2 {
		t.Fatalf("source_load_channel_not_found bucket = %d, want 2", got["source_load_channel_not_found"])
	}
	if got["reorder_source_set_drift"] != 1 {
		t.Fatalf("reorder_source_set_drift bucket = %d, want 1", got["reorder_source_set_drift"])
	}
}

func TestParseAutoPrioritizeSkipReasonBucketsReturnsNilWhenMissing(t *testing.T) {
	if got := ParseAutoPrioritizeSkipReasonBuckets("channels=1 analyzed=0 skipped_channels=0"); got != nil {
		t.Fatalf("ParseAutoPrioritizeSkipReasonBuckets(missing) = %#v, want nil", got)
	}
	if got := ParseAutoPrioritizeSkipReasonBuckets("skip_reason_buckets=none"); got != nil {
		t.Fatalf("ParseAutoPrioritizeSkipReasonBuckets(none) = %#v, want nil", got)
	}
}

func TestClassifyAnalysisError(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "http status",
			in:   "ffprobe failed: exit status 1: stream: Server returned 429 Too Many Requests {  }",
			want: "http_429",
		},
		{
			name: "decode error",
			in:   "decode ffprobe JSON: invalid character 'h' looking for beginning of value",
			want: "decode_ffprobe_json",
		},
		{
			name: "timeout",
			in:   "context deadline exceeded",
			want: "timeout",
		},
		{
			name: "other",
			in:   "unexpected parser branch",
			want: "other",
		},
	}

	for _, tc := range tests {
		got := classifyAnalysisError(tc.in)
		if got != tc.want {
			t.Fatalf("%s: classifyAnalysisError(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}
