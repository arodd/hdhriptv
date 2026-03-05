package m3u

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	input := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" tvg-logo="https://img/logo.png" group-title="News",BBC News
http://example.com/news.ts?token=one
#EXTINF:-1 group-title="Sports",Sports One
http://example.com/sports.m3u8
`

	items, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}

	if !strings.HasPrefix(items[0].ItemKey, "src:bbc.news:") {
		t.Fatalf("items[0].ItemKey = %q, want src:bbc.news:*", items[0].ItemKey)
	}
	if items[0].ChannelKey != "tvg:bbc.news" {
		t.Fatalf("items[0].ChannelKey = %q, want tvg:bbc.news", items[0].ChannelKey)
	}
	if items[0].TVGLogo != "https://img/logo.png" {
		t.Fatalf("items[0].TVGLogo = %q, want logo URL", items[0].TVGLogo)
	}
	if items[0].Group != "News" {
		t.Fatalf("items[0].Group = %q, want News", items[0].Group)
	}

	if !strings.HasPrefix(items[1].ItemKey, "src:") {
		t.Fatalf("items[1].ItemKey = %q, want src:*", items[1].ItemKey)
	}
	if got := len(strings.TrimPrefix(items[1].ItemKey, "src:")); got != 16 {
		t.Fatalf("hashed key length = %d, want 16", got)
	}
	if items[1].ChannelKey != "name:sports one" {
		t.Fatalf("items[1].ChannelKey = %q, want name:sports one", items[1].ChannelKey)
	}
}

func TestItemKeyIgnoresVolatileQueryAndFragment(t *testing.T) {
	inputA := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" group-title="News",BBC News
http://example.com/stream.ts?token=abc&auth=123#frag
`
	inputB := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" group-title="News",BBC News
http://example.com/stream.ts?token=def&auth=456
`

	a, err := Parse(strings.NewReader(inputA))
	if err != nil {
		t.Fatalf("Parse(A) error = %v", err)
	}
	b, err := Parse(strings.NewReader(inputB))
	if err != nil {
		t.Fatalf("Parse(B) error = %v", err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("unexpected item counts: %d vs %d", len(a), len(b))
	}
	if a[0].ItemKey != b[0].ItemKey {
		t.Fatalf("stable keys differ: %q vs %q", a[0].ItemKey, b[0].ItemKey)
	}
	if a[0].ChannelKey != b[0].ChannelKey {
		t.Fatalf("channel keys differ: %q vs %q", a[0].ChannelKey, b[0].ChannelKey)
	}
}

func TestItemKeyKeepsNonVolatileQueryParameters(t *testing.T) {
	inputA := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" group-title="News",BBC News HD
http://example.com/stream.ts?variant=hd
`
	inputB := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" group-title="News",BBC News SD
http://example.com/stream.ts?variant=sd
`

	a, err := Parse(strings.NewReader(inputA))
	if err != nil {
		t.Fatalf("Parse(A) error = %v", err)
	}
	b, err := Parse(strings.NewReader(inputB))
	if err != nil {
		t.Fatalf("Parse(B) error = %v", err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("unexpected item counts: %d vs %d", len(a), len(b))
	}
	if a[0].ItemKey == b[0].ItemKey {
		t.Fatalf("distinct variant query values collapsed to one item_key: %q", a[0].ItemKey)
	}
}

func TestItemKeyNormalizesNonVolatileQueryOrdering(t *testing.T) {
	inputA := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" group-title="News",BBC News
http://example.com/stream.ts?variant=hd&lang=en
`
	inputB := `#EXTM3U
#EXTINF:-1 tvg-id="bbc.news" group-title="News",BBC News
http://example.com/stream.ts?lang=en&variant=hd
`

	a, err := Parse(strings.NewReader(inputA))
	if err != nil {
		t.Fatalf("Parse(A) error = %v", err)
	}
	b, err := Parse(strings.NewReader(inputB))
	if err != nil {
		t.Fatalf("Parse(B) error = %v", err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("unexpected item counts: %d vs %d", len(a), len(b))
	}
	if a[0].ItemKey != b[0].ItemKey {
		t.Fatalf("query ordering changed item_key: %q vs %q", a[0].ItemKey, b[0].ItemKey)
	}
}

func TestDuplicateTVGIDKeepsDistinctSources(t *testing.T) {
	input := `#EXTM3U
#EXTINF:-1 tvg-id="dup.id" group-title="Group",Duplicate One
http://example.com/one.ts
#EXTINF:-1 tvg-id="dup.id" group-title="Group",Duplicate Two
http://example.com/two.ts
`
	items, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}
	if items[0].ChannelKey != "tvg:dup.id" || items[1].ChannelKey != "tvg:dup.id" {
		t.Fatalf("channel keys = %q and %q, want tvg:dup.id", items[0].ChannelKey, items[1].ChannelKey)
	}
	if items[0].ItemKey == items[1].ItemKey {
		t.Fatalf("duplicate sources collapsed to one item_key: %q", items[0].ItemKey)
	}
}

func TestTVGIDCaseInsensitiveNormalization(t *testing.T) {
	inputUpper := `#EXTM3U
#EXTINF:-1 tvg-id="CNN.us" group-title="News",CNN Upper
http://example.com/cnn.ts?token=one
`
	inputLower := `#EXTM3U
#EXTINF:-1 tvg-id="cnn.US" group-title="News",CNN Lower
http://example.com/cnn.ts?token=two
`

	upper, err := Parse(strings.NewReader(inputUpper))
	if err != nil {
		t.Fatalf("Parse(upper) error = %v", err)
	}
	lower, err := Parse(strings.NewReader(inputLower))
	if err != nil {
		t.Fatalf("Parse(lower) error = %v", err)
	}
	if len(upper) != 1 || len(lower) != 1 {
		t.Fatalf("unexpected item counts: %d vs %d", len(upper), len(lower))
	}
	if upper[0].ChannelKey != "tvg:cnn.us" || lower[0].ChannelKey != "tvg:cnn.us" {
		t.Fatalf("channel keys = %q and %q, want tvg:cnn.us", upper[0].ChannelKey, lower[0].ChannelKey)
	}
	if upper[0].ItemKey != lower[0].ItemKey {
		t.Fatalf("item keys differ by tvg-id case: %q vs %q", upper[0].ItemKey, lower[0].ItemKey)
	}
}

func TestParseEachMatchesParse(t *testing.T) {
	input := buildBenchmarkPlaylist(2000)

	expected, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	actual := make([]Item, 0, len(expected))
	count, err := ParseEach(strings.NewReader(input), func(item Item) error {
		actual = append(actual, item)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseEach() error = %v", err)
	}
	if count != len(expected) {
		t.Fatalf("ParseEach count = %d, want %d", count, len(expected))
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("ParseEach output mismatch")
	}
}

func TestParseEachCallbackError(t *testing.T) {
	input := `#EXTM3U
#EXTINF:-1 group-title="News",News One
http://example.com/news-1.ts
#EXTINF:-1 group-title="News",News Two
http://example.com/news-2.ts
`

	wantErr := errors.New("stop")
	count, err := ParseEach(strings.NewReader(input), func(item Item) error {
		if strings.Contains(item.Name, "Two") {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ParseEach() error = %v, want %v", err, wantErr)
	}
	if count != 1 {
		t.Fatalf("ParseEach count = %d, want 1", count)
	}
}

func BenchmarkParse(b *testing.B) {
	input := buildBenchmarkPlaylist(10000)
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for i := 0; i < b.N; i++ {
		if _, err := Parse(strings.NewReader(input)); err != nil {
			b.Fatalf("Parse() error = %v", err)
		}
	}
}

func BenchmarkParseEach(b *testing.B) {
	input := buildBenchmarkPlaylist(10000)
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for i := 0; i < b.N; i++ {
		count, err := ParseEach(strings.NewReader(input), func(Item) error { return nil })
		if err != nil {
			b.Fatalf("ParseEach() error = %v", err)
		}
		if count != 10000 {
			b.Fatalf("ParseEach count = %d, want 10000", count)
		}
	}
}

func buildBenchmarkPlaylist(itemCount int) string {
	var b strings.Builder
	b.Grow(itemCount * 120)
	b.WriteString("#EXTM3U\n")
	for i := 0; i < itemCount; i++ {
		fmt.Fprintf(&b, "#EXTINF:-1 tvg-id=\"bench.%d\" group-title=\"Bench\",Bench %d\n", i, i)
		fmt.Fprintf(&b, "http://example.com/bench/%d.ts?token=%d\n", i, i)
	}
	return b.String()
}
