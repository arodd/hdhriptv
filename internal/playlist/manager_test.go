package playlist

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestManagerParseEachMatchesParse(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil)
	input := `#EXTM3U
#EXTINF:-1 tvg-id="cnn.us" group-title="News",CNN
http://example.com/cnn.ts
#EXTINF:-1 group-title="Sports",Sports One
http://example.com/sports.ts
`

	expected, err := manager.Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	actual := make([]Item, 0, len(expected))
	count, err := manager.ParseEach(strings.NewReader(input), func(item Item) error {
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

func TestManagerFetchAndParseEach(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("request method = %s, want GET", r.Method)
		}
		_, _ = w.Write([]byte(`#EXTM3U
#EXTINF:-1 group-title="News",News One
http://example.com/news-1.ts
#EXTINF:-1 group-title="News",News Two
http://example.com/news-2.ts
`))
	}))
	defer server.Close()

	manager := NewManager(server.Client())
	got := make([]Item, 0, 2)
	count, err := manager.FetchAndParseEach(t.Context(), server.URL, func(item Item) error {
		got = append(got, item)
		return nil
	})
	if err != nil {
		t.Fatalf("FetchAndParseEach() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("FetchAndParseEach count = %d, want 2", count)
	}
	if len(got) != 2 {
		t.Fatalf("emitted item count = %d, want 2", len(got))
	}
	if got[0].Name != "News One" || got[1].Name != "News Two" {
		t.Fatalf("unexpected names = [%q,%q]", got[0].Name, got[1].Name)
	}
}

func TestManagerFetchAndParseEachCallbackError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`#EXTM3U
#EXTINF:-1 group-title="News",News One
http://example.com/news-1.ts
`))
	}))
	defer server.Close()

	manager := NewManager(server.Client())
	wantErr := errors.New("stop")
	_, err := manager.FetchAndParseEach(t.Context(), server.URL, func(Item) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("FetchAndParseEach() error = %v, want %v", err, wantErr)
	}
}
