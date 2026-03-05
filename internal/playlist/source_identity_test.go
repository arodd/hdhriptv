package playlist

import "testing"

func TestCanonicalPlaylistSourceName(t *testing.T) {
	if got, want := CanonicalPlaylistSourceName("  BackUp  "), "backup"; got != want {
		t.Fatalf("CanonicalPlaylistSourceName() = %q, want %q", got, want)
	}
	if got := CanonicalPlaylistSourceName("   "); got != "" {
		t.Fatalf("CanonicalPlaylistSourceName(empty) = %q, want empty", got)
	}
}

func TestCanonicalPlaylistSourceURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "scheme and host normalized",
			raw:  "  HTTP://Example.COM/live/Foo.m3u8?Token=ABC#Frag  ",
			want: "http://example.com/live/Foo.m3u8?Token=ABC#Frag",
		},
		{
			name: "parse fallback lowercases raw value",
			raw:  "HTTP://EXAMPLE.COM/%zz",
			want: "http://example.com/%zz",
		},
		{
			name: "empty string",
			raw:  "   ",
			want: "",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := CanonicalPlaylistSourceURL(tc.raw); got != tc.want {
				t.Fatalf("CanonicalPlaylistSourceURL(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
