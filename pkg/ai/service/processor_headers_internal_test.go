package service

import (
	"bytes"
	"log/slog"
	"net/http"
	neturl "net/url"
	"strings"
	"testing"
)

func TestSameOrigin(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"http://processor:8000", "http://processor:8000/api/v1/a2a/x", true},
		{"http://PROCESSOR:8000", "http://processor:8000/x", true},
		{"http://processor", "http://processor:80/x", true},
		{"https://processor", "https://processor:443/x", true},
		{"http://processor", "http://processor:8080/x", false},
		{"http://processor:8000", "https://processor:8000/x", false},
		{"http://processor:8000", "http://elsewhere:8000/x", false},
	}
	for _, tc := range cases {
		a, _ := neturl.Parse(tc.a)
		b, _ := neturl.Parse(tc.b)
		if got := sameOrigin(a, b); got != tc.want {
			t.Errorf("sameOrigin(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// The warning has to say which side is wrong: our config, or the bot's URL.
func TestApplyProcessorHeaders_LogsWhyItDropped(t *testing.T) {
	cases := map[string]struct{ env, reason string }{
		"unset":     {"", "is unset or has no host"},
		"no host":   {"processor:8000", "is unset or has no host"},
		"elsewhere": {"http://processor:8000", "outgoing_url is not the AI Processor"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })
			t.Setenv(processorURLEnv, tc.env)

			req, _ := http.NewRequest(http.MethodPost, "http://elsewhere:8000/x", nil)
			applyProcessorHeaders(req, map[string]string{"X-Request-Context": "ctx-1"})

			if req.Header.Get("X-Request-Context") != "" {
				t.Fatal("header was sent")
			}
			if !strings.Contains(logs.String(), tc.reason) {
				t.Errorf("log = %q, want reason %q", logs.String(), tc.reason)
			}
		})
	}
}
