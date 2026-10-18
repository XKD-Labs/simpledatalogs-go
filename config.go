package simpledatalogs

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// hardMaxRequestBytes is the ingest API's 512 KB body limit; no batch passes it, even before gzip.
const hardMaxRequestBytes = 524_288

// Config holds the client's settings; the zero value reads the key from SIMPLEDATALOGS_KEY.
type Config struct {
	// Key is the ingest key; empty reads SIMPLEDATALOGS_KEY. Use a secret key (sdl_sec_…) on servers.
	Key string

	// Endpoint is the ingest host; empty reads SIMPLEDATALOGS_ENDPOINT, else DefaultEndpoint.
	Endpoint string

	// FlushInterval is how often the background goroutine sends (default 5 s).
	FlushInterval time.Duration

	// MaxBatchEvents is the most lines per request (default 50).
	MaxBatchEvents int

	// MaxRequestBytes is the most bytes per request before gzip (default 256,000; never above 512 KB).
	MaxRequestBytes int

	// MaxQueueEvents and MaxQueueBytes bound the queue; past them the oldest lines go (defaults 10,000 and 10 MB).
	MaxQueueEvents int
	MaxQueueBytes  int

	// RequestTimeout bounds each request (default 15 s).
	RequestTimeout time.Duration

	// DisableGzip sends bodies uncompressed; by default bodies of 1 KB and more are gzipped.
	DisableGzip bool

	// DefaultProps go on every line, under the call's own props.
	DefaultProps Props

	// HTTPClient sends the requests (default: a client of its own); redirects are never followed.
	HTTPClient *http.Client

	// Logger receives the SDK's own warnings, each kind once (default: text on stderr).
	Logger *slog.Logger

	// Disabled keeps the client silent: nothing is queued or sent.
	Disabled bool
}

// ErrMissingKey is returned by New when there is no key and SIMPLEDATALOGS_KEY is empty.
var ErrMissingKey = errors.New("simpledatalogs: no ingest key (set Config.Key or SIMPLEDATALOGS_KEY)")

// settings are a Config with every default applied and every cap clamped.
type settings struct {
	ingestKey       string
	ingestURL       string
	flushInterval   time.Duration
	maxBatchEvents  int
	maxRequestBytes int
	maxQueueEvents  int
	maxQueueBytes   int
	requestTimeout  time.Duration
	shouldGzip      bool
	isDisabled      bool
}

// resolveSettings applies the environment and the defaults, and checks the key and endpoint.
func resolveSettings(config Config) (settings, error) {
	// The key decides the environment; without one nothing can be sent.
	ingestKey := strings.TrimSpace(config.Key)

	if ingestKey == "" {
		ingestKey = strings.TrimSpace(os.Getenv("SIMPLEDATALOGS_KEY"))
	}

	// The endpoint is the host; the path is the ingest API's.
	endpoint := strings.TrimSpace(config.Endpoint)

	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("SIMPLEDATALOGS_ENDPOINT"))
	}

	if endpoint == "" {
		endpoint = DefaultEndpoint
	}

	endpoint = strings.TrimRight(endpoint, "/")

	resolved := settings{
		ingestKey:       ingestKey,
		ingestURL:       endpoint + "/v1/ingest",
		flushInterval:   durationOrDefault(config.FlushInterval, 5*time.Second),
		maxBatchEvents:  intOrDefault(config.MaxBatchEvents, 50),
		maxRequestBytes: min(max(intOrDefault(config.MaxRequestBytes, 256_000), 1024), hardMaxRequestBytes),
		maxQueueEvents:  intOrDefault(config.MaxQueueEvents, 10_000),
		maxQueueBytes:   max(intOrDefault(config.MaxQueueBytes, 10_000_000), 1024),
		requestTimeout:  durationOrDefault(config.RequestTimeout, 15*time.Second),
		shouldGzip:      !config.DisableGzip,
		isDisabled:      config.Disabled,
	}

	if ingestKey == "" && !config.Disabled {
		return resolved, ErrMissingKey
	}

	if !strings.HasPrefix(endpoint, "https://") && !strings.HasPrefix(endpoint, "http://") {
		return resolved, fmt.Errorf("simpledatalogs: the endpoint %q is not an http(s) URL", endpoint)
	}

	return resolved, nil
}

// durationOrDefault is the value when positive, else the fallback.
func durationOrDefault(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}

	return fallback
}

// intOrDefault is the value when positive, else the fallback.
func intOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}

	return fallback
}
