package simpledatalogs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// plan is a named string type, sent as its value.
type plan string

// flushWithin flushes with a deadline and returns the error.
func flushWithin(client *Client, timeout time.Duration) error {
	flushContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return client.Flush(flushContext)
}

func TestATrackedEventReachesIngestWithTheContractHeaders(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})

	client.Track("signup", Props{"plan": plan("pro"), "seats": 3, "trial": false}, WithUserID("42"))

	if err := flushWithin(client, 5*time.Second); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	request := fake.recorded()[0]
	expectedHeaders := map[string]string{
		"Authorization": "Bearer " + testKey,
		"Content-Type":  "application/x-ndjson",
		"User-Agent":    "simpledatalogs-go/" + Version,
	}

	for headerName, expected := range expectedHeaders {
		if got := request.headers.Get(headerName); got != expected {
			t.Errorf("%s: got %q, want %q", headerName, got, expected)
		}
	}

	if request.path != "/v1/ingest" {
		t.Errorf("path: got %q", request.path)
	}

	line := request.lines(t)[0]
	props := line["props"].(map[string]any)

	if line["event"] != "signup" || line["user_id"] != "42" || props["plan"] != "pro" || props["seats"] != float64(3) || props["trial"] != false {
		t.Errorf("unexpected line: %v", line)
	}

	if eventID, _ := line["event_id"].(string); len(eventID) != 36 || eventID[14] != '4' {
		t.Errorf("event_id is not a UUIDv4: %v", line["event_id"])
	}

	if _, err := time.Parse(time.RFC3339, line["ts"].(string)); err != nil {
		t.Errorf("ts is not ISO 8601: %v", line["ts"])
	}
}

func TestEventIDAndTSAreSetBeforeTheFirstAttemptAndKeptOnRetries(t *testing.T) {
	fake := newFakeIngest(t, scriptedAnswer{status: 503, headers: map[string]string{"Retry-After": "0"}}, scriptedAnswer{status: 500}, scriptedAnswer{status: 202})
	client := newTestClient(t, fake, Config{})

	client.Track("checkout", nil)
	time.Sleep(10 * time.Millisecond)

	if err := flushWithin(client, 10*time.Second); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	requests := fake.recorded()

	if len(requests) != 3 {
		t.Fatalf("got %d requests, want 3", len(requests))
	}

	for _, request := range requests[1:] {
		if !bytes.Equal(request.body, requests[0].body) {
			t.Fatalf("a retry changed the body:\n%s\n%s", requests[0].body, request.body)
		}
	}
}

func TestLargeBodiesAreGzippedAndStayUnderTheLimits(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{MaxRequestBytes: 20_000})

	for lineIndex := range 200 {
		client.Log(LevelInfo, fmt.Sprintf("line %d %s", lineIndex, strings.Repeat("x", 300)), nil)
	}

	if err := flushWithin(client, 10*time.Second); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if accepted := len(fake.acceptedLines(t)); accepted != 200 {
		t.Fatalf("got %d lines, want 200", accepted)
	}

	for _, request := range fake.recorded() {
		if request.headers.Get("Content-Encoding") != "gzip" || len(request.body) > 20_000 || len(request.lines(t)) > 50 {
			t.Fatalf("a request broke a cap: gzip %q, %d bytes, %d lines", request.headers.Get("Content-Encoding"), len(request.body), len(request.lines(t)))
		}
	}
}

func TestSmallBodiesAreNotGzipped(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})

	client.Track("tiny", nil)
	_ = flushWithin(client, 5*time.Second)

	if fake.recorded()[0].headers.Get("Content-Encoding") != "" {
		t.Fatalf("a tiny body should not be gzipped")
	}
}

func TestTheRequestCapNeverPasses512KB(t *testing.T) {
	client := newTestClient(t, newFakeIngest(t), Config{MaxRequestBytes: 10_000_000})

	if client.settings.maxRequestBytes != 524_288 {
		t.Fatalf("got %d", client.settings.maxRequestBytes)
	}
}

func TestAnOversizedEventIsSkippedAndAnOversizedLogLineIsCut(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{MaxRequestBytes: 4096})

	client.Track("huge", Props{"blob": strings.Repeat("x", 10_000)})
	client.Log(LevelError, strings.Repeat("y", 20_000), Props{"order_id": 7})

	if err := flushWithin(client, 5*time.Second); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	accepted := fake.acceptedLines(t)

	if len(accepted) != 1 || accepted[0]["props"].(map[string]any)["_truncated"] != true || len(accepted[0]["message"].(string)) >= 4096 {
		t.Fatalf("unexpected lines: %d", len(accepted))
	}
}

func TestTheBackgroundGoroutineFlushesOnItsInterval(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{FlushInterval: 100 * time.Millisecond})

	client.Track("background", nil)
	fake.waitForRequests(t, 1)
}

func TestAFullBatchWakesTheGoroutineAtOnce(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{FlushInterval: time.Minute, MaxBatchEvents: 5})

	for eventIndex := range 5 {
		client.Track("burst", Props{"index": eventIndex})
	}

	fake.waitForRequests(t, 1)
}

func TestCloseFlushesAndLaterCallsAreIgnored(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{FlushInterval: time.Minute})

	client.Track("last_words", nil)

	closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Close(closeContext); err != nil {
		t.Fatalf("Close: %v", err)
	}

	client.Track("after_close", nil)

	if accepted := fake.acceptedLines(t); len(accepted) != 1 || accepted[0]["event"] != "last_words" {
		t.Fatalf("unexpected lines: %v", accepted)
	}
}

func TestA401KeepsTheLinesAndFlushReportsThemUnsent(t *testing.T) {
	fake := newFakeIngest(t, scriptedAnswer{status: 401})
	client := newTestClient(t, fake, Config{})

	client.Track("kept", nil)
	err := flushWithin(client, time.Second)

	if !errors.Is(err, ErrUndelivered) || client.Pending() != 1 || len(fake.recorded()) != 1 {
		t.Fatalf("got %v with %d pending and %d requests", err, client.Pending(), len(fake.recorded()))
	}
}

func TestFlushWaitsOutRetryAfterWithinItsDeadline(t *testing.T) {
	fake := newFakeIngest(t, scriptedAnswer{status: 503, headers: map[string]string{"Retry-After": "1"}}, scriptedAnswer{status: 202})
	client := newTestClient(t, fake, Config{})

	client.Track("patient", nil)
	startedAt := time.Now()

	if err := flushWithin(client, 5*time.Second); err != nil || time.Since(startedAt) < time.Second {
		t.Fatalf("got %v after %v", err, time.Since(startedAt))
	}
}

func TestFlushGivesUpWhenRetryAfterPassesItsDeadline(t *testing.T) {
	fake := newFakeIngest(t, scriptedAnswer{status: 429, headers: map[string]string{"Retry-After": "3600"}})
	client := newTestClient(t, fake, Config{})

	client.Track("rate_limited", nil)
	startedAt := time.Now()

	if err := flushWithin(client, 2*time.Second); !errors.Is(err, ErrUndelivered) || time.Since(startedAt) > 1500*time.Millisecond {
		t.Fatalf("got %v after %v", err, time.Since(startedAt))
	}
}

func TestFlushWithoutADeadlineTriesOnce(t *testing.T) {
	fake := newFakeIngest(t, scriptedAnswer{status: 503})
	client := newTestClient(t, fake, Config{})

	client.Track("once", nil)

	if err := client.Flush(context.Background()); !errors.Is(err, ErrUndelivered) || len(fake.recorded()) != 1 {
		t.Fatalf("got %v and %d requests", err, len(fake.recorded()))
	}
}

func TestANetworkErrorKeepsTheLines(t *testing.T) {
	client, err := New(Config{Key: testKey, Endpoint: "http://127.0.0.1:9", RequestTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	if err != nil {
		t.Fatalf("New: %v", err)
	}

	client.Track("offline", nil)

	if err := client.Flush(context.Background()); !errors.Is(err, ErrUndelivered) || client.Pending() != 1 {
		t.Fatalf("got %v with %d pending", err, client.Pending())
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	fake := newFakeIngest(t, scriptedAnswer{status: 307, headers: map[string]string{"Location": "http://example.invalid/steal"}})
	client := newTestClient(t, fake, Config{})

	client.Track("stay_here", nil)

	if err := client.Flush(context.Background()); !errors.Is(err, ErrUndelivered) || len(fake.recorded()) != 1 {
		t.Fatalf("got %v and %d requests", err, len(fake.recorded()))
	}
}

func TestBadInputNeverPanics(t *testing.T) {
	var warnings bytes.Buffer
	client := newTestClient(t, newFakeIngest(t), Config{Logger: slog.New(slog.NewTextHandler(&warnings, nil))})

	client.Track("", nil)
	client.Track("ok", Props{"nested": map[string]int{"a": 1}, "nan": nanValue(), "kept": 1, "nil": nil})
	client.Log("", "", nil)
	client.Log("warn", "with nil option", nil, nil)

	if client.Pending() != 3 || !strings.Contains(warnings.String(), "non-empty event name") {
		t.Fatalf("got %d pending; warnings: %s", client.Pending(), warnings.String())
	}
}

func TestANilClientIsANoOp(t *testing.T) {
	var client *Client

	client.Track("nothing", nil)
	client.Log(LevelInfo, "nothing", nil)
	client.SetEnabled(true)

	if client.Pending() != 0 || client.Flush(context.Background()) != nil || client.Close(context.Background()) != nil {
		t.Fatalf("a nil client should do nothing")
	}
}

func TestPropsAreCappedAtOneHundred(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{DefaultProps: Props{"service": "api"}})

	wideProps := Props{}

	for propIndex := range 150 {
		wideProps[fmt.Sprintf("prop_%03d", propIndex)] = propIndex
	}

	client.Track("wide", wideProps)
	_ = flushWithin(client, 5*time.Second)

	if propCount := len(fake.acceptedLines(t)[0]["props"].(map[string]any)); propCount != 100 {
		t.Fatalf("got %d props", propCount)
	}
}

func TestDefaultPropsSitUnderTheCallProps(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{DefaultProps: Props{"service": "api", "region": "eu"}})

	client.Track("ping", Props{"region": "us"})
	_ = flushWithin(client, 5*time.Second)

	props := fake.acceptedLines(t)[0]["props"].(map[string]any)

	if len(props) != 2 || props["region"] != "us" || props["service"] != "api" {
		t.Fatalf("got %v", props)
	}
}

func TestLogLevelsAreNormalized(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})

	client.Log("WARN", "disk almost full", nil)
	client.Log("fatal", "out of memory", nil)
	client.Log("custom", "kept as sent", nil)
	_ = flushWithin(client, 5*time.Second)

	var levels []string

	for _, line := range fake.acceptedLines(t) {
		levels = append(levels, line["level"].(string))
	}

	if strings.Join(levels, ",") != "warning,critical,custom" {
		t.Fatalf("got %v", levels)
	}
}

func TestTimestampsCanBeGiven(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})

	client.Track("backfilled", nil, WithTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("CET", 3600))))
	_ = flushWithin(client, 5*time.Second)

	if ts := fake.acceptedLines(t)[0]["ts"]; ts != "2026-01-02T02:04:05.000Z" {
		t.Fatalf("got %v", ts)
	}
}

func TestAMissingKeyIsAnError(t *testing.T) {
	t.Setenv("SIMPLEDATALOGS_KEY", "")

	if client, err := New(Config{}); !errors.Is(err, ErrMissingKey) || client != nil {
		t.Fatalf("got %v", err)
	}
}

func TestTheKeyAndEndpointComeFromTheEnvironment(t *testing.T) {
	fake := newFakeIngest(t)
	t.Setenv("SIMPLEDATALOGS_KEY", "sdl_sec_from_env")
	t.Setenv("SIMPLEDATALOGS_ENDPOINT", fake.server.URL+"/")

	client, err := New(Config{})

	if err != nil {
		t.Fatalf("New: %v", err)
	}

	client.Track("configured", nil)
	_ = client.Close(context.Background())

	if got := fake.recorded()[0].headers.Get("Authorization"); got != "Bearer sdl_sec_from_env" {
		t.Fatalf("got %q", got)
	}
}

func TestTheDefaultEndpointIsTheEdgeHost(t *testing.T) {
	resolved, err := resolveSettings(Config{Key: "sdl_sec_x"})

	if err != nil || resolved.ingestURL != "https://edge.simpledatalogs.com/v1/ingest" {
		t.Fatalf("got %q, %v", resolved.ingestURL, err)
	}
}

func TestOptingOutDeletesUnsentLines(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{FlushInterval: time.Minute})

	client.Track("before_opt_out", nil)
	client.SetEnabled(false)
	client.Track("while_off", nil)

	if client.Pending() != 0 {
		t.Fatalf("opting out should empty the queue")
	}

	client.SetEnabled(true)
	client.Track("after_opt_in", nil)
	_ = flushWithin(client, 5*time.Second)

	if accepted := fake.acceptedLines(t); len(accepted) != 1 || accepted[0]["event"] != "after_opt_in" {
		t.Fatalf("got %v", accepted)
	}
}

func TestConcurrentTrackingIsSafe(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{FlushInterval: 20 * time.Millisecond, MaxBatchEvents: 7})
	done := make(chan struct{})

	for goroutineIndex := range 8 {
		go func() {
			for eventIndex := range 50 {
				client.Track("concurrent", Props{"goroutine": goroutineIndex, "index": eventIndex})
			}

			done <- struct{}{}
		}()
	}

	for range 8 {
		<-done
	}

	if err := flushWithin(client, 10*time.Second); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	seenIDs := map[any]bool{}

	for _, line := range fake.acceptedLines(t) {
		seenIDs[line["event_id"]] = true
	}

	if len(seenIDs) != 400 {
		t.Fatalf("got %d distinct events, want 400", len(seenIDs))
	}
}

// nanValue is a NaN the compiler can't fold.
func nanValue() float64 {
	zero := 0.0
	return zero / zero
}
