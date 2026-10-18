package simpledatalogs

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestRecordsBecomeLogLinesWithLevelTimeAndAttrs(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})
	logger := slog.New(NewHandler(client, nil)).With("service", "api").WithGroup("http")

	logger.Warn("Payment failed", "status", 502, "took", 1500*time.Millisecond, slog.Group("route", "name", "checkout"))
	_ = flushWithin(client, 5*time.Second)

	line := fake.acceptedLines(t)[0]
	props := line["props"].(map[string]any)
	expectedProps := map[string]any{"service": "api", "http.status": float64(502), "http.took": "1.5s", "http.route.name": "checkout"}

	if line["level"] != "warning" || line["message"] != "Payment failed" || len(props) != len(expectedProps) {
		t.Fatalf("unexpected line: %v", line)
	}

	for propName, expected := range expectedProps {
		if props[propName] != expected {
			t.Errorf("%s: got %v, want %v", propName, props[propName], expected)
		}
	}

	if ts, err := time.Parse(time.RFC3339, line["ts"].(string)); err != nil || time.Since(ts) > time.Minute {
		t.Errorf("ts should be the record's time: %v", line["ts"])
	}
}

func TestIdentityAttrsFillIdentityFields(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})
	logger := slog.New(NewHandler(client, nil))

	logger.Info("signed in", "user_id", 42, "session_id", "s-1", "err", errors.New("none"))
	_ = flushWithin(client, 5*time.Second)

	line := fake.acceptedLines(t)[0]

	if line["user_id"] != "42" || line["session_id"] != "s-1" || line["props"].(map[string]any)["err"] != "none" {
		t.Fatalf("unexpected line: %v", line)
	}
}

func TestSlogLevelsMapToTheIngestLevels(t *testing.T) {
	fake := newFakeIngest(t)
	client := newTestClient(t, fake, Config{})
	logger := slog.New(NewHandler(client, &HandlerOptions{Level: slog.Level(-8)}))

	for _, level := range []slog.Level{-8, slog.LevelDebug, slog.LevelInfo, 2, slog.LevelWarn, slog.LevelError, slog.LevelError + 4} {
		logger.Log(context.Background(), level, "level")
	}

	_ = flushWithin(client, 5*time.Second)

	var levels []string

	for _, line := range fake.acceptedLines(t) {
		levels = append(levels, line["level"].(string))
	}

	if strings.Join(levels, ",") != "debug,debug,info,info,warning,error,critical" {
		t.Fatalf("got %v", levels)
	}
}

func TestTheHandlerSkipsRecordsBelowItsLevel(t *testing.T) {
	client := newTestClient(t, newFakeIngest(t), Config{})
	logger := slog.New(NewHandler(client, nil))

	logger.Debug("too quiet")

	if client.Pending() != 0 {
		t.Fatalf("a debug record should be skipped at the default info level")
	}
}

func TestTheSDKNeverShipsItsOwnWarnings(t *testing.T) {
	client := newTestClient(t, newFakeIngest(t), Config{})
	client.logger = slog.New(NewHandler(client, nil))

	client.Track("", nil)

	if client.Pending() != 0 {
		t.Fatalf("the SDK's own warning was shipped")
	}
}
