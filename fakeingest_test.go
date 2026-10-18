package simpledatalogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// testKey is shaped like a real secret key, for tests only.
const testKey = "sdl_sec_test_0000000000000000"

// scriptedAnswer is one status the fake ingest answers, with its headers.
type scriptedAnswer struct {
	status  int
	headers map[string]string
}

// recordedRequest is one request as ingest saw it.
type recordedRequest struct {
	path    string
	headers http.Header
	rawBody []byte
	body    []byte
	status  int
}

// lines decodes the NDJSON body.
func (request recordedRequest) lines(t *testing.T) []map[string]any {
	t.Helper()

	var decodedLines []map[string]any

	for _, line := range bytes.Split(request.body, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var decoded map[string]any

		if err := json.Unmarshal(line, &decoded); err != nil {
			t.Fatalf("a line is not JSON: %q", line)
		}

		decodedLines = append(decodedLines, decoded)
	}

	return decodedLines
}

// fakeIngest answers from a script (the last answer repeats) and records every request.
type fakeIngest struct {
	server   *httptest.Server
	mutex    sync.Mutex
	answers  []scriptedAnswer
	requests []recordedRequest
}

// newFakeIngest starts a fake that answers 202 unless answers are given; it stops with the test.
func newFakeIngest(t *testing.T, answers ...scriptedAnswer) *fakeIngest {
	t.Helper()

	if len(answers) == 0 {
		answers = []scriptedAnswer{{status: 202}}
	}

	fake := &fakeIngest{answers: answers}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)

	return fake
}

// handle records the request and answers the next scripted status.
func (fake *fakeIngest) handle(writer http.ResponseWriter, request *http.Request) {
	rawBody, _ := io.ReadAll(request.Body)
	body := rawBody

	if request.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(bytes.NewReader(rawBody))

		if err == nil {
			body, _ = io.ReadAll(gzipReader)
		}
	}

	fake.mutex.Lock()
	scripted := fake.answers[min(len(fake.requests), len(fake.answers)-1)]
	fake.requests = append(fake.requests, recordedRequest{path: request.URL.Path, headers: request.Header.Clone(), rawBody: rawBody, body: body, status: scripted.status})
	fake.mutex.Unlock()

	for headerName, headerValue := range scripted.headers {
		writer.Header().Set(headerName, headerValue)
	}

	writer.WriteHeader(scripted.status)
	_, _ = writer.Write([]byte(`{"accepted":"x"}`))
}

// recorded is a copy of the requests so far.
func (fake *fakeIngest) recorded() []recordedRequest {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()

	return append([]recordedRequest(nil), fake.requests...)
}

// acceptedLines is every line of every request answered 202.
func (fake *fakeIngest) acceptedLines(t *testing.T) []map[string]any {
	t.Helper()

	var accepted []map[string]any

	for _, request := range fake.recorded() {
		if request.status == 202 {
			accepted = append(accepted, request.lines(t)...)
		}
	}

	return accepted
}

// waitForRequests blocks until at least count requests arrived, or fails the test.
func (fake *fakeIngest) waitForRequests(t *testing.T, count int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for len(fake.recorded()) < count {
		if time.Now().After(deadline) {
			t.Fatalf("waited for %d requests, got %d", count, len(fake.recorded()))
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// newTestClient builds a client against the fake that is closed when the test ends.
func newTestClient(t *testing.T, fake *fakeIngest, config Config) *Client {
	t.Helper()

	if config.Key == "" {
		config.Key = testKey
	}

	if config.Endpoint == "" {
		config.Endpoint = fake.server.URL
	}

	// Warnings are expected in these tests; keep the output quiet.
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	client, err := New(config)

	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = client.Close(closeContext)
	})

	return client
}
