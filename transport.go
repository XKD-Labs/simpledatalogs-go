package simpledatalogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"time"
)

// gzipMinBytes is the body size from which gzip pays off.
const gzipMinBytes = 1024

// maxAnswerBytes is the most of an answer body read before closing; ingest's answers are tiny.
const maxAnswerBytes = 65536

// transport POSTs bodies to the ingest URL.
type transport struct {
	ingestURL  string
	ingestKey  string
	shouldGzip bool
	timeout    time.Duration
	httpClient *http.Client
}

// newTransport copies the given HTTP client (or makes one) so that redirects are never followed.
func newTransport(resolved settings, httpClient *http.Client) *transport {
	ownClient := &http.Client{}

	if httpClient != nil {
		copied := *httpClient
		ownClient = &copied
	}

	// A redirect is answered as its 3xx status, so the key never follows a POST elsewhere.
	ownClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	return &transport{
		ingestURL:  resolved.ingestURL,
		ingestKey:  resolved.ingestKey,
		shouldGzip: resolved.shouldGzip,
		timeout:    resolved.requestTimeout,
		httpClient: ownClient,
	}
}

// send POSTs one NDJSON body; any HTTP status is an answer, a timeout or network failure is an error.
func (sender *transport) send(ctx context.Context, body []byte) (answer, error) {
	requestContext, cancel := context.WithTimeout(ctx, sender.timeout)
	defer cancel()

	// Gzip counts its compressed bytes against the 512 KB limit, so larger bodies shrink.
	isGzipped := sender.shouldGzip && len(body) >= gzipMinBytes

	if isGzipped {
		body = gzipBody(body)
	}

	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, sender.ingestURL, bytes.NewReader(body))

	if err != nil {
		return answer{}, err
	}

	request.Header.Set("Authorization", "Bearer "+sender.ingestKey)
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("User-Agent", userAgent)

	if isGzipped {
		request.Header.Set("Content-Encoding", "gzip")
	}

	response, err := sender.httpClient.Do(request)

	if err != nil {
		return answer{}, err
	}

	defer response.Body.Close()

	// Read the small answer, so the connection goes back to the pool.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxAnswerBytes))

	return answer{status: response.StatusCode, retryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now())}, nil
}

// gzipBody compresses a body; writing to memory can't fail.
func gzipBody(body []byte) []byte {
	var compressed bytes.Buffer

	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write(body)
	_ = writer.Close()

	return compressed.Bytes()
}
