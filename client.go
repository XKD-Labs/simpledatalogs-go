package simpledatalogs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrUndelivered is returned by Flush and Close when lines are still unsent at the end.
var ErrUndelivered = errors.New("simpledatalogs: lines are still unsent")

// internalAttrKey marks the SDK's own warnings, so the slog Handler never ships them back.
const internalAttrKey = "simpledatalogs_internal"

// Client queues events and log lines and sends them in the background; safe for concurrent use.
type Client struct {
	settings     settings
	queue        *queue
	transport    *transport
	defaultProps map[string]any
	logger       *slog.Logger
	warnedKinds  sync.Map
	isEnabled    atomic.Bool
	isClosed     atomic.Bool
	sendSlot     chan struct{}
	wake         chan struct{}
	stopWorker   context.CancelFunc
	workerDone   chan struct{}
	closeOnce    sync.Once
}

// New builds a client and starts its background goroutine; a config error returns a nil client, which is a no-op.
func New(config Config) (*Client, error) {
	resolved, err := resolveSettings(config)

	if err != nil {
		return nil, err
	}

	logger := config.Logger

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	client := &Client{
		settings:   resolved,
		transport:  newTransport(resolved, config.HTTPClient),
		logger:     logger,
		sendSlot:   make(chan struct{}, 1),
		wake:       make(chan struct{}, 1),
		workerDone: make(chan struct{}),
	}

	client.queue = newQueue(resolved, client.warn)
	client.defaultProps = cleanProps(config.DefaultProps, client.warn)
	client.isEnabled.Store(!resolved.isDisabled)

	// Keys that don't look like ingest keys still run, with a warning.
	if !resolved.isDisabled && !strings.HasPrefix(resolved.ingestKey, "sdl_") {
		client.warn("key_shape", "the ingest key doesn't start with sdl_; check you copied the whole key")
	}

	// The worker stops when Close cancels its context.
	workerContext, stopWorker := context.WithCancel(context.Background())
	client.stopWorker = stopWorker

	go client.runWorker(workerContext)

	return client, nil
}

// Track queues an event with optional props and options; it never blocks on the network.
func (client *Client) Track(event string, props Props, options ...Option) {
	if client == nil {
		return
	}

	defer client.recoverPanic()

	// An empty name can't become an event.
	trimmedEvent := strings.TrimSpace(event)

	if trimmedEvent == "" {
		client.warn("event_name", "Track needs a non-empty event name; the event was skipped")
		return
	}

	wireLine := newWireEvent(options)
	wireLine.Event = trimmedEvent
	wireLine.Props = mergeProps(client.defaultProps, cleanProps(props, client.warn))

	client.enqueue(wireLine, false)
}

// Log queues a log line (debug, info, notice, warning, error, critical) with optional props; it never blocks.
func (client *Client) Log(level Level, message string, props Props, options ...Option) {
	if client == nil {
		return
	}

	defer client.recoverPanic()

	wireLine := newWireEvent(options)
	wireLine.Level = normalizeLevel(level)
	wireLine.Message = &message
	wireLine.Props = mergeProps(client.defaultProps, cleanProps(props, client.warn))

	client.enqueue(wireLine, true)
}

// SetEnabled turns sending on or off; off is an opt-out that deletes every unsent line.
func (client *Client) SetEnabled(isEnabled bool) {
	if client == nil {
		return
	}

	client.isEnabled.Store(isEnabled && client.settings.ingestKey != "")

	if !isEnabled {
		client.queue.clear()
	}
}

// Pending is the number of lines not yet acknowledged by a 202.
func (client *Client) Pending() int {
	if client == nil {
		return 0
	}

	return client.queue.pending()
}

// Flush sends what is queued; with a ctx deadline it waits out retries until then, without one it tries once.
func (client *Client) Flush(ctx context.Context) error {
	if client == nil {
		return nil
	}

	// One send at a time, shared with the background goroutine.
	select {
	case client.sendSlot <- struct{}{}:
	case <-ctx.Done():
		return client.undeliveredError(ctx.Err())
	}

	defer func() { <-client.sendSlot }()

	if client.drain(ctx, true) {
		return nil
	}

	return client.undeliveredError(ctx.Err())
}

// Close stops the background goroutine and flushes once more within ctx; later calls are ignored.
func (client *Client) Close(ctx context.Context) error {
	if client == nil {
		return nil
	}

	var flushErr error

	client.closeOnce.Do(func() {
		client.isClosed.Store(true)

		// Stop the worker first (an in-flight batch stays queued), so the last flush runs here alone.
		client.stopWorker()

		select {
		case <-client.workerDone:
		case <-ctx.Done():
		}

		flushErr = client.Flush(ctx)
	})

	return flushErr
}

// enqueue encodes one line, applies the size cap, queues it, and wakes the worker for a full batch.
func (client *Client) enqueue(wireLine wireEvent, isLogLine bool) {
	if !client.isEnabled.Load() || client.isClosed.Load() {
		return
	}

	line, err := encodeLine(wireLine)

	if err != nil {
		client.warn("encode", fmt.Sprintf("a line could not be encoded and was skipped: %v", err))
		return
	}

	// A line no request can hold: a log line is cut down, an event is skipped.
	maxLineBytes := client.settings.maxRequestBytes - 1

	if len(line) > maxLineBytes {
		if !isLogLine {
			client.warn("event_too_large", fmt.Sprintf("an event of %d bytes is over the request limit and was skipped", len(line)))
			return
		}

		client.warn("log_line_truncated", fmt.Sprintf("a log line of %d bytes was cut to fit one request", len(line)))
		line, err = truncatedLogLine(wireLine, len(line), maxLineBytes)

		if err != nil || len(line) > maxLineBytes {
			client.warn("event_too_large", "a log line is over the request limit even when cut, and was skipped")
			return
		}
	}

	if client.queue.enqueue(line) {
		client.signalWake()
	}
}

// signalWake nudges the worker without ever blocking the caller.
func (client *Client) signalWake() {
	select {
	case client.wake <- struct{}{}:
	default:
	}
}

// runWorker sleeps until the interval passes, a pause ends or a full batch waits, then drains.
func (client *Client) runWorker(ctx context.Context) {
	defer close(client.workerDone)

	for {
		waitTime := client.queue.waitTime()

		if waitTime <= 0 {
			waitTime = client.settings.flushInterval
		}

		timer := time.NewTimer(waitTime)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-client.wake:
			timer.Stop()
		case <-timer.C:
		}

		client.drainFromWorker(ctx)
	}
}

// drainFromWorker takes the send slot and drains once, recovering from any panic.
func (client *Client) drainFromWorker(ctx context.Context) {
	defer client.recoverPanic()

	select {
	case client.sendSlot <- struct{}{}:
	case <-ctx.Done():
		return
	}

	defer func() { <-client.sendSlot }()

	client.drain(ctx, false)
}

// drain sends batches until the queue is empty; when retrying it waits out pauses that end before ctx's deadline.
func (client *Client) drain(ctx context.Context, isRetrying bool) bool {
	for {
		batchEntries := client.queue.nextBatch()

		// Nothing left (or sending is off): done.
		if len(batchEntries) == 0 || !client.isEnabled.Load() {
			return len(batchEntries) == 0
		}

		// Paused: wait it out only when retrying and the pause ends before the deadline.
		if waitTime := client.queue.waitTime(); waitTime > 0 {
			deadline, hasDeadline := ctx.Deadline()

			if !isRetrying || !hasDeadline || time.Now().Add(waitTime).After(deadline) {
				return false
			}

			if !sleepContext(ctx, waitTime) {
				return false
			}

			continue
		}

		if ctx.Err() != nil {
			return false
		}

		// Without a deadline a failure ends this pass; the pause decides the next.
		batchOutcome := client.sendBatch(ctx, batchEntries)

		if batchOutcome == outcomeFailed {
			if _, hasDeadline := ctx.Deadline(); !isRetrying || !hasDeadline {
				return false
			}
		}
	}
}

// sendBatch posts one batch and applies the answer.
func (client *Client) sendBatch(ctx context.Context, batchEntries []entry) outcome {
	var body bytes.Buffer

	for _, queued := range batchEntries {
		body.Write(queued.line)
		body.WriteByte('\n')
	}

	ingestAnswer, err := client.transport.send(ctx, body.Bytes())

	if err != nil {
		client.warn("network", fmt.Sprintf("send failed, will retry: %v", err))
		return client.queue.applyNetworkError()
	}

	return client.queue.applyAnswer(batchEntries, ingestAnswer)
}

// undeliveredError wraps ErrUndelivered with the count left and the context's error, if any.
func (client *Client) undeliveredError(ctxErr error) error {
	pendingCount := client.queue.pending()

	if pendingCount == 0 {
		return nil
	}

	if ctxErr != nil {
		return fmt.Errorf("%w (%d lines): %w", ErrUndelivered, pendingCount, ctxErr)
	}

	return fmt.Errorf("%w (%d lines)", ErrUndelivered, pendingCount)
}

// warn logs a warning once per kind, so a long outage never floods the app's logs.
func (client *Client) warn(kind string, message string) {
	if _, wasWarned := client.warnedKinds.LoadOrStore(kind, true); wasWarned {
		return
	}

	client.logger.Warn("simpledatalogs: "+message, slog.Bool(internalAttrKey, true))
}

// recoverPanic turns a panic inside the SDK into a warning, so the host app never crashes because of it.
func (client *Client) recoverPanic() {
	if recovered := recover(); recovered != nil {
		client.warn("internal", fmt.Sprintf("internal error, nothing was raised to the app: %v", recovered))
	}
}

// sleepContext waits for the duration or until ctx is done; true when the full wait passed.
func sleepContext(ctx context.Context, waitTime time.Duration) bool {
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
