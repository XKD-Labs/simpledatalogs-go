package simpledatalogs

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// outcome is what one batch's answer meant for the drain loop.
type outcome int

const (
	outcomeSent outcome = iota
	outcomeSplit
	outcomeDropped
	outcomeFailed
)

// answer is the status ingest gave and its Retry-After (zero when none).
type answer struct {
	status     int
	retryAfter time.Duration
}

// entry is one queued NDJSON line with an id, so equal lines stay distinct.
type entry struct {
	id   uint64
	line []byte
}

// size is the line's bytes in a body, newline included.
func (queued entry) size() int {
	return len(queued.line) + 1
}

// queue is the bounded line queue and the retry state; every method is safe for concurrent use.
type queue struct {
	mutex                sync.Mutex
	entries              []entry
	queuedBytes          int
	nextEntryID          uint64
	maxBatchEvents       int
	maxRequestBytes      int
	maxQueueEvents       int
	maxQueueBytes        int
	currentBatchEvents   int
	failureCount         int
	invalidKeyProbeCount int
	backoffUntil         time.Time
	invalidKeyUntil      time.Time
	now                  func() time.Time
	randomFraction       func() float64
	warn                 func(kind string, message string)
}

// newQueue builds a queue from the resolved caps.
func newQueue(resolved settings, warn func(kind string, message string)) *queue {
	return &queue{
		maxBatchEvents:     resolved.maxBatchEvents,
		maxRequestBytes:    resolved.maxRequestBytes,
		maxQueueEvents:     resolved.maxQueueEvents,
		maxQueueBytes:      resolved.maxQueueBytes,
		currentBatchEvents: resolved.maxBatchEvents,
		now:                time.Now,
		randomFraction:     rand.Float64,
		warn:               warn,
	}
}

// enqueue adds lines, drops the oldest past the caps, and reports whether a full batch waits.
func (lineQueue *queue) enqueue(lines ...[]byte) bool {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	for _, line := range lines {
		lineQueue.nextEntryID++
		lineQueue.entries = append(lineQueue.entries, entry{id: lineQueue.nextEntryID, line: line})
		lineQueue.queuedBytes += len(line) + 1
	}

	lineQueue.trim()

	return len(lineQueue.entries) >= lineQueue.currentBatchEvents
}

// pending is the number of lines not yet acknowledged by a 202.
func (lineQueue *queue) pending() int {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	return len(lineQueue.entries)
}

// nextBatch is the oldest lines that fit one request, by count and by bytes; at least one when any wait.
func (lineQueue *queue) nextBatch() []entry {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	batchEntries := make([]entry, 0, min(len(lineQueue.entries), lineQueue.currentBatchEvents))
	bodyBytes := 0

	for _, queued := range lineQueue.entries {
		if len(batchEntries) >= lineQueue.currentBatchEvents {
			break
		}

		if len(batchEntries) > 0 && bodyBytes+queued.size() > lineQueue.maxRequestBytes {
			break
		}

		batchEntries = append(batchEntries, queued)
		bodyBytes += queued.size()
	}

	return batchEntries
}

// waitTime is how long until sending is allowed again; zero when it is allowed now.
func (lineQueue *queue) waitTime() time.Duration {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	pausedUntil := lineQueue.backoffUntil

	if lineQueue.invalidKeyUntil.After(pausedUntil) {
		pausedUntil = lineQueue.invalidKeyUntil
	}

	return max(pausedUntil.Sub(lineQueue.now()), 0)
}

// applyAnswer applies one ingest answer: 202 removes, 413 splits, 415 drops, 401 pauses, anything else backs off.
func (lineQueue *queue) applyAnswer(batchEntries []entry, ingestAnswer answer) outcome {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	switch ingestAnswer.status {
	case 202:
		// Only a 202 means stored: the batch leaves, the counters reset, and the batch size grows back after a 413.
		lineQueue.remove(batchEntries)
		lineQueue.failureCount = 0
		lineQueue.invalidKeyProbeCount = 0
		lineQueue.currentBatchEvents = min(lineQueue.maxBatchEvents, lineQueue.currentBatchEvents*2)
		return outcomeSent

	case 415:
		// Unsupported as sent: resending unchanged can never work, so the batch is dropped.
		lineQueue.warn("rejected_415", "ingest refused a batch as unsupported (415); it was dropped")
		lineQueue.remove(batchEntries)
		return outcomeDropped

	case 413:
		// Too large: halve the batch, or drop a lone line that can never be accepted.
		if len(batchEntries) > 1 {
			lineQueue.currentBatchEvents = max(1, len(batchEntries)/2)
			return outcomeSplit
		}

		lineQueue.warn("rejected_413", "ingest refused a line as too large (413); it was dropped")
		lineQueue.remove(batchEntries)
		return outcomeDropped

	case 401:
		// Unknown or revoked key: keep everything, re-probe after a minute and then every 10 minutes.
		lineQueue.warn("invalid_key", "the ingest key was refused (401); lines are kept and the key is re-checked every 10 minutes")
		probeDelay := invalidKeyProbe

		if lineQueue.invalidKeyProbeCount == 0 {
			probeDelay = invalidKeyFirstProbe
		}

		lineQueue.invalidKeyProbeCount++
		lineQueue.invalidKeyUntil = lineQueue.now().Add(honorRetryAfter(probeDelay, ingestAnswer.retryAfter))
		return outcomeFailed

	default:
		// Anything else (408, 429, 503, 5xx, a redirect) wasn't stored: back off and retry.
		lineQueue.backOff(ingestAnswer.retryAfter)
		return outcomeFailed
	}
}

// applyNetworkError handles a timeout or network error: unknown whether it was stored, so keep it and back off.
func (lineQueue *queue) applyNetworkError() outcome {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	lineQueue.backOff(0)

	return outcomeFailed
}

// clear forgets every queued line (the opt-out).
func (lineQueue *queue) clear() {
	lineQueue.mutex.Lock()
	defer lineQueue.mutex.Unlock()

	lineQueue.entries = nil
	lineQueue.queuedBytes = 0
}

// backOff pauses sending for a full-jitter backoff that is never shorter than Retry-After; the mutex is held.
func (lineQueue *queue) backOff(retryAfter time.Duration) {
	lineQueue.failureCount++
	lineQueue.backoffUntil = lineQueue.now().Add(retryDelay(lineQueue.failureCount, retryAfter, lineQueue.randomFraction()))
}

// remove drops the given entries, wherever they are now; the mutex is held.
func (lineQueue *queue) remove(batchEntries []entry) {
	batchIDs := make(map[uint64]struct{}, len(batchEntries))

	for _, sent := range batchEntries {
		batchIDs[sent.id] = struct{}{}
	}

	keptEntries := lineQueue.entries[:0]
	keptBytes := 0

	for _, queued := range lineQueue.entries {
		if _, isSent := batchIDs[queued.id]; !isSent {
			keptEntries = append(keptEntries, queued)
			keptBytes += queued.size()
		}
	}

	lineQueue.entries = keptEntries
	lineQueue.queuedBytes = keptBytes
}

// trim drops the oldest lines while the queue is over its line or byte cap; the mutex is held.
func (lineQueue *queue) trim() {
	droppedCount := 0

	for len(lineQueue.entries) > 0 && (len(lineQueue.entries) > lineQueue.maxQueueEvents || lineQueue.queuedBytes > lineQueue.maxQueueBytes) {
		lineQueue.queuedBytes -= lineQueue.entries[0].size()
		lineQueue.entries = lineQueue.entries[1:]
		droppedCount++
	}

	if droppedCount > 0 {
		lineQueue.warn("queue_full", fmt.Sprintf("the queue is over its cap; dropped the oldest %d unsent lines", droppedCount))
	}
}
