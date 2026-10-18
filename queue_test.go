package simpledatalogs

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// testQueue is a queue with a hand-moved clock, fixed jitter, and the warning kinds it raised.
type testQueue struct {
	*queue
	clock       time.Time
	warnedKinds []string
}

// newTestQueue builds a queue with the default caps, changed by the given function.
func newTestQueue(jitter float64, adjust func(*settings)) *testQueue {
	resolved := settings{maxBatchEvents: 50, maxRequestBytes: 256_000, maxQueueEvents: 10_000, maxQueueBytes: 10_000_000}

	if adjust != nil {
		adjust(&resolved)
	}

	tested := &testQueue{clock: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	tested.queue = newQueue(resolved, func(kind string, _ string) { tested.warnedKinds = append(tested.warnedKinds, kind) })
	tested.queue.now = func() time.Time { return tested.clock }
	tested.queue.randomFraction = func() float64 { return jitter }

	return tested
}

// testLines are distinct lines of the given size.
func testLines(count int, size int) [][]byte {
	lines := make([][]byte, count)

	for lineIndex := range lines {
		line := []byte(fmt.Sprintf("%05d", lineIndex))

		for len(line) < size {
			line = append(line, 'x')
		}

		lines[lineIndex] = line
	}

	return lines
}

func TestRetryDelayGrowsFromOneSecondAndCapsAtFiveMinutes(t *testing.T) {
	cases := map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 50: 5 * time.Minute}

	for failureCount, expected := range cases {
		if delay := retryDelay(failureCount, 0, 1); delay != expected {
			t.Errorf("failure %d: got %v, want %v", failureCount, delay, expected)
		}
	}
}

func TestRetryDelayIsFullJitterWithAFloor(t *testing.T) {
	if delay := retryDelay(5, 0, 0.5); delay != 8*time.Second {
		t.Errorf("got %v, want 8s", delay)
	}

	if delay := retryDelay(5, 0, 0); delay != 250*time.Millisecond {
		t.Errorf("got %v, want 250ms", delay)
	}
}

func TestRetryAfterIsAFloorCappedAtAnHour(t *testing.T) {
	if delay := retryDelay(1, 30*time.Second, 0.1); delay != 30*time.Second {
		t.Errorf("got %v, want 30s", delay)
	}

	if delay := retryDelay(1, 24*time.Hour, 0.1); delay != time.Hour {
		t.Errorf("got %v, want 1h", delay)
	}
}

func TestRetryAfterReadsSecondsAndHTTPDates(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	inTwoMinutes := now.Add(2 * time.Minute).Format(http.TimeFormat)

	cases := map[string]time.Duration{
		"120":                           2 * time.Minute,
		inTwoMinutes:                    2 * time.Minute,
		"Mon, 01 Jan 2001 00:00:00 GMT": 0,
		"soon":                          0,
		"":                              0,
	}

	for headerValue, expected := range cases {
		if parsed := parseRetryAfter(headerValue, now); parsed != expected {
			t.Errorf("%q: got %v, want %v", headerValue, parsed, expected)
		}
	}
}

func TestA202RemovesTheBatchAndResetsTheBackoff(t *testing.T) {
	tested := newTestQueue(1, nil)
	tested.enqueue(testLines(3, 10)...)
	batch := tested.nextBatch()

	if tested.applyAnswer(batch, answer{status: 503}) != outcomeFailed || tested.waitTime() != time.Second {
		t.Fatalf("a 503 should back off 1 s, waiting %v", tested.waitTime())
	}

	tested.clock = tested.clock.Add(time.Second)

	if tested.applyAnswer(batch, answer{status: 202}) != outcomeSent || tested.pending() != 0 || tested.waitTime() != 0 {
		t.Fatalf("a 202 should empty the queue and clear the pause")
	}
}

func Test503And429HonorRetryAfterAndKeepTheBatch(t *testing.T) {
	tested := newTestQueue(0, nil)
	tested.enqueue(testLines(2, 10)...)

	tested.applyAnswer(tested.nextBatch(), answer{status: 503, retryAfter: 42 * time.Second})

	if tested.waitTime() != 42*time.Second || tested.pending() != 2 {
		t.Fatalf("503: waiting %v with %d pending", tested.waitTime(), tested.pending())
	}

	tested.applyAnswer(tested.nextBatch(), answer{status: 429, retryAfter: time.Hour})

	if tested.waitTime() != time.Hour || tested.pending() != 2 {
		t.Fatalf("429: waiting %v with %d pending", tested.waitTime(), tested.pending())
	}
}

func TestEveryNon202StatusIsRetried(t *testing.T) {
	for _, status := range []int{200, 204, 301, 400, 403, 404, 408, 500, 502, 504} {
		tested := newTestQueue(1, nil)
		tested.enqueue(testLines(1, 10)...)

		if tested.applyAnswer(tested.nextBatch(), answer{status: status}) != outcomeFailed || tested.pending() != 1 {
			t.Errorf("status %d should keep the line and back off", status)
		}
	}
}

func TestA401PausesAMinuteThenTenMinutesAndKeepsEverything(t *testing.T) {
	tested := newTestQueue(1, nil)
	tested.enqueue(testLines(5, 10)...)

	tested.applyAnswer(tested.nextBatch(), answer{status: 401})

	if tested.waitTime() != time.Minute {
		t.Fatalf("first 401: waiting %v", tested.waitTime())
	}

	tested.clock = tested.clock.Add(time.Minute)
	tested.applyAnswer(tested.nextBatch(), answer{status: 401})

	if tested.waitTime() != 10*time.Minute || tested.pending() != 5 {
		t.Fatalf("second 401: waiting %v with %d pending", tested.waitTime(), tested.pending())
	}
}

func TestA413HalvesTheBatchAndA202DoublesItBack(t *testing.T) {
	tested := newTestQueue(1, func(resolved *settings) { resolved.maxBatchEvents = 8 })
	tested.enqueue(testLines(20, 10)...)

	if tested.applyAnswer(tested.nextBatch(), answer{status: 413}) != outcomeSplit || len(tested.nextBatch()) != 4 {
		t.Fatalf("a 413 should halve the batch to 4")
	}

	tested.applyAnswer(tested.nextBatch(), answer{status: 202})

	if len(tested.nextBatch()) != 8 {
		t.Fatalf("a 202 should double the batch back to 8")
	}
}

func TestA413ForASingleLineDropsOnlyThatLine(t *testing.T) {
	tested := newTestQueue(1, func(resolved *settings) { resolved.maxBatchEvents = 1 })
	tested.enqueue(testLines(2, 10)...)

	if tested.applyAnswer(tested.nextBatch(), answer{status: 413}) != outcomeDropped || tested.pending() != 1 {
		t.Fatalf("a lone line answered 413 should be dropped")
	}
}

func TestA415DropsTheBatch(t *testing.T) {
	tested := newTestQueue(1, func(resolved *settings) { resolved.maxBatchEvents = 2 })
	tested.enqueue(testLines(3, 10)...)

	if tested.applyAnswer(tested.nextBatch(), answer{status: 415}) != outcomeDropped || tested.pending() != 1 {
		t.Fatalf("a 415 should drop the batch")
	}

	if len(tested.warnedKinds) != 1 || tested.warnedKinds[0] != "rejected_415" {
		t.Fatalf("a 415 should warn, got %v", tested.warnedKinds)
	}
}

func TestBatchesRespectTheCountAndByteCaps(t *testing.T) {
	byBytes := newTestQueue(1, func(resolved *settings) { resolved.maxRequestBytes = 1024 })
	byBytes.enqueue(testLines(40, 99)...)

	if batchSize := len(byBytes.nextBatch()); batchSize != 10 {
		t.Errorf("byte cap: got %d lines, want 10", batchSize)
	}

	byCount := newTestQueue(1, func(resolved *settings) { resolved.maxBatchEvents = 7 })
	byCount.enqueue(testLines(40, 10)...)

	if batchSize := len(byCount.nextBatch()); batchSize != 7 {
		t.Errorf("count cap: got %d lines, want 7", batchSize)
	}
}

func TestTheQueueDropsTheOldestOverItsCaps(t *testing.T) {
	byCount := newTestQueue(1, func(resolved *settings) { resolved.maxQueueEvents = 5 })
	byCount.enqueue(testLines(8, 10)...)

	if byCount.pending() != 5 || string(byCount.nextBatch()[0].line[:5]) != "00003" {
		t.Fatalf("the oldest 3 lines should be gone")
	}

	byBytes := newTestQueue(1, func(resolved *settings) { resolved.maxQueueBytes = 1100 })
	byBytes.enqueue(testLines(20, 99)...)

	if byBytes.pending() != 11 {
		t.Fatalf("got %d lines, want 11", byBytes.pending())
	}
}

func TestLinesQueuedDuringASendSurviveIts202(t *testing.T) {
	tested := newTestQueue(1, nil)
	tested.enqueue(testLines(2, 10)...)
	batch := tested.nextBatch()

	tested.enqueue([]byte("late"))
	tested.applyAnswer(batch, answer{status: 202})

	if remaining := tested.nextBatch(); len(remaining) != 1 || string(remaining[0].line) != "late" {
		t.Fatalf("the late line should remain")
	}
}
