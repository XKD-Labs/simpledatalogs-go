package simpledatalogs

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The retry policy: full-jitter backoff from 1 s to 5 min, with Retry-After as a floor.
const (
	initialBackoff = time.Second
	maxBackoff     = 5 * time.Minute

	// minRetryDelay keeps a burst of failures from spinning.
	minRetryDelay = 250 * time.Millisecond

	// maxRetryAfter is the longest Retry-After honoured in one wait; over quota asks an hour.
	maxRetryAfter = time.Hour

	// After a 401 the key is re-probed after a minute, then every 10 minutes.
	invalidKeyFirstProbe = time.Minute
	invalidKeyProbe      = 10 * time.Minute
)

// retryDelay is the wait before the next attempt: full jitter under 1 s * 2^(n-1), capped at 5 min, never under Retry-After.
func retryDelay(failureCount int, retryAfter time.Duration, jitterFraction float64) time.Duration {
	// Cap the exponent so the doubling can never overflow.
	doublingCount := min(max(failureCount-1, 0), 30)
	ceiling := min(maxBackoff, initialBackoff*time.Duration(1<<doublingCount))

	// Full jitter picks uniformly under the ceiling, then the floor and Retry-After apply.
	clampedFraction := min(max(jitterFraction, 0), 1)

	return honorRetryAfter(max(minRetryDelay, time.Duration(float64(ceiling)*clampedFraction)), retryAfter)
}

// honorRetryAfter raises the delay to Retry-After, within a sane bound; a zero Retry-After means none.
func honorRetryAfter(delay time.Duration, retryAfter time.Duration) time.Duration {
	return max(delay, min(retryAfter, maxRetryAfter))
}

// parseRetryAfter reads delay-seconds or an HTTP-date; absent or unreadable is zero.
func parseRetryAfter(headerValue string, now time.Time) time.Duration {
	trimmedValue := strings.TrimSpace(headerValue)

	if trimmedValue == "" {
		return 0
	}

	// delay-seconds is a bare non-negative integer.
	if seconds, err := strconv.ParseUint(trimmedValue, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second
	}

	// An HTTP-date in the past means retry now.
	retryAt, err := http.ParseTime(trimmedValue)

	if err != nil {
		return 0
	}

	return max(retryAt.Sub(now), 0)
}
