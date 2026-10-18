// Package simpledatalogs sends events and log lines to simpledatalogs (https://simpledatalogs.com).
//
// Track and Log only queue; a background goroutine sends batches every few seconds.
// Every line gets an event_id and a ts when it is queued, so retries never count twice.
// Only a 202 counts as stored: anything else is retried with full-jitter backoff, honoring Retry-After.
// Nothing ever panics into the host app, and a nil *Client is a silent no-op.
//
//	sdl, err := simpledatalogs.New(simpledatalogs.Config{}) // reads SIMPLEDATALOGS_KEY
//	if err != nil {
//		log.Printf("simpledatalogs is off: %v", err)
//	}
//	defer sdl.Close(context.Background())
//
//	sdl.Track("signup", simpledatalogs.Props{"plan": "pro"}, simpledatalogs.WithUserID("42"))
//	sdl.Log(simpledatalogs.LevelError, "Payment failed", simpledatalogs.Props{"order_id": 7})
//
// NewHandler returns a slog.Handler that ships log records the same way.
package simpledatalogs
