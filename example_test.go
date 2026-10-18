package simpledatalogs_test

import (
	"context"
	"log"
	"log/slog"
	"time"

	simpledatalogs "github.com/XKD-Labs/simpledatalogs-go"
)

// Example sends an event and a log line, then flushes on the way out.
func Example() {
	sdl, err := simpledatalogs.New(simpledatalogs.Config{Disabled: true})

	if err != nil {
		log.Printf("simpledatalogs is off: %v", err)
	}

	// Close flushes what is left; give it a deadline so shutdown never hangs.
	defer func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sdl.Close(closeContext)
	}()

	sdl.Track("signup", simpledatalogs.Props{"plan": "pro"}, simpledatalogs.WithUserID("42"))
	sdl.Log(simpledatalogs.LevelError, "Payment failed", simpledatalogs.Props{"order_id": 7})
}

// ExampleNewHandler ships slog records at warning and above.
func ExampleNewHandler() {
	sdl, _ := simpledatalogs.New(simpledatalogs.Config{Disabled: true})

	logger := slog.New(simpledatalogs.NewHandler(sdl, &simpledatalogs.HandlerOptions{Level: slog.LevelWarn}))
	logger.Warn("disk almost full", "free_mb", 120)
}
