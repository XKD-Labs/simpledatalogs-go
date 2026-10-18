---
description: Send events and slog records from Go to simpledatalogs in one minute.
---

# Go

## Install

```sh
go get github.com/XKD-Labs/simpledatalogs-go
```

```sh
# .env
SIMPLEDATALOGS_KEY=sdl_sec_…
```

## Add it

```go
import simpledatalogs "github.com/XKD-Labs/simpledatalogs-go"

sdl, err := simpledatalogs.New(simpledatalogs.Config{})
if err != nil {
	log.Printf("simpledatalogs is off: %v", err)
}
defer sdl.Close(context.Background())

sdl.Track("signup", simpledatalogs.Props{"plan": "pro"}, simpledatalogs.WithUserID("42"))
```

A background goroutine sends every 5 seconds; `Close` sends what is left.

## Send your logs

```go
logger := slog.New(simpledatalogs.NewHandler(sdl, &simpledatalogs.HandlerOptions{Level: slog.LevelWarn}))
slog.SetDefault(logger)

slog.Error("Payment failed", "order_id", 7, "err", err)
```

## In the first minute

- Your `Track` events within 5 seconds.
- Warnings and errors on the **Logs** tab, with attributes as props (groups as dotted names).

## Defaults

| Feature | Default |
|---|---|
| Sending | every 5 s, or at 50 lines; retried until a `202` |
| Queue | memory, at most 10,000 lines or 10 MB |
| Shutdown | `Close(ctx)` flushes, retrying until ctx's deadline |
| Autocapture | none: only what you track and what your loggers emit at the handler's level |

Everything else is in the [Go SDK](https://github.com/XKD-Labs/simpledatalogs-go) README and on [pkg.go.dev](https://pkg.go.dev/github.com/XKD-Labs/simpledatalogs-go).
