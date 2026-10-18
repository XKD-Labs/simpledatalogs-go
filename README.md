# simpledatalogs (Go)

Sends events and log lines from Go services, workers and CLIs to [simpledatalogs](https://simpledatalogs.com), following the [sender rules](https://simpledatalogs.com/docs/api/sender-rules).
Standard library only, Go 1.22+.

## Install

```sh
go get github.com/XKD-Labs/simpledatalogs-go
```

## Quick start

```go
import simpledatalogs "github.com/XKD-Labs/simpledatalogs-go"

sdl, err := simpledatalogs.New(simpledatalogs.Config{}) // reads SIMPLEDATALOGS_KEY=sdl_sec_…
if err != nil {
	log.Printf("simpledatalogs is off: %v", err)
}
defer sdl.Close(context.Background())

sdl.Track("signup", simpledatalogs.Props{"plan": "pro"}, simpledatalogs.WithUserID("42"))
sdl.Log(simpledatalogs.LevelError, "Payment failed", simpledatalogs.Props{"order_id": 7})
```

A background goroutine sends every 5 seconds. `Close` sends what is left; call it before `main` returns.
Use a **secret key** (`sdl_sec_…`) on servers; public keys are for apps and web pages.

`New` returns an error only for a missing key or a malformed endpoint, and then a nil `*Client`, whose methods do nothing. So a missing key never crashes the app.

## Configuration

| `Config` field | Default | What it does |
|---|---|---|
| `Key` | `SIMPLEDATALOGS_KEY` | The ingest key |
| `Endpoint` | `SIMPLEDATALOGS_ENDPOINT`, else `https://edge.simpledatalogs.com` | The ingest host; `/v1/ingest` is added |
| `FlushInterval` | `5s` | How often the background goroutine sends |
| `MaxBatchEvents` | `50` | Lines per request |
| `MaxRequestBytes` | `256000` | Bytes per request before gzip; never above 512 KB |
| `MaxQueueEvents` / `MaxQueueBytes` | `10000` / `10000000` | The queue's caps; past them the oldest unsent lines are dropped, with a warning |
| `RequestTimeout` | `15s` | Per request |
| `DisableGzip` | `false` | Bodies of 1 KB and more are gzipped unless set |
| `DefaultProps` | none | Props added to every line, under the call's own |
| `HTTPClient` | its own | Copied; redirects are never followed |
| `Logger` | text on stderr | Where the SDK's own warnings go, once per kind |
| `Disabled` | `false` | Keeps the client silent (tests, local runs) |

## Events

```go
sdl.Track("checkout", simpledatalogs.Props{"plan": "pro", "seats": 3, "trial": false},
	simpledatalogs.WithUserID(user.ID),
	simpledatalogs.WithTime(finishedAt),
)
```

- Every line gets a UUID `event_id` and a `ts` when you call `Track` or `Log`, so a retry never counts twice.
- Props are strings, numbers, booleans and named types over them; `fmt.Stringer`s send their text. Anything else is skipped with a warning. At most 100 props.
- Identity is per call: `WithUserID`, `WithDeviceID`, `WithSessionID`, `WithAnonymousID`.
- `SetEnabled(false)` is an opt-out: it deletes every unsent line and ignores new ones until `SetEnabled(true)`.

## Flush and Close

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := sdl.Close(ctx); err != nil {
	log.Printf("simpledatalogs: %v", err) // errors.Is(err, simpledatalogs.ErrUndelivered)
}
```

- With a deadline, `Flush` and `Close` retry until the queue is empty or the deadline passes, waiting out `Retry-After`s that end in time.
- Without one, they try once and stop at the first failure.
- In an HTTP server, call `Close` after `server.Shutdown`, so the last requests' events go too.

## Logs (slog)

```go
logger := slog.New(simpledatalogs.NewHandler(sdl, &simpledatalogs.HandlerOptions{Level: slog.LevelWarn}))

logger.Error("Payment failed", "order_id", 7, "user_id", user.ID, "err", err)
```

- Each record becomes one log line with its level, time and message.
- Attributes become props; groups become dotted names (`http.status`). Errors and `Stringer`s send their text, durations read as `1.5s`.
- Top-level `user_id`, `device_id`, `session_id` and `anonymous_id` fill the identity fields.
- Levels map to `debug`, `info`, `warning`, `error`, and `critical` from `slog.LevelError+4`.
- To keep your current output too, fan out to both handlers (for example with your own small multi-handler).

## Delivery

- **Batches:** every 5 s, or at once when 50 lines wait; each request stays under 256 KB before gzip, so far under the 512 KB limit.
- **Only a `202` counts as stored.** Everything else is retried with full-jitter backoff from 1 s to 5 min, never sooner than `Retry-After` (`429`, `503`).
- `401` keeps every line and re-checks the key after 1 minute, then every 10 minutes. `413` halves the batch (a single line too large is dropped); `415` drops the batch with one warning.
- **Memory only:** the queue is bounded by `MaxQueueEvents` and `MaxQueueBytes`, and lines still queued when the process dies are lost. For log files that must survive crashes, ship the files with [Vector or Fluent Bit](https://simpledatalogs.com/docs/integrations/log-shippers).
- No call panics into your app or waits on the network.

## Privacy

- The SDK sends only what you pass: no IP addresses, hostnames, environment variables or request data are added.
- slog attributes reach ingest as you logged them, so leave personal data out of them.
- An email address in `user_id` is stored as a keyed pseudonym unless the project's owner chose otherwise.
- `SetEnabled(false)` deletes unsent lines.

## Links

- Docs: https://simpledatalogs.com/docs
- Ingest API: https://simpledatalogs.com/docs/api/ingest
- Package docs: https://pkg.go.dev/github.com/XKD-Labs/simpledatalogs-go
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: [SECURITY.md](SECURITY.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- License: [MIT](LICENSE)
