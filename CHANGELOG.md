# Changelog

All notable changes to the `github.com/XKD-Labs/simpledatalogs-go` module. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the module uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

The first public release.

### Added

- `New`, `Track`, `Log`, `SetEnabled`, `Pending`, and context-aware `Flush` and `Close`; a nil `*Client` is a no-op.
- A background goroutine that batches every 5 seconds, or at once for a full batch.
- A bounded in-memory queue, gzip, retries with full-jitter backoff, `Retry-After`, `401` pauses, `413` splitting and `415` drops.
- `NewHandler`, a `slog.Handler` that ships records with their level, time and attributes.
- Standard library only; Go 1.22 or newer.
