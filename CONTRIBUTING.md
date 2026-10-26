# Contributing

Thanks for helping. Bug reports, fixes and docs improvements are welcome; for a larger change, open an issue first so we can agree on the shape.

## Run the tests

The tests need only Docker; nothing is installed on your machine.

```sh
docker run --rm -v "$PWD":/w -w /w golang:1.27 sh -c 'test -z "$(gofmt -l .)" && go vet ./... && go test -race -count=1 ./...'
```

Check the oldest supported Go too:

```sh
docker run --rm -v "$PWD":/w -w /w -e GOTOOLCHAIN=local golang:1.22 go test -count=1 ./...
```

With Go installed locally: `go test -race ./...`.

The tests start a fake ingest server on `127.0.0.1`; they never reach the internet.

## Check for known vulnerabilities

```sh
docker run --rm -v "$PWD":/w -w /w golang:1.27 sh -c 'go install golang.org/x/vuln/cmd/govulncheck@v1.9.0 && govulncheck ./...'
```

## Rules the code follows

- Standard library only; keep it that way.
- Public calls never panic into the host app and never wait on the network; a nil `*Client` stays a no-op.
- Behaviour follows the [sender rules](https://simpledatalogs.com/docs/api/sender-rules); a change there comes with a test against the fake ingest.
- Every function has a one-line comment, and names say what they hold.

## Releases

Maintainers bump `Version` in `version.go` and `CHANGELOG.md`, run the tests, then push a tag `v` plus that version (e.g. `v0.1.0`). Nothing runs on GitHub; ask the Go proxy to fetch the tag so pkg.go.dev shows it at once:

```sh
GOPROXY=https://proxy.golang.org go list -m github.com/XKD-Labs/simpledatalogs-go@v0.1.0
```

## Security

Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.
