# Contributing

## Setup

- Go 1.26 or newer.
- No Docker or Kafka needed: tests use franz-go's in-memory `kfake` cluster, a throwaway PKI, and fake Schema Registry servers.

```sh
make check    # go vet + golangci-lint + race-enabled tests + build
```

`make lint` builds a pinned `golangci-lint` with your local toolchain, so it always understands the Go version you use.

## Standards

The linter enforces these:

- **Small units.** Production files have at most 199 lines and functions at most 50 lines (`revive` file-length-limit, `funlen`). Split by responsibility instead of compressing code.
- **Readable code.** Blank lines between logical steps (validation, setup, processing, cleanup); one operation per line.
- **Idiomatic Go.** `gofmt` and `goimports` (local imports grouped last), wrapped errors with context, `errors.Is`/`As`, no naked returns, contexts passed explicitly.
- **SOLID and DRY.** Small interfaces at the point of use (`scan.Sink`, `scan.Matcher`, `record.Renderer`, `scan.OffsetLister`, `codec.SchemaResolver`). Strategy tables instead of growing switches (`auth.mechanismBuilders`). One generic `define[T]` for every tool.
- **Security linting.** `gosec` is on. Each `//nolint` needs a specific linter and a reason.

## Tests

- Write the failing test first: a test that reproduces the bug, or a table case for the new behaviour.
- Prefer table-driven tests. Test helpers call `t.Helper()`.
- End-to-end tests go through MCP (`internal/server/*_test.go`) against fake clusters.
- Docs are tested. `examples/*.yaml` must load, CEL in `docs/filtering.md` must compile, and every tool must have a section in `docs/tools.md`. Update docs in the same change.
- Before claiming a performance improvement, add or extend a benchmark (`go test -bench . ./internal/...`) and compare runs.

## Commits

Keep commits focused, with an imperative subject line, and explain *why* in the body when it isn't obvious.
