# Development Workflow

Use this reference for code changes, reviews, and PR preparation.

## Baseline

- Confura is a Go project. `README.md` states Go 1.22 or later is required.
- Main binary target: `bin/confura`.
- Preferred build command: `make build`.
- Direct build command: `go build -o bin/confura`.
- `Makefile` injects version, build date, and git commit through `-ldflags`.

## Before Editing

1. Check `git status --short`.
2. Identify whether the task is Core Space, eSpace, CfxBridge, or shared.
3. Read the nearest source and tests before changing behavior.
4. Preserve unrelated dirty files. Do not clean backup files or generated outputs unless the user asked.

## Editing Rules

- Follow existing package structure and helper APIs.
- Prefer targeted changes over cross-cutting refactors.
- Run `gofmt` on touched `.go` files.
- Keep docs aligned when behavior changes user-visible RPC behavior, CLI usage, deployment, config, or dashboards.
- For generated proto files, update the `.proto` source and regenerate instead of manually editing `.pb.go`.

## Test Strategy

Run the narrowest meaningful tests first:

- Local utility or data-structure changes: `go test ./util/...` or the specific package.
- RPC handler/API changes: test the relevant `rpc`, `rpc/handler`, `store`, or `virtualfilter` packages.
- Sync/storage changes: test `./sync/...`, `./store/...`, and related packages.
- Routing/node changes: test `./node/...`.
- Shared config, middleware, storage contracts, or public behavior changes: run `go test ./...` if feasible.

When changing behavior that depends on MySQL, Redis, InfluxDB, or live fullnodes, separate package tests from integration validation. Package tests should cover parsing, routing decisions, split calculations, and error construction where possible; runtime validators should cover live data consistency.

If tests cannot run because dependencies, network, database, or sandbox permissions are unavailable, say exactly what was skipped and why.

## Runtime Validation Tools

Confura includes command-line validators under `confura test`:

- `confura test cfx`: compare Core Space JSON-RPC proxy data with a fullnode.
- `confura test eth`: compare eSpace JSON-RPC proxy data with a fullnode.
- `confura test ws`: compare websocket Pub/Sub proxy data with a fullnode.
- `confura test vf`: compare Virtual Filter proxy behavior with a fullnode.

Use these for operator validation or integration-style guidance, not as a replacement for Go package tests during code changes.

## Contribution Notes

The README contribution guidance says:

- Base pull requests on `main`.
- Use `gofmt`.
- Document exported code according to Go conventions.
- Prefix commit messages with modified package names, for example `cfx sync: add nearhead memory cache`.
