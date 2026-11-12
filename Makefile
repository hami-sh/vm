.PHONY: build vet test fixture run verify e2e

build:
	go build ./...

vet:
	go vet ./...

test:
	go test -race ./...

# Rewrite fixtures/snapshot.json after changing internal/api (Phase 0 owner only).
fixture:
	go test ./internal/api -update

run:
	go run ./cmd/controlplane

# Real actors and SQLite run in-process under the race detector.
verify: vet test
	go run -race ./cmd/vmctl verify --repeat 20

# Separate integration gate; fails until the HTTP server is wired to web.
e2e:
	go run ./cmd/vmctl e2e
