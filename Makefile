.PHONY: build vet test fixture run verify

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

# This gate intentionally fails against a health-only server stub.
verify: vet test
	go run ./cmd/vmctl verify --repeat 20
	go run ./cmd/vmctl e2e
