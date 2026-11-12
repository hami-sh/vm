.PHONY: build vet test fixture run verify e2e

VERIFY_PARALLEL ?= 4

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
# Preserve every batch; publish image evidence only after the suite succeeds.
verify: vet test
	@mkdir -p runs
	@dir=$$(mktemp -d runs/verify-XXXXXX); \
	go run -race ./cmd/vmctl verify --repeat 20 --parallel $(VERIFY_PARALLEL) --output "$$dir" && \
	rm -rf runs/latest && cp -a "$$dir" runs/latest

# Separate integration gate; fails until the HTTP server is wired to web.
e2e:
	go run ./cmd/vmctl e2e
