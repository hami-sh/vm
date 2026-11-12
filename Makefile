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

# The one command. W5 extends it with `vmctl verify --repeat 20` and the
# end-to-end test once they exist; until then it is vet + race tests.
verify: vet test
