.PHONY: local lint test test-integration

# A local Temporal server with priority and fairness enabled.
local:
	temporal server start-dev --ui-port 8080 --db-filename .temporal.db \
		--dynamic-config-value matching.useNewMatcher=true \
		--dynamic-config-value matching.enableFairness=true

lint:
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 -checks "all,-SA1019" ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 -tags integration -checks "all,-SA1019" ./...
	go vet ./...
	go vet -tags integration ./...
	test -z "$$(gofmt -l .)"

test:
	go test -race -count=1 -cover ./...

# Starts its own Temporal dev server, using a temporal binary on PATH or downloading one.
test-integration:
	go test -race -count=1 -tags integration -timeout=300s ./...
