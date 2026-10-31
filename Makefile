GO            ?= go
BIN           := $(CURDIR)/bin
BINARY        := kafka-mcp
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS       := -s -w -X main.version=$(VERSION)

# The linter is built with the local toolchain so it understands the same Go version.
GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT         := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build ./bin/kafka-mcp
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/$(BINARY) .

.PHONY: test
test: ## Run tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and print per-package coverage
	$(GO) test -count=1 -coverprofile=$(BIN)/coverage.out ./...
	$(GO) tool cover -func=$(BIN)/coverage.out | tail -1

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run golangci-lint (includes the 199-line file and 50-line function limits)
	$(GOLANGCI_LINT) run ./...

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format code (gofmt + goimports)
	$(GOLANGCI_LINT) fmt ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: tidy
tidy: ## Tidy go.mod and fail if it changed
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

.PHONY: check
check: vet lint test build ## Everything CI should run

.PHONY: clean
clean: ## Remove build outputs
	rm -rf $(BIN)

$(GOLANGCI_LINT):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@mv $(BIN)/golangci-lint $(GOLANGCI_LINT)
