all: help

ifndef GOBIN
export GOBIN := $(GOPATH)/bin
endif

GO111MODULE := on
GOFILES = go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}{{range .TestGoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' ./...

.PHONY: fmt
fmt: ## Format source code
	@golangci-lint run -c .golangci-fmt.yml --fix

.PHONY: check
check: ## Perform static code analysis
check: .check-go-version .check-copyright .check-comments .check-errors-wrap \
.check-log-capital-letter .check-timeutc .check-lint .check-vendor

.PHONY: .check-go-version
.check-go-version:
	@[[ "`go version`" =~ "`cat .go-version`" ]] || echo "[WARNING] Required Go version `cat .go-version` found `go version | grep -o -E '1\.[0-9\.]+'`"

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /scyllaclient/internal/ ]] || \
		[[ $$f =~ /mermaidclient/internal/ ]] || \
		[[ $$f =~ /rclone/rcserver/internal/ ]] || \
		[[ $$f =~ /mock_.*_test[.]go ]] || \
		[[ "`head -n 1 $$f`" == "// Copyright (C) 2017 ScyllaDB" ]] || \
		(echo $$f; false); \
	done

.PHONY: .check-comments
.check-comments:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ _string\.go ]] || \
		[[ $$f =~ /mermaidclient/internal/ ]] || \
		[[ $$f =~ /scyllaclient/internal/ ]] || \
		! e=`pcregrep -noM '$$\n\n\s+//\s*[a-z].*' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-errors-wrap
.check-errors-wrap:
	@set -e; for f in `$(GOFILES)`; do \
		! e=`grep -n errors.Wrap $$f | grep "failed to"` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-log-capital-letter
.check-log-capital-letter:
	@set -e; for f in `$(GOFILES)`; do \
		! e=`grep -n -E '\.(Error|Info|Debug)\(ctx, "[a-z]' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-timeutc
.check-timeutc:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /internal/ ]] || \
		! e=`grep -n 'time.\(Now\|Parse(\|Since\)' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-lint
.check-lint:
	@$(GOBIN)/golangci-lint run ./...

.PHONY: .check-vendor
.check-vendor:
	@e=`go mod verify` || (echo $$e; false)

.PHONY: test
test: ## Run unit and integration tests
test: unit-test integration-test

.PHONY: unit-test
unit-test: ## Run unit tests
	@echo "==> Running tests (race)"
	@go test -cover -race ./...

.PHONY: integration-test
integration-test: ## Run integration tests
integration-test:
	@$(MAKE) pkg-integration-test PKG=./internal/cqlping
	@$(MAKE) pkg-integration-test PKG=./scyllaclient
	@$(MAKE) pkg-integration-test PKG=./service/backup
	@$(MAKE) pkg-integration-test PKG=./service/cluster
	@$(MAKE) pkg-integration-test PKG=./service/healthcheck
	@$(MAKE) pkg-integration-test PKG=./service/repair
	@$(MAKE) pkg-integration-test PKG=./service/scheduler
	@$(MAKE) pkg-integration-test PKG=./service/secrets/dbsecrets
	@$(MAKE) pkg-integration-test PKG=./schema/cql

# Load Minio config for INTEGRATION_TEST_ARGS
include testing/.env

INTEGRATION_TEST_ARGS := -cluster 192.168.100.100 \
-managed-cluster 192.168.100.11,192.168.100.12,192.168.100.13,192.168.100.21,192.168.100.22,192.168.100.23 \
-agent-auth-token token \
-s3-data-dir $(PWD)/testing/minio/data -s3-endpoint $(MINIO_ENDPOINT) -s3-access-key-id $(MINIO_ACCESS_KEY) -s3-secret-access-key $(MINIO_SECRET_KEY)

.PHONY: pkg-integration-test
pkg-integration-test: ## Run integration tests for a package, requires PKG parameter
pkg-integration-test: RUN=Integration
pkg-integration-test:
	@echo "==> Running integration tests for package $(PKG)"
	@go test -cover -v -tags integration -run $(RUN) $(PKG) $(INTEGRATION_TEST_ARGS) $(ARGS)

.PHONY: pkg-stress-test
pkg-stress-test: ## Run unit tests for a package in parallel in a loop to detect sporadic failures, requires PKG parameter
	@echo "==> Running stress tests for package $(PKG) (race)"
	@go test -race -c -o stress.test $(PKG)
	@cd $(PKG); $(GOBIN)/stress $(PWD)/stress.test

.PHONY: start-dev-env
start-dev-env: ## Start testing containers and run server
start-dev-env: .testing-up dev-agent dev-cli dev-server

.PHONY: .testing-up
.testing-up:
	@make -C testing build down up

.PHONY: dev-env-status
dev-env-status:  ## Checks status of docker containers and cluster nodes
	@make -C testing status

.PHONY: dev-agent
dev-agent: ## Build development agent binary and deploy it to testing containers
	@echo "==> Building agent"
	@go build -mod=vendor -race -o ./agent.dev ./cmd/agent
	@echo "==> Deploying agent to testing containers"
	@make -C testing deploy-agent restart-agent

.PHONY: dev-cli
dev-cli: ## Build development cli binary
	@echo "==> Building sctool"
	@go build -mod=vendor -o ./sctool.dev ./cmd/sctool

.PHONY: dev-server
dev-server: ## Build and run development server
	@echo "==> Building scylla-manager"
	@go build -mod=vendor -race -o ./scylla-manager.dev ./cmd/scylla-manager
	@echo
	@./scylla-manager.dev -c testing/scylla-manager/scylla-manager.yaml; rm -f ./scylla-manager.dev

.PHONY: clean
clean: ## Remove dev build artifacts (*.dev files)
	@rm -rf agent.dev sctool.dev scylla-manager.dev stress.test

.PHONY: mrproper
mrproper: ## Clean go caches
	@go clean -r -cache -testcache -modcache ./...

.PHONY: generate
generate:  ## Recreate autogenerated resources
	@go generate ./...

.PHONY: vendor
vendor: ## Fix dependencies and make vendored copies
	@go mod tidy
	@go mod vendor

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
