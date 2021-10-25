all: help

# Prevent invoking make with a package specific test without a constraining a
# package.
ifneq "$(filter pkg-%,$(MAKECMDGOALS))" ""
ifeq "$(PKG)" ""
$(error Please specify package name with PKG e.g. PKG=./pkg/scyllaclient)
endif
endif

# Default package
PKG := ./pkg/...

GIT_ROOT = $(shell git rev-parse --show-toplevel)
TESTING_ROOT = $(GIT_ROOT)/testing
GOBIN ?= $(shell pwd)/bin
GO111MODULE := on
GOFILES = go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}{{range .TestGoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' $(PKG)


.PHONY: fmt
fmt: ## Format source code
	@$(GOBIN)/golangci-lint run -c .golangci-fmt.yml --fix $(PKG)

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
		[[ $$f =~ /mock_.*_test\.go ]] || \
		[[ $$f =~ _gen\.go ]] || \
		[[ `head -n 1 $$f` =~ '// Copyright (C) ' ]] || \
		(echo $$f; false); \
	done

.PHONY: .check-comments
.check-comments:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ _string\.go ]] || \
		! e=`grep -Pzo '\n\n\s+//\s*[a-z].*' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-errors-wrap
.check-errors-wrap:
	@set -e; for f in `$(GOFILES)`; do \
		! e=`grep -n -E 'errors\.(Errorf|New|Wrap|Wrapf)' $$f | grep -E '("| )fail'` || \
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
		[[ $$f =~ /timeutc/ ]] || \
		! e=`grep -n 'time.\(Now\|Parse(\|Since\)' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-lint
.check-lint:
	@$(GOBIN)/golangci-lint run $(PKG)

.PHONY: .check-vendor
.check-vendor:
	@e=`go mod verify` || (echo $$e; false)

.PHONY: test
test: ## Run unit and integration tests
test: unit-test integration-test

.PHONY: unit-test
unit-test: ## Run unit tests
	@echo "==> Running tests (race)"
	@go test -cover -race $(PKG)

.PHONY: integration-test
integration-test: ## Run integration tests
integration-test:
	@$(MAKE) pkg-integration-test PKG=./pkg/ping/cqlping
	@$(MAKE) pkg-integration-test PKG=./pkg/ping/dynamoping
	@$(MAKE) pkg-integration-test PKG=./pkg/scyllaclient
	@$(MAKE) pkg-integration-test PKG=./pkg/service/backup
	@$(MAKE) pkg-integration-test PKG=./pkg/service/cluster
	@$(MAKE) pkg-integration-test PKG=./pkg/service/healthcheck
	@$(MAKE) pkg-integration-test PKG=./pkg/service/repair
	@$(MAKE) pkg-integration-test PKG=./pkg/service/scheduler
	@$(MAKE) pkg-integration-test PKG=./pkg/store
	@$(MAKE) pkg-integration-test PKG=./pkg/schema/migrate
	@$(MAKE) pkg-integration-test PKG=./pkg/util/netwait

# Load Minio config for INTEGRATION_TEST_ARGS
include testing/.env

INTEGRATION_TEST_ARGS := -cluster 192.168.100.100 \
-managed-cluster 192.168.100.11,192.168.100.12,192.168.100.13,192.168.100.21,192.168.100.22,192.168.100.23 \
-user cassandra -password cassandra \
-agent-auth-token token \
-s3-data-dir $(PWD)/testing/minio/data -s3-provider Minio -s3-endpoint $(MINIO_ENDPOINT) -s3-access-key-id $(MINIO_USER_ACCESS_KEY) -s3-secret-access-key $(MINIO_USER_SECRET_KEY)

SSL_FLAGS := -ssl-ca-file $(TESTING_ROOT)/$(SSL_AUTHORITY_CRT) \
-ssl-key-file $(TESTING_ROOT)/$(SSL_CLIENT_KEY) \
-ssl-cert-file $(TESTING_ROOT)/$(SSL_CLIENT_CRT) \
-gocql.port $(SSL_PORT)

.PHONY: pkg-integration-test
pkg-integration-test: ## Run integration tests for a package, requires PKG parameter
pkg-integration-test: RUN=Integration
pkg-integration-test:
	@echo "==> Running integration tests for package $(PKG)"
	@go test -cover -v -tags integration -run $(RUN) $(PKG) $(INTEGRATION_TEST_ARGS) $(SSL_FLAGS) $(ARGS)

.PHONY: pkg-stress-test
pkg-stress-test: ## Run unit tests for a package in parallel in a loop to detect sporadic failures, requires PKG parameter
pkg-stress-test: RUN=Test
pkg-stress-test:
	@echo "==> Running stress tests for package $(PKG) (race)"
	@go test -race -c -o stress.test $(PKG)
	@cd $(PKG); $(GOBIN)/stress $(PWD)/stress.test -test.run $(RUN)

.PHONY: start-dev-env
start-dev-env: ## Start testing containers and run server
start-dev-env: .testing-up deploy-agent build-cli run-server

.PHONY: .testing-up
.testing-up:
	@make -C testing build down up

.PHONY: dev-env-status
dev-env-status:  ## Checks status of docker containers and cluster nodes
	@make -C testing status

.PHONY: build-agent
build-agent: ## Build development agent binary
	@echo "==> Building agent"
ifeq ($(SM_AGENT_STATIC_BUILD),yes)
	@CGO_ENABLED=0 go build -trimpath -mod=vendor -o ./scylla-manager-agent.dev ./pkg/cmd/agent
else
	@go build -trimpath -mod=vendor -race -o ./scylla-manager-agent.dev ./pkg/cmd/agent
endif

.PHONY: deploy-agent
deploy-agent: build-agent ## Deploy it to testing containers
	@echo "==> Deploying agent to testing containers"
	@make -C testing deploy-agent restart-agent

.PHONY: build-cli
build-cli: ## Build development cli binary
	@echo "==> Building sctool"
	@go build -trimpath -mod=vendor -o ./sctool.dev ./pkg/cmd/sctool

.PHONY: build-server
build-server: ## Build development server
	@echo "==> Building scylla-manager"
	@CGO_ENABLED=0 go build -trimpath -mod=vendor -o ./scylla-manager.dev ./pkg/cmd/scylla-manager

.PHONY: run-server
run-server: build-server ## Build and run development server
	@./scylla-manager.dev -c testing/scylla-manager/scylla-manager.yaml; rm -f ./scylla-manager.dev

.PHONY: build
build: build-cli build-agent build-server ## Build all project binaries

.PHONY: filler-tool
filler-tool: ## Build "filler" tool
	@echo "==> Building filler"
	@go build -trimpath -mod=vendor ./pkg/tools/filler/cmd/filler

.PHONY: clean
clean: ## Remove dev build artifacts (*.dev files)
	@rm -rf agent.dev sctool.dev scylla-manager.dev stress.test \
	filler

.PHONY: mrproper
mrproper: ## Clean go caches
	@go clean -r -cache -testcache -modcache ./...

.PHONY: generate
generate:  ## Recreate autogenerated resources
	@go generate $(PKG)

.PHONY: vendor
vendor: ## Fix dependencies and make vendored copies
	@go mod tidy
	@go mod vendor

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
