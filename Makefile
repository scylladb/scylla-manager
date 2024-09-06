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

SHELL := /bin/bash
GIT_ROOT = $(shell git rev-parse --show-toplevel)
GOBIN ?= $(shell pwd)/bin
GOFILES = go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}{{range .TestGoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' $(PKG)

SCYLLA_VERSION?=scylla:6.0.1
IP_FAMILY?=IPV4
RAFT_SCHEMA?=none
TABLETS?=enabled

MANAGER_CONFIG := testing/scylla-manager/scylla-manager.yaml
PUBLIC_NET := 192.168.200.
MINIO_ENDPOINT := https://192.168.200.99:9000
ifeq ($(IP_FAMILY), IPV6)
	MANAGER_CONFIG := testing/scylla-manager/scylla-manager-ipv6.yaml
	PUBLIC_NET := 2001:0DB9:200::
	MINIO_ENDPOINT := https://[2001:0DB9:200::99]:9000
endif

.PHONY: fmt
fmt: ## Format source code
	@$(GOBIN)/golangci-lint run -c .golangci-fmt.yml --fix $(PKG)

.PHONY: check
check: ## Perform static code analysis
check: .check-go-version .check-copyright .check-comments \
.check-log-capital-letter .check-timeutc .check-lint .check-vendor

.PHONY: .check-go-version
.check-go-version:
	@[[ "`go version`" =~ "`cat .go-version`" ]] || echo "[WARNING] Required Go version `cat .go-version` found `go version | grep -o -E '1\.[0-9\.]+'`"

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /mock_.*_test\.go ]] || \
		[[ $$f =~ _gen\.go ]] || \
		[[ `head -n 1 $$f` =~ '// Copyright (C) ' ]] || [[ `head -n 1 $$f` =~ 'DO NOT EDIT' ]] || (echo $$f; false); \
	done

.PHONY: .check-comments
.check-comments:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ _string\.go ]] || \
		! e=`grep -Pzo '\n\n\s+//\s*[a-z].*' $$f` || \
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
	@$(MAKE) pkg-integration-test PKG=./pkg/service/restore
	@$(MAKE) pkg-integration-test PKG=./pkg/service/cluster
	@$(MAKE) pkg-integration-test PKG=./pkg/service/healthcheck
	@$(MAKE) pkg-integration-test PKG=./pkg/service/repair
	@$(MAKE) pkg-integration-test PKG=./pkg/service/scheduler
	@$(MAKE) pkg-integration-test PKG=./pkg/store
	@$(MAKE) pkg-integration-test PKG=./pkg/schema/migrate
	@$(MAKE) pkg-integration-test PKG=./v3/pkg/util/netwait

# Load Minio config for INTEGRATION_TEST_ARGS
include testing/.env

INTEGRATION_TEST_ARGS := -cluster $(PUBLIC_NET)100 \
-managed-cluster $(PUBLIC_NET)11,$(PUBLIC_NET)12,$(PUBLIC_NET)13,$(PUBLIC_NET)21,$(PUBLIC_NET)22,$(PUBLIC_NET)23 \
-test-network $(PUBLIC_NET) \
-managed-second-cluster $(PUBLIC_NET)31,$(PUBLIC_NET)32 \
-user cassandra -password cassandra \
-agent-auth-token token \
-s3-data-dir ./testing/minio/data -s3-provider Minio -s3-endpoint $(MINIO_ENDPOINT) -s3-access-key-id $(MINIO_USER_ACCESS_KEY) -s3-secret-access-key $(MINIO_USER_SECRET_KEY)

SSL_FLAGS := -ssl-ca-file ./testing/$(SSL_AUTHORITY_CRT) -ssl-key-file ./testing/$(SSL_CLIENT_KEY) -ssl-cert-file ./testing/$(SSL_CLIENT_CRT) -gocql.port $(SSL_PORT)

CURRENT_UID = $(shell id -u)
CURRENT_GID = $(shell id -g)

.PHONY: pkg-integration-test
pkg-integration-test: ## Run integration tests for a package, requires PKG parameter
pkg-integration-test: RUN=Integration
pkg-integration-test:
	@echo "==> Running integration tests for package $(PKG)"
	@docker kill scylla_manager_server 2> /dev/null || true
	@CGO_ENABLED=0 GOOS=linux go test -tags integration -c -o ./integration-test.dev $(PKG)
	@PWD=`pwd`; echo $(INTEGRATION_TEST_ARGS) $(SSL_FLAGS) $(ARGS) | sed -e "s=./testing=${PWD}/testing=g"
	@docker run --name "scylla_manager_server" \
		--network host \
		-v "/tmp:/tmp" \
		-v "$(PWD)/integration-test.dev:/usr/bin/integration-test:ro" \
		-v "$(PWD)/testing:/integration-test/testing" \
		-v "$(PWD)/$(PKG)/testdata:/integration-test/testdata" \
		-w "/integration-test" \
		-u $(CURRENT_UID):$(CURRENT_GID) \
		-i --read-only --rm ubuntu integration-test -test.v -test.run $(RUN) $(INTEGRATION_TEST_ARGS) $(SSL_FLAGS) $(ARGS)

.PHONY: api-integration-test
api-integration-test: build-cli clean-server run-server
	@echo "==> Running API integration"
	@docker build -t scylla-manager-api-tests -f testing/api-tests.Dockerfile .
	@docker run --name scylla_manager_server_api_integration \
		--network host \
		-e "CGO_ENABLED=0" \
		-e "GOOD=linux" \
		-e "GOCACHE=/tmp/go-build" \
		-v "$(PWD):/scylla-manager" \
		-w "/scylla-manager" \
		-u $(CURRENT_UID):$(CURRENT_GID) \
		-i -d --rm scylla-manager-api-tests sleep infinity
	@docker exec -it scylla_manager_server_api_integration make build-cli OUTPUT=sctool.api-tests
	@docker exec -it scylla_manager_server_api_integration go test -tags api_integration ./pkg/command/... -test.v -test.run IntegrationAPI
	@$(MAKE) clean-server

.PHONY: pkg-stress-test
pkg-stress-test: ## Run unit tests for a package in parallel in a loop to detect sporadic failures, requires PKG parameter
pkg-stress-test: RUN=Test
pkg-stress-test:
	@echo "==> Running stress tests for package $(PKG) (race)"
	@go test -race -c -o stress.test $(PKG)
	@cd $(PKG); $(GOBIN)/stress $(PWD)/stress.test -test.run $(RUN)

.PHONY: start-dev-env
start-dev-env: ## Start testing containers
start-dev-env: .testing-up deploy-agent build-cli

.PHONY: .testing-up
.testing-up:
	@IPV6=$(IPV6) SCYLLA_VERSION=$(SCYLLA_VERSION) RAFT_SCHEMA=$(RAFT_SCHEMA) TABLETS=$(TABLETS) make -C testing build down up

.PHONY: dev-env-status
dev-env-status:  ## Checks status of docker containers and cluster nodes
	@IPV6=$(IPV6) make -C testing status

.PHONY: build-agent
build-agent: ## Build development agent binary
	@echo "==> Building agent"
	@CGO_ENABLED=0 GOOS=linux go build -trimpath -mod=vendor -o ./scylla-manager-agent.dev ./pkg/cmd/agent

.PHONY: deploy-agent
deploy-agent: build-agent ## Deploy it to testing containers
	@echo "==> Deploying agent to testing containers"
	@IPV6=$(IPV6) make -C testing deploy-agent restart-agent

.PHONY: build-cli
build-cli: ## Build development cli binary
build-cli: OUTPUT=./sctool.dev
build-cli:
	@echo "==> Building sctool"
	@go build -trimpath -mod=vendor -o $(OUTPUT) ./pkg/cmd/sctool

.PHONY: build-server
build-server: ## Build development server
	@echo "==> Building scylla-manager"
	@CGO_ENABLED=0 GOOS=linux go build -trimpath -mod=vendor -o ./scylla-manager.dev ./pkg/cmd/scylla-manager

.PHONY: clean-server
clean-server: ## Remove development server container
	@echo "==> Removing (if exists) scylla_manager_server container"
	@docker rm -f scylla_manager_server 2> /dev/null || true
	@docker rm -f scylla_manager_server_api_integration 2> /dev/null || true

.PHONY: run-server
run-server: build-server ## Build and run development server
	@docker run --name "scylla_manager_server" \
		--network scylla_manager_second \
		-p "5080:5080" \
		-p "5443:5443" \
		-p "5090:5090" \
		-v "$(PWD)/scylla-manager.dev:/usr/bin/scylla-manager:ro" \
		-v "$(PWD)/sctool.dev:/usr/bin/sctool:ro" \
		-v "$(PWD)/$(MANAGER_CONFIG):/etc/scylla-manager/scylla-manager.yaml:ro" \
		-v "/tmp:/tmp" \
		-d --read-only --rm scylladb/scylla-manager-dev scylla-manager
	@docker network connect scylla_manager_public scylla_manager_server

.PHONY: build
build: build-cli build-agent build-server ## Build all project binaries

.PHONY: docs
docs: export TZ = UTC ## set UTC as the default timezone in generated docs
docs: build ## Update docs/source with generated code
	@rm -rf docs/source/sctool/partials
	@SCYLLA_MANAGER_CLUSTER="" ./sctool doc -o docs/source/sctool/partials

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
