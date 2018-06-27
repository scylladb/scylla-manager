all: clean check test

ifndef GOBIN
export GOBIN := $(GOPATH)/bin
endif

GOFILES := go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}{{range .TestGoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' ./...

# clean removes the build files.
.PHONY: clean
clean:
	@go clean -r

# check does static code analysis.
.PHONY: check
check: .check-copyright .check-timeutc .check-misspell .check-lint .check-vendor

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /scyllaclient/internal/ ]] || \
		[[ $$f =~ /mermaidclient/internal/ ]] || \
		[[ $$f =~ .*_mock[.]go ]] || \
		[ "`head -n 1 $$f`" == "// Copyright (C) 2017 ScyllaDB" ] || \
		(echo $$f; false); \
	done

.PHONY: .check-timeutc
.check-timeutc:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /internal/timeutc/ ]] || \
		[[ $$f =~ /internal/retryablehttp/ ]] || \
		[[ $$f =~ /mermaidclient/internal/ ]] || \
		[[ $$f =~ /scyllaclient/internal/ ]] || \
		[ "`grep 'time.\(Now\|Parse(\|Since\)' $$f`" == "" ] || \
		(echo $$f; false); \
	done

.PHONY: .check-misspell
.check-misspell:
	@$(GOBIN)/misspell ./...

.PHONY: .check-lint
.check-lint:
	@$(GOBIN)/golangci-lint run -s --disable-all -E govet -E errcheck -E staticcheck \
	-E gas -E typecheck -E unused -E structcheck -E varcheck -E ineffassign -E deadcode \
	-E gofmt -E golint -E gosimple -E unconvert -E dupl -E depguard -E gocyclo \
	--tests=false \
	--exclude-use-default=false \
	--exclude='composite literal uses unkeyed fields' \
	--exclude='Error return value of `.+\.Close` is not checked' \
	--exclude='G104' \
	--exclude='G304' \
	./...

.PHONY: .check-vendor
.check-vendor:
	@$(GOBIN)/dep ensure -no-vendor -dry-run

# fmt formats the source code.
.PHONY: fmt
fmt:
	@go fmt ./...

# test runs unit and integration tests.
.PHONY: test
test: unit-test integration-test

# unit-test runs unit tests.
.PHONY: unit-test
unit-test:
	@echo "==> Running tests (race)..."
	@go test -cover -race ./...

INTEGRATION_TEST_ARGS := -cluster 192.168.100.100 -managed-cluster 192.168.51.11

# integration-test runs integration tests.
.PHONY: integration-test
integration-test:
	@echo "==> Running integration tests..."
	@go test -cover -race -v -tags integration -run Integration ./internal/ssh $(INTEGRATION_TEST_ARGS)
	@go test -cover -race -v -tags integration -run Integration ./scyllaclient $(INTEGRATION_TEST_ARGS)
	@go test -cover -race -v -tags integration -run Integration ./cluster $(INTEGRATION_TEST_ARGS)
	@go test -cover -race -v -tags integration -run Integration ./repair $(INTEGRATION_TEST_ARGS)
	@go test -cover -race -v -tags integration -run Integration ./sched $(INTEGRATION_TEST_ARGS)
	@go test -cover -race -v -tags integration -run Integration ./schema/cql $(INTEGRATION_TEST_ARGS)

# dev-server runs development server.
.PHONY: dev-server
dev-server:
	@echo "==> Building development server..."
	@go build -o ./scylla-manager.dev ./cmd/scylla-manager
	@echo "==> Running development server..."
	@./scylla-manager.dev -c testing/scylla-manager/scylla-manager.yaml --developer-mode; rm -f ./scylla-manager.dev

# dev-server-debug runs development server with dlv debugger.
.PHONY: dev-server-debug
dev-server-debug:
	@echo "==> Building development server..."
	@go build -gcflags='-N -l' -o ./scylla-manager.dev ./cmd/scylla-manager
	@echo "==> Running development server in debug mode..."
	@$(GOBIN)/dlv --listen=:2345 --headless=true --api-version=2 exec ./scylla-manager.dev -- -c testing/scylla-manager.yaml --developer-mode

# dev-server-kill stops all dev-server instances.
.PHONY: dev-server-kill
dev-server-kill:
	@killall -9 scylla-manager.dev

# dev-cli builds development cli binary.
.PHONY: dev-cli
dev-cli:
	@echo "==> Building development cli..."
	@go build -o ./sctool.dev ./cmd/sctool/

# gen regenetates source code and other resources.
.PHONY: gen
gen:
	@echo "==> Generating..."
	@go generate ./...

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
get-tools: GOPATH := $(shell mktemp -d)
get-tools:
	@echo "==> Installing tools at $(GOBIN)..."
	@mkdir -p $(GOBIN)

# development tools
	@go get -u github.com/derekparker/delve/cmd/dlv
	@go get -u github.com/fatih/gomodifytags
	@go get -u github.com/go-swagger/go-swagger/cmd/swagger
	@go get -u github.com/golang/dep/cmd/dep
	@go get -u github.com/golang/mock/mockgen
# linters
	@go get -u github.com/client9/misspell/cmd/misspell
	@go get -u github.com/golangci/golangci-lint/cmd/golangci-lint

	@rm -Rf $(GOPATH)
