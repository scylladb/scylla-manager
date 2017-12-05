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
check: .check-copyright .check-fmt .check-vet .check-lint .check-ineffassign .check-mega .check-misspell .check-vendor

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /scylla/internal/ ]] || \
		[[ $$f =~ /mermaidclient/internal/ ]] || \
		[[ $$f =~ .*_mock[.]go ]] || \
		[ "`head -n 1 $$f`" == "// Copyright (C) 2017 ScyllaDB" ] || \
		(echo $$f; false); \
	done

.PHONY: .check-fmt
.check-fmt:
	@go fmt ./... | tee /dev/stderr | ifne false

.PHONY: .check-vet
.check-vet:
	@go vet ./...

.PHONY: .check-lint
.check-lint:
	@$(GOBIN)/golint `go list ./...` \
	| tee /dev/stderr | ifne false

.PHONY: .check-ineffassign
.check-ineffassign:
	@$(GOBIN)/ineffassign `$(GOFILES)`

.PHONY: .check-mega
.check-mega:
	@$(GOBIN)/megacheck ./...

.PHONY: .check-misspell
.check-misspell:
	@$(GOBIN)/misspell ./...

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

INTEGRATION_TEST_ARGS := -cluster 172.16.1.100 -managed-cluster 172.16.1.10

# integration-test runs integration tests.
.PHONY: integration-test
integration-test: unit-test
	@echo "==> Running integration tests..."
	@go test -cover -race -tags integration -run Integration ./ssh $(INTEGRATION_TEST_ARGS)
	@go test -cover -race -tags integration -run Integration ./repair $(INTEGRATION_TEST_ARGS)

# dev-server runs development server.
.PHONY: dev-server
dev-server:
	@echo "==> Building development server..."
	@go build -o ./scylla-mgmt.dev ./cmd/scylla-mgmt
	@echo "==> Running development server..."
	@./scylla-mgmt.dev -c testing/scylla_cluster/scylla-mgmt.yaml --developer-mode; rm -f ./scylla-mgmt.dev

# dev-cli builds development cli binary.
.PHONY: dev-cli
dev-cli:
	@echo "==> Building development cli..."
	@go build -o ./sctool ./cmd/sctool/

# gen regenetates source code and other resources.
.PHONY: gen
gen:
	@echo "==> Generating..."
	@go generate ./...

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
get-tools: GOPATH := $(shell mktemp -d)
get-tools:
	@echo "==> Installing tools..."

	@go get -u github.com/golang/dep/cmd/dep
	@go get -u github.com/golang/lint/golint
	@go get -u github.com/golang/mock/mockgen

	@go get -u github.com/client9/misspell/cmd/misspell
	@go get -u github.com/fatih/gomodifytags
	@go get -u github.com/gordonklaus/ineffassign
	@go get -u github.com/go-swagger/go-swagger/cmd/swagger
	@go get -u honnef.co/go/tools/cmd/megacheck

	@rm -Rf $(GOPATH)
