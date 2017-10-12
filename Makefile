all: clean check test

GOFILES := go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}{{range .TestGoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' ./...

# clean removes the build files.
.PHONY: clean
clean:
	@go clean -r

# check does static code analysis.
.PHONY: check
check: .check-copyright .check-fmt .check-vet .check-lint .check-ineffassign .check-mega .check-misspell

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /scylla/internal/ ]] || \
		[[ $$f =~ /restapiclient/ ]] || \
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
	@golint `go list ./...` \
	| tee /dev/stderr | ifne false

.PHONY: .check-ineffassign
.check-ineffassign:
	@ineffassign `$(GOFILES)`

.PHONY: .check-mega
.check-mega:
	@megacheck ./...

.PHONY: .check-misspell
.check-misspell:
	@misspell ./...

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

# integration-test runs integration tests.
.PHONY: integration-test
integration-test: unit-test
	@echo "==> Running integration tests..."
	@go test -cover -tags integration -run Integration ./repair -cluster "172.16.1.10"

# dev-server runs development server.
.PHONY: dev-server
dev-server:
	@echo "==> Running development server..."
	@go run ./cmd/scylla-mgmt/*.go server -config-file docker/scylla-mgmt.yaml -debug

# gen regenetates source code and other resources.
.PHONY: gen
gen:
	@echo "==> Generating..."
	@go generate ./...

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
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
