all: clean check test

gobuild = go build -ldflags "-X main.version=`git describe --always`"
gofiles = go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' ./...

# clean removes the build files.
.PHONY: clean
clean:
	@rm -Rf release
	@go clean -r

# check does static code analysis.
.PHONY: check
check: .check-copyright .check-fmt .check-vet .check-lint .check-misspell .check-ineffassign

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(gofiles)`; do \
		[[ $$f =~ /scylla/internal/ ]] || \
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

.PHONY: .check-misspell
.check-misspell:
	@misspell ./...

.PHONY: .check-ineffassign
.check-ineffassign:
	@ineffassign `$(gofiles)`

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
	@go test -cover -race ./...

# integration-test runs integration tests.
.PHONY: integration-test
integration-test:
	@go test -cover -tags integration -run Integration ./repair -cluster "172.16.1.10,172.16.1.20"

# gen regenetates source code and other resources.
.PHONY: gen
gen:
	rm -Rf scylla/internal/*
	swagger generate client -A scylladb -f swagger/scylla-api.json -t scylla/internal
	go generate ./...

# release creates a release built in release directory.
release: clean
	@mkdir -p release/linux_amd64
	@GOOS=linux GOARCH=amd64 $(gobuild) -race -o release/linux_amd64/mgmtd-race ./cmd/mgmtd
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(gobuild) -o release/linux_amd64/mgmtd ./cmd/mgmtd

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
get-tools:
	go get -u github.com/golang/dep/cmd/dep
	go get -u github.com/golang/lint/golint

	go get -u github.com/client9/misspell/cmd/misspell
	go get -u github.com/gordonklaus/ineffassign
	go get -u github.com/go-swagger/go-swagger/cmd/swagger
	go get -u github.com/golang/mock/mockgen
