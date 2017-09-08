all: check test

# check does static code analysis.
.PHONY: check
check:
	@go fmt ./... | ifne false
	@go vet ./...
	@golint `go list ./...` \
	| grep -v 'uuid.go:7:6:' ||: \
	| ifne false
	@misspell ./...
	@ineffassign ./

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
	@go test -cover -race ./... | grep -v 'no test files' ||:

# integration-test runs integration tests.
.PHONY: integration-test
integration-test:
	@go test -cover -tags integration -run Integration ./repair -cluster "172.16.1.10,172.16.1.20"

# gen regenetates source code and other resources.
.PHONY: gen
gen:
	rm -Rf dbapi/internal/*
	swagger generate client -A scylladb -f swagger/scylla-api.json -t dbapi/internal

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
get-tools:
	go get -u github.com/golang/dep/cmd/dep
	go get -u github.com/golang/lint/golint

	go get -u github.com/client9/misspell/cmd/misspell
	go get -u github.com/gordonklaus/ineffassign
	go get -u github.com/go-swagger/go-swagger/cmd/swagger
