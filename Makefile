all: check test

# check does static code analysis.
.PHONY: check
check:
	gofmt -s -l . | ifne false
	go vet ./...
	golint -set_exit_status ./...
	misspell ./...
	ineffassign .

# test runs unit tests.
.PHONY: test
test:
	go test -cover -race ./...

# gen regenetates source code and other resources.
.PHONY: gen
gen:
	rm -Rf dbapi/internal/*
	swagger generate client -A scylladb -f swagger/scylla-api.json -t dbapi/internal

.PHONY: vendor
vendor:
	dep ensure

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
get-tools:
	# install up-to-date go tools
	go get -u github.com/golang/dep/cmd/dep
	go get -u github.com/golang/lint/golint
	# install additional tools
	go get -u github.com/client9/misspell/cmd/misspell
	go get -u github.com/gordonklaus/ineffassign
	go get -u github.com/go-swagger/go-swagger/cmd/swagger
