all: clean build

gobuild = go build -ldflags "-X main.version=`git describe --always`"

.PHONY: clean
clean:
	@rm -Rf build

build:
	@mkdir -p build/linux_amd64
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(gobuild) -o build/linux_amd64/scylla-mgmt ../cmd/scylla-mgmt
	@GOOS=linux GOARCH=amd64 $(gobuild) -race -o build/linux_amd64/scylla-mgmt-race ../cmd/scylla-mgmt
