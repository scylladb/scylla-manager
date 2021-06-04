RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION_NAME := $(VERSION)-$(RELEASE)

GORELEASER := $(shell git rev-parse --show-toplevel)/bin/goreleaser --rm-dist

.PHONY: release
release:
	ifeq "$(VERSION)" ""
	$(error Please specify VERSION e.g. VERSION=2.0)
	endif
	git tag $(VERSION_NAME)
	$(GORELEASER) --skip-publish

.PHONY: snapshot
snapshot:
	$(GORELEASER) --snapshot
