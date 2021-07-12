RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)

ifeq "$(VERSION)" ""
$(error Please specify VERSION e.g. VERSION=2.0)
endif

TAG := $(shell git describe --tags)

VERSION_NAME := $(VERSION)-$(RELEASE)

ifneq "$(TAG)" ""
$(shell git tag $(VERSION_NAME))
else
$(shell git tag -d $(VERSION_NAME))
endif

GORELEASER := goreleaser --rm-dist

.PHONY: release
release:
	$(GORELEASER) --skip-publish --skip-validate

.PHONY: snapshot
snapshot:
	$(GORELEASER) --snapshot

.PHONY: clean
clean:
	@rm -Rf release
