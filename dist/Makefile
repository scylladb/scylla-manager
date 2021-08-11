RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)

ifeq "$(VERSION)" ""
	$(error Please specify VERSION e.g. VERSION=2.0)
endif

TAG := $(shell git describe --tags)
VERSION_NAME := $(VERSION)-$(RELEASE)

$(shell echo $(VERSION) > .version)
$(shell echo $(RELEASE) > .release)
$(shell git tag $(VERSION_NAME))

GORELEASER := goreleaser --rm-dist

.PHONY: release
release:
	$(GORELEASER) --skip-publish --skip-validate
	$(shell git tag -d $(TAG))

.PHONY: snapshot
snapshot:
	$(GORELEASER) --snapshot

.PHONY: clean
clean:
	@rm -Rf release
