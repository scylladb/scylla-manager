RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION	     ?= "2.6-dev"

TAG := $(foreach var,$(shell git tag --contains $(git describe --always)),$(shell git tag -d $(var)))

ifdef $$VERSION
VERSION := $$VERSION
endif

VERSION_NAME := $(VERSION)-$(RELEASE)
TAG := $(shell git tag $(VERSION_NAME))

GORELEASER := goreleaser --rm-dist

.PHONY: release
release:
	$(shell echo $(VERSION) > .version)
	$(shell echo $(RELEASE) > .release)
	$(GORELEASER) --skip-publish --skip-validate

.PHONY: snapshot
snapshot:
	$(GORELEASER) --snapshot

.PHONY: clean
clean:
	@rm -Rf release
	@git tag -d $(TAG)