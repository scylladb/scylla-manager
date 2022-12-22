RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION	     ?= 3.1-dev

ifdef $$VERSION
VERSION := $$VERSION
endif

VERSION_NAME := $(VERSION)-$(RELEASE)
$(shell echo $(VERSION) > .version)
$(shell echo $(RELEASE) > .release)

GORELEASER := goreleaser --rm-dist
.PHONY: release
release:
	git tag $(VERSION_NAME) || true
	$(GORELEASER) --skip-publish --skip-validate

.PHONY: snapshot
snapshot:
	GORELEASER_CURRENT_TAG=$(VERSION_NAME) $(GORELEASER) --snapshot

.PHONY: clean
clean:
	@rm -Rf release
