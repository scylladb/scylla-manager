RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION         ?=3.3.0
PUBLISH      := 0

ifdef $$VERSION
VERSION := $$VERSION
endif

ifeq ($(PUBLISH),0)
publish = --skip=publish
VERSION_NAME := $(VERSION)-$(RELEASE)
else
VERSION_NAME := v$(VERSION)
endif

$(shell echo $(VERSION) > .version)
$(shell echo $(RELEASE) > .release)

GORELEASER := goreleaser --clean
.PHONY: release
release:
	git tag $(VERSION_NAME) || true
	$(GORELEASER) $(publish) --skip=validate --config .goreleaser.yaml
	$(GORELEASER) --skip=validate --config .goreleaser-docker.yaml

.PHONY: snapshot
snapshot:
	GORELEASER_CURRENT_TAG=$(VERSION_NAME)
	$(GORELEASER) --snapshot --skip=publish --skip=validate --config .goreleaser.yaml
	$(GORELEASER) --skip=validate --config .goreleaser-docker.yaml

.PHONY: clean
clean:
	@rm -Rf release release-docker .version .release
