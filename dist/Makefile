RELEASE      := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION      ?=3.7.0
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
	git tag -f $(VERSION_NAME)
	$(GORELEASER) $(publish) --skip=validate --config .goreleaser.yaml
	SKIP_LATEST_RELEASE=true $(GORELEASER) --skip=validate --config .goreleaser-docker.yaml

.PHONY: snapshot
snapshot:
	git tag -f $(VERSION_NAME)
	$(GORELEASER) --snapshot --skip=publish --skip=validate --config .goreleaser.yaml
	$(GORELEASER) --skip=validate --config .goreleaser-docker.yaml

.PHONY: clean
clean:
	@rm -Rf release release-docker .version .release
