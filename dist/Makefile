all: help

GIT_ROOT := $(shell git rev-parse --show-toplevel)

VERSION := 2.0.2
RELEASE := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION_NAME := $(VERSION)-$(RELEASE)

DEB_TARGET ?= "stretch"

GO_VERSION := 1.13.5
GO_HASH := 512103d7ad296467814a6e3f635631bd35574cab3369a97a323c9a585ccaa569
GO_URL := https://storage.googleapis.com/golang/go$(GO_VERSION).linux-amd64.tar.gz
GO_BUNDLE := .go$(GO_VERSION).linux-amd64.tar.gz

$(GO_BUNDLE):
	@echo "==> Downloading Go $(GO_VERSION)..."
	@curl -sSq $(GO_URL) -o $(GO_BUNDLE)
	@[ "`sha256sum $(GO_BUNDLE) | cut -c1-64`" == "$(GO_HASH)" ]

.PHONY: clean
clean: ## Remove release and all the temporary files
	@rm -Rf /tmp/mermaid_rpm.*
	@rm -Rf release

release/scylla-manager-$(VERSION_NAME).tar.gz:
	@echo "==> Archiving..."
	@mkdir -p release
	@cd $(GIT_ROOT); git archive --format tar.gz \
	--prefix scylla-manager-$(VERSION_NAME)/ \
	-o $(PWD)/release/scylla-manager-$(VERSION_NAME).tar.gz HEAD

release/rpm: $(GO_BUNDLE) release/scylla-manager-$(VERSION_NAME).tar.gz ## Create RPM package
	$(eval RPMBUILD := $(shell mktemp -t -d mermaid_rpm.XXXX))
	@echo "==> Building RPM in $(RPMBUILD)..."
	@mkdir -p $(RPMBUILD)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

	@tar -xzf $(GO_BUNDLE) -C $(RPMBUILD)/BUILD
	@cp release/scylla-manager-$(VERSION_NAME).tar.gz $(RPMBUILD)/SOURCES

	@rpmbuild -bb --target x86_64 \
	--define "mermaid_version $(VERSION)" \
	--define "mermaid_release $(RELEASE)" \
	--define "_topdir $(RPMBUILD)"  \
	redhat/scylla-manager.spec > /dev/null

	@mkdir -p release/rpm
	@cp $(RPMBUILD)/RPMS/x86_64/*.rpm release/rpm
	@rm -Rf $(RPMBUILD)

release/deb: release/scylla-manager-$(VERSION_NAME).tar.gz ## Create DEB package
	@echo "==> Building DEB for $(DEB_TARGET)..."
	@mkdir -p release/deb

	@cd $(GIT_ROOT); cp dist/release/scylla-manager-$(VERSION_NAME).tar.gz ../scylla-manager-server_$(VERSION_NAME).orig.tar.gz
	@cd $(GIT_ROOT); MERMAID_VERSION=$(VERSION) MERMAID_RELEASE=$(RELEASE) ./dist/debian/build_deb.sh --target $(DEB_TARGET)
	@cd $(GIT_ROOT); rm -rf debian

.PHONY: inspect/rpm
inspect/rpm: ## Display RPM package meta-data
	@rpm -qpli release/rpm/scylla-manager{,-server,-client,-agent}-$(VERSION_NAME).x86_64.rpm

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
