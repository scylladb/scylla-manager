all: help

GIT_ROOT := $(shell git rev-parse --show-toplevel)

VERSION := 2.5.rc1
RELEASE := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION_NAME := $(VERSION)-$(RELEASE)

GO_VERSION = $(shell cat ../.go-version)

.PHONY: clean
clean: ## Remove release and all the temporary files
	@rm -Rf /tmp/scylla_manager_rpm.*
	@rm -Rf release

release/scylla-manager-$(VERSION_NAME).tar.gz:
	@echo "==> Archiving..."
	@mkdir -p release
	@cd $(GIT_ROOT); git archive --format tar.gz \
	--prefix scylla-manager-$(VERSION_NAME)/ \
	-o $(PWD)/release/scylla-manager-$(VERSION_NAME).tar.gz HEAD

release/rpm: $(GO_BUNDLE) release/scylla-manager-$(VERSION_NAME).tar.gz ## Create RPM package
	$(eval RPMBUILD := $(shell mktemp -t -d scylla_manager_rpm.XXXX))
	@echo "==> Building RPM in $(RPMBUILD)..."
	@mkdir -p $(RPMBUILD)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
	@cp release/scylla-manager-$(VERSION_NAME).tar.gz $(RPMBUILD)/SOURCES

	@GO_VERSION=$(GO_VERSION) rpmbuild -bb --target x86_64 \
	--define '_binary_payload w2.xzdio' \
	--define "scylla_manager_version $(VERSION)" \
	--define "scylla_manager_release $(RELEASE)" \
	--define "_topdir $(RPMBUILD)"  \
	--define "__os_install_post %{nil}" \
	legacy/redhat/scylla-manager.spec > /dev/null

	@mkdir -p release/rpm
	@cp $(RPMBUILD)/RPMS/x86_64/*.rpm release/rpm
	@rm -Rf $(RPMBUILD)

release/deb: ## Create DEB package
release/deb: DEB_TARGET=stretch
release/deb: release/scylla-manager-$(VERSION_NAME).tar.gz
	@echo "==> Building DEB for $(DEB_TARGET)..."
	@mkdir -p release/deb

	@cd $(GIT_ROOT); cp dist/release/scylla-manager-$(VERSION_NAME).tar.gz ../scylla-manager-server_$(VERSION_NAME).orig.tar.gz
	@cd $(GIT_ROOT); GO_VERSION=$(GO_VERSION) SCYLLA_MANAGER_VERSION=$(VERSION) SCYLLA_MANAGER_RELEASE=$(RELEASE) ./dist/legacy/debian/build_deb.sh --target $(DEB_TARGET)
	@cd $(GIT_ROOT); rm -rf debian

.PHONY: inspect/rpm
inspect/rpm: ## Display RPM package meta-data
	@rpm -qpli release/rpm/scylla-manager{,-server,-client,-agent}-$(VERSION_NAME).x86_64.rpm

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
