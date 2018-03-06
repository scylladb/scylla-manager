all: clean release

VERSION := 1.1.0
RELEASE := 0.$(shell date +%Y%m%d).$(shell git describe --always)
VERSION_NAME := $(VERSION)-$(RELEASE)

.PHONY: clean
clean:
	@rm -Rf /tmp/mermaid_rpm.*
	@rm -Rf release

release: release/scylla-manager-$(VERSION_NAME).tar
release/scylla-manager-$(VERSION_NAME).tar:
	@echo "==> Archiving..."
	@mkdir -p release
	@cd `git rev-parse --show-toplevel`; git archive \
	--format tar \
	--prefix scylla-manager-$(VERSION_NAME)/ \
	-o $(PWD)/release/scylla-manager-$(VERSION_NAME).tar HEAD

release: release/rpm
release/rpm: release/scylla-manager-$(VERSION_NAME).tar
	@echo "==> Building RPM..."
	$(eval RPMBUILD := $(shell mktemp -t -d mermaid_rpm.XXXX))
	@mkdir -p $(RPMBUILD)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

	@cp scylla-manager.spec $(RPMBUILD)/SPECS/scylla-manager-$(VERSION_NAME).spec
	@cp release/scylla-manager-$(VERSION_NAME).tar $(RPMBUILD)/SOURCES

	@rpmbuild -bb --target x86_64 --quiet \
	--define "mermaid_version $(VERSION)" \
	--define "mermaid_release $(RELEASE)" \
	--define "_topdir $(RPMBUILD)"  \
	$(RPMBUILD)/SPECS/scylla-manager-$(VERSION_NAME).spec > /dev/null

	@mkdir -p release/rpm
	@cp $(RPMBUILD)/RPMS/x86_64/*.rpm release/rpm

.PHONY: release/docker
release/docker: release/rpm
	@echo "==> Building docker image..."
	docker build --build-arg VERSION_NAME=$(VERSION_NAME) -t scylla-manager:$(VERSION_NAME) -f docker/Dockerfile .

.PHONY: inspect
inspect:
	@rpm -qpli release/rpm/scylla-manager{,-server,-client}-$(VERSION_NAME).x86_64.rpm
