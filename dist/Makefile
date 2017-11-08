all: clean package

VERSION := 0
ifndef RELEASE
RELEASE := $(shell date +%Y%m%d)git$(shell git describe --always)
endif
VERSION_NAME := $(VERSION)-$(RELEASE)

.PHONY: clean
clean:
	@rm -Rf .rpmbuild.*
	@rm -Rf release

release: release/scylla-mgmt-$(VERSION_NAME).tar
release/scylla-mgmt-$(VERSION_NAME).tar:
	@echo "==> Archiving..."
	@mkdir -p release
	@cd `git rev-parse --show-toplevel`; git archive \
	--format tar \
	--prefix scylla-mgmt-$(VERSION_NAME)/ \
	-o $(PWD)/release/scylla-mgmt-$(VERSION_NAME).tar HEAD

release: release/rpm
release/rpm: release/scylla-mgmt-$(VERSION_NAME).tar
	@echo "==> Building RPM..."
	$(eval RPMBUILD := $(PWD)/$(shell mktemp -d .rpmbuild.XXXX))
	@mkdir -p $(RPMBUILD)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

	@ln scylla-mgmt.spec $(RPMBUILD)/SPECS/scylla-mgmt-$(VERSION_NAME).spec
	@ln release/scylla-mgmt-$(VERSION_NAME).tar $(RPMBUILD)/SOURCES

	@rpmbuild -bb --target x86_64 --quiet \
	--define "mermaid_version $(VERSION)" \
	--define "mermaid_release $(RELEASE)" \
	--define "_topdir $(RPMBUILD)"  \
	$(RPMBUILD)/SPECS/scylla-mgmt-$(VERSION_NAME).spec > /dev/null

	@mkdir -p release/rpm
	@ln $(RPMBUILD)/RPMS/x86_64/*.rpm release/rpm

.PHONY: inspect
inspect:
	@rpm -qpli release/rpm/scylla-mgmt{,-client}-$(VERSION_NAME).x86_64.rpm
