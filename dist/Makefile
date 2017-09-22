all: clean package

VERSION := 1.0
ifndef RELEASE
RELEASE := $(shell date +%Y%m%d).$(shell git describe --always)
endif
VERSION_NAME := $(VERSION)-$(RELEASE)

GOBUILD := go build -ldflags "-X main.version=$(VERSION_NAME)"
PKG := github.com/scylladb/mermaid/cmd/scylla-mgmt

.PHONY: clean
clean:
	@rm -Rf release
	@rm -Rf .rpmbuild.*

release:
	@mkdir -p release
	@cd `git rev-parse --show-toplevel`; git archive \
	--format tar \
	--prefix scylla-mgmt-$(VERSION_NAME)/ \
	-o $(PWD)/release/scylla-mgmt-$(VERSION_NAME).tar HEAD

	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) -o release/linux_amd64/scylla-mgmt $(PKG)

release/rpm: release
	$(eval RPMBUILD := $(PWD)/$(shell mktemp -d .rpmbuild.XXXX))
	@mkdir -p $(RPMBUILD)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

	@ln scylla-mgmt.spec $(RPMBUILD)/scylla-mgmt-$(VERSION_NAME).spec
	@ln conf/*.yaml $(RPMBUILD)/BUILD
	@ln systemd/*.service $(RPMBUILD)/BUILD

	@ln release/scylla-mgmt-$(VERSION_NAME).tar $(RPMBUILD)/SOURCES
	@ln release/linux_amd64/scylla-mgmt $(RPMBUILD)/BUILD
	@ln -f ../schema/cql/*.cql $(RPMBUILD)/BUILD

	@rpmbuild -ba --target x86_64 --quiet \
	--define "version $(VERSION)" \
	--define "release $(RELEASE)" \
	--define "_topdir $(RPMBUILD)" \
	$(RPMBUILD)/scylla-mgmt-$(VERSION_NAME).spec

	@mkdir -p release/rpm
	@ln $(RPMBUILD)/RPMS/x86_64/*.rpm release/rpm
	@ln $(RPMBUILD)/SRPMS/*.rpm release/rpm

.PHONY: package
package: release/rpm

inspect:
	@rpm -qpli release/rpm/scylla-mgmt-$(VERSION_NAME).x86_64.rpm