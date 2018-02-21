%define debug_package %{nil}
%global go_version 1.10.2
%global go_url https://storage.googleapis.com/golang/go%{go_version}.linux-amd64.tar.gz
%global pkg_name github.com/scylladb/mermaid

Name:           scylla-manager
Version:        %{mermaid_version}
Release:        %{mermaid_release}
Summary:        Scylla Manager meta package
Group:          Applications/Databases

License:        Proprietary
URL:            http://www.scylladb.com/
Source0:        %{name}-%{version}-%{release}.tar

BuildRequires:  curl
ExclusiveArch:  x86_64
Requires: scylla-enterprise scylla-manager-server = %{version}-%{release} scylla-manager-client = %{version}-%{release} psmisc

%description
Scylla is a highly scalable, eventually consistent, distributed, partitioned row
database. %{name} is a meta package that installs all scylla-manager* packages as
well as scylla database server.

%prep
%setup -q -T -b 0 -n %{name}-%{version}-%{release}

%build
curl -sSq -L %{go_url} | tar zxf - -C %{_builddir}
mkdir -p src/%{dirname:%{pkg_name}}
ln -s $PWD src/%{pkg_name}

(
  set -e

  export GOROOT=%{_builddir}/go
  export GOPATH=$PWD

  export GOOS=linux
  export GOARCH=amd64
  export CGO_ENABLED=0

  GO=$GOROOT/bin/go
  GOLDFLAGS="-w -extldflags '-static' -X %{pkg_name}.version=%{version}_%{release}"

  mkdir -p release/bash_completion
  $GO run `$GO list -f '{{range .GoFiles}}{{ $.Dir }}/{{ . }} {{end}}' %{pkg_name}/cmd/sctool/` _bashcompletion > release/bash_completion/sctool.bash

  $GO build -ldflags "-B 0x$(head -c40 < /dev/urandom | xxd -p -c40) $GOLDFLAGS" -o release/linux_amd64/scylla-manager %{pkg_name}/cmd/scylla-manager
  $GO build -ldflags "-B 0x$(head -c40 < /dev/urandom | xxd -p -c40) $GOLDFLAGS" -o release/linux_amd64/sctool %{pkg_name}/cmd/sctool
)

%install
mkdir -p %{buildroot}%{_bindir}/
mkdir -p %{buildroot}%{_sbindir}/
mkdir -p %{buildroot}%{_sysconfdir}/bash_completion.d/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-manager/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-manager/cql/
mkdir -p %{buildroot}%{_unitdir}/
mkdir -p %{buildroot}%{_prefix}/lib/scylla-manager/
mkdir -p %{buildroot}%{_sharedstatedir}/scylla-manager/

install -m755 release/linux_amd64/* %{buildroot}%{_bindir}/
install -m644 release/bash_completion/* %{buildroot}%{_sysconfdir}/bash_completion.d/
install -m644 dist/etc/*.yaml %{buildroot}%{_sysconfdir}/scylla-manager/
install -m644 dist/etc/*.tpl %{buildroot}%{_sysconfdir}/scylla-manager/
install -m755 dist/scripts/* %{buildroot}%{_prefix}/lib/scylla-manager/
install -m644 dist/systemd/*.service %{buildroot}%{_unitdir}/
install -m644 schema/cql/*.cql %{buildroot}%{_sysconfdir}/scylla-manager/cql/

ln -sf %{_prefix}/lib/scylla-manager/scyllamgr_setup %{buildroot}%{_sbindir}/
ln -sf %{_prefix}/lib/scylla-manager/scyllamgr_ssh_test %{buildroot}%{_sbindir}/
ln -sf %{_prefix}/lib/scylla-manager/scyllamgr_ssl_cert_gen %{buildroot}%{_sbindir}/

%files
%defattr(-,root,root)
%{_prefix}/lib/scylla-manager/scyllamgr_setup
%{_prefix}/lib/scylla-manager/scyllamgr_ssh_test
%{_sbindir}/scyllamgr_setup
%{_sbindir}/scyllamgr_ssh_test


%package server
Summary: Scylla Manager server

%{?systemd_requires}
BuildRequires: systemd

%description server
Scylla is a highly scalable, eventually consistent, distributed, partitioned row
database. %{name} is the the Scylla Manager server. It automates
the database management tasks.

%files server
%defattr(-,root,root)
%{_bindir}/scylla-manager
%{_prefix}/lib/scylla-manager/scyllamgr_ssl_cert_gen
%{_sbindir}/scyllamgr_ssl_cert_gen
%config(noreplace) %{_sysconfdir}/scylla-manager/*.yaml
%config(noreplace) %{_sysconfdir}/scylla-manager/*.tpl
%{_sysconfdir}/scylla-manager/cql/*.cql
%{_unitdir}/*.service
%attr(0700, scylla-manager, scylla-manager) %{_sharedstatedir}/scylla-manager

%pre server
getent group  scylla-manager || /usr/sbin/groupadd -r scylla-manager &> /dev/null || :
getent passwd scylla-manager || /usr/sbin/useradd \
 -g scylla-manager -d %{_sharedstatedir}/scylla-manager -s /sbin/nologin -r scylla-manager &> /dev/null || :

%post server
/usr/bin/sh %{_sbindir}/scyllamgr_ssl_cert_gen
%systemd_post %{name}.service

%preun server
%systemd_preun %{name}.service

%postun server
%systemd_postun_with_restart %{name}.service


%package client
Summary: Scylla Manager CLI
Requires: bash-completion

%description client
Scylla is a highly scalable, eventually consistent, distributed, partitioned row
database. %{name} is the CLI for interacting with the Scylla Manager server.

%files client
%defattr(-,root,root)
%{_bindir}/sctool
%{_sysconfdir}/bash_completion.d/sctool.bash
