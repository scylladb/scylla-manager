%global import_path     github.com/scylladb/mermaid/pkg
%global user            scylla-manager
%global debug_package   %{nil}

Name:           scylla-manager
Version:        %{mermaid_version}
Release:        %{mermaid_release}
Summary:        Scylla Manager meta package
Group:          Applications/Databases

License:        Proprietary
URL:            http://www.scylladb.com/
Source0:        %{name}-%{version}-%{release}.tar.gz

BuildRequires:  curl
ExclusiveArch:  x86_64
Requires: scylla-enterprise scylla-manager-server = %{version}-%{release} scylla-manager-client = %{version}-%{release}

%global common_description Scylla is a highly scalable, eventually consistent, distributed, partitioned row database.
%description
%{common_description} This package is a meta package that installs the Scylla Manager server and client as well as Scylla database server that is used for storage.

%prep
%setup -q -n %{name}-%{version}-%{release}

%build
GOROOT="$(pwd)/../go/"
GOGCFLAGS="all=-trimpath=${GOPATH}"
GOLDFLAGS="-w -extldflags '-static' -X %{import_path}.version=%{version}-%{release}"

GO="${GOROOT}/bin/go"

CGO_ENABLED=0 ${GO} build -a -mod vendor \
-gcflags "${GOGCFLAGS}" -ldflags "${GOLDFLAGS} -B 0x$(head -c20 < /dev/urandom | xxd -p -c20)" \
-o release/linux_amd64/%{name} %{import_path}/cmd/%{name}

CGO_ENABLED=0 ${GO} build -a -mod vendor \
-gcflags "${GOGCFLAGS}" -ldflags "${GOLDFLAGS} -B 0x$(head -c20 < /dev/urandom | xxd -p -c20)" \
-o release/linux_amd64/sctool %{import_path}/cmd/sctool

CGO_ENABLED=0 ${GO} build -a -mod vendor \
-gcflags "${GOGCFLAGS}" -ldflags "${GOLDFLAGS} -B 0x$(head -c20 < /dev/urandom | xxd -p -c20)" \
-o release/linux_amd64/%{name}-agent %{import_path}/cmd/agent

mkdir -p release/bash_completion
./release/linux_amd64/sctool _bashcompletion > release/bash_completion/sctool.bash

%install
mkdir -p %{buildroot}%{_bindir}/
mkdir -p %{buildroot}%{_sbindir}/
mkdir -p %{buildroot}%{_sysconfdir}/bash_completion.d/
mkdir -p %{buildroot}%{_sysconfdir}/%{name}/
mkdir -p %{buildroot}%{_sysconfdir}/%{name}/cql/
mkdir -p %{buildroot}%{_sysconfdir}/%{name}-agent/
mkdir -p %{buildroot}%{_unitdir}/
mkdir -p %{buildroot}%{_prefix}/lib/%{name}/
mkdir -p %{buildroot}%{_sharedstatedir}/%{name}/
mkdir -p %{buildroot}%{_docdir}/%{name}-server/
mkdir -p %{buildroot}%{_docdir}/%{name}-client/
mkdir -p %{buildroot}%{_docdir}/%{name}-agent/

install -m755 release/linux_amd64/* %{buildroot}%{_bindir}/
install -m644 release/bash_completion/* %{buildroot}%{_sysconfdir}/bash_completion.d/
install -m644 dist/etc/%{name}/* %{buildroot}%{_sysconfdir}/%{name}/
install -m644 dist/etc/%{name}-agent/* %{buildroot}%{_sysconfdir}/%{name}-agent/
install -m755 dist/scripts/* %{buildroot}%{_prefix}/lib/%{name}/
install -m644 dist/systemd/*.service %{buildroot}%{_unitdir}/
install -m644 dist/systemd/*.timer %{buildroot}%{_unitdir}/
install -m644 schema/*.cql %{buildroot}%{_sysconfdir}/%{name}/cql/
ln -sf %{_prefix}/lib/%{name}/scyllamgr_setup %{buildroot}%{_sbindir}/
ln -sf %{_prefix}/lib/%{name}/scyllamgr_auth_token_gen %{buildroot}%{_sbindir}/
ln -sf %{_prefix}/lib/%{name}/scyllamgr_ssl_cert_gen %{buildroot}%{_sbindir}/
install -m644 license/LICENSE.PROPRIETARY %{buildroot}%{_docdir}/%{name}-server/LICENSE
install -m644 license/LICENSE.3RD_PARTY.%{name}-server %{buildroot}%{_docdir}/%{name}-server/LICENSE.3RD_PARTY
install -m644 license/LICENSE.PROPRIETARY %{buildroot}%{_docdir}/%{name}-client/LICENSE
install -m644 license/LICENSE.3RD_PARTY.%{name}-client %{buildroot}%{_docdir}/%{name}-client/LICENSE.3RD_PARTY
install -m644 license/LICENSE.PROPRIETARY %{buildroot}%{_docdir}/%{name}-agent/LICENSE
install -m644 license/LICENSE.3RD_PARTY.%{name}-agent %{buildroot}%{_docdir}/%{name}-agent/LICENSE.3RD_PARTY

%files
%defattr(-,root,root)

%package server
Summary: Scylla Manager server

%{?systemd_requires}
BuildRequires: systemd
Requires: bash openssl yum-utils

%description server
%{common_description} This package provides the Scylla Manager server that manages maintenance tasks for Scylla database clusters.

%files server
%defattr(-,root,root)
%{_bindir}/%{name}
%{_prefix}/lib/%{name}/scyllamgr_setup
%{_prefix}/lib/%{name}/scyllamgr_ssl_cert_gen
%{_sbindir}/scyllamgr_setup
%{_sbindir}/scyllamgr_ssl_cert_gen
%config(noreplace) %{_sysconfdir}/%{name}/%{name}.yaml
%{_sysconfdir}/%{name}/cql/*.cql
%{_unitdir}/%{name}.service
%{_unitdir}/%{name}-check-for-updates.service
%{_unitdir}/%{name}-check-for-updates.timer
%license %{_docdir}/%{name}-server/LICENSE
%license %{_docdir}/%{name}-server/LICENSE.3RD_PARTY
%attr(0700, %{user}, %{user}) %{_sharedstatedir}/%{user}

%pre server
getent group  %{user} || /usr/sbin/groupadd -r %{user} > /dev/null
getent passwd %{user} || /usr/sbin/useradd -g %{user} -d %{_sharedstatedir}/%{user} -m -s /sbin/nologin -r %{user} > /dev/null

%post server
%{_sbindir}/scyllamgr_ssl_cert_gen
%{_bindir}/scylla-manager check-for-updates --install
%systemd_post %{name}.service

%preun server
%systemd_preun %{name}.service

%postun server
%systemd_postun_with_restart %{name}.service


%package client
Summary: Scylla Manager CLI
Requires: bash-completion

%description client
%{common_description} This package provides sctool, the CLI for interacting with the Scylla Manager server.

%files client
%defattr(-,root,root)
%{_bindir}/sctool
%{_sysconfdir}/bash_completion.d/sctool.bash
%license %{_docdir}/%{name}-client/LICENSE
%license %{_docdir}/%{name}-client/LICENSE.3RD_PARTY


%package agent
Summary: Scylla Manager Agent

%{?systemd_requires}
BuildRequires: systemd
Requires: openssl

%description agent
%{common_description} This package provides the Scylla Manager Agent installed alongside Scylla database server.

%files agent
%defattr(-,root,root)
%{_bindir}/%{name}-agent
%{_prefix}/lib/%{name}/scyllamgr_ssl_cert_gen
%{_prefix}/lib/%{name}/scyllamgr_auth_token_gen
%{_sbindir}/scyllamgr_auth_token_gen
%{_sbindir}/scyllamgr_ssl_cert_gen
%config(noreplace) %{_sysconfdir}/%{name}-agent/%{name}-agent.yaml
%{_unitdir}/%{name}-agent.service
%license %{_docdir}/%{name}-agent/LICENSE
%license %{_docdir}/%{name}-agent/LICENSE.3RD_PARTY
%attr(0700, %{user}, %{user}) %{_sharedstatedir}/%{user}

%pre agent
getent group  scylla || /usr/sbin/groupadd scylla 2> /dev/null ||:
getent passwd scylla || /usr/sbin/useradd -g scylla -s /sbin/nologin -r -d %{_sharedstatedir}/scylla scylla 2> /dev/null ||:
getent group  %{user} || /usr/sbin/groupadd -r %{user} > /dev/null ||:
getent passwd %{user} || /usr/sbin/useradd -g %{user} -d %{_sharedstatedir}/%{user} -m -s /sbin/nologin -r %{user} > /dev/null ||:
usermod -ou $(id -u scylla) %{user}

%post agent
%{_sbindir}/scyllamgr_ssl_cert_gen
%systemd_post %{name}-agent.service

%preun agent
%systemd_preun %{name}-agent.service

%postun agent
%systemd_postun_with_restart %{name}-agent.service
