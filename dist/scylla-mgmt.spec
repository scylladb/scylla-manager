%define debug_package %{nil}
%global go_version 1.9.1
%global go_url https://storage.googleapis.com/golang/go%{go_version}.linux-amd64.tar.gz
%global mermaid_pkg github.com/scylladb/mermaid

Name:           scylla-mgmt
Version:        %{mermaid_version}
Release:        %{mermaid_release}
Summary:        Scylla database management server
Group:          Applications/Databases

License:        Proprietary
URL:            http://www.scylladb.com/
Source0:        %{name}-%{version}-%{release}.tar

BuildRequires:  curl
ExclusiveArch:  x86_64

%description
Scylla is a highly scalable, eventually consistent, distributed, partitioned
row DB.

%{name} is the Scylla database management daemon.

%prep
%setup -q -T -b 0 -n %{name}-%{version}-%{release}

%build
curl -sSq -L %{go_url} | tar zxf - -C %{_builddir}
mkdir -p src/%{dirname:%{mermaid_pkg}}
ln -s $PWD src/%{mermaid_pkg}

(
  set -e
  export GOOS=linux
  export GOARCH=amd64
  export CGO_ENABLED=0
  export GOROOT=%{_builddir}/go
  export GOPATH=$PWD

  GO=$GOROOT/bin/go
  GOLDFLAGS="-X main.version=%{version}_%{release}"

  $GO build -o release/linux_amd64/scylla-mgmt -ldflags "$GOLDFLAGS" %{mermaid_pkg}/cmd/scylla-mgmt
  $GO build -o release/linux_amd64/sctool -ldflags "$GOLDFLAGS" %{mermaid_pkg}/cmd/sctool
)

%install
mkdir -p %{buildroot}%{_bindir}/
mkdir -p %{buildroot}%{_sysconfdir}/bash_completion.d/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-mgmt/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-mgmt/cql/
mkdir -p %{buildroot}%{_unitdir}/

install -m755 release/linux_amd64/* %{buildroot}%{_bindir}/
install -m644 dist/bash_completion/*.bash %{buildroot}%{_sysconfdir}/bash_completion.d/
install -m644 dist/etc/*.yaml %{buildroot}%{_sysconfdir}/scylla-mgmt/
install -m644 dist/etc/*.tpl %{buildroot}%{_sysconfdir}/scylla-mgmt/
install -m644 dist/systemd/*.service %{buildroot}%{_unitdir}/
install -m644 schema/cql/*.cql %{buildroot}%{_sysconfdir}/scylla-mgmt/cql/

%pre
getent group  scylla || /usr/sbin/groupadd scylla 2> /dev/null || :
getent passwd scylla || /usr/sbin/useradd -g scylla -s /sbin/nologin -r scylla 2> /dev/null || :

%files
%defattr(-,root,root)

%config(noreplace) %{_sysconfdir}/scylla-mgmt/*.yaml
%config(noreplace) %{_sysconfdir}/scylla-mgmt/*.tpl
%{_sysconfdir}/scylla-mgmt/cql/*.cql
%{_bindir}/scylla-mgmt
%{_unitdir}/*.service

%package client
Summary: Scylla database management CLI
Requires: bash-completion

%description client
Scylla is a highly scalable, eventually consistent, distributed, partitioned
row DB.

sctool is the CLI for interacting with the Scylla database management.

%files client
%defattr(-,root,root)
%{_bindir}/sctool
%{_sysconfdir}/bash_completion.d/sctool.bash
