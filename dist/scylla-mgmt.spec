%define debug_package %{nil}
%global golang_version 1.9.1
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

%prep
%setup -q -T -b 0 -n %{name}-%{version}-%{release}

%build
curl -sSq -L https://storage.googleapis.com/golang/go%{golang_version}.linux-amd64.tar.gz | \
  tar zxf - -C %{_builddir}
mkdir -p src/$(dirname %{mermaid_pkg})
ln -s $PWD src/%{mermaid_pkg}
GOOS=linux GOARCH=amd64 GOROOT=%{_builddir}/go GOPATH=$PWD CGO_ENABLED=0 %{_builddir}/go/bin/go build \
  -ldflags "-X main.version=%{version}_%{release}" \
  -o release/linux_amd64/scylla-mgmt %{mermaid_pkg}/cmd/scylla-mgmt

%install
mkdir -p %{buildroot}%{_bindir}/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-mgmt/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-mgmt/cql/
mkdir -p %{buildroot}%{_unitdir}/

install -m755 release/linux_amd64/scylla-mgmt %{buildroot}%{_bindir}/
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
