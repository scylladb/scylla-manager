Name:           scylla-mgmt
Version:        %{version}
Release:        %{release}
Summary:        Scylla database management server
Group:          Applications/Databases

License:        Proprietary
URL:            http://www.scylladb.com/
Source0:        %{name}-%{version}-%{release}.tar

%description
Scylla is a highly scalable, eventually consistent, distributed, partitioned
row DB.

%prep

%build

%install
mkdir -p %{buildroot}%{_bindir}/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-mgmt/
mkdir -p %{buildroot}%{_sysconfdir}/scylla-mgmt/cql/
mkdir -p %{buildroot}%{_unitdir}/

install -m755 scylla-mgmt %{buildroot}%{_bindir}/
install -m644 *.yaml %{buildroot}%{_sysconfdir}/scylla-mgmt/
install -m644 *.tpl %{buildroot}%{_sysconfdir}/scylla-mgmt/
install -m644 *.cql %{buildroot}%{_sysconfdir}/scylla-mgmt/cql/
install -m644 *.service %{buildroot}%{_unitdir}/

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
