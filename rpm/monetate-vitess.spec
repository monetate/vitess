Name:           monetate-vitess
Version:        22.0.4
Release:        1%{?dist}
Summary:        Vitess
License:        Apache-2.0

Source0:	    monetate-vitess.tar

%description
Vitess binaries and admin web pages.

# golang compiler does not create build-id section
%define _missing_build_ids_terminate_build 0
%global debug_package %{nil}

%prep
%setup -n vitess -q

# empty build, sources are the build output
%build

%install
mkdir -p $RPM_BUILD_ROOT/vt/bin
for f in mysqlctl mysqlctld vtadmin vtbackup vtctld vtctlclient vtctldclient vtgate vtorc vttablet; do
    cp bin/$f $RPM_BUILD_ROOT/vt/bin/$f
done
for f in etcd etcdctl etcdutl; do
    cp dist/etcd/etcd-v3.5.25-linux-amd64/$f $RPM_BUILD_ROOT/vt/bin/$f
done
mkdir -p $RPM_BUILD_ROOT/vt/web/vtadmin
cp -rp web/vtadmin/build/* $RPM_BUILD_ROOT/vt/web/vtadmin

%pre
getent group vitess >/dev/null || groupadd -r vitess
getent passwd vitess >/dev/null || useradd -r -g vitess vitess

%files
%defattr(0644, vitess, vitess, 0755)
%dir /vt
%dir /vt/bin
%dir /vt/web
%dir /vt/web/vtadmin
%attr(0755, vitess, vitess) /vt/bin/*
/vt/web/vtadmin/*

%changelog
* Wed Feb 28 2024 Jeffrey J. Persch <jjpersch@monetate.com> 22.0.4-monetate1
- Initial version.