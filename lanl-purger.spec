%define debug_package	%{nil}
%define	_release	1	

Name:		lanl-purger
Summary:	purging system for LANL
Version:	1.0.0
Release:	%{_release}%{?dist}
License:	LANS LLC
Group:		System Environment/Filesystems
Source:		lanl-purger-%{version}.tar.gz
URL:		http://sourceforge.net/projects/lanl-purger/
BuildRoot:	%{_tmppath}/lanl-purger-%{version}-root
Requires:       mpi
BuildRequires:  libconfig
BuildRequires:  mpi-devel

%description
LANL Purger System 
Parallel treewalk, Database configuration, Purger, and utilities 
to manage parallel filesystem.  Designed to manage millions of files
and clean out old files on scratch filesystem.

%prep
%{__rm} -rf %{buildroot}
%setup -q -n lanl-purger-%{version}

%build
%{__make}

%install
%{__mkdir_p} %{buildroot}/etc/fsdb
%{__mkdir_p} %{buildroot}%{_sbindir}
%{__install} -m 0755 parallel/prm/prm %{buildroot}%{_sbindir}/prm
%{__install} -m 0755 parallel/treewalk/pstat %{buildroot}%{_sbindir}/pstat
%{__install} -m 0755 serial/purger/purger %{buildroot}%{_sbindir}/purger
%{__install} -m 0755 serial/util/update_expired %{buildroot}%{_sbindir}/update_expired
%{__install} -m 0644 etc/db.conf %{buildroot}/etc/fsdb/db.conf
%{__install} -m 0644 postgres_setup/extra.sql %{buildroot}/etc/fsdb/extra.sql
%{__install} -m 0644 postgres_setup/generate_partitions.py %{buildroot}/etc/fsdb/generate_partitions.py
%{__install} -m 0644 postgres_setup/init.sql %{buildroot}/etc/fsdb/init.sql
%{__install} -m 0644 postgres_setup/INSTALL %{buildroot}/etc/fsdb/INSTALL
%{__install} -m 0644 postgres_setup/new_partition.sql %{buildroot}/etc/fsdb/new_partition.sql
%{__install} -m 0644 postgres_setup/new_tables.sql %{buildroot}/etc/fsdb/new_tables.sql
%{__install} -m 0644 postgres_setup/README %{buildroot}/etc/fsdb/README
%{__install} -m 0644 postgres_setup/scratch.sql %{buildroot}/etc/fsdb/scratch.sql

%clean
if [ %{buildroot} != "/" ]; then
   %{__rm} -rf %{buildroot}
fi

%post

%preun

%files
%defattr(-,root,root,0755)
%{_sbindir}/prm
%{_sbindir}/pstat
%{_sbindir}/purger
%{_sbindir}/update_expired
%config(noreplace) /etc/fsdb/db.conf
/etc/fsdb/extra.sql
/etc/fsdb/generate_partitions.py
/etc/fsdb/generate_partitions.pyc
/etc/fsdb/generate_partitions.pyo
/etc/fsdb/init.sql
/etc/fsdb/INSTALL
/etc/fsdb/new_partition.sql
/etc/fsdb/new_tables.sql
/etc/fsdb/README
/etc/fsdb/scratch.sql
%doc COPYRIGHT

%changelog
* Mon Jan 17 2011 Ben McClelland <ben@lanl.gov>
- Initial package version

