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
BuildRequires:  postgresql-devel

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
#%{__mkdir_p} %{buildroot}/etc/purger
#%{__mkdir_p} %{buildroot}%{_sbindir}
#%{__install} -m 0755 src/prm/prm %{buildroot}%{_sbindir}/prm
#%{__install} -m 0755 src/treewalk/pstat %{buildroot}%{_sbindir}/pstat
#%{__install} -m 0755 src/purger/purger %{buildroot}%{_sbindir}/purger
#%{__install} -m 0755 src/util/update_expired %{buildroot}%{_sbindir}/update_expired
#%{__install} -m 0644 etc/purger/purger.conf %{buildroot}/etc/purger/purger.conf
#%{__install} -m 0644 etc/purger/postgres_setup/extra.sql %{buildroot}/etc/purger/extra.sql
#%{__install} -m 0644 etc/purger/postgres_setup/generate_partitions.py %{buildroot}/etc/purger/generate_partitions.py
#%{__install} -m 0644 etc/purger/postgres_setup/init.sh %{buildroot}/etc/purger/init.sh
#%{__install} -m 0644 etc/purger/postgres_setup/init.sql %{buildroot}/etc/purger/init.sql
#%{__install} -m 0644 etc/purger/postgres_setup/INSTALL %{buildroot}/etc/purger/INSTALL
#%{__install} -m 0644 etc/purger/postgres_setup/new_partition.sql %{buildroot}/etc/purger/new_partition.sql
#%{__install} -m 0644 etc/purger/postgres_setup/new_tables.sql %{buildroot}/etc/purger/new_tables.sql
#%{__install} -m 0644 etc/purger/postgres_setup/README %{buildroot}/etc/purger/README
#%{__install} -m 0644 etc/purger/postgres_setup/scratch.sql %{buildroot}/etc/purger/scratch.sql
%{__make} install DESTDIR=%{buildroot}

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
%config(noreplace) /etc/purger/purger.conf
/etc/purger/postgres_setup/extra.sql
/etc/purger/postgres_setup/generate_partitions.py
/etc/purger/postgres_setup/generate_partitions.pyc
/etc/purger/postgres_setup/generate_partitions.pyo
/etc/purger/postgres_setup/init.sql
/etc/purger/postgres_setup/INSTALL
/etc/purger/postgres_setup/new_partition.sql
/etc/purger/postgres_setup/new_tables.sql
/etc/purger/postgres_setup/README
/etc/purger/postgres_setup/scratch.sql
%doc AUTHORS
%doc COPYRIGHT
%doc LICENSE
%changelog
* Tue May 31 2011 Jharrod LaFon <jlafon@lanl.gov>
- Updated file locations
* Mon Jan 17 2011 Ben McClelland <ben@lanl.gov>
- Initial package version

