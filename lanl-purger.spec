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
%configure
%{__make}

%install
%{__make} install DESTDIR=%{buildroot}
mkdir -p %{buildroot}/%{_sysconfdir}/purger
mkdir -p %{buildroot}/%{_sysconfdir}/purger/postgres_setup
install -m644 etc/purger/purger.conf %{buildroot}%{_sysconfdir}/purger/purger.conf
install -m644 etc/purger/postgres_setup/extra.sql %{buildroot}%{_sysconfdir}/purger/postgres_setup/extra.sql
install -m644 etc/purger/postgres_setup/generate_partitions.py %{buildroot}%{_sysconfdir}/purger/postgres_setup/generate_partitions.py
install -m644 etc/purger/postgres_setup/init.sh %{buildroot}%{_sysconfdir}/purger/postgres_setup/init.sh
install -m644 etc/purger/postgres_setup/init.sql %{buildroot}%{_sysconfdir}/purger/postgres_setup/init.sql
install -m644 etc/purger/postgres_setup/INSTALL %{buildroot}%{_sysconfdir}/purger/postgres_setup/INSTALL
install -m644 etc/purger/postgres_setup/new_partition.sql %{buildroot}%{_sysconfdir}/purger/postgres_setup/new_partition.sql
install -m644 etc/purger/postgres_setup/new_tables.sql %{buildroot}%{_sysconfdir}/purger/postgres_setup/new_tables.sql
install -m644 etc/purger/postgres_setup/README %{buildroot}%{_sysconfdir}/purger/postgres_setup/README
install -m644 etc/purger/postgres_setup/scratch.sql %{buildroot}%{_sysconfdir}/purger/postgres_setup/scratch.sql

%clean
if [ %{buildroot} != "/" ]; then
   %{__rm} -rf %{buildroot}
fi

%post

%preun

%files
%defattr(-,root,root,0755)
%{_bindir}/*
%config %{_sysconfdir}/purger/purger.conf
%dir %{_sysconfdir}/purger/
%dir %{_sysconfdir}/purger/postgres_setup
%{_sysconfdir}/purger/postgres_setup
%doc AUTHORS
%doc COPYRIGHT
%doc LICENSE

%changelog
* Tue May 31 2011 Jharrod LaFon <jlafon@lanl.gov>
- Updated file locations
* Mon Jan 17 2011 Ben McClelland <ben@lanl.gov>
- Initial package version
