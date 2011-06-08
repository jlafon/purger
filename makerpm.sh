#!/bin/sh
# Author: Jharrod LaFon
# Script to build the rpm
# Uses a temporary directory, does NOT need root
# You should have a .rpmmacros in your ~ directory.
set -x

# For this to work, I must have the following line un-commented line in my ~/.rpmmacros file:
# %_topdir $HOME/rpm

# Check for Makefile
if [ ! -e Makefile ]
then
	./configure
fi

# Build root
dir=$HOME

# See if it exists
if [ ! -d $dir ]
then
	mkdir $dir
fi

rpm=$dir/rpm
name=lanl-purger-1.0.0
src=$dir/rpm/SOURCES
spec=lanl-purger.spec

# Make sure it's empty
pushd $dir
rm -rf $rpm

# Make rpmbuild directories
mkdir rpm && 
mkdir rpm/SOURCES &&
mkdir rpm/SRPMS && 
mkdir rpm/SPECS &&
mkdir rpm/BUILD &&
mkdir rpm/RPMS &&

# Run make dist
popd && 
make distcheck &&

# Copy source over
echo "Copying ${name}.tar.gz to $src" 
cp ${name}.tar.gz $src &&

# Copy spec file over
cp $spec $rpm/SPECS &&
pushd $rpm/SPECS &&

# Beware of nodeps, building this way may need some shared libs in LD_LIBRARY_PATH 
rpmbuild -ba --nodeps $spec &&
popd
