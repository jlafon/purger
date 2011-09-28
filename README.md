purger 2.0.0
============
Purger is a program to get information on a large number of files located on a
parallel file system.

Usage
-----
See the man pages for treewalk(1), warnusers(1), and reaper(1) for details.

In general, use treewalk to populate the database. Then, use warnusers to
notify your users. Finally, use reaper to delete the old files.

Dependencies
------------
* libcircle <http://github.com/hpc/libcircle>
* openssl <http://www.openssl.org/>
* redis <http://redis.io/>
