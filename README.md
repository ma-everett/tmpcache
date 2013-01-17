tmpcache
========
version 0
---------

In memory temporary cache written in C with 
[Crossroads I/O library](http://www.crossroads.io) providing the network
interface, for GNU/Linux.

This is by no means a finished production ready solution for caching data,
there is much still to do with the (no-existing) client library. At present,
the project also lacks documentation. 

Build and Install
------------------

You will need Crossroads I/O library installed and the scons build system. 

> scons

At the present time, I would not choice to install or use tmpcache
(especially) for production. 

Running
----------------

> tmpcache -m 64MB -s 1MB -w [waddress] -r [raddress] -c [dir]

Where _waddress_ (for writing) and _raddress_ (for reading) is 
an Crossroads adddress in one of the following forms.

1. IPC:///tmp/write.tmpcache.ipc
2. TCP://127.0.0.1:8888 

Maximum memory is set with the *m* argument, the default is 64MB. 

The maximum size of the data to cache, as set with the *s* argument, the
default is 1MB. 

_dir_ is the directory (absolute path) where the cache will reside. To 
improve the performance, use a *tmpfs* for the cache directory. 

Creating a Snapshot
--------------------

Because we use the filesystem, you can use the standard commands for copying
files, however *tmpcache* provides to alternative methods of creating a
snaphot/copy of the cache. The first is to directly write the contents of
the cache to another cache.

> tmpcache -s 1 -c [dir] -w [destination]

Where _destination_ is another cache. The second method is to create a 
[cdb](http://www.corpit.ru/mjt/tinycdb.html#intro) database.

> tmpcache -s [filename].cdb -c [dir]

Where _filename_ is an absolute path. This will create a database file which
we can then create a readonly cache. 

> tmpcache -c [filename].cdb -r [raddress]
 
