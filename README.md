tmpcache
========
version 0 | A Work in Progress
---------

In memory temporary cache written in C with 
[Crossroads I/O library](http://www.crossroads.io) providing the network
interface, for GNU/Linux.

This is by no means a finished production ready solution for caching data,
there is much still to do. 

Build and Install
------------------

You will need [Crossroads I/O library](http://www.crossroads.io) installed. 
Optionally you may want [Tiny CDB library](http://www.corpit.ru/mjt/tinycdb.html) installed for snapshotting. 

    > ./configure
    > make

Currently no install target, as there is no production ready code to use.

Running
----------------

    > tmpcache -m 64MB -s 1MB -w [waddress] -r [raddress] -c [dir]

Where _waddress_ (for writing) and _raddress_ (for reading) is 
an Crossroads adddress in one of the following forms.

1. IPC:///tmp/write.tmpcache
2. TCP://127.0.0.1:8888 

Maximum memory is set with the _m_ argument, the default is 64MB. 

The maximum size of the data to cache, as set with the _s_ argument, the
default is 1MB. 

_dir_ is the directory (absolute path) where the cache will reside. To 
improve the performance, use a *tmpfs* for the cache directory. 

You can run numerous copies of *tmpcache* provided you use different network addresses. For instance, you can run many read-only instance, each with a different network address and a single write-only instance. 

Running with Tmpfs
-------------------

Create a *tmpfs* 

    > sudo mount -t tmpfs -o size=64m tmpfs /srv/tmpcache
    > sudo chown tmpcache:tmpcache /srv/tmpcache

Run tmpcache instance for writing on an inter-process communication address.

    > tmpcache -m 64MB -s 1MB -w ipc:///tmp/run/tmpcache.write -c /srv/tmpcache &

Now run a read instace on a TCP network address

    > tmpcache -r tcp://localhost:9898 -c /srv/tmpcache &
 
*tmpcache* can be stopped by sending a TERM signal. 

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
 
 
<script src="https://gist.github.com/4658985.js"></script>
