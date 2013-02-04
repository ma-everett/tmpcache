tmpcache
========
version 0 | A Work in Progress
---------

In memory temporary cache written in C with 
[ZeroMQ](http://www.zeromq.org) providing the network
interface, for GNU/Linux.

This is by no means a finished production ready solution for caching data,
there is much still to do. 

Build and Install
------------------

You will need [ZeroMQ](http://www.zeromq.org) 3.x installed. 
Optionally you may want [Tiny CDB library](http://www.corpit.ru/mjt/tinycdb.html) installed for snapshotting. 

    > ./configure
    > make

Currently no install target, as there is no production ready code to use.

Running
----------------

    > tmpcache_host dir raddress waddress 

Where _waddress_ (for writing) and _raddress_ (for reading) is 
an zmq adddress in one of the following forms.

1. ipc:///tmp/write.tmpcache
2. tcp://127.0.0.1:8888 

_dir_ is the directory (absolute path) where the cache will reside. To 
improve the performance, use a *tmpfs* for the cache directory. 

You can run numerous copies of *tmpcache* provided you use different network addresses. For instance, you can run many read-only instance, each with a different network address and a single write-only instance. 

Running with Tmpfs
-------------------

Create a *tmpfs* 

    > sudo mount -t tmpfs -o size=64m tmpfs /srv/tmpcache
    > sudo chown tmpcache:tmpcache /srv/tmpcache

Run tmpcache instance for writing on an inter-process communication address.

    > tmpcache_write /srv/tmpcache ipc:///tmp/run/tmpcache.write &

Now run a read instace on a TCP network address

    > tmpcache_read /srv/tmpcache tcp://localhost:9898
 
*tmpcache* can be stopped by sending a TERM signal. 

