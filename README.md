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

Snapshotting to CDB or another cache
------------------------------------

    > tmpcache_dump /srv/tmpcache ipc:///tmp/run/tmpcache2.write

Using [ezcdb](http://b0llix.net/ezcdb/) to create a cdb file

    > tmpcache_dump /srv/tmpcache | ezcdb make cache.cdb

We can then use the new cdb file as a read-only cache

    > tmpcache_read cache.cdb tcp://localhost:9898

Reading from the cache
----------------------

The default read will connect to the read service, ask for the data of the
key supplied (_foo_ in the example) and wait for a reply. The default timeout
for the wait is 1 second. 

    > tmpcache_readfrom foo tcp://localhost:9898

We can change the _timeout_ and the number of _tries_ 

    > tmpcache_readfrom --tries=3 --timeout=3 foo tcp://localhost:9898

Now the maximum amount of time for an answer from the read service will be
9 seconds. We can redirect the output as standard.

    > tmpcache_readfrom --tries=3 --timeout=1 foo tcp://localhost:9898 > foo 
