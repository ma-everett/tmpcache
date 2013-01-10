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

> mt_cache -m 64MB -s 1MB -w [waddress] -r [raddress]

Where _waddress_ (for writing) and _raddress_ (for reading) is 
an Crossroads adddress in one of the following forms.

1. IPC:///tmp/write.tmpcache.ipc
2. TCP://127.0.0.1:8888 

Maximum memory is set with the *m* argument, the default is 64MB. 

The maximum size of the data to cache, as set with the *s* argument, the
default is 1MB. 
 
