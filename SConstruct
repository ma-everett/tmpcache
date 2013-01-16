
src = Split("""
    bstrlib/bstrlib.c
    bstrlib/bsafe.c
""")

Library(target='bstrlib',source=src);

src = Split("""
    PMurHash.c
    tmpcache.c
""")

Library(target='tmpcache',source=src);

libs = Split( """
  bstrlib
  tmpcache 
  xs
  pthread  
  rt
  m
  dl
	""")

flags = '-g -Wall -O2 -Werror -ansi -DCACHE_MEM_DEBUG -DCACHE_DEBUG -DCACHE_USESYSLOG';

  
Program(target='client',LIBS=libs,LIBPATH='.',source='client.c');

Program(target='read',LIBS=libs,LIBPATH='.',source='read.c',CCFLAGS=flags);
Program(target='write',LIBS=libs,LIBPATH='.',source='write.c',CCFLAGS=flags);
Program(target='tmpcache',LIBS=libs,LIBPATH='.',source='cache.c',CCFLAGS=flags);
Program(target='snapshot',LIBS=libs,LIBPATH='.',source='snapshot.c');

