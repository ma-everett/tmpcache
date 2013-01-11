
src = Split("""
    PMurHash.c
    tmpcache.c
""")

Library(target='tmpcache',source=src);

libs = Split( """
  tmpcache 
  xs
  pthread  
  rt
  m
  dl
	""")

flags = '-g -Wall -O3 -Werror -ansi -DCACHE_MEM_DEBUG -DCACHE_MT';

Program(target='mt_cache',LIBS=libs,LIBPATH='.',source='mt_cache.c',CCFLAGS=flags);  
Program(target='client',LIBS=libs,LIBPATH='.',source='client.c');


