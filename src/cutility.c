
/* tmpcache/cutility.c - move to utility*/

#include "cache.h"
#include <stdlib.h>

/*FIXME*/
void *c_malloc (unsigned int size,void *hint)
{
    return malloc(size);
}

void  c_free   (void *p,void *hint) 
{
  free (p);
}

#if defined HAVE_LIBCDB
uint32_t c_iscdbfile (bstring cachepath)
{
  bstring ff = bmidstr (cachepath,blength(cachepath) - 4,4);
  bstring sf = bfromcstr (".cdb");
  uint32_t r = biseq (ff,sf);

  bdestroy (ff);
  bdestroy (sf);
  return r;
}
#endif
    
uint32_t c_filterkey (bstring key) {

  const bstring tar = bfromcstr("/");

  if (binstr(key,0,tar) == BSTR_ERR)
    return 0;

  return 1;
}
