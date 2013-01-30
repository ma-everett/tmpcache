
/* tmpcache/utility.c */

#include "cache.h"
#include <stdlib.h>

void *c_malloc (unsigned int size,void *hint)
{
  return malloc(size);
}

void  c_free   (void *p,void *hint) 
{
  free (p);
}

#if defined HAVE_LIBCDB
int c_iscdbfile (bstring cachepath)
{
  bstring ff = bmidstr (cachepath,blength(cachepath) - 4,4);
  bstring sf = bfromcstr (".cdb");
  int r = biseq (ff,sf);

  bdestroy (ff);
  bdestroy (sf);
  return r;
}
#endif
    
int c_filterkey (bstring key) {

  const bstring tar = bfromcstr("/");

  if (binstr(key,0,tar) == BSTR_ERR)
    return 1;

  return 0;
}
