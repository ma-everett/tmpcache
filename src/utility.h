
/* tmpcache/utility.h */
#ifndef TMPCACHE_UTILITY_H
#define TMPCACHE_UTILITY_H 1

#include "bstrlib/bstrlib.h"
#define btocstr(bs) (char *)(bs)->data

int tc_validaddress (bstring address);
int tc_checkaddress (bstring address);
long long tc_strtobytecount (bstring str);


#endif
