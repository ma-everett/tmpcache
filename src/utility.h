
/* tmpcache/utility.h */
#ifndef TMPCACHE_UTILITY_H
#define TMPCACHE_UTILITY_H 1

#include <stdint.h>
#include "bstrlib/bstrlib.h"
#define btocstr(bs) (char *)(bs)->data

uint32_t tc_validaddress (bstring address);
uint32_t tc_checkaddress (bstring address);
uint64_t tc_strtobytecount (bstring str);


#endif
