
#include "utility.h"

#include <unistd.h>
#include <sys/stat.h>

/* checks for a valid network address, compatible with
 * crossroads.io message library and with tmpcache.
 * TCP or IPC. return value is 1 on valid and 0 for 
 * an invalid address. 
 */

int tc_validaddress (bstring address) 
{
  if (blength(address) <= 6)
    return 0;

  bstring protocol = bmidstr(address,0,6);
   
  if (biseq (protocol, bfromcstr("ipc://")) == 1) {

    bdestroy(protocol);

    /* FIXME: improve */

    return 1;
  }

  if (biseq (protocol,bfromcstr("tcp://")) == 1) {

    bdestroy (protocol);

    /* FIXME: improve */
    
    return 1;
  }   
  
  bdestroy (protocol);
  return 0;
}

/* checks for a valid path, if an IPC address
 */

int tc_checkaddress (bstring address)
{
  bstring protocol = bmidstr(address,0,6);

  if (biseq (protocol,bfromcstr("tcp://")) == 1)
    return 1;

  bstring path = bmidstr (address,6,blength(address) - 6);

  struct stat fstat;
  if (stat(btocstr(path),&fstat) != 0) {
    bdestroy (path);
    return 0;
  }

  bdestroy (path);
  return 1;
}

long long tc_strtobytecount (bstring str)
{
  if (blength(str) >= 50)
    return 0;

  char buffer[50];
  memset (&buffer[0],'\0',50);

  int n;
  long long d = 1024;
  int r = sscanf (btocstr(str),"%d%s",&n,&buffer[0]);
  if (r != 2)
    return 0;

  char *bp = &buffer[0];
  while (*bp != '\0') {
    
    if (*bp == 'B' || *bp == 'b') {
      d = (long long)1024;
      break;
    }

    if (*bp == 'M' || *bp == 'm') {
      d = (long long)(1024 * 1024);
      break;
    }

    if (*bp == 'G' || *bp == 'g') {
      d = (long long)(1024 * 1024) * 1024;
      break;
    }
  }

  return (long long) n * d;
}



