
#include "utility.h"

#include <stdlib.h>
#include <stdio.h>

int main(void) {

  bstring addr0 = bfromcstr("tcp://127.0.0.1:5555");
  bstring addr1 = bfromcstr("ipc:///tmp/run/read.ipc");

  printf("%s (%s)\n",btocstr(addr0),(tc_validaddress(addr0)) ? "pass" : "fail");
  printf("%s (%s)\n",btocstr(addr1),(tc_validaddress(addr1)) ? "pass" : "fail");


  bdestroy (addr0);
  bdestroy (addr1);

  return 0;
}
