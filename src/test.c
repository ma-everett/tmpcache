
#include "bstrlib/bstrlib.h"
#include <unistd.h>
#include <stdlib.h>

#include <stdio.h>
#include <string.h>
#include <cdb.h>
#include <fcntl.h>

typedef struct cdb_make cdbm_t;

int main(void) {

  cdbm_t cdb;
  
  int fd = open("test.cdb", O_RDWR|O_CREAT);
  if (fd == -1) {
    
    printf("error opening test.cdb\n");
    return -1;
  }
  
  if (cdb_make_start(&cdb,fd) != 0) {

    printf("error cdb make\n");
    close(fd);
    return -1;
  }

  char *foo = malloc(3);
  memcpy (foo,"bar",3);
  int r = cdb_make_add(&cdb,"foo",3,foo,3);
  printf ("foo = %d\n",r);


  r = cdb_make_finish (&cdb);
  printf ("cdb make finish %d\n",r);
  r = close (fd);
  printf ("close %d\n",r);

  return 0;
}
