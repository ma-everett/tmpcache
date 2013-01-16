
/* tmpcache/write.c - development file only */

#include "bstrlib/bstrlib.h"
#include "bstrlib/bsafe.h"

#include <unistd.h>
#include <stdlib.h>

extern int mkstemp (char *);

#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <string.h>

#include <assert.h>

#include "PMurHash.h"
#include <xs/xs.h>



int u_term;

void signal_handler(int signo)
{
  if (signo == SIGINT || signo == SIGTERM)
    u_term = 1;
}

void catch_signals(void) {

  signal(SIGINT,signal_handler);
  signal(SIGTERM,signal_handler);
}

int main(void)
{
  u_term = 0;

  char tempbuf[ 35 ];
  memcpy(&tempbuf[0],"/mnt/git/tmpcache/test/temp_XXXXXX",34);
  tempbuf[ 34 ] = '\0';

  
  /*
  int rfp = mkstemp(&tempbuf[0]);
  assert (rfp != -1);

  printf("tmpfile - %s\n",tempbuf);
  
  if (rfp)
    close(rfp);
  
  int r = rename(tempbuf,"/mnt/git/tmpcache/test/foo");
  assert (r != -1);

  r = remove(tempbuf);
  */


  bstring rootpath = bfromcstr("./test");
  int sbufsize = 256 - (blength(rootpath) + 2);
  char sbuf[ sbufsize ];


  void * ctx = xs_init();
  assert(ctx);

  void * writesck = xs_socket(ctx,XS_PULL);
  assert(writesck);

  int r = xs_bind(writesck,"ipc:///mnt/git/tmpcache/run/write.cache0");
  assert (r != -1);

  xs_pollitem_t pitems[1];
  pitems[0].socket = writesck;
  pitems[0].events = XS_POLLIN;

  xs_msg_t msg_key;
  xs_msg_init (&msg_key);
  
  xs_msg_t msg_part;
  xs_msg_init (&msg_part);

  catch_signals();
  for (;;) {

    int count = xs_poll(pitems,1,(1000) * 3);

    if (u_term == 1)
      break;

    if (count == 0)
      continue;

    r = xs_recvmsg(writesck,&msg_key,0);
    assert(r != -1);
    /* FIXME, check for next part first */
    r = xs_recvmsg(writesck,&msg_part,0);
    assert(r != -1);

    memset (&sbuf[0],'\0',256);

    memcpy (&sbuf[0],xs_msg_data(&msg_key),xs_msg_size(&msg_key));
      
    bstring key = bformat("%s/%s\0",(const char *)rootpath->data,sbuf);
     
    int size = xs_msg_size(&msg_part);

    /* FIXME, if blank then DELETE. Also check the size fits! */
    if (size == 0) {

      printf("> deleting %s\n",(const char *)key->data);

      /*
      FILE * f = fopen((const char *)key->data,"w");
      if (f) {
	fclose(f);
      }
      */

      r = remove((const char *)key->data);  
      continue;
    }


    printf("> writing %s - %d%s\n",(const char *)key->data,
	   (size <= 1024) ? size : size / 1024, (size <= 1024) ? "b" : "Kb");

    memcpy(&tempbuf[0],"/mnt/git/tmpcache/test/temp_XXXXXX",34);
    int fp = mkstemp(&tempbuf[0]);

    if (fp) {
     
      write(fp,xs_msg_data(&msg_part),xs_msg_size(&msg_part));

      close(fp);

      r = rename(tempbuf,(char *)key->data);
      printf("> renaming %s to %s (%s)\n",tempbuf,(char *)key->data,(r == 0) ? "pass" : "fail");
    }
      
      
    

    bdestroy (key);
    
  }

  bdestroy (rootpath);

  r = xs_close (writesck);
  assert (r != -1);

  r = xs_term (ctx);
  assert (r != -1);  
  
  return 0;
}

