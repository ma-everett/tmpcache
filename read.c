
/* tmpcache/tead.c - development file only */

/* read only needs to know about the mount point */

#include "bstrlib/bstrlib.h"
#include "bstrlib/bsafe.h"

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>

#include <assert.h>
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


void xsfree (void *data, void *hint) 
{
  free (data);
}

int main(void)
{
  u_term = 0;

  void *ctx = xs_init();
  assert(ctx);

  void *read = xs_socket(ctx,XS_XREP);
  assert(read);

  int r = xs_bind(read,"ipc:///mnt/git/tmpcache/run/read.cache0");
  assert(r != -1);

  xs_msg_t msg_ident;
  xs_msg_init (&msg_ident);

  xs_msg_t msg_blank;
  xs_msg_init (&msg_blank);

  xs_msg_t msg_key;
  xs_msg_init (&msg_key);

  xs_msg_t msg_part;

  catch_signals();

  xs_pollitem_t pitems[1];
  pitems[0].socket = read;
  pitems[0].events = XS_POLLIN;

  bstring rootpath = bfromcstr("./test"); 
  int sbufsize = 256 - (blength(rootpath) + 2);
  char sbuf[ sbufsize ];
  

  for (;;) {

    int count = xs_poll (pitems,1,(1000 * 3));
 
    if (u_term == 1) {
	break;
    }
    
    if (count == 0) {
      continue;
    }

    r = xs_recvmsg (read,&msg_ident,0);
    assert (r != -1);
    r = xs_recvmsg (read,&msg_blank,0);
    assert (r != -1);
    r = xs_recvmsg (read,&msg_key,0);

    /* look up key by file name operation : */
    memset (&sbuf[0],'\0',256);
    memcpy (&sbuf[0],xs_msg_data(&msg_key),xs_msg_size(&msg_key));
      
    bstring key = bformat("%s/%s\0",(const char *)rootpath->data,sbuf);
    printf("> looking up %s\n",(const char *)key->data);
    
    FILE *fp = fopen((const char *)key->data,"r");

    if (fp) { /* copy into part then send else miss */

      long cpos = ftell(fp);
      fseek(fp,0,SEEK_END);
      size_t bufsize = ftell(fp);
      fseek(fp,cpos,SEEK_SET);

      if (bufsize) {
	char *filebuffer = (char *)malloc(bufsize);
	fread(&filebuffer[0],bufsize,1,fp);
	fclose (fp);
	
	int kb = (bufsize <= 1024) ? bufsize : bufsize / 1024;      
	
	printf(">> file found - %d%s\n",kb,(bufsize <= 1024) ? "b" : "Kb");
	xs_msg_init_data (&msg_part,filebuffer,bufsize,xsfree,NULL);
      } else {

	fclose(fp);
	
	printf(">> miss\n");
	xs_msg_init (&msg_part);
      }

     

    } else { /* miss */

      printf(">> miss\n");
      xs_msg_init (&msg_part);
    }    

    bdestroy (key);

    /* for the moment just send back a blank for miss */
    r = xs_sendmsg (read,&msg_ident,XS_SNDMORE);
    assert (r != -1);
    r = xs_sendmsg (read,&msg_blank,XS_SNDMORE);
    assert (r != -1);
    r = xs_sendmsg (read,&msg_key,XS_SNDMORE);
    assert (r != -1);
    r = xs_sendmsg (read,&msg_part,0);
    assert (r != -1);   
  }

  bdestroy (rootpath);

  xs_msg_close (&msg_ident);
  xs_msg_close (&msg_blank);
  xs_msg_close (&msg_key);

  r = xs_close (read);
  assert (r != -1);

  r = xs_term (ctx);
  assert (r != -1);

  return 0;
}

