
/* tmpcache/write.c */

#include "cache.h"

#include <xs/xs.h>
#include <syslog.h>


unsigned int writecontentstofile(bstring key,char *data,unsigned int dsize,char *tmp,unsigned int maxsize)
{
  int fp = mkstemp (tmp);
  int r = 0;
  if (fp) {

    write(fp,data,(dsize < maxsize) ? dsize : maxsize);
    close(fp);

    r = rename(tmp,(char *)key->data);
    syslog(LOG_DEBUG,"%s renaming %s to %s (%s)",tmp,(char *)key->data,(r == 0) ? "pass" : "fail");
  }

  return dsize;
}

#if defined HAVE_LIBCDB
unsigned int writecontentstocdb(bstring key,char *data,unsigned int dsize,char *tmp,unsigned int maxsize)
{
  return dsize; /* no nothing */
}
#endif

#define xs_assert(s) if (!(s)) {\
  syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));\
  goto error;}

void c_writefromcache (bstring address,bstring cachepath,int maxsize,c_signalf signal) 
{
  int sbufsize = 256 - (blength(cachepath) + 2);
  char sbuf[ sbufsize ];
  bstring tmp = bformat("%s/temp_XXXXXX\0",(const char *)cachepath);
  char tempbuf[ blength(tmp) ];
  memcpy (&tempbuf[0],tmp->data,blength(tmp));

  c_writef writef = writecontentstofile;

  int r = -1;
  int usecdb = 0;

#if defined HAVE_LIBCDB
  
  usecdb = c_iscdbfile(cachepath);
  
  if (usecdb) {
    
    writef = writecontentstocdb;
  }
#endif

  void *ctx = xs_init();
  if (!ctx) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    goto exitearly;
  }

  void *sock = xs_socket (ctx,XS_XREP);
  if (!sock) {
    
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_term(ctx);
    goto exitearly;
  }

  r = xs_bind (sock,(const char *)address->data);
  if (r == -1) {

    syslog (LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_close (sock);
    xs_term (ctx);
    goto exitearly;
  }
  
  xs_msg_t msg_key;
  xs_msg_init (&msg_key);
  
  xs_msg_t msg_part;
  xs_msg_init (&msg_part);

  xs_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = XS_POLLIN;

  for (;;) {

    int count = xs_poll (pitems,1,(1000 * 3)); /*FIXME*/

    if ((*signal)() == 1)
      break;

    if (count == 0)
      continue;

    r = xs_recvmsg (sock,&msg_key,0);
    xs_assert (r != -1);
    r = xs_recvmsg (sock,&msg_part,0);
    xs_assert (r != -1);

    memset (&sbuf[0],'\0',256);
    memcpy (&sbuf[0],xs_msg_data(&msg_key),xs_msg_size(&msg_key));

#if defined HAVE_LIBCDB
    bstring key = (usecdb) ? bfromcstr(sbuf) : bformat("%s/%s\0",(const char *)cachepath->data,sbuf);
#else
    bstring key = bformat("%s/%s\0",(const char *)cachepath->data,sbuf);
#endif

    int size = xs_msg_size (&msg_part);
    
    if (size == 0) {/*FIXME*/

      continue;
    }

    if (!usecdb) {
      
      memcpy (&tempbuf[0],tmp->data,blength(tmp));
    }

    r = writef(key,xs_msg_data(&msg_part),xs_msg_size(&msg_part),tempbuf,maxsize);

    bdestroy (key);
  }

  bdestroy (tmp);

  xs_msg_close (&msg_key);
  xs_msg_close (&msg_part);

 error:

  r = xs_close (sock);
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s", __FUNCTION__,xs_strerror(xs_errno()));
    xs_term (ctx);
    return;
  }

  r = xs_term(ctx);
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
    return;
  }

 exitearly:
  return;
}
    
 
