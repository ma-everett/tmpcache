
/* tmpcache/write.c */

#include "cache.h"

#include <xs/xs.h>
#include <syslog.h>


unsigned int writecontentstofile(bstring key,bstring cachepath,char *data,unsigned int dsize,unsigned int maxsize)
{
  bstring tmp = bformat("%s/temp_XXXXXX",btocstr(cachepath));
 
  int fp = mkstemp ((char *)tmp->data); 
 
  int r = 0;
  if (fp) {

    write(fp,data,(dsize < maxsize) ? dsize : maxsize);
    close(fp);
    /*printf("%s %s -> %s\n",__FUNCTION__,btocstr(tmp), btocstr(key));*/

    r = rename((char *)tmp->data,(char *)key->data);
    /*syslog(LOG_DEBUG,"%s renaming %s to %s (%s)",btocstr(tmp),btocstr(key),(r == 0) ? "pass" : "fail");*/
  }

  bdestroy (tmp);

  return dsize;
}

#if defined HAVE_LIBCDB
unsigned int writecontentstocdb(bstring key,bstring cachepath,char *data,unsigned int dsize,unsigned int maxsize)
{
  return dsize; /* no nothing */
}
#endif

#define xs_assert(s) if (!(s)) {\
  syslog(LOG_ERR,"%s - %s",__FUNCTION__,xs_strerror(xs_errno()));\
  goto error;}

#define xs_assertmsg(s,str) if (!(s)) {\
  syslog(LOG_ERR,"%s - %s : %s",__FUNCTION__,(str),xs_strerror(xs_errno()));\
  goto error;}

void c_writefromcache (bstring address,bstring cachepath,int maxsize,c_signalf signal) 
{
  int sbufsize = 256 - (blength(cachepath) + 2);
  char sbuf[ sbufsize ];

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

  void *sock = xs_socket (ctx,XS_PULL);
  if (!sock) {
    
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_term(ctx);
    goto exitearly;
  }

  int64_t maxmsgsize = maxsize;
  r = xs_setsockopt (sock,XS_MAXMSGSIZE,&maxmsgsize,sizeof(maxmsgsize));
  xs_assertmsg (r == 0,"xs setsockopt maxmsgsize error");

  r = xs_bind (sock,btocstr(address));
  if (r == -1) {

    syslog (LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_close (sock);
    xs_term (ctx);
    goto exitearly;
  }
  
  xs_msg_t msg_key;
  r = xs_msg_init (&msg_key);
  xs_assertmsg (r != -1,"xs key init error");

  xs_msg_t msg_part;
  r = xs_msg_init (&msg_part);
  xs_assertmsg (r != -1,"xs part init error");

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
    xs_assertmsg (r != -1,"xs recvmsg error");
    r = xs_recvmsg (sock,&msg_part,0);
    xs_assertmsg (r != -1,"xs recvmsg error");

    memset (&sbuf[0],'\0',sbufsize); 
    int sizemax = 256 - (blength(cachepath) + 2);
    memcpy (&sbuf[0],xs_msg_data(&msg_key),(xs_msg_size(&msg_key) < sizemax) ? xs_msg_size(&msg_key) : sizemax);
     
    bstring key = bfromcstr(sbuf);
    int filtered = c_filterkey(key);
        
    if (filtered) {
      syslog (LOG_DEBUG,"%s! %s filtered",__FUNCTION__,btocstr(key));
      bdestroy (key);
      continue;
    }
    
#if defined HAVE_LIBCDB
    key = (usecdb) ? bfromcstr(sbuf) : bformat("%s/%s\0",btocstr(cachepath),sbuf);
#else
    key = bformat("%s/%s\0",btocstr(cachepath),sbuf);
#endif

    int size = xs_msg_size (&msg_part);
  
    if (size == 0 && !usecdb) {
      
      r = remove(btocstr(key)); /*FIXME*/
      syslog (LOG_DEBUG,"%s> deleting %s",__FUNCTION__,btocstr(key));
      continue;
    }
    
    int dsize = (*writef)(key,cachepath,xs_msg_data(&msg_part),xs_msg_size(&msg_part),maxsize);
    bdestroy (key);
  }

  r = xs_msg_close (&msg_key);
  xs_assertmsg (r != -1,"xs key close error");
  r = xs_msg_close (&msg_part);
  xs_assertmsg (r != -1,"xs part close error");

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
    
