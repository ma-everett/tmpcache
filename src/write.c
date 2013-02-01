
/* tmpcache/write.c */

#include "cache.h"

#include <xs/xs.h>
#include <syslog.h>


uint64_t writecontentstofile(bstring key,bstring cachepath,char *data,uint64_t dsize,uint64_t maxsize)
{
  bstring tmp = bformat("%s/temp_XXXXXX",btocstr(cachepath));
 
  int32_t fp = mkstemp ((char *)tmp->data); /*FIXME: correct type, is it unsigned? */ 
 
  int32_t r = 0;
  if (fp) {

    write(fp,data,(dsize < maxsize) ? dsize : maxsize);
    close(fp);
    r = rename((char *)tmp->data,(char *)key->data);
  }

  bdestroy (tmp);

  return dsize;
}

#if defined HAVE_LIBCDB
uint64_t writecontentstocdb(bstring key,bstring cachepath,char *data,uint64_t dsize,uint64_t maxsize)
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

void tc_writefromcache (tc_writeconfig_t *config)
{
  uint64_t sbufsize = 256 - (blength(config->cachepath) + 2);
  char sbuf[ sbufsize ];

  c_writef writef = writecontentstofile;

  int32_t r = -1;
  uint32_t usecdb = 0;

#if defined HAVE_LIBCDB
  
  usecdb = c_iscdbfile(config->cachepath);
  
  if (usecdb) {
    
    writef = writecontentstocdb;
  }
#endif

  void *ctx, *sock;
  
  ctx = xs_init();
  if (ctx == NULL) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    goto exitearly;
  }

  sock = xs_socket (ctx,XS_PULL);
  if (sock == NULL) {
    
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_term(ctx);
    goto exitearly;
  }

  int64_t maxmsgsize = config->maxsize;
  r = xs_setsockopt (sock,XS_MAXMSGSIZE,&maxmsgsize,sizeof(maxmsgsize));
  xs_assertmsg (r == 0,"xs setsockopt maxmsgsize error");

  r = xs_bind (sock,btocstr(config->address));
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

    uint32_t count = xs_poll (pitems,1,(1000 * 3)); /*FIXME*/

    if ((*config->signalf)() == 1)
      break;

    if (count == 0)
      continue;

    r = xs_recvmsg (sock,&msg_key,0);
    xs_assertmsg (r != -1,"xs recvmsg error");
    r = xs_recvmsg (sock,&msg_part,0);
    xs_assertmsg (r != -1,"xs recvmsg error");

    memset (&sbuf[0],'\0',sbufsize); 
    uint64_t sizemax = 256 - (blength(config->cachepath) + 2);
    memcpy (&sbuf[0],xs_msg_data(&msg_key),(xs_msg_size(&msg_key) < sizemax) ? xs_msg_size(&msg_key) : sizemax);
     
    bstring key = bfromcstr(sbuf);
    uint32_t filtered = c_filterkey(key);
        
    if (filtered) {
      syslog (LOG_DEBUG,"%s! %s filtered",__FUNCTION__,btocstr(key));
      bdestroy (key);
      continue;
    }
    
#if defined HAVE_LIBCDB
    key = (usecdb) ? bfromcstr(sbuf) : bformat("%s/%s\0",btocstr(config->cachepath),sbuf);
#else
    key = bformat("%s/%s\0",btocstr(config->cachepath),sbuf);
#endif

    uint64_t size = xs_msg_size (&msg_part);
  
    if (size == 0 && !usecdb) {
      
      r = remove(btocstr(key)); /*FIXME*/
      syslog (LOG_DEBUG,"%s> deleting %s",__FUNCTION__,btocstr(key));
      continue;
    }
    
    uint64_t dsize = (*writef)(key,config->cachepath,xs_msg_data(&msg_part),
			       xs_msg_size(&msg_part),config->maxsize);
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
    
