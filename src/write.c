
/* tmpcache/write.c */

#include "cache.h"

#include <zmq.h>
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


#define zmq_assertmsg(s,str) if (!(s)) {\
  (*config->errorf)(str);\
  goto error;}

tc_writeinfo_t * tc_writefromcache (tc_writeconfig_t *config)
{
  tc_writeinfo_t *info;
  info = NULL;

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

  void *ctx,*sock;
  
  ctx = zmq_ctx_new(); /*xs_init();*/
  if (ctx == NULL) {
    /*FIXME*/
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,zmq_strerror(zmq_errno()));
    goto exitearly;
  }
  
  sock = zmq_socket (ctx,ZMQ_PULL);
  if (sock == NULL) {
    /*FIXME*/
    syslog(LOG_ERR,"%s! XS_PULL %s",__FUNCTION__,zmq_strerror(zmq_errno()));
    zmq_ctx_destroy(ctx); /*xs_term(ctx);*/
    goto exitearly;
  }

  /*
  int64_t maxmsgsize = config->maxsize;
  r = xs_setsockopt (sock,XS_MAXMSGSIZE,&maxmsgsize,sizeof(maxmsgsize));
  xs_assertmsg (r == 0,"xs setsockopt maxmsgsize error");
 
  */
  r = zmq_bind (sock,btocstr(config->address));
  if (r == -1) {

    syslog (LOG_ERR,"%s! %s",__FUNCTION__,zmq_strerror(zmq_errno()));
    
    zmq_close (sock);
    zmq_ctx_destroy (ctx);
    goto exitearly;
  }

  info = (tc_writeinfo_t *)c_malloc(sizeof(tc_writeinfo_t),NULL);
  info->numofwrites = 0;
  info->lowestwrite = config->maxsize;
  info->largestwrite = 0;
 
  zmq_msg_t msg_key;
  r = zmq_msg_init (&msg_key);
  zmq_assertmsg (r != -1,"key init error");

  zmq_msg_t msg_part;
  r = zmq_msg_init (&msg_part);
  zmq_assertmsg (r != -1,"part init error");

  zmq_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = ZMQ_POLLIN;

  for (;;) {

    uint32_t count = zmq_poll (pitems,1,(1000 * 3)); /*FIXME*/

    if ((*config->signalf)() == 1)
      break;

    if (count == 0)
      continue;

    r = zmq_recvmsg (sock,&msg_key,0);
    zmq_assertmsg (r != -1,"recvmsg error");
    r = zmq_recvmsg (sock,&msg_part,0);
    zmq_assertmsg (r != -1,"recvmsg error");

    memset (&sbuf[0],'\0',sbufsize); 
    uint64_t sizemax = 256 - (blength(config->cachepath) + 2);
    memcpy (&sbuf[0],zmq_msg_data(&msg_key),(zmq_msg_size(&msg_key) < sizemax) ? zmq_msg_size(&msg_key) : sizemax);
     
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

    uint64_t size = zmq_msg_size (&msg_part);
  
    if (size == 0 && !usecdb) {
      
      r = remove(btocstr(key)); /*FIXME*/
      syslog (LOG_DEBUG,"%s> deleting %s",__FUNCTION__,btocstr(key));
      continue;
    }
    
    uint64_t dsize = (*writef)(key,config->cachepath,zmq_msg_data(&msg_part),
			       zmq_msg_size(&msg_part),config->maxsize);
    bdestroy (key);

    if (dsize) {
      
      info->numofwrites ++;
      info->largestwrite = (dsize > info->largestwrite) ? dsize : info->largestwrite;
      info->lowestwrite =  (dsize < info->lowestwrite) ? dsize : info->lowestwrite;
    }
  }

  r = zmq_msg_close (&msg_key);
  zmq_assertmsg (r != -1,"key close error");
  r = zmq_msg_close (&msg_part);
  zmq_assertmsg (r != -1,"part close error");

  zmq_unbind (sock,btocstr(config->address)); /*FIXME, return check*/

 error:

  r = zmq_close (sock);
  if (r == -1) {
    
    (*config->errorf)("error on close");  
    zmq_ctx_destroy (ctx);
    goto exitearly;
  }

  r = zmq_ctx_destroy(ctx);
  if (r == -1) {
 
    (*config->errorf)("error on ctx destroy");
  }

 exitearly:
  return info;
}
    
