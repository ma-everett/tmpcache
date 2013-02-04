
/* tmpcache/read.c */
#include "cache.h"

#include <syslog.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <zmq.h>

/* read data from a file */
uint64_t readcontentsfromfile (void *hint,bstring key,char *data,uint64_t dsize)
{

  struct stat statbuf;
  int32_t r = stat(btocstr(key),&statbuf);
  if (r != 0)
    return 0;

  uint64_t bufsize = 0;
  FILE *fp = fopen(btocstr(key),"r");
  if (fp) {
    
    uint64_t cpos = ftell(fp);
    fseek(fp,0,SEEK_END);
    bufsize = ftell(fp);
    fseek(fp,cpos,SEEK_SET);
    
    if (bufsize) {
      
      fread (data,(bufsize < dsize) ? bufsize : dsize,1,fp);
      uint64_t Kb = (bufsize <= 1024) ? bufsize : bufsize / 1024;
    
    }
    fclose (fp);
  }
   

  return (bufsize < dsize) ? bufsize : dsize;
}

#if defined HAVE_LIBCDB
/* read data from a constant database file */
uint64_t readcontentsfromcdb (void *hint,bstring key,char *data,uint64_t dsize)
{
  cdb_t *cdb = (cdb_t *)hint;
  uint32_t vlen = 0, vpos = 0;

  if (cdb_find(cdb,btocstr(key),blength(key)) > 0) {

    vpos = cdb_datapos(cdb);
    vlen = cdb_datalen(cdb);

    cdb_read (cdb,data,(vlen < dsize) ? vlen : dsize,vpos);
    uint64_t Kb = (vlen < 1024) ? vlen : vlen / 1024;

    return (vlen < dsize) ? vlen : dsize;
  } 

  return 0;
}
#endif



#define zmq_assert(s) if (!(s)) {\
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,zmq_strerror(zmq_errno()));	\
    goto error;}

#define zmq_assertmsg(s,str) if (!(s)) {\
  syslog(LOG_ERR,"%s - %s : %s",__FUNCTION__,(str),zmq_strerror(zmq_errno()));\
  goto error;}

void tc_readfromcache (tc_readconfig_t * config)
{
  uint32_t sbufsize = 256 - (blength(config->cachepath) + 2);
  char sbuf[ sbufsize ];
  
  c_readf readf = readcontentsfromfile;
  void *hint = NULL;

  int32_t r = -1;

#if defined HAVE_LIBCDB

  cdb_t cdb;
  int32_t usecdb = c_iscdbfile(config->cachepath);
  if (usecdb) {

    syslog(LOG_INFO,"%s -> trying to open cdb file %s",__FUNCTION__,btocstr(config->cachepath));

    int32_t fd = open(btocstr(config->cachepath),O_RDONLY); /*FIXME : unsigned ? */
    if (!fd) {
      
      syslog(LOG_ERR,"%s, cdb init error, failed to open file",__FUNCTION__);
      goto exitearly;
    }
    
    if (cdb_init(&cdb,fd) != 0) {
      
      syslog(LOG_ERR,"%s, cdb init error",__FUNCTION__);
      goto exitearly;
    }
      
    readf = readcontentsfromcdb;
    hint = (void *)&cdb;
  }

#endif

  void *ctx, *sock;

  ctx = zmq_ctx_new(); /*xs_init();*/
  if (ctx == NULL) {
    
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,zmq_strerror(zmq_errno()));
    goto exitearly;
  }
 
  sock = zmq_socket (ctx,ZMQ_XREP);
  if (sock == NULL) {

    syslog(LOG_ERR,"cannot open XREP socket - %s",zmq_strerror(zmq_errno()));
    zmq_ctx_destroy(ctx); /*xs_term(ctx);*/
    goto exitearly;
  }
 
  /*
  uint32_t rcvhwm = 500;
  r = zmq_setsockopt (sock,ZMQ_RCVHWM,&rcvhwm,sizeof(rcvhwm));
  zmq_assertmsg (r == 0,"xs setsockopt rcvhwm error");
  */

  r = zmq_bind (sock,btocstr(config->address));
  if (r == -1) {

    syslog(LOG_ERR,"cannot not open %s - %s",btocstr(config->address),zmq_strerror(zmq_errno()));
    zmq_close (sock);
    zmq_ctx_destroy(ctx); /*xs_term (ctx);*/
    goto exitearly;
  }


  zmq_msg_t msg_ident;
  r = zmq_msg_init (&msg_ident);
  zmq_assertmsg (r != -1,"ident init error");

  zmq_msg_t msg_blank;
  r = zmq_msg_init (&msg_blank);
  zmq_assertmsg (r != -1,"blank init error");

  zmq_msg_t msg_key;
  r = zmq_msg_init (&msg_key);
  zmq_assertmsg (r != -1,"key init error");
  
  zmq_msg_t msg_part;

  zmq_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = ZMQ_POLLIN;

  uint32_t count  = 0;
  
  for (;;) {
    
    if (count == 0)
      count = zmq_poll (pitems,1,(1000 * 3)); /*FIXME*/

    if ((*config->signalf)() == 1)
      break;
    
    if (count == 0) {
    
      continue;
    }

    r = zmq_recvmsg (sock,&msg_ident,0);
    zmq_assertmsg (r != -1,"recvmsg ident error");
    r = zmq_recvmsg (sock,&msg_blank,0);
    zmq_assertmsg (r != -1,"recvmsg blank error");
    r = zmq_recvmsg (sock,&msg_key,0);
    zmq_assertmsg (r != -1,"recvmsg key error");

    count--;

    memset (&sbuf[0],'\0',256); /*FIXME, possible out of bounds error*/
    memcpy (&sbuf[0],zmq_msg_data(&msg_key),zmq_msg_size(&msg_key));

    bstring key = bfromcstr(sbuf);
    int32_t filtered = c_filterkey(key);
    /* filter */
    if (filtered) 
      syslog(LOG_DEBUG,"%s! %s filtered\n",__FUNCTION__,btocstr(key));


#if defined HAVE_LIBCDB
    key = (usecdb) ? bfromcstr(sbuf) : bformat("%s/%s\0",btocstr(config->cachepath),sbuf);
#else 
    key = bformat ("%s/%s\0",btocstr(config->cachepath),sbuf);
#endif

    uint64_t rsize = 0;
    void *data = NULL;
    
    if (!filtered) {
      data = (void *)c_malloc (config->size,NULL);
      rsize = (*readf)(hint,key,data,config->size);
    }    

    if (rsize) {
      r = zmq_msg_init_data (&msg_part,data,rsize,c_free,NULL);
      zmq_assertmsg (r != -1,"msg part init error");
    } else {
      r = zmq_msg_init (&msg_part);
      zmq_assertmsg (r != -1,"msg part init error");
    }   

    bdestroy (key);

    r = zmq_sendmsg (sock,&msg_ident,ZMQ_SNDMORE);
    zmq_assertmsg (r != -1,"sendmsg ident error");
    r = zmq_sendmsg (sock,&msg_blank,ZMQ_SNDMORE);
    zmq_assertmsg (r != -1,"sendmsg blank error");
    r = zmq_sendmsg (sock,&msg_key,ZMQ_SNDMORE);
    zmq_assertmsg (r != -1,"sendmsg key error");
    r = zmq_sendmsg (sock,&msg_part,0);
    zmq_assertmsg (r != -1,"sendmsg part error");

  } /*for loop*/

 error:
  
  r = zmq_msg_close (&msg_ident);
  zmq_assertmsg (r != -1,"close ident error");
  r = zmq_msg_close (&msg_blank);
  zmq_assertmsg (r != -1,"close blank error");
  r = zmq_msg_close (&msg_key);
  zmq_assertmsg (r != -1,"close key error");

  zmq_unbind (sock,btocstr(config->address)); /*FIXME, check return*/
  
  r = zmq_close (sock);
  if (r == -1) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,zmq_strerror(zmq_errno()));
    zmq_ctx_destroy(ctx); /*xs_term (ctx);*/
    goto exitearly;
  }

  r = zmq_ctx_destroy(ctx); /*xs_term (ctx);*/
  if (r == -1) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,zmq_strerror(zmq_errno()));
    goto exitearly;
  }

#if defined HAVE_LIBCDB
  if (usecdb)
    cdb_free (&cdb);
#endif

 exitearly:

  return;  
}
