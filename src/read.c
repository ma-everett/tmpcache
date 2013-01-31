
/* tmpcache/read.c */
#include "cache.h"

#include <syslog.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <xs/xs.h>

/* read data from a file */
unsigned int readcontentsfromfile (void *hint,bstring key,char *data,unsigned int dsize)
{

  struct stat statbuf;
  int r = stat(btocstr(key),&statbuf);
  if (r != 0)
    return 0;

  unsigned int bufsize = 0;
  FILE *fp = fopen(btocstr(key),"r");
  if (fp) {
    
    long cpos = ftell(fp);
    fseek(fp,0,SEEK_END);
    bufsize = ftell(fp);
    fseek(fp,cpos,SEEK_SET);
    
    if (bufsize) {
      
      fread (data,(bufsize < dsize) ? bufsize : dsize,1,fp);
      int Kb = (bufsize <= 1024) ? bufsize : bufsize / 1024;
    
    }
    fclose (fp);
  }
   

  return (bufsize < dsize) ? bufsize : dsize;
}

#if defined HAVE_LIBCDB
/* read data from a constant database file */
unsigned int readcontentsfromcdb (void *hint,bstring key,char *data,unsigned int dsize)
{
  cdb_t *cdb = (cdb_t *)hint;
  unsigned int vlen = 0, vpos = 0;

  if (cdb_find(cdb,btocstr(key),blength(key)) > 0) {

    vpos = cdb_datapos(cdb);
    vlen = cdb_datalen(cdb);

    cdb_read (cdb,data,(vlen < dsize) ? vlen : dsize,vpos);
    int Kb = (vlen < 1024) ? vlen : vlen / 1024;

    /*
    syslog(LOG_DEBUG,"%s> looking up %s, %d%s\n",__FUNCTION__,btocstr(key),
	   Kb, (vlen <= 1024) ? "b" : "Kb");
    */
  } else {
    /*
    syslog(LOG_DEBUG,"%s> looking up %s, miss\n",__FUNCTION__,btocstr(key));
    */
    }

  return (vlen < dsize) ? vlen : dsize;
}
#endif



#define xs_assert(s) if (!(s)) {\
    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));	\
    goto error;}

#define xs_assertmsg(s,str) if (!(s)) {\
  syslog(LOG_ERR,"%s - %s : %s",__FUNCTION__,(str),xs_strerror(xs_errno()));\
  goto error;}

void c_readfromcache (bstring address, bstring cachepath, int maxsize, c_signalf signal) 
{
  int sbufsize = 256 - (blength(cachepath) + 2);
  char sbuf[ sbufsize ];
  
  c_readf readf = readcontentsfromfile;
  void *hint = NULL;

  int r = -1;

#if defined HAVE_LIBCDB

  cdb_t cdb;
  int usecdb = c_iscdbfile(cachepath);
  if (usecdb) {

    syslog(LOG_INFO,"%s -> trying to open cdb file %s",__FUNCTION__,btocstr(cachepath));

    int fd = open(btocstr(cachepath),O_RDONLY);
    if (!fd) {
      
      syslog(LOG_ERR,"%s, cdb init error",__FUNCTION__);
      goto exitearly;
    }
    
    cdb_init(&cdb,fd);
    readf = readcontentsfromcdb;
    hint = (void *)&cdb;
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
 
  int rcvhwm = 500;
  r = xs_setsockopt (sock,XS_RCVHWM,&rcvhwm,sizeof(rcvhwm));
  xs_assertmsg (r == 0,"xs setsockopt rcvhwm error");

  r = xs_bind (sock,btocstr(address));
  if (r == -1) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_close (sock);
    xs_term (ctx);
    goto exitearly;
  }

  xs_msg_t msg_ident;
  r = xs_msg_init (&msg_ident);
  xs_assertmsg (r != -1,"xs ident init error");

  xs_msg_t msg_blank;
  r = xs_msg_init (&msg_blank);
  xs_assertmsg (r != -1,"xs blank init error");

  xs_msg_t msg_key;
  r = xs_msg_init (&msg_key);
  xs_assertmsg (r != -1,"xs key init error");
  
  xs_msg_t msg_part;

  xs_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = XS_POLLIN;

  int count  = 0;
  
  for (;;) {
    
    if (count == 0)
      count = xs_poll (pitems,1,(1000 * 3)); /*FIXME*/

    if ((*signal)() == 1)
      break;
    
    if (count == 0) {
    
      continue;
    }

    r = xs_recvmsg (sock,&msg_ident,0);
    xs_assertmsg (r != -1,"xs recvmsg ident error");
    r = xs_recvmsg (sock,&msg_blank,0);
    xs_assertmsg (r != -1,"xs recvmsg blank error");
    r = xs_recvmsg (sock,&msg_key,0);
    xs_assertmsg (r != -1,"xs recvmsg key error");

    count--;

    memset (&sbuf[0],'\0',256); /*FIXME, possible out of bounds error*/
    memcpy (&sbuf[0],xs_msg_data(&msg_key),xs_msg_size(&msg_key));

    bstring key = bfromcstr(sbuf);
    int filtered = c_filterkey(key);
    /* filter */
    if (filtered) 
      syslog(LOG_DEBUG,"%s! %s filtered\n",__FUNCTION__,btocstr(key));


#if defined HAVE_LIBCDB
    key = (usecdb) ? bfromcstr(sbuf) : bformat("%s/%s\0",btocstr(cachepath),sbuf);
#else 
    key = bformat ("%s/%s\0",btocstr(cachepath),sbuf);
#endif

    unsigned int rsize = 0;
    void *data = NULL;
    
    if (!filtered) {
      data = (void *)c_malloc (maxsize,NULL);
      rsize = (*readf)(hint,key,data,maxsize);
    }    

    if (rsize) {
      r = xs_msg_init_data (&msg_part,data,rsize,c_free,NULL);
      xs_assertmsg (r != -1,"xs msg part init error");
    } else {
      r = xs_msg_init (&msg_part);
      xs_assertmsg (r != -1,"xs msg part init error");
    }   

    bdestroy (key);

    r = xs_sendmsg (sock,&msg_ident,XS_SNDMORE);
    xs_assertmsg (r != -1,"xs sendmsg ident error");
    r = xs_sendmsg (sock,&msg_blank,XS_SNDMORE);
    xs_assertmsg (r != -1,"xs sendmsg blank error");
    r = xs_sendmsg (sock,&msg_key,XS_SNDMORE);
    xs_assertmsg (r != -1,"xs sendmsg key error");
    r = xs_sendmsg (sock,&msg_part,0);
    xs_assertmsg (r != -1,"xs sendmsg part error");

  } /*for loop*/

 error:
  
  r = xs_msg_close (&msg_ident);
  xs_assertmsg (r != -1,"xs close ident error");
  r = xs_msg_close (&msg_blank);
  xs_assertmsg (r != -1,"xs close blank error");
  r = xs_msg_close (&msg_key);
  xs_assertmsg (r != -1,"xs close key error");
  
  r = xs_close (sock);
  if (r == -1) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    xs_term (ctx);
    goto exitearly;
  }

  r = xs_term (ctx);
  if (r == -1) {

    syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));
    goto exitearly;
  }

#if defined HAVE_LIBCDB
  if (usecdb)
    cdb_free (&cdb);
#endif

 exitearly:

  return;  
}
