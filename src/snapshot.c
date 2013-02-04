

/* tmpcache/snapshot.c */

#include "cache.h"

#include <dirent.h>
#include <sys/stat.h>
#include <zmq.h>
#include <syslog.h>
#include <stdio.h>


tc_snapshotinfo_t * tc_snapshotcachetostdout (tc_snapshotconfig_t *config) 
{
  DIR *d;
  
  d = opendir(btocstr(config->cachepath));
  if (!d) {
    /*FIXME*/
    syslog(LOG_ERR,"%s! error opening directory %s\n",__FUNCTION__,btocstr(config->cachepath));
    return;
  } 

  struct dirent *dir;
  
  size_t bufsize = 0;
  char *filebuffer = NULL;

  while ((dir = readdir(d)) != NULL) {

    if (strcmp(dir->d_name, ".")== 0 || strcmp(dir->d_name,"..") == 0)
      continue;
    
    bstring filename = bformat("%s/%s",btocstr(config->cachepath),dir->d_name);
    
    struct stat statbuf;
    stat(btocstr(filename),&statbuf);
    
    if ( ! S_ISDIR(statbuf.st_mode)) {
    
      FILE *fp = fopen(btocstr(filename),"r");
      if (fp) {
	uint64_t cpos = ftell(fp);
	fseek(fp,0,SEEK_END);
	bufsize = ftell(fp);
	fseek(fp,cpos,SEEK_SET);
	
	filebuffer = (char *)c_malloc(bufsize + 1,NULL); /*FIXME*/
	filebuffer[bufsize] = '\0';
	fread(&filebuffer[0],bufsize,1,fp);
	fclose(fp);
      }

      fprintf(stdout,"+%ld,%ld:%s->%s\n",strlen(dir->d_name),bufsize,dir->d_name,filebuffer);    
      c_free (filebuffer,NULL);
    }

    if ((*config->signalf)())
      break;             
  }

  return NULL;
}

#define zmq_assertmsg(s,str) if (!(s)) { \
  (*config->errorf)(str);\
  goto error;}

tc_snapshotinfo_t * tc_snapshotcachetoaddress (tc_snapshotconfig_t *config) 
{
  DIR *d;
 
  d = opendir(btocstr(config->cachepath));
  if (!d) {
    /*FIXME*/
    syslog(LOG_ERR,"%s! error opening directory %s\n",__FUNCTION__,btocstr(config->cachepath));
    return;
  }

  struct dirent *dir;
  
  void *ctx, *sock;
  ctx = zmq_ctx_new();
  if (ctx == NULL) {
    (*config->errorf)("failed to create context");
    return;
  }

  sock = zmq_socket (ctx,ZMQ_PUSH);
  if (sock == NULL) {
    (*config->errorf)("failed to create socket");
    return;
  }

  int32_t r = 0;

  r = zmq_connect (sock,btocstr(config->address));
  zmq_assertmsg (r != -1,"failed to connect");
  
  size_t bufsize = 0;
  char *filebuffer = NULL;

  while ((dir = readdir(d)) != NULL) {

    if (strcmp(dir->d_name, ".")== 0 || strcmp(dir->d_name,"..") == 0)
      continue;
    
    bstring filename = bformat("%s/%s",btocstr(config->cachepath),dir->d_name);
    
    struct stat statbuf;
    stat(btocstr(filename),&statbuf);
    
    if ( ! S_ISDIR(statbuf.st_mode)) {

      FILE *fp = fopen(btocstr(filename),"r");
      if (fp) {
	long cpos = ftell(fp);
	fseek(fp,0,SEEK_END);
	bufsize = ftell(fp);
	fseek(fp,cpos,SEEK_SET);
	
	filebuffer = (char *)c_malloc(bufsize + 1,NULL); /*FIXME*/
	filebuffer[bufsize] = '\0';
	fread(&filebuffer[0],bufsize,1,fp);
	fclose(fp);
      }

      void *kdata = c_malloc(strlen(dir->d_name),NULL);
      memcpy(kdata,dir->d_name,strlen(dir->d_name));
      
      zmq_msg_t msg_ident;
      zmq_msg_init_data(&msg_ident,kdata,strlen(dir->d_name),c_free,NULL);
      
      r = zmq_sendmsg(sock,&msg_ident,ZMQ_SNDMORE);
      zmq_assertmsg(r != -1,"error sendmsg");
      
      zmq_msg_t msg_part;
      zmq_msg_init_data(&msg_part,filebuffer,(bufsize - 1),c_free,NULL);
      
      r = zmq_sendmsg(sock,&msg_part,0);
      zmq_assertmsg(r != -1,"error sendmsg");
      
      zmq_msg_close(&msg_ident);
      zmq_msg_close(&msg_part);
      
    }

    if ((*config->signalf)())
      break;
  }

  r = zmq_disconnect (sock,btocstr(config->address));
  zmq_assertmsg (r != -1,"disconnect error");

  r = zmq_close (sock);
  zmq_assertmsg (r != -1,"error close");

  r = zmq_ctx_destroy (ctx);
  zmq_assertmsg (r != -1,"error on term");

 error:
  return NULL;
}


