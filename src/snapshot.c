

/* tmpcache/snapshot.c */

#include "cache.h"

#include <dirent.h>
#include <sys/stat.h>
#include <xs/xs.h>
#include <syslog.h>
#include <stdio.h>

#define xs_assert(s) if (!(s)) {\
  syslog(LOG_ERR,"%s! %s",__FUNCTION__,xs_strerror(xs_errno()));\
  goto error;}


void writecontentstoserver (DIR *d, bstring address, bstring rootpath) 
{
  struct dirent *dir;
  
  void * ctx = xs_init ();
  xs_assert (ctx);
  
  void * sock = xs_socket (ctx,XS_PUSH);
  xs_assert (sock);

  int r = 0;

  size_t bufsize = 0;
  char *filebuffer = NULL;

  while ((dir = readdir(d)) != NULL) {

    if (strcmp(dir->d_name, ".")== 0 || strcmp(dir->d_name,"..") == 0)
      continue;
    
    bstring filename = bformat("%s/%s",(const char *)rootpath->data,dir->d_name);
    
    struct stat statbuf;
    stat((const char *)filename->data,&statbuf);
    
    if ( ! S_ISDIR(statbuf.st_mode)) {

      FILE *fp = fopen((const char *)filename->data,"r");
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
      
      xs_msg_t msg_ident;
      xs_msg_init_data(&msg_ident,kdata,strlen(dir->d_name),c_free,NULL);
      
      r = xs_sendmsg(sock,&msg_ident,XS_SNDMORE);
      xs_assert(r != -1);
      
      xs_msg_t msg_part;
      xs_msg_init_data(&msg_part,filebuffer,(bufsize - 1),c_free,NULL);
      
      r = xs_sendmsg(sock,&msg_part,0);
      xs_assert(r != -1);
      
      xs_msg_close(&msg_ident);
      xs_msg_close(&msg_part);
      
    }
  }

  r = xs_close (sock);
  xs_assert (r != -1);

  r = xs_term (ctx);
  xs_assert (r != -1);

 error:
  return;
}

#if defined HAVE_LIBCDB
void writecontentstocdbfile (DIR *d, bstring address, bstring rootpath) 
{  
  struct dirent *dir;
  cdbm_t cdb;
  
  int fd = open((const char *)rootpath->data, O_RDWR|O_CREAT);
  if (!fd) {
    
    syslog (LOG_ERR,"%s, cdb init error",__FUNCTION__);
    return;
  }
  
  cdb_make_start(&cdb,fd);
  
  size_t bufsize = 0;
  char *filebuffer = NULL;

  while ((dir = readdir(d)) != NULL) {

    if (strcmp(dir->d_name, ".")== 0 || strcmp(dir->d_name,"..") == 0)
      continue;
    
    bstring filename = bformat("%s/%s",(const char *)rootpath->data,dir->d_name);
    
    struct stat statbuf;
    stat((const char *)filename->data,&statbuf);
    
    if ( ! S_ISDIR(statbuf.st_mode)) {

      FILE *fp = fopen((const char *)filename->data,"r");
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

      cdb_make_add(&cdb,dir->d_name,strlen(dir->d_name),filebuffer,bufsize);
    }
  }
 
  cdb_make_finish(&cdb);
}
#endif





void c_snapshotcache (bstring address,bstring cachepath) 
{
  DIR *d;
 
  d = opendir((const char *)cachepath->data);
  if (!d) {
    syslog(LOG_ERR,"%s! error opening directory %s\n",__FUNCTION__,(const char *)cachepath->data);
    return;
  }

  void (*writef) (DIR *,bstring,bstring) = writecontentstoserver;


#if defined HAVE_LIBCDB

  if (c_iscdbfile (address))
    writef = writecontentstocdbfile;

#endif


  (*writef) (d,address,cachepath);
}
