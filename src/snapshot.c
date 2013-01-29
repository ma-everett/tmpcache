

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
    
    bstring filename = bformat("%s/%s",btocstr(rootpath),dir->d_name);
    
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
  
  bstring tmpaddress = bformat("%s.tmp",btocstr(address));

  int fd = open((char *)tmpaddress->data, O_RDWR|O_CREAT);
  if (fd == -1) {
    
    syslog (LOG_ERR,"%s, cdb (file %s) open error",__FUNCTION__,btocstr(tmpaddress));
    return;
  }
  
  if (cdb_make_start(&cdb,fd) != 0) {

    close(fd);
    syslog (LOG_ERR,"%s, cdb init error",__FUNCTION__);
    return;
  }
  
  size_t bufsize = 0;
  char *filebuffer = NULL;
  int r = 0;
  int numofwrites = 0;

  while ((dir = readdir(d)) != NULL) {

    if (strcmp(dir->d_name, ".")== 0 || strcmp(dir->d_name,"..") == 0)
      continue;
    
    bstring filename = bformat("%s/%s",btocstr(rootpath),dir->d_name);
    
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

      r = cdb_make_add(&cdb,dir->d_name,strlen(dir->d_name),filebuffer,bufsize);
      c_free (filebuffer,NULL);

      if (r == 0) {
	numofwrites ++; 
	syslog (LOG_DEBUG,"%s %s --> cdb, %db",__FUNCTION__,btocstr(filename),bufsize);

      } else {

	syslog (LOG_DEBUG,"%s %s --> error, %s %db",__FUNCTION__,btocstr(filename),dir->d_name,bufsize);
      }
    }
  }
 
  if (cdb_make_finish(&cdb) != 0) {
    
    syslog (LOG_ERR,"%s unable to finish cdb file %s",__FUNCTION__,btocstr(address));
  }
  
  close (fd); /*FIXME*/

  rename (btocstr(tmpaddress),btocstr(address)); /*FIXME */
  
  syslog (LOG_DEBUG,"%s number of writes to cdb file %d",__FUNCTION__,numofwrites);
}
#endif



void c_snapshotcache (bstring address,bstring cachepath) 
{
  DIR *d;
 
  d = opendir(btocstr(cachepath));
  if (!d) {
    syslog(LOG_ERR,"%s! error opening directory %s\n",__FUNCTION__,btocstr(cachepath));
    return;
  }

  void (*writef) (DIR *,bstring,bstring) = writecontentstoserver;


#if defined HAVE_LIBCDB

  if (c_iscdbfile (address))
    writef = writecontentstocdbfile;

#endif


  (*writef) (d,address,cachepath);
}
