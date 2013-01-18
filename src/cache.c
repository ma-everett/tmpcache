
/* tmpcache/cache.c 
 * - options:
 *            read | write | delete
 *            snapshot to cdb then read only
 */

#include "../config.h"

#include "bstrlib/bstrlib.h"
#include "bstrlib/bsafe.h"

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
extern int mkstemp (char *);

#include <stdio.h>
#include <signal.h>
#include <pthread.h>
#include <string.h>

#include <xs/xs.h>
#include <argp.h>

#include <assert.h> /* FIXME */
#include <syslog.h>

#if defined HAVE_LIBCDB
#include <cdb.h>
#include <fcntl.h>

typedef struct cdb cdb_t;
typedef struct cdb_make cdbm_t;
#endif

#define CACHE_VERSION "cache 0"

const char *argp_program_version = CACHE_VERSION;
const char *argp_program_bugaddress = "<..>";

static char argp_doc[] = "tmpcache, a filesystem cache using message passing"; 
static char args_doc[] = ""; /*FIXME*/

static struct argp_option argp_options[] = {
  {"snapshop", 's', "", 0, "Snapshot to stdout in cdb format"},
#if defined HAVE_LIBCDB
  {"cache",    'c', "DIR|FILE",0,"tmpfs path or cdb file"},
#else
  {"cache",    'c', "DIR",0, "tmpfs path"},
#endif
  {"memory",   'm', "64MB", 0, "Total memory"},
  {"datasize", 'd', "1MB",  0, "Max storage size per data"},
  {"write",    'w', "ADDR", 0, "Write address for cache"},
  {"read",     'r', "ADDR", 0, "Read address for cache"},
  {0}
};

typedef struct arguments {

  long long memory;
  long long size;
  char *waddress;
  char *raddress;
  char *cache;
  char *snapshot;

  void *ctx;

} arguments_t;

unsigned int u_term;


unsigned int readfromfile (void *hint,bstring key,void *data,unsigned int dlen)
{  
  unsigned int bufsize = 0;
  FILE *fp = fopen((const char *)key->data,"r");
  if (fp) {

    long cpos = ftell(fp);
    fseek(fp,0,SEEK_END);
    bufsize = ftell(fp);
    fseek(fp,cpos,SEEK_SET);
      
    if (bufsize) { /* FOUND */
      
      fread(data,(bufsize < dlen) ? bufsize : dlen,1,fp);
      
      int Kb = (bufsize <= 1024) ? bufsize : bufsize / 1024;

      printf("%s> looking up %s, %d%s\n",__FUNCTION__,(const char *)key->data,
	     Kb,(bufsize <= 1024) ? "b" : "Kb");
      
      syslog (LOG_DEBUG,"%s> %s, %d%s",__FUNCTION__,(const char *)key->data,
	      Kb, (bufsize <= 1024) ? "b" : "Kb");

    } else { /* MISS */

      printf("%s> looking up %s,miss\n",__FUNCTION__,(const char *)key->data);
      
      syslog (LOG_DEBUG,"%s> %s, miss",__FUNCTION__,(const char *)key->data);
    }

    fclose(fp); 
  }

  return (bufsize < dlen) ? bufsize : dlen;
}

#if defined HAVE_LIBCDB

unsigned int readfromcdb (void *_cdb,bstring key,void *data,unsigned int dlen)
{
  cdb_t *cdb = (cdb_t *)_cdb;
  unsigned int vlen = 0,vpos = 0;

  if (cdb_find(cdb,(const char *)key->data,blength(key)) > 0) {

    vpos = cdb_datapos(cdb);
    vlen = cdb_datalen(cdb);
    
    cdb_read(cdb,data,(vlen < dlen) ? vlen : dlen, vpos);

    int Kb = (vlen <= 1024) ? vlen : vlen / 1024;

    printf("%s> looking up %s, %d%s\n",__FUNCTION__,(const char *)key->data,
	   Kb, (vlen <= 1024) ? "b" : "Kb");

    syslog (LOG_DEBUG,"%s> %s, %d%s",__FUNCTION__,(const char *)key->data,
	    Kb, (vlen <= 1024) ? "b" : "Kb");

  } else { /*MISS*/
    
    printf("%s> looking up %s, miss\n",__FUNCTION__,(const char *)key->data);

    syslog (LOG_DEBUG,"%s> %s, miss",__FUNCTION__,(const char *)key->data);
  }  

  return (vlen < dlen) ? vlen : dlen;
}
#endif


void xsfree (void *data, void *hint)
{
  free (data);
}

#define xs_assert(s) if (!(s)) {\
       printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));\
       goto error; }

void * readcache (void *arg) 
{
  arguments_t *args = (arguments_t *)arg;
  bstring address = bfromcstr(args->raddress);
  bstring rootpath = bfromcstr(args->cache);
  int sbufsize = 256 - (blength(rootpath) + 2);
  char sbuf[ sbufsize ];
  
  unsigned int (*readf) (void *,bstring,void *,unsigned int) = readfromfile;
  void *hint = NULL;

#if defined HAVE_LIBCDB

  bstring fileformat = bmidstr(rootpath,blength(rootpath) - 4, 4);
  bstring cdbformat = bfromcstr(".cdb");
  cdb_t cdb;
  int ss = 0;

  if (biseq(fileformat,cdbformat)) { 

    /* check if the cache address is a cdb file (ends in .cdb) */
    int fd = open((const char *)rootpath->data, O_RDONLY);
    if (!fd) {
      
      printf("%s, cdb init error\n",__FUNCTION__);

      syslog (LOG_ERR,"%s, cdb init error",__FUNCTION__);

      goto exitearly;
    }

    cdb_init(&cdb,fd);
    
    readf = readfromcdb;
    hint = (void *)&cdb;  
    ss = 1;
  }
#endif
  
  void *ctx = xs_init();

  if (!ctx) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  void *sock = xs_socket(ctx,XS_XREP);
  if (!sock) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }
  
  int r = xs_bind(sock,(const char *)address->data);
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }

  syslog (LOG_INFO,"%s, opening read connection %s on cache %s",
	  __FUNCTION__,(const char *)address->data,args->cache);

    
  bdestroy (address);
  
  xs_msg_t msg_ident;
  xs_msg_init (&msg_ident);

  xs_msg_t msg_blank;
  xs_msg_init (&msg_blank);

  xs_msg_t msg_key;
  xs_msg_init (&msg_key);

  xs_msg_t msg_part;
  
  xs_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = XS_POLLIN;

  for(;;) {

    int count = xs_poll (pitems,1,(1000 * 3)); /*FIXME*/
    
    if (u_term)
      break;

    if (count == 0)
      continue;

    r = xs_recvmsg (sock,&msg_ident,0); 
    xs_assert(r != -1);
    r = xs_recvmsg (sock,&msg_blank,0);
    xs_assert(r != -1);
    r = xs_recvmsg (sock,&msg_key,0); /* FIXME: check size */
    xs_assert(r != -1);

    memset (&sbuf[0],'\0',256);
    memcpy (&sbuf[0],xs_msg_data(&msg_key),xs_msg_size(&msg_key));
    
#if defined HAVE_LIBCDB
    bstring key = (ss == 1) ? bfromcstr(sbuf) : bformat("%s/%s\0",(const char *)rootpath->data,sbuf);
#else 
    bstring key = bformat("%s/%s\0",(const char *)rootpath->data,sbuf);
#endif

    void *data = (void *)malloc(args->size);

    unsigned int rsize = (*readf)(hint,key,data,(unsigned int)args->size);
    
    if (rsize) 
      xs_msg_init_data(&msg_part,data,rsize,xsfree,NULL);
    else
      xs_msg_init (&msg_part);
    
    bdestroy (key);

    r = xs_sendmsg (sock,&msg_ident,XS_SNDMORE);
    xs_assert(r != -1);
    r = xs_sendmsg (sock,&msg_blank,XS_SNDMORE);
    xs_assert (r != -1);
    r = xs_sendmsg (sock,&msg_key,XS_SNDMORE);
    xs_assert (r != -1);
    r = xs_sendmsg (sock,&msg_part,0);
    xs_assert (r != -1);
  }

  bdestroy (rootpath);

  xs_msg_close (&msg_ident);
  xs_msg_close (&msg_blank);
  xs_msg_close (&msg_key);

 error:
  
  syslog (LOG_INFO,"%s, closing read connection %s to cache %s",__FUNCTION__,
	  args->raddress,args->cache);


  r = xs_close (sock); 
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  r = xs_term (ctx);
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

 exitearly:

#if defined HAVE_LIBCDB
  
  if (biseq(fileformat,cdbformat)) { 
    cdb_free (&cdb);
  }

  bdestroy(fileformat);
  bdestroy(cdbformat);
#endif

  return NULL;
}




void * writecache (void *arg)
{
  arguments_t *args = (arguments_t *)arg;
  bstring address = bfromcstr(args->waddress);
  bstring rootpath = bfromcstr(args->cache);
  int sbufsize = 256 - (blength(rootpath) + 2);
  char sbuf[ sbufsize ];
  char tempbuf[ 35 ];
  memcpy(&tempbuf[0],"/mnt/git/tmpcache/test/temp_XXXXXX",34);
  tempbuf[ 34 ] = '\0';

  void *ctx = xs_init();
  if (!ctx) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  void *sock = xs_socket(ctx,XS_PULL);
  if (!sock) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }
  
  int r = xs_bind(sock,(const char *)address->data);
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }

  syslog (LOG_INFO,"%s, opening write connection %s on cache %s",
	  __FUNCTION__,(const char *)address->data,args->cache);

    
  bdestroy (address);

  xs_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = XS_POLLIN;

  xs_msg_t msg_key;
  xs_msg_init (&msg_key);
  
  xs_msg_t msg_part;
  xs_msg_init (&msg_part);

  for (;;) {
    int count = xs_poll (pitems,1,(1000 * 3)); /*FIXME*/
    
    if (u_term)
      break;

    if (count == 0)
      continue;

    r = xs_recvmsg (sock,&msg_key,0); /* FIXME: check size */
    xs_assert(r != -1);

    memset (&sbuf[0],'\0',256);
    memcpy (&sbuf[0],xs_msg_data(&msg_key),xs_msg_size(&msg_key));
    
    bstring key = bformat("%s/%s\0",(const char *)rootpath->data,sbuf);
    
    r = xs_recvmsg(sock,&msg_part,0);
    xs_assert(r != -1); 

    int size = xs_msg_size(&msg_part);

    /* FIXME, if blank then DELETE. Also check the size fits! */
    if (size == 0) {

      printf("> deleting %s\n",(const char *)key->data);

      r = remove((const char *)key->data);  
      continue;
    }

    printf("> writing %s - %d%s\n",(const char *)key->data,
	   (size <= 1024) ? size : size / 1024, (size <= 1024) ? "b" : "Kb");

    memcpy(&tempbuf[0],"/mnt/git/tmpcache/test/temp_XXXXXX",34);
    int fp = mkstemp(&tempbuf[0]);

    if (fp) {
     
      write(fp,xs_msg_data(&msg_part),xs_msg_size(&msg_part));

      close(fp);

      r = rename(tempbuf,(char *)key->data);
      printf("> renaming %s to %s (%s)\n",tempbuf,(char *)key->data,(r == 0) ? "pass" : "fail");
    }
    
    bdestroy (key);

  }

  bdestroy (rootpath);

  xs_msg_close (&msg_key);
  xs_msg_close (&msg_part);

 error:
  

  syslog (LOG_INFO,"%s, closing write connection %s to cache %s",__FUNCTION__,
	  args->waddress,args->cache);


  r = xs_close (sock); 
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  r = xs_term (ctx);
  if (r == -1) {

    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));

    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  return NULL;
}

int snapshotstdout (void *hint, const char *key, int klen, char *data, int dlen) {

  printf("+%d,%d:%s->%s\n",klen,dlen,key,data);
  return 1;
}

#if defined HAVE_LIBCDB
int snapshotcdbout (void *_cdb, const char *key, int klen, char *data, int dlen) {

  cdbm_t * cdbm = (cdbm_t *)_cdb;
  return cdb_make_add(cdbm,key,klen,data,dlen);
}
#endif

void *snapshotcache (void * arg) 
{
  arguments_t *args = (arguments_t *)arg;

  /* There are two versions of this snapshot, the first outputs directly
   * to stdout and the other version writes messages to the write address
   * with can be a write process for another cache.
   */
  DIR *d;
  struct dirent *dir;
  void *ctx,*sock;
  ctx = NULL; sock = NULL;
  int r = 0;

  bstring snapshotpath = bfromcstr(args->snapshot);
  bstring rootpath = bfromcstr(args->cache);
  d = opendir((const char *)rootpath->data);
  if (!d) {
    printf("%s! error opening directory %s\n",__FUNCTION__,(const char *)rootpath->data);
    return NULL;
  }

  int (*writef) (void *, const char *, int, char *, int) = snapshotstdout;
  void * hint = NULL;

#if defined HAVE_LIBCDB

  bstring fileformat = bmidstr(snapshotpath,blength(snapshotpath) - 4, 4);
  bstring cdbformat = bfromcstr(".cdb");
  cdbm_t cdb;

  if (biseq(fileformat,cdbformat)) { 

    /* check if the cache address is a cdb file (ends in .cdb) */
    int fd = open((const char *)snapshotpath->data, O_RDWR|O_CREAT);
    if (!fd) {
      
      printf("%s, cdb init error\n",__FUNCTION__);
      syslog (LOG_ERR,"%s, cdb init error",__FUNCTION__);

      goto exitearly;
    }

    cdb_make_start(&cdb,fd);

    writef = snapshotcdbout;
    hint = (void *)&cdb;
  }
#endif

  if (args->waddress) { 
    
    ctx = xs_init();
    assert(ctx); /*FIXME*/

    sock = xs_socket(ctx,XS_PUSH);
    assert(sock); /*FIXME */

    r = xs_connect(sock,args->waddress);
    xs_assert(r != -1);  
  } 

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
	size_t bufsize = ftell(fp);
	fseek(fp,cpos,SEEK_SET);
	
	char *filebuffer = (char *)malloc(bufsize + 1); /*FIXME*/
	filebuffer[bufsize] = '\0';
	fread(&filebuffer[0],bufsize,1,fp);
	fclose(fp);

	if (args->waddress) {
	  
	  void *kdata = malloc(strlen(dir->d_name));
	  memcpy(kdata,dir->d_name,strlen(dir->d_name));
	  
	  xs_msg_t msg_ident;
	  xs_msg_init_data(&msg_ident,kdata,strlen(dir->d_name),xsfree,NULL);

	  r = xs_sendmsg(sock,&msg_ident,XS_SNDMORE);
	  assert(r != -1);/*FIXME*/

	  xs_msg_t msg_part;
	  xs_msg_init_data(&msg_part,filebuffer,(bufsize - 1),xsfree,NULL);
	  
	  r = xs_sendmsg(sock,&msg_part,0);
	  assert(r != -1); /*FIXME*/

	  xs_msg_close(&msg_ident);
	  xs_msg_close(&msg_part);	  

	} else {

	  (*writef)(hint,dir->d_name,strlen(dir->d_name),filebuffer,(bufsize - 1));	
	  free (filebuffer);
	}
      }
   
    }
    bdestroy (filename);
  }

  error:

  bdestroy (rootpath);
  bdestroy (snapshotpath);

  if (args->waddress) {

    r = xs_close(sock);
    assert(r != -1); /*FIXME*/
    r = xs_term(ctx);
    assert(r != -1);/*FIXME*/
  } 

 exitearly:
#if HAVE_LIBCDB

 if (biseq(fileformat,cdbformat)) { 
   cdb_make_finish(&cdb);
 }

 bdestroy(fileformat);
 bdestroy(cdbformat);

#endif
    
  return NULL;
}
  





































static error_t parseoptions (int key, char *arg, struct argp_state *state);
static struct argp argp = {argp_options,parseoptions,args_doc,argp_doc};

long long str2bytes (const char *str) 
{
  char buffer[256];
  memset(&buffer[0],'\0',256);

  int n;
  long long d = 1024;
  int r = sscanf(str,"%d%s",&n,&buffer[0]);
  assert(r == 2); /* FIXME */

  char *bp = &buffer[0];
  while (*bp != '\0') {
    
    unsigned char c = toupper ((unsigned char)*bp);

    if (c == 'B') {
      d = n * (long long)1024;
      break;
    }

    if (c == 'M') {
      d = n * (long long)1024 * 1024;
      break;
    }

    if (c == 'G') {
      d = n * (long long)(1024 * 1024) * 1024;
      break;
    }
  }
      
  return d;
}

static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 's':
    arguments->snapshot = arg;
    break;
  case 'c':
    if (arg)
      arguments->cache = arg;
    else 
      argp_usage(state);
    break;
  case 'm' :
    arguments->memory = arg ? str2bytes(arg) : arguments->memory;
    break;
  case 'd' :
    arguments->size = arg ? str2bytes(arg) : arguments->size;
    break;
  case 'w' :
    if (arg)
      arguments->waddress = arg; /*FIXME:check address is correct*/
    else
      argp_usage(state);
    break;
  case 'r' :
    if (arg)
      arguments->raddress = arg; /*FIXME:check address is correct*/
    else 
      argp_usage(state);
    break;

  case ARGP_KEY_ARG:
    if (state->arg_num > 1)
      argp_usage(state);

    break;

  case ARGP_KEY_END:
    break;

  default:
    return ARGP_ERR_UNKNOWN;
  }

  return 0;
}


void signalhandler (int signo)
{
  if (signo == SIGINT || signo == SIGTERM)
    u_term = 1;
}

int main (int argc, char **argv) 
{
  arguments_t options;
  options.memory = 64 * (1024 * 1024);
  options.size = 1 * (1024 * 1024);
  options.waddress = NULL;
  options.raddress = NULL;
  options.cache = NULL;
  options.snapshot = NULL;

  argp_parse (&argp,argc,argv,0,0,&options);

  u_term = 0;
  signal (SIGINT,signalhandler);
  signal (SIGTERM,signalhandler);

  openlog (NULL,LOG_PID | LOG_NDELAY,LOG_USER);
 
 /* check for snapshot mode : */
  if (options.cache && options.snapshot) {

    snapshotcache((void *)&options);
    goto exit;
  }

  if (options.waddress && !options.raddress) {
    
    writecache((void *)&options);
    goto exit;
  }
  
  if (options.raddress && !options.waddress) {

    readcache((void *)&options);
    goto exit;
  }

  if (options.waddress && options.raddress) {
    
    printf("using threaded model\n");

    /* run thread for read */
    pthread_t read_t;
    if (pthread_create(&read_t,NULL,readcache,(void *)&options) != 0) {
      printf("pthread_create error\n");
      syslog (LOG_ERR,"read thread creation error, aborting.");
      abort();
    }

    writecache((void *)&options);

    pthread_join (read_t,NULL);
  }  


 exit:

  closelog();
  exit(0);
}
