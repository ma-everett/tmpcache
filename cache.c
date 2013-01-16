
/* tmpcache/cache.c 
 * - options:
 *            read | write | delete
 *            snapshot to cdb then read only
 */

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

#if defined CACHE_USESYSLOG
#include <syslog.h>
#endif

/* cdb format :
   +klen,dlen:key->data
*/


#define CACHE_VERSION "cache 0"

const char *argp_program_version = CACHE_VERSION;
const char *argp_program_bugaddress = "<..>";

static char argp_doc[] = "tmpcache, a filesystem cache using message passing"; 
static char args_doc[] = ""; /*FIXME*/

static struct argp_option argp_options[] = {
  {"snapshop", 's', "", 0, "Snapshot to stdout in cdb format"},
  {"cache",    'c', "DIR|FILE",0,"Tmpfs path or cdb file"},
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
  unsigned int snapshot;

  void *ctx;

} arguments_t;

unsigned int u_term;

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
  
  void *ctx = xs_init();
  if (!ctx) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif 
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  void *sock = xs_socket(ctx,XS_XREP);
  if (!sock) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }
  
  int r = xs_bind(sock,(const char *)address->data);
  if (r == -1) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }


#if defined CACHE_USESYSLOG
  syslog (LOG_INFO,"%s, opening read connection %s on cache %s",
	  __FUNCTION__,(const char *)address->data,args->cache);
#endif
    
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
    
    bstring key = bformat("%s/%s\0",(const char *)rootpath->data,sbuf);

    FILE *fp = fopen((const char *)key->data,"r");
    if (fp) {
      long cpos = ftell(fp);
      fseek(fp,0,SEEK_END);
      size_t bufsize = ftell(fp);
      fseek(fp,cpos,SEEK_SET);
      
      if (bufsize) {
	char *filebuffer = (char *)malloc(bufsize);
	fread(&filebuffer[0],bufsize,1,fp);
	fclose(fp);

	int Kb = (bufsize <= 1024) ? bufsize : bufsize / 1024;
	printf("%s> looking up %s, %d%s\n",__FUNCTION__,(const char *)key->data,
	       Kb,(bufsize <= 1024) ? "b" : "Kb");

#if defined CACHE_USESYSLOG /*TODO: add level of logging*/
	syslog (LOG_DEBUG,"%s> %s, %d%s",__FUNCTION__,(const char *)key->data,
		Kb, (bufsize <= 1024) ? "b" : "Kb");
#endif

	xs_msg_init_data (&msg_part,filebuffer,bufsize,xsfree,NULL);
      } else {

	fclose (fp);
	printf("%s> looking up %s, miss\n",__FUNCTION__,(const char *)key->data);

#if defined CACHE_USESYSLOG
	syslog (LOG_DEBUG,"%s> %s, miss",__FUNCTION__,(const char *)key->data);
#endif

      }
    } else { /*MISS*/

      printf("%s> looking up %s, miss\n",__FUNCTION__,(const char *)key->data);
      xs_msg_init (&msg_part);
    
#if defined CACHE_USESYSLOG
      syslog (LOG_DEBUG,"%s> %s, miss",__FUNCTION__,(const char *)key->data);
#endif      
    }

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
  
#if defined CACHE_USESYSLOG
  syslog (LOG_INFO,"%s, closing read connection %s to cache %s",__FUNCTION__,
	  args->raddress,args->cache);
#endif

  r = xs_close (sock); 
  if (r == -1) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif 
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  r = xs_term (ctx);
  if (r == -1) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

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
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif 
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  void *sock = xs_socket(ctx,XS_PULL);
  if (!sock) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }
  
  int r = xs_bind(sock,(const char *)address->data);
  if (r == -1) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    goto error;
  }


#if defined CACHE_USESYSLOG
  syslog (LOG_INFO,"%s, opening write connection %s on cache %s",
	  __FUNCTION__,(const char *)address->data,args->cache);
#endif
    
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
  
#if defined CACHE_USESYSLOG
  syslog (LOG_INFO,"%s, closing write connection %s to cache %s",__FUNCTION__,
	  args->waddress,args->cache);
#endif

  r = xs_close (sock); 
  if (r == -1) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif 
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  r = xs_term (ctx);
  if (r == -1) {
#if defined CACHE_USESYSLOG
    syslog (LOG_ERR,"%s, xs error : %s",__FUNCTION__,xs_strerror(xs_errno()));
#endif
    printf("%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  return NULL;
}


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

  bstring root = bfromcstr(args->cache);
  d = opendir((const char *)root->data);
  if (!d) {
    printf("%s! error opening directory %s\n",__FUNCTION__,(const char *)root->data);
    return NULL;
  }

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
    
    bstring filename = bformat("%s/%s",(const char *)root->data,dir->d_name);

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

	
	  printf("+%d,%d:%s->%s\n",strlen(dir->d_name),(bufsize - 1),dir->d_name,filebuffer);
	  free (filebuffer);
	}
      }
   
    }
    bdestroy (filename);
  }

  error:

  if (args->waddress) {

    r = xs_close(sock);
    assert(r != -1); /*FIXME*/
    r = xs_term(ctx);
    assert(r != -1);/*FIXME*/
  } 

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
    arguments->snapshot = 1;
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
  options.snapshot = 0;

  argp_parse (&argp,argc,argv,0,0,&options);

  u_term = 0;
  signal (SIGINT,signalhandler);
  signal (SIGTERM,signalhandler);

#if defined CACHE_USESYSLOG
  openlog (NULL,LOG_PID | LOG_NDELAY,LOG_USER);
#endif
 
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
      
#if defined CACHE_USESYSLOG
      syslog (LOG_ERR,"read thread creation error, aborting.");
#endif

      abort();
    }

    writecache((void *)&options);

    pthread_join (read_t,NULL);
  }  


 exit:

#if defined CACHE_USESYSLOG
  closelog();
#endif

  exit(0);
}
