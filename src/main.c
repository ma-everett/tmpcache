
/* tmpcache/main.c */

#include "cache.h"

#include <signal.h>
#include <pthread.h>
#include <argp.h>
#include <syslog.h>
#include <assert.h>

#define CACHE_VERSION "cache 0"

const char *argp_program_version = CACHE_VERSION; /*FIXME*/
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

} arguments_t;

unsigned int u_term;


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

unsigned int checksignal (void) 
{
  return (u_term);
}

void *readcache (void *op) {

  arguments_t *options = (arguments_t *)op;

  bstring address = bfromcstr(options->raddress);
  bstring cachepath = bfromcstr(options->cache);

  syslog (LOG_INFO,"reading cache from %s @ %s",(char *)cachepath->data,(char *)address->data);
  
  c_readfromcache (address,cachepath,options->size,checksignal);

  syslog (LOG_INFO,"closing cache %s @ %s for reading",(char *)cachepath->data,(char *)address->data);
  
  bdestroy (address);
  bdestroy (cachepath);

  return NULL;
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

  openlog (NULL,LOG_PID|LOG_NDELAY,LOG_USER);

  if (options.snapshot) {

    bstring address = bfromcstr(options.snapshot);
    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"snapshoting cache from %s to %s",(char *)cachepath->data,(char *)address->data);
    
    c_snapshotcache (address,cachepath);

    syslog (LOG_INFO,"done snapshoting cache from %s to %s",(char *)cachepath->data,(char *)address->data);

    bdestroy (address);
    bdestroy (cachepath);
    
    goto finish;
  }

  if (options.raddress && !options.waddress) {

    bstring address = bfromcstr(options.raddress);
    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"reading cache from %s @ %s",(char *)cachepath->data,(char *)address->data);
    
    c_readfromcache (address,cachepath,options.size,checksignal);

    syslog (LOG_INFO,"closing cache %s @ %s for reading",(char *)cachepath->data,(char *)address->data);
    
    bdestroy (address);
    bdestroy (cachepath);

    goto finish;
  }
  
  if (options.waddress && !options.raddress) {

    bstring address = bfromcstr(options.waddress);
    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"writing cache from %s @ %s",(char *)cachepath->data,(char *)address->data);

    c_writefromcache (address,cachepath,options.size,checksignal);

    syslog (LOG_INFO,"closing cache %s @ %s for writing",(char *)cachepath->data,(char *)address->data);

    bdestroy (address);
    bdestroy (cachepath);

    goto finish;
  }

  if (options.waddress && options.raddress) {

    pthread_t read_t;
    if (pthread_create(&read_t,NULL,readcache,(void *)&options) != 0) {
      syslog (LOG_ERR,"read thread creation error");
      abort();
    }

    bstring address = bfromcstr(options.waddress);
    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"writing cache from %s @ %s",(char *)cachepath->data,(char *)address->data);

    c_writefromcache (address,cachepath,options.size,checksignal);

    syslog (LOG_INFO,"closing cache %s @ %s for writing",(char *)cachepath->data,(char *)address->data);

    bdestroy (address);
    bdestroy (cachepath);

    pthread_join (read_t,NULL);
  }


 finish:

  closelog ();

  exit(0);
}
