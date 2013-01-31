
/* tmpcache/main.c */

#include "utility.h"
#include "cache.h" /*FIXME, this needs retiring for seperated utility code*/

#include <signal.h>
#include <pthread.h>
#include <argp.h>
#include <syslog.h>
#include <assert.h>

const char *argp_program_version = PACKAGE_STRING;
const char *argp_program_bugaddress = PACKAGE_BUGREPORT;

static char argp_doc[] = "tmpcache, a filesystem cache using message passing"; 
static char args_doc[] = ""; /*FIXME*/

static struct argp_option argp_options[] = {
  {"timeout",  't', "", 0, "Responsive timeout"},
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
  bstring waddress;
  bstring raddress;
  char *cache; /*FIXME, maybe should be a bstring */
  char *snapshot; /*FIXME, should also be a bstring */
  int timeout;

} arguments_t;

unsigned int u_term;

static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 't':
    arguments->timeout = atoi(arg);
    break;
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
    arguments->memory = arg ? tc_strtobytecount(bfromcstr(arg)) : arguments->memory;
    break;
  case 'd' :
    arguments->size = arg ? tc_strtobytecount(bfromcstr(arg)) : arguments->size;
    break;
  case 'w' :
    arguments->waddress = bfromcstr(arg);
    if (!tc_validaddress(arguments->waddress))
      argp_usage(state);

    break;
  case 'r' :
    arguments->raddress = bfromcstr(arg);
    if (!tc_validaddress(arguments->raddress))
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

static struct argp argp = {argp_options,parseoptions,args_doc,argp_doc};

void signalhandler (int signo)
{
  if (signo == SIGINT || signo == SIGTERM)
    u_term = 1;
}

unsigned int checksignal (void) 
{
  return (u_term);
}

int main (int argc, char **argv) 
{
  arguments_t options;
  options.memory = 64 * (1024 * 1024);
  options.size = 1 * (1024 * 1024);
  options.cache = NULL;
  options.snapshot = NULL;
  options.timeout = 5;

  argp_parse (&argp,argc,argv,0,0,&options);

  u_term = 0;
  signal (SIGINT,signalhandler);
  signal (SIGTERM,signalhandler);

  openlog (NULL,LOG_PID|LOG_NDELAY,LOG_USER);

  /* TODO: rather then passing specific details to the service, instead 
   * pass the options and some additional annotation per service.
   * 
   * options.cache and snapshot need to be converted to bstrings and 
   * also be checked whilst option parsing. 
   */


  /* Handling the Snapshot option here
   */

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

  /* Just handling the Read service here
   */

  if (blength(options.raddress) && ! blength(options.waddress)) {

    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"reading cache from %s @ %s",btocstr(cachepath),btocstr(options.raddress));
    
    c_readfromcache (options.raddress,cachepath,options.size,checksignal);

    syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(cachepath),btocstr(options.raddress));    
    bdestroy (cachepath);

    goto finish;
  }
  
  /* Just handling the Write service here
   */

  if (blength(options.waddress) && ! blength(options.raddress)) {

    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(cachepath),btocstr(options.waddress));

    c_writefromcache (options.waddress,cachepath,options.size,checksignal);

    syslog (LOG_INFO,"closing cache %s @ %s for writing",btocstr(cachepath),btocstr(options.waddress));
    bdestroy (cachepath);

    goto finish;
  }

  /* For both Read|Write service we need to 
   * run an additional thread for read. 
   */

  if (blength(options.waddress) && blength(options.raddress)) {

    void *readcache (void *op) {

      arguments_t *options = (arguments_t *)op;

      bstring cachepath = bfromcstr(options->cache);

      syslog (LOG_INFO,"reading cache from %s @ %s",btocstr(cachepath),btocstr(options->raddress));

      c_readfromcache (options->raddress,cachepath,options->size,checksignal);

      syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(cachepath),btocstr(options->raddress));
  
      bdestroy (cachepath);

      return NULL;
    }

    pthread_t read_t;
    if (pthread_create(&read_t,NULL,readcache,(void *)&options) != 0) {
      syslog (LOG_ERR,"read thread creation error");
      abort();
    }

    bstring cachepath = bfromcstr(options.cache);

    syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(cachepath),btocstr(options.waddress));

    c_writefromcache (options.waddress,cachepath,options.size,checksignal);

    syslog (LOG_INFO,"closing cache %s @ %s for writing",btocstr(cachepath),btocstr(options.waddress));

    bdestroy (cachepath);

    pthread_join (read_t,NULL);
  }


 finish:

  closelog ();

  bdestroy (options.waddress);
  bdestroy (options.raddress);


  exit(0);
}
