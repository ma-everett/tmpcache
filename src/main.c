
/* tmpcache/main.c */

#include "utility.h"
#include "cache.h" /*FIXME, this needs retiring for seperated utility code*/

#include <stdint.h>
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

  uint64_t memory;
  uint64_t size;
  bstring waddress;
  bstring raddress;
  bstring cache; /*FIXME, maybe should be a bstring */
  bstring snapshot; /*FIXME, should also be a bstring */
  uint64_t timeout;

} arguments_t;

uint32_t u_term;

static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 't':
    arguments->timeout = (uint64_t)atoi(arg);
    break;
  case 's':
    arguments->snapshot = bfromcstr(arg);
    
    break;
  case 'c':
    arguments->cache = bfromcstr(arg);
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

void signalhandler (int32_t signo)
{
  if (signo == SIGINT || signo == SIGTERM)
    u_term = 1;
}

uint32_t checksignal (void) 
{
  return (u_term);
}

int main (int argc, char **argv) 
{
  arguments_t options;
  options.memory = 64 * (1024 * 1024);
  options.size = 1 * (1024 * 1024);
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

  if (blength(options.snapshot)) {

    tc_snapshotconfig_t config;
    if (blength(options.cache) == 0 && blength(options.waddress) == 0) {/*stdout*/
      
      config.cachepath = bstrcpy(options.snapshot);
    } else {
    
      config.address = bstrcpy(options.waddress);
      config.cachepath = bstrcpy(options.snapshot);
    }
 
    syslog (LOG_INFO,"snapshoting cache from %s",btocstr(config.cachepath));
    
    tc_snapshotcache (&config);

    syslog (LOG_INFO,"%s snapshot complete",btocstr(config.cachepath));
  
    if (blength(config.address))
      bdestroy (config.address);
    
    bdestroy (config.cachepath);
    
    goto finish;
  }

  /* Just handling the Read service here
   */

  if (blength(options.raddress) && ! blength(options.waddress)) {

    tc_readconfig_t config;
    config.address = bstrcpy(options.raddress);
    config.cachepath = bstrcpy(options.cache); /*FIXME*/
    config.size = options.size;
    config.signalf = checksignal;

    syslog (LOG_INFO,"reading cache from %s @ %s",btocstr(config.cachepath),btocstr(config.address));
    
    tc_readfromcache (&config);

    syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(config.cachepath),btocstr(config.address));    
    
    bdestroy (config.address);
    bdestroy (config.cachepath);
   
    goto finish;
  }
  
  /* Just handling the Write service here
   */

  if (blength(options.waddress) && ! blength(options.raddress)) {

    tc_writeconfig_t config;
    config.address = bstrcpy(options.waddress);
    config.cachepath = bstrcpy(options.cache);
    config.size = options.size;
    config.maxsize = options.memory;
    config.signalf = checksignal;

    syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(config.cachepath),btocstr(config.address));

    tc_writefromcache (&config);

    syslog (LOG_INFO,"closing cache %s @ %s for writing",btocstr(config.cachepath),btocstr(config.address));

    bdestroy (config.address);
    bdestroy (config.cachepath);

    goto finish;
  }

  /* For both Read|Write service we need to 
   * run an additional thread for read. 
   */

  if (blength(options.waddress) && blength(options.raddress)) {

    void *readcache (void *op) {
      
      arguments_t *options = (arguments_t *)op;

      tc_readconfig_t config;
      config.address = bstrcpy(options->raddress);
      config.cachepath = bstrcpy(options->cache); /*FIXME*/
      config.size = options->size;
      config.signalf = checksignal;
      
      syslog (LOG_INFO,"reading cache from %s @ %s",btocstr(config.cachepath),btocstr(config.address));
      
      tc_readfromcache (&config);
      
      syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(config.cachepath),btocstr(config.address));    
      
      bdestroy (config.address);
      bdestroy (config.cachepath);
   
      return NULL;
    }

    pthread_t read_t;
    if (pthread_create(&read_t,NULL,readcache,(void *)&options) != 0) {
      syslog (LOG_ERR,"read thread creation error");
      abort();
    }
 
    tc_writeconfig_t config;
    config.address = bstrcpy(options.waddress);
    config.cachepath = bstrcpy(options.cache);
    config.size = options.size;
    config.maxsize = options.memory;
    config.signalf = checksignal;

    syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(config.cachepath),btocstr(config.address));

    tc_writefromcache (&config);

    syslog (LOG_INFO,"closing cache %s @ %s for writing",btocstr(config.cachepath),btocstr(config.address));

    bdestroy (config.address);
    bdestroy (config.cachepath);
  

    pthread_join (read_t,NULL);
  }


 finish:

  closelog ();
 

  /* FIXME: must be a better way than this!?*/
  /*
    bdestroy (options.waddress);

    bdestroy (options.raddress);

    bdestroy (options.cache);

    bdestroy (options.snapshot);
  */
  exit(0);
}
