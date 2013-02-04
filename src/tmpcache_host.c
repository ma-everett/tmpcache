
/* tmpcache/tmpcache_host.c */

#include <argtable2.h>
#include <syslog.h>
#include <signal.h>
#include <zmq.h>
#include <pthread.h>

#include "cache.h"

struct arg_lit *verbose, *fsyslog,*help;
struct arg_file *cachepath, *netaddress0, *netaddress1;
struct arg_end *end;

uint32_t u_term;

void signalhandler (int32_t signo)
{
  if (signo == SIGINT || signo == SIGTERM)
    u_term = 1;
}

uint32_t checksignal (void) 
{
  return (u_term);
}

void printerror (const char *msg) {

  fprintf(stderr,"%s - %s",msg,zmq_strerror(zmq_errno()));
}

void logerror (const char *msg) {

  syslog (LOG_ERR,"%s - %s",msg,zmq_strerror(zmq_errno()));
}


int main (int argc, char **argv) {

  void *argtable[] = {
    help = arg_lit0("h","help","print this screen"),
    verbose = arg_lit0("v","verbose","tell me everything"),
    fsyslog = arg_lit0(NULL,"syslog","use syslog"),
    cachepath = arg_file1(NULL,NULL,"cachepath","directory or .cdb database file"),
    netaddress0 = arg_file1(NULL,NULL,"read_address","zmq read network address"),
    netaddress1 = arg_file1(NULL,NULL,"write_address","zmq write network address"),
    end = arg_end(20),
  };
  
  int32_t nerrors = arg_parse(argc,argv,argtable);
  
  if (help->count) {
    
    fprintf(stdout,"tmpcache %s - version 0\n",__FUNCTION__);
    arg_print_syntaxv(stdout,argtable,"\n\n");
    arg_print_glossary (stdout,argtable,"%-25s %s\n");
   
    goto finish;
  }

  if (verbose->count) {

    fprintf(stdout,"tmpcache host - version 0\n");
    int32_t major,minor,patch;
    zmq_version (&major,&minor,&patch);
    fprintf(stdout,"compiled with zmq support %d.%d.%d\n",major,minor,patch);

    goto finish;
  }

  if (nerrors) {
    
    arg_print_errors (stdout,end,"");
    arg_print_syntaxv(stdout,argtable,"\n\n");
    goto finish;
  }

 
  u_term = 0;
  signal (SIGINT,signalhandler);
  signal (SIGTERM,signalhandler);

  if (fsyslog->count)
    openlog (NULL,LOG_PID|LOG_NDELAY,LOG_USER);

  void *read (void *arg) {

    tc_readconfig_t *config = (tc_readconfig_t *)arg;

    if (fsyslog->count)
      syslog (LOG_INFO,"reading cache from %s @ %s",btocstr(config->cachepath),btocstr(config->address));
    
    tc_readfromcache (config);
    
    if (fsyslog->count)
      syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(config->cachepath),btocstr(config->address));     
  }

  tc_readconfig_t rconfig;
  rconfig.cachepath = bfromcstr (cachepath->filename[0]);
  /*TODO: check for .cdb*/
  rconfig.address = bfromcstr (netaddress1->filename[0]);
  /*TODO: check for correct address*/
  rconfig.size = 1 * (1024 * 1024);
  rconfig.signalf = checksignal;
  rconfig.errorf = (fsyslog->count) ? logerror : printerror;

  pthread_t read_t;
  if (pthread_create(&read_t,NULL,read,(void *)&rconfig) != 0) {
    if (fsyslog->count)
      syslog (LOG_ERR,"read thread creation error");
    goto finish;
  }

  tc_writeconfig_t wconfig;
  wconfig.cachepath = bfromcstr (cachepath->filename[0]);
  wconfig.address = bfromcstr (netaddress0->filename[0]);
  wconfig.size = 1 * (1024 * 1024);
  wconfig.maxsize = 64 * (1024 * 1024);
  wconfig.signalf = checksignal;
  wconfig.errorf = (fsyslog->count) ? logerror : printerror;
   
  if (fsyslog->count)
    syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(wconfig.cachepath),btocstr(wconfig.address));

  tc_writefromcache (&wconfig);  
  pthread_join (read_t,NULL);

  if (fsyslog->count) {
    syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(wconfig.cachepath),btocstr(wconfig.address));
    closelog();
  }

  bdestroy (rconfig.cachepath);
  bdestroy (rconfig.address);

 finish:

  arg_freetable(argtable,sizeof(argtable)/sizeof(argtable[0]));
  exit(0);
}
