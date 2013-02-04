
/* tmpcache/tmpcache_write.c */

#include <argtable2.h>
#include <syslog.h>
#include <signal.h>

#include "cache.h"

struct arg_lit *verbose, *fsyslog,*help;
struct arg_file *cachepath, *netaddress0;
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

int main (int argc, char **argv) {

  void *argtable[] = {
    help = arg_lit0("h","help","print this screen"),
    verbose = arg_lit0("v","verbose","tell me everything"),
    fsyslog = arg_lit0(NULL,"syslog","use syslog"),
    cachepath = arg_file1(NULL,NULL,"cachepath","directory or .cdb database file"),
    netaddress0 = arg_file1(NULL,NULL,"address","crossroads.io read network address "),
    
    end = arg_end(20),
  };
  
  int32_t nerrors = arg_parse(argc,argv,argtable);
  
  if (help->count) {
    
    fprintf(stdout,"tmpcache %s - version 0\n",__FUNCTION__); /*FIXME*/
    arg_print_syntaxv(stdout,argtable,"\n\n");
    arg_print_glossary (stdout,argtable,"%-25s %s\n");
   
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
  openlog (NULL,LOG_PID|LOG_NDELAY,LOG_USER);

  tc_writeconfig_t config;
  config.cachepath = bfromcstr (cachepath->filename[0]);
  /*TODO: check for .cdb*/
  config.address = bfromcstr (netaddress0->filename[0]);
  /*TODO: check for correct address*/
  config.size = 1 * (1024 * 1024);
  config.maxsize = 64 * (1024 * 1024);
  config.signalf = checksignal;

  syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(config.cachepath),btocstr(config.address));
    
  tc_writefromcache (&config);

  syslog (LOG_INFO,"closing cache %s @ %s for reading",btocstr(config.cachepath),btocstr(config.address));    
    
  bdestroy (config.address);
  bdestroy (config.cachepath);

  closelog();

 finish:

  arg_freetable(argtable,sizeof(argtable)/sizeof(argtable[0]));
  exit(0);
}
