
/* tmpcache/tmpcache_write.c */

#include <argtable2.h>
#include <syslog.h>
#include <signal.h>
#include <zmq.h>

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

void printerror (const char *msg) {

  fprintf(stderr,"%s - %s",msg,zmq_strerror(zmq_errno()));
}

void logerror (const char *msg) {

  syslog(LOG_ERR,"%s - %s",msg,zmq_strerror(zmq_errno()));
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

  tc_writeconfig_t config;
  config.cachepath = bfromcstr (cachepath->filename[0]);
  /*TODO: check for .cdb*/
  config.address = bfromcstr (netaddress0->filename[0]);
  /*TODO: check for correct address*/
  config.size = 1 * (1024 * 1024);
  config.maxsize = 64 * (1024 * 1024);
  config.signalf = checksignal;
  config.errorf = (fsyslog->count) ? logerror : printerror;

  if (fsyslog->count)  {
    openlog (NULL,LOG_PID|LOG_NDELAY,LOG_USER);
    syslog (LOG_INFO,"writing cache from %s @ %s",btocstr(config.cachepath),btocstr(config.address));
  }

  tc_writeinfo_t * info = tc_writefromcache (&config);

  if (fsyslog->count) {
    if (info) {
      syslog (LOG_INFO,"closing cache %s @ %s for writing, %d writes, largest %db, lowest %db",
	      btocstr(config.cachepath),btocstr(config.address),info->numofwrites,info->largestwrite,info->lowestwrite);    
    } else {
      
      syslog (LOG_INFO,"closing cache %s @ %s for writing",
	      btocstr(config.cachepath),btocstr(config.address));
    }
    closelog();
  } else {
    if (info) {
      fprintf (stdout,"%d writes, largest %db, lowest %db\n",
	       info->numofwrites,info->largestwrite,info->lowestwrite);
    } 
  }    

  bdestroy (config.address);
  bdestroy (config.cachepath);

  if (info) 
    c_free (info,NULL);

 finish:

  arg_freetable(argtable,sizeof(argtable)/sizeof(argtable[0]));
  exit(0);
}
