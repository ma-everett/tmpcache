
/* tmpcache/client.c 
 *
 * > echo "bar" > client -k foo -w ipc:///srv/run/write.tmpcache
 * > foo.txt < client -k foo -r ipc:///srv/run/read.tmpcache
 */

#include "bstrlib/bstrlib.h"
#include "bstrlib/bsafe.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <argp.h>

#include <xs/xs.h>

#define CACHE_VERSION "tmpcache 0" /*FIXME - move to include file*/

const char *argp_program_version = CACHE_VERSION; /*FIXME*/
const char *argp_program_bugaddress = "<..>";

static char argp_doc[] = "tmpcache, a filesystem cache using message passing"; 
static char args_doc[] = ""; /*FIXME*/

static struct argp_option argp_options[] = {
  {"key",      'k', "key",  0, "Key, or label for data"},
  {"write",    'w', "ADDR", 0, "Write address for cache"},
  {"read",     'r', "ADDR", 0, "Read address for cache"},
  {0}
};

typedef struct arguments {

  char *waddress;
  char *raddress;
  char *key;

} arguments_t;

static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 'k' :
    arguments->key = arg;
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

static struct argp argp = {argp_options,parseoptions,args_doc,argp_doc};

#define xs_assert(s) if (!(s)) {\
    fprintf(stderr,"%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));	\
    goto error;}

void xs_free (void *p, void *hint) { free (p); }

int main (int argc, char **argv) 
{
  arguments_t options;
  options.waddress = NULL;
  options.raddress = NULL;
  options.key = NULL;
 
  argp_parse (&argp,argc,argv,0,0,&options);

  if (!options.waddress && !options.raddress)
    exit(0);

  if (!options.key)
    exit(0);

  int r = 0;
  int interface = 0;
  void *sock = NULL;
  void *ctx = xs_init();
  assert(ctx); /*FIXME*/

  /* read : */
  if (options.raddress && options.key) {
    
    sock = xs_socket (ctx,XS_REQ);
    assert(sock); /*FIXME*/

    r = xs_connect (sock,options.raddress);
    xs_assert (r != -1);
    
    interface = r;

    xs_msg_t msg_key;
    char *key = malloc(sizeof(char) * strlen(options.key));
    memcpy (key,options.key,strlen(options.key));
    xs_msg_init_data (&msg_key,key,strlen(options.key),xs_free,NULL);

    xs_msg_t msg_part;
    xs_msg_init (&msg_part);

    r = xs_sendmsg (sock,&msg_key,0);
    xs_assert(r != -1);

    r = xs_recvmsg (sock,&msg_key,0);
    xs_assert(r != -1);
    
    r = xs_recvmsg (sock,&msg_part,0);
    xs_assert(r != -1);
    
    if (xs_msg_size(&msg_part)) {

      puts(xs_msg_data(&msg_part));
    }

    xs_msg_close (&msg_key);
    xs_msg_close (&msg_part);

    xs_shutdown (sock,interface);

    goto error; /*FIXME, bad flow*/
  } 

  /* write : */
  if (options.waddress && options.key) {

    sock = xs_socket (ctx,XS_PUSH);
    assert(sock); /*FIXME*/

    r = xs_connect (sock,options.waddress);
    xs_assert (r != -1);

    interface = r;

    /* read a 1Mb buffer from stdin */
    char *buffer = malloc(sizeof(char) * (1024 * 1024));
    if (buffer == NULL)
      {
	perror("Failed to allocate content");
	exit(1); /*FIXME, poor flow */
      }

    fgets(buffer,(1024 * 1024),stdin);
 	
    xs_msg_t msg_key;
    void *key = malloc(sizeof(char) * strlen(options.key));
    memcpy (key,options.key,strlen(options.key));
    xs_msg_init_data (&msg_key,key,strlen(options.key),xs_free,NULL);

    xs_msg_t msg_part;
    xs_msg_init_data (&msg_part,buffer,strlen(buffer),xs_free,NULL);  

    r = xs_sendmsg (sock,&msg_key,XS_SNDMORE|XS_DONTWAIT);
    xs_assert (r != -1);

    r = xs_sendmsg (sock,&msg_part,XS_DONTWAIT);
    xs_assert (r != -1);   

    xs_shutdown (sock,interface);
    
    goto error;
  }

 error:
  xs_close (sock);
  xs_term (ctx);

  exit(0);
}
