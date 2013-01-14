
/* tmpcache.h */
#ifndef TMPCACHE_H
#define TMPCACHE_H 1

/* options : */
#define TMPCACHE_READ 3
#define TMPCACHE_WRITE 4
#define TMPCACHE_DELETE 5

/* 0. init, create a context
 * 1. configure caches 
 * 2. open connections
 * 3. run operations
 * 4. close connections
 * 5. finish
 */

typedef void *(*tmpcache_mallocf)(int,void *);
typedef void (*tmpcache_freef)(void *,void *);
typedef int (*tmpcache_choosef) (const char *,int,int *,int,void *);

extern int tmpcache_hash (const char *key,const int klen);

extern void * tmpcache_init (void);

extern void * tmpcache_custom (tmpcache_mallocf mallocf,
			       tmpcache_freef freef, void *hint);

extern int tmpcache_option(void *ctx,const int option, tmpcache_choosef f, void * hint);

/* TODO: add some additional options per connection */

extern int tmpcache_term (void *ctx);

extern int tmpcache_open (void *ctx);
extern int tmpcache_close (void *ctx);

extern int tmpcache_includecache(void *ctx,const char *waddr,const int wlen,
				 const char *raddr, const int rlen);

extern int tmpcache_connect (void *ctx);
extern int tmpcache_disconnect (void *ctx);

/* TODO: each operation should have a user supplied function for testing etc, 
 * plus an option for setting the type of connection
 */


extern int tmpcache_write  (void *ctx,const char *key, const int klen, void *data,int dlen);
extern int tmpcache_read   (void *ctx,const char *key, const int klen, void *buffer, int blen);
extern int tmpcache_delete (void *ctx,const char *key, const int klen);



#endif
