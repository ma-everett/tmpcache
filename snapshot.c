
#include <dirent.h> 
#include <sys/types.h> 
#include <sys/param.h> 
#include <sys/stat.h> 
#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h>
#include <string.h> 



#include "bstrlib/bstrlib.h"
#include "bstrlib/bsafe.h"


int main(void)
{
  DIR           *d;
  struct dirent *dir;
  
  bstring root = bfromcstr("/mnt/git/tmpcache/test");

  d = opendir("./test");
  if (d)
  {
    while ((dir = readdir(d)) != NULL)
    { 
      if( strcmp( dir->d_name, "." ) == 0 || strcmp( dir->d_name, ".." ) == 0 ) 
	continue;
      if ( dir->d_type == DT_DIR) { /* directory */

	/* we ignore directories - because tmpcache doesn't deal with them */

      } else { /* file */

	bstring filename = bformat("%s/%s",(const char *)root->data, dir->d_name);

	//printf("filename = %s\n",(const char *)filename->data);

	struct stat s;
	stat(dir->d_name,&s);

	FILE *fp = fopen((const char *)filename->data,"r");
	if (fp) {
	  long cpos = ftell(fp);
	  fseek(fp,0,SEEK_END);
	  size_t bufsize = ftell(fp);
	  fseek(fp,cpos,SEEK_SET);
      
	  char *filebuffer = (char *)malloc(bufsize);
	  fread(&filebuffer[0],bufsize,1,fp);
	  fclose(fp);

	  printf("+%d,%d:%s->%s\n", strlen(dir->d_name),(bufsize - 1),dir->d_name,filebuffer);
	  free (filebuffer);
	}
	
	bdestroy (filename);
      }
    }

    closedir(d);
  }

  bdestroy (root);

  return(0);
}
