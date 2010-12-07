#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

int main(int argc, char *argv[])
{
  struct stat st;

  lstat(argv[1], &st);

  printf("%s\n", argv[1]);
  printf("ctime = %ju \n", st.st_ctime);
  printf("mtime = %ju \n", st.st_mtime);
  printf("atime = %ju \n", st.st_atime);
  printf("ino = %ju \n", st.st_ino);

  return 0;
}
