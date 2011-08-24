#include <stdio.h>
#include <stdlib.h>
/*
 * Checks file for filesystem bug
 */
int check_file(char * filename)
{
    int buf[4096];
    FILE * fd = fopen(filename, "r");
    if(fd == NULL)
        return 2 && fclose(fd);
    if(fseek(fd,-4096,SEEK_END) != 0)
        return 3 && fclose(fd);
    if(fread(buf,1,4096,fd) != 4096)
        return 4 && fclose(fd);
    fclose(fd);
    int * i = &buf[0];
    // ignore EOF
    int * x = &buf[4094];
    while(*x == 0 && x-- != i)
        ;
    if(x == i)
        return 0;
    return 1;
}
