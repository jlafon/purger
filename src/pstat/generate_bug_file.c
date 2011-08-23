#include<stdio.h>
#include<stdlib.h>
int main()
{
    FILE * fd = fopen("test.dat","w+");
    srand(time(NULL));
    int limit = rand() % 100000;
    int i = 0;
    for(i = 0; i < limit; i++)
        fputc(20,fd);
    while(i++ % 4096 != 0)
        fputc(0,fd);
    fclose(fd);
 
}
