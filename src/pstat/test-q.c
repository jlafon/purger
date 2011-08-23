#include "pstat.h"

void main()
{
    work_queue q;
    q.base = (char *) malloc(sizeof(char) * MAX_STRING_LEN * INITIAL_QUEUE_SIZE);
    q.strings = (char **) malloc(sizeof(char*) * INITIAL_QUEUE_SIZE);
    q.head = q.base;
    q.end = q.base + (MAX_STRING_LEN*INITIAL_QUEUE_SIZE);
    q.count = 0;
    logfd = fopen("/dev/stdout","w+");
    pushq(&q,"test1");
    pushq(&q,"test2");
    pushq(&q,"test3");
    pushq(&q,"test4");
    pushq(&q,"test5");
    pushq(&q,"test6");
    pushq(&q,"test7");
    char temp[256];
    printq(&q);
    popq(&q,temp);
    popq(&q,temp);
    popq(&q,temp);
    pushq(&q,"testa");
    pushq(&q,"testb");
    pushq(&q,"testc");
    printq(&q);
}
