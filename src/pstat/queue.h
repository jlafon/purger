#ifndef QUEUE_H
#define QUEUE_H

#define MAX_STRING_LEN 2048*sizeof(char)
#define INITIAL_QUEUE_SIZE 400000

typedef struct work_queue
{
    /* Base of memory pool */
    char * base;
    /* End of memory pool */
    char * end;
    /* Location of next string */
    char * next;
    /* Location of next free byte */
    char * head;
    char ** strings;
    int count;
    int num_stats;
} work_queue;


/*! \brief Pushes a path onto the work queue
 * Updates head to point to the next available empty memory
 * Updates the count 
 */
int pushq( work_queue * qp, char * str );
/*! \brief Removes an item from the queue
 * Updates head to point to the next free memory
 * Updates next to point to the last string in the queue
 * Copies the string being popped into str
 */
int popq( work_queue * qp, char * str );
#endif
