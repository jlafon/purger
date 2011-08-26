#include<string.h>
#include<assert.h>
#include "worker.h"
/*! \brief Pushes a path onto the work queue
 * Updates head to point to the next available empty memory
 * Updates the count 
 */
int pushq( work_queue * qp, char * str )
{
    //fprintf(logfd,"Pushed: %s\n",str); 
    assert(strlen(str) > 0); 
    if(qp->count > 1)
        assert(qp->strings[qp->count-1] + MAX_STRING_LEN < qp->end);
    /* copy the string */
    strcpy(qp->head, str);
    qp->strings[qp->count] = qp->head; 
    assert(strlen(qp->head) < MAX_STRING_LEN);
    /* Make head point to the character after the string */
    qp->head = qp->head + strlen(str) + 1;
    
    /* Make the head point to the next available memory */
    qp->count = qp->count + 1;
    return 0;
}

/*! \brief Removes an item from the queue
 * Updates head to point to the next free memory
 * Updates next to point to the last string in the queue
 * Copies the string being popped into str
 */
int popq( work_queue * qp, char * str )
{
    if(qp->count == 0)
        return 0;
    /* Copy last element into str */
    strcpy(str,qp->strings[qp->count-1]);
    qp->head = qp->strings[qp->count-1];
    qp->count = qp->count - 1;
   // fprintf(logfd,"Popped: %s\n",str);
    return 0;
}
