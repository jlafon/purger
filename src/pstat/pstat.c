#include "pstat.h"
#include <assert.h>
#include <mpi.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdlib.h>

/* Checks for incoming token */
int check_for_term( state_st * st )
{
    /* If I have the token (I am already idle) */
    if(st->have_token)
    {
        /* The master rank generates the original WHITE token */
        if(st->rank == 0)
        {
        if(st->verbose)
            { 
                fprintf(logfd,"Master generating WHITE token.\n");
                fflush(logfd);
            }
            st->incoming_token = WHITE;
            MPI_Ssend(&st->incoming_token, 1, MPI_INT, (st->rank+1)%st->size, TOKEN, MPI_COMM_WORLD);
            st->token = WHITE;
            st->have_token = 0;
        /* Immediately post a receive to listen for the token when it comes back around */
            MPI_Irecv(&st->incoming_token, 1, MPI_INT, st->token_partner, TOKEN, MPI_COMM_WORLD, &st->term_request);
            st->term_pending_receive = 1;
        }
        else
        {
        /* In this case I am not the master rank. */
        /* Turn it black if I am in a black state, and forward it since I am idle. */
        /* Then I turn my state white. */ 
            if(st->token == BLACK)
                st->incoming_token = BLACK;
            MPI_Ssend(&st->incoming_token, 1, MPI_INT, (st->rank+1)%st->size, TOKEN, MPI_COMM_WORLD);
            st->token = WHITE;
            st->have_token = 0;
        /* Immediately post a receive to listen for the token when it comes back around */
            MPI_Irecv(&st->incoming_token, 1, MPI_INT, st->token_partner, TOKEN, MPI_COMM_WORLD, &st->term_request);
            st->term_pending_receive = 1;
        }
        return 0;
    }
    /* If I don't have the token. */
    else
    {
        /*  Check to see if I have posted a receive */
        if(!st->term_pending_receive)
        {
            st->incoming_token = -1;
            MPI_Irecv(&st->incoming_token, 1, MPI_INT, st->token_partner, TOKEN, MPI_COMM_WORLD, &st->term_request);
            st->term_pending_receive = 1;
        }
        st->term_flag = 0;
        /* Check to see if my pending receive has completed */
        MPI_Test(&st->term_request, &st->term_flag, &st->term_status);
        if(!st->term_flag)
        {
            return 0;
        }
        /* If I get here, then I received the token */
        st->term_pending_receive = 0;
        st->have_token = 1;
        /* Check for termination */
        if(st->incoming_token == TERMINATE)
        {
            st->token = TERMINATE;
            MPI_Send(&st->token, 1, MPI_INT, (st->rank+1)%st->size,TOKEN,MPI_COMM_WORLD);
            return TERMINATE;
        }
        if(st->token == BLACK && st->incoming_token == BLACK)
            st->token = WHITE;
        if(st->rank == 0 && st->incoming_token == WHITE)
        {
            if(st->verbose)
            {
                fprintf(logfd,"Master has detected termination.\n");
                fflush(logfd);
            }
            st->token = TERMINATE;
            MPI_Ssend(&st->token, 1, MPI_INT,1, TOKEN, MPI_COMM_WORLD);
            return TERMINATE;
        }
    }
    return 0;
}

/* Processes work queue elements */
int process_work( work_queue * qp, state_st * state )
{
    static DIR *current_dir;
    static char temp[MAX_STRING_LEN];
    static char stat_temp[MAX_STRING_LEN];
    static struct dirent *current_ent; 
    static struct stat st;
    /* Pop an item off the queue */ 
    popq(qp,temp);

    /* Try and stat it, checking to see if it is a link */
    if(lstat(temp,&st) != EXIT_SUCCESS)
    {
            perror("Unable to stat file");
    }
    /* Check to see if it is a directory.  If so, put it's children in the queue */
    else if(S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
    {
        current_dir = opendir(temp);
        if(!current_dir)
            perror("Unable to open dir");
        else
        {
        /* Read in each directory entry */
            while((current_ent = readdir(current_dir)) != NULL)
            {
    //           if(++count > 100)
//        {
//            count = 0;
//            check_for_requests(qp,state);
//        }
        /* We don't care about . or .. */
            if((strncmp(current_ent->d_name,".",2)) && (strncmp(current_ent->d_name,"..",3)))
                {
                    strcpy(stat_temp,temp);
                    strncat(stat_temp,"/",1);
                    strcat(stat_temp,current_ent->d_name);
                    pushq(qp,stat_temp);
                }
            }
        }
        closedir(current_dir);
    }
    qp->num_stats++;
    return 0;
}
/* Requests work from other ranks */
int request_work( work_queue * qp, state_st * st)
{
    if(!st->work_pending_request)
    {
        st->work_request_tries = 0;
        if(st->verbose)
        {
            fprintf(logfd,"Sending work request to %d...",st->next_processor);
            fflush(logfd);
        }
        MPI_Ssend(&st->next_processor,1,MPI_INT,st->next_processor,WORK_REQUEST,MPI_COMM_WORLD);
        if(st->verbose)
        {
            fprintf(logfd,"done.\n");
            fflush(logfd);
        }
        MPI_Irecv(st->work_offsets,INITIAL_QUEUE_SIZE/2,MPI_INT,st->next_processor,WORK,MPI_COMM_WORLD,&st->work_request);
        st->work_pending_request = 1;
        return 0;
    }
    if(MPI_Test(&st->work_request, &st->work_flag, &st->work_status) != MPI_SUCCESS)
    {
        fprintf(logfd,"Error: MPI_Test.\n");
        fflush(logfd);
    }
    if(!st->work_flag)
    {

        if(st->verbose)
        {
            fprintf(logfd,"Work request sent, but no response yet. %d attempts.\n",st->work_request_tries++);
            fflush(logfd);
        }
        if(st->work_request_tries == 1000)
        {
            if(st->verbose)
            {
                fprintf(logfd,"Canceling request to %d...",st->next_processor);
                fflush(logfd);
            }
            MPI_Cancel(&st->work_request);
            MPI_Wait(&st->work_request,&st->work_status);
            if(st->verbose)
            {
                fprintf(logfd,"done\n");
                fflush(logfd);
            }
            st->next_processor = (st->next_processor+1) % st->size;
            if(st->next_processor == st->rank)
                st->next_processor = (st->next_processor+1) % st->size;
            st->work_pending_request = 0;
            if(st->verbose)
            {
                fprintf(logfd,"Request cancelled, trying %d\n",st->next_processor);
                fflush(logfd);
            }
            st->work_request_tries = 0;
        }
        return 0;
    }
    st->next_processor = (st->next_processor+1) % st->size;
    if(st->next_processor == st->rank)
        st->next_processor = (st->next_processor+1) % st->size;
    st->work_pending_request = 0;
    if(st->work_offsets[0] == 0)
    {
        return 0;
    }
    if(st->verbose)
    {
        fprintf(logfd,"Getting work from %d, %d items.\n",st->work_status.MPI_SOURCE, st->work_offsets[0]);
        fflush(logfd);
    }
    MPI_Recv(qp->base,(st->work_offsets[1]+1)*sizeof(char),MPI_BYTE,st->work_status.MPI_SOURCE,WORK,MPI_COMM_WORLD,&st->work_status);
    qp->count = st->work_offsets[0];
    int i = 0;
    for(i= 0; i < qp->count; i++)
        qp->strings[i] = qp->base + st->work_offsets[i+2];
    qp->head = qp->strings[qp->count-1] + strlen(qp->strings[qp->count-1]);
    if(st->verbose)
    {
        fprintf(logfd,"Received items.  Queue size now %d\n",qp->count);
        fflush(logfd);
    }
    st->work_pending_request = 0;
    return 0;
}
/* Checks for outstanding work requests */
int check_for_requests( work_queue * qp, state_st * st)
{
    static int recv_buf = 0;
    static int source;
    int i;

    if(!st->request_pending_receive)
    {
        MPI_Irecv(&recv_buf, 1, MPI_INT, MPI_ANY_SOURCE, WORK_REQUEST, MPI_COMM_WORLD, &st->request_request);
        st->request_pending_receive = 1;
    }
    MPI_Test(&st->request_request, &st->request_flag, &st->request_status);
    if(!st->request_flag)
        return 0;
    source = st->request_status.MPI_SOURCE;
    st->request_pending_receive = 0;
    if(qp->count < 10)
    {
        if(st->verbose)
        {
            fprintf(logfd,"Received work request from %d, but have no work.\n",source);
            fflush(logfd);
        }
        recv_buf = 0;
        MPI_Ssend(&recv_buf, 1, MPI_INT, source, WORK, MPI_COMM_WORLD);
        if(st->verbose)
        {
            fprintf(logfd,"Response sent to %d, have no work.\n",source);
            fflush(logfd);
        }
    }
    else
    {
        if(st->verbose)
        {
            fprintf(logfd,"Received work request from %d...\n",source);
            fflush(logfd);
        }
        if(source < st->rank || source == st->token_partner)
            st->token = BLACK;
        char * b = qp->strings[(qp->count/2)];
        char * e = qp->strings[qp->count-1];
        size_t diff = e-b;
        diff += strlen(e);
        /* offsets[0] = number of strings */
        /* offsets[1] = size of all strings being sent */
        st->request_offsets[0] = qp->count-(qp->count/2);
        st->request_offsets[1] = diff;
        
        int j = qp->count / 2;
        for(i=2; i < qp->count - qp->count/2 + 2; i++)
            st->request_offsets[i] = qp->strings[j++] - b;
        /* offsets[qp->count - qp->count/2+2]  is the size of the last string */
        st->request_offsets[qp->count - qp->count/2+2] = strlen(qp->strings[j-1]);
        if(st->verbose)
        {
            fprintf(logfd,"\tSending offsets for %d items to %d...",st->request_offsets[0],source);
            fflush(logfd);
        }
        MPI_Ssend(st->request_offsets, INITIAL_QUEUE_SIZE/2, MPI_INT, source, WORK, MPI_COMM_WORLD);
        if(st->verbose)
        {
            fprintf(logfd,"done.\n");
            fflush(logfd);
            fprintf(logfd,"\tSending buffer to %d...",source);
            fflush(logfd);
        }
        MPI_Ssend(b, (diff+1)*sizeof(char), MPI_BYTE, source, WORK, MPI_COMM_WORLD);
        if(st->verbose)
        {
            fprintf(logfd,"done.\n");
            fflush(logfd);
        }
        qp->count = qp->count - st->request_offsets[0];
        if(st->verbose)
        {
            fprintf(logfd,"sent %d items to %d.\n",st->request_offsets[0],source);
            fflush(logfd);
        }
    }
    return 0;
}

/* Parses command line arguments */
int parse_args( int argc, char *argv[] , options * opts )
{
    static struct option long_options[] = 
    {
    {"db",        required_argument, 0, 'd'},
    {"path",    required_argument, 0, 'p'},
    {"restart",    required_argument, 0, 'r'},
    {"help",    no_argument,       0, 'h'},
    {"verbose", no_argument,    0,    'v'},
    {0,0,0,0}
    };
    int option_index = 0;
    int c = 0;
    while((c = getopt_long(argc,argv, "d:p:r:h", long_options, &option_index)) != -1)
    {
        switch(c)
        {
            case 'p':
                    snprintf(opts->beginning_path, strlen(optarg)+1, "%s", optarg);
                    break;
            case 'd':
                    break;
            case 'r':
                    break;
            case 'v':
            opts->verbose = 1;
                    break;
            case 'h':
                    return -1;
                    break; // just for fun
            default:
                    return 0; 
        }
    }
    return 0;
}
/* Prints a queue */
void printq( work_queue * qp )
{
    int i = 0;
    for(i = 0; i < qp->count; i++)
       fprintf(logfd,"\t[%d] %s\n",i,qp->strings[i]);
    fprintf(logfd,"\n");
}

/* Pushes a path onto the work queue
 * Updates head to point to the next available empty memory
 * Updates the count 
 */
int pushq( work_queue * qp, char * str )
{
    assert(qp->strings[qp->count-1] + MAX_STRING_LEN < qp->end);
    qp->strings[qp->count] = qp->head; 
    /* copy the string */
    strcpy(qp->head, str);
    
    /* Make head point to the character after the string */
    qp->head = qp->head + strlen(qp->head) + 1;

    /* NULL terminate the string */
    qp->head[0] = '\0';
    
    /* Make the head point to the next available memory */
    qp->head = qp->head + 1;
    qp->count = qp->count + 1;
    return 0;
}

/* Removes an item from the queue
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
    qp->count = qp->count - 1;
    return 0;
}


