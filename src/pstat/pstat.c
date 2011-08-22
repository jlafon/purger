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
            MPI_Send(&st->incoming_token, 1, MPI_INT, (st->rank+1)%st->size, TOKEN, MPI_COMM_WORLD);
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
            MPI_Send(&st->incoming_token, 1, MPI_INT, (st->rank+1)%st->size, TOKEN, MPI_COMM_WORLD);
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
            MPI_Send(&st->token, 1, MPI_INT,1, TOKEN, MPI_COMM_WORLD);
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
            fprintf(logfd,"Error: Couldn't stat \"%s\"\n",temp);
            MPI_Abort(MPI_COMM_WORLD,-1);
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
                    strcat(stat_temp,"/");
                    strcat(stat_temp,current_ent->d_name);
                    pushq(qp,stat_temp);
                }
            }
        }
        closedir(current_dir);
    }
    else if(S_ISREG(st.st_mode))
    {
    /*    if(redis_context == NULL)
            redis_context = redisConnect("127.0.0.1", 6379);

        if (redis_context->err)
            fprintf(logfd,"Redis context error: %s\n", redis_context->errstr);

        if(redisAsyncCommand(redis_context, NULL, NULL,
                "SET d_name %s", current_ent->d_name) != REDIS_OK)
            fprintf(logfd,"Redis command error.\n");
    */
    }
    qp->num_stats++;
    return 0;
}
int get_next_proc(int current, int rank, int size)
{
    int result = rand() % size;
    while(result == rank)
        result = rand() % size;
    return result;
}
int wait_on_mpi_request( MPI_Request * req, MPI_Status * stat, int timeout)
{
    int tries = 0;
    int flag = 0;
    fflush(logfd);
    while(!flag && tries++ < timeout)
    {
        MPI_Test(req,&flag,stat);
    }
    if(tries > timeout)
    {
        fprintf(logfd,"Cancelling...");
        fflush(logfd);
        MPI_Cancel(req);
        fprintf(logfd,"Cancelled.\n");
        fflush(logfd);
   //     MPI_Wait(req,stat);
        return -1;
    }
    return MPI_SUCCESS;
}
int wait_on_probe(int source, int tag,int timeout)
{
    int flag = 0;
    int tries = 0;
    MPI_Status temp;
    while(!flag && tries++ < timeout)
    {
        MPI_Iprobe(source, tag, MPI_COMM_WORLD, &flag,&temp);
    }
    //fprintf(logfd,"count: %d flag: %d tries %d\n",temp._count,flag,tries);
    //fflush(logfd);
    if(flag && tries < timeout)
        return temp._count;
    else
        return 0;
}
/* Requests work from other ranks */
int request_work( work_queue * qp, state_st * st)
{
    int temp_buffer = 3;
    /* Check to see if a request has been posted */
    if(st->verbose)
    {
        fprintf(logfd,"Sending work request to %d...",st->next_processor);
        fflush(logfd);
    }
    /* Since there is no pending request, post one */
    MPI_Send(&temp_buffer,1,MPI_INT,st->next_processor,WORK_REQUEST,MPI_COMM_WORLD);
    if(st->verbose)
    {
        fprintf(logfd,"done.\n");
        fflush(logfd);
    }
    st->work_offsets[0] = 0;
    /* Since a work request has been posted, post a corresponding receive for the offsets */
    if(st->verbose)
    {
        fprintf(logfd,"Getting response from %d...",st->next_processor);
        fflush(logfd);
    }
    MPI_Request temp_recv_request;
    st->work_offsets[0] = 0;
    int size = wait_on_probe(st->next_processor, WORK, 100000);
    if(size == 0)
    {
        st->next_processor = get_next_proc(st->next_processor, st->rank, st->size);
        return 0;
    }
    else if(size == 1)
    {
        int buf;
        MPI_Recv(&buf, 1, MPI_INT, st->next_processor, WORK, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        st->next_processor = get_next_proc(st->next_processor, st->rank, st->size);
        return 0;
    }
    fprintf(logfd,"Received message with %d elements.\n",size);
    fflush(logfd);
    MPI_Recv(st->work_offsets,size,MPI_INT,st->next_processor,WORK,MPI_COMM_WORLD,&st->work_offsets_status);
    int source = st->next_processor;
    st->next_processor = get_next_proc(st->next_processor, st->rank, st->size);
    int chars = st->work_offsets[1];
    int items = st->work_offsets[0];
    if(items == 0)
    {
        return 0;
    }
    //if(st->verbose)
    //{
        fprintf(logfd,"Getting work from %d, %d items.\n",source, items);
        fflush(logfd);
    //}
    
   // MPI_Request temp_request;
    size = wait_on_probe(source,WORK,1000000);
    if(size == 0)
        return 0;
    fprintf(logfd,"Message pending with %d size\n",size);
    fflush(logfd);
    MPI_Recv(qp->base,(chars+1)*sizeof(char),MPI_BYTE,source,WORK,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    qp->count = items;
    int i = 0;
    for(i= 0; i < qp->count; i++)
    {
        qp->strings[i] = qp->base + st->work_offsets[i+2];
        if(st->verbose)
        {
            fprintf(logfd,"Item [%d] Offset [%d] String [%s]\n",i,st->work_offsets[i+2],qp->strings[i]);
            fflush(logfd);
        }
    }
    assert(qp->strings[0] == qp->base);
    qp->head = qp->strings[qp->count-1] + strlen(qp->strings[qp->count-1]);
    if(st->verbose)
    {
        fprintf(logfd,"Received items.  Queue size now %d\n",qp->count);
        fflush(logfd);
    }
    //printq(qp);
    return 0;
}
void send_no_work( int dest, state_st * st )
{
    int no_work = 0;
    if(st->verbose)
    {
        fprintf(logfd,"Received work request from %d, but have no work.\n",dest);
        fflush(logfd);
    }
    MPI_Request temp;
    MPI_Isend(&no_work, 1, MPI_INT, dest, WORK, MPI_COMM_WORLD, &temp);
    wait_on_mpi_request(&temp, MPI_STATUS_IGNORE,10000);
    if(st->verbose)
    {
        fprintf(logfd,"Response sent to %d, have no work.\n",dest);
        fflush(logfd);
    }
}
/* Distributes a random amount of the local work queue to the n requestors */
void send_work_to_many( work_queue * qp, state_st * st, int * requestors, int rcount)
{
    assert(rcount > 0);
    /* Random number between rcount+1 and qp->count */
    int total_amount = rand() % (qp->count-(rcount+1)) + rcount+1;
    if(st->verbose)
    {
        fprintf(logfd,"Queue size: %d, Total_amount: %d\n",qp->count,total_amount);
        fflush(logfd);
    }
    int i = 0;
    /* Get size of chunk */
    int increment = total_amount / rcount;
    for(i = 0; i < rcount; i ++)
    {
        total_amount -= increment;
        if(total_amount < increment)
            increment += total_amount;
        if(send_work( qp, st, requestors[i], increment) < 0)
        {
            i++;
            if(i == rcount)
                return;
            else if(i == rcount - 1)
                send_work(qp, st, requestors[i], increment + total_amount);
            else
                send_work(qp, st, requestors[i], increment + increment);
        }
    }
}
int send_work( work_queue * qp, state_st * st, int dest, int count )
{
    if(dest < st->rank || dest == st->token_partner)
        st->token = BLACK;
    char * b = qp->strings[qp->count-1-count];
    char * e = qp->strings[qp->count-1];
    size_t diff = e-b;
    diff += strlen(e);
    /* offsets[0] = number of strings */
    /* offsets[1] = number of chars being sent */
    st->request_offsets[0] = count;
    st->request_offsets[1] = diff;
    assert(diff < (INITIAL_QUEUE_SIZE*MAX_STRING_LEN)); 
    int j = qp->count-count-1;
    int i = 0;
    for(i=0; i < st->request_offsets[0]; i++)
    {
        st->request_offsets[i+2] = qp->strings[j++] - b;
        if(st->verbose)
        {
            fprintf(logfd,"[j=%d] Base address: %p, String[%d] address: %p, String \"%s\" Offset: %u\n",j,b,i,qp->strings[j-1],qp->strings[j-1],st->request_offsets[i+2]);
            fflush(logfd);
        }
    }
    /* offsets[qp->count - qp->count/2+2]  is the size of the last string */
    st->request_offsets[count+2] = strlen(qp->strings[qp->count-1]);
    if(st->verbose)
    {
        fprintf(logfd,"\tSending offsets for %d items to %d...",st->request_offsets[0],dest);
        //print_offsets(st->request_offsets,st->request_offsets[0]+2);
        fflush(logfd);
    }
    MPI_Request temp_request;
    MPI_Issend(st->request_offsets, st->request_offsets[0]+2, MPI_INT, dest, WORK, MPI_COMM_WORLD,&temp_request);
    if(wait_on_mpi_request(&temp_request, MPI_STATUS_IGNORE,10000000) != MPI_SUCCESS)
    {
        fprintf(logfd,"Cancelled offset send\n");
        fflush(logfd);
        return -1;
    }
    //MPI_Wait(&temp_request,MPI_STATUS_IGNORE);
    if(st->verbose)
    {
        fprintf(logfd,"done.\n");
        fflush(logfd);
        fprintf(logfd,"\tSending buffer to %d...",dest);
        fflush(logfd);
    }
    MPI_Request temp_send_request;
    MPI_Issend(b, (diff+1)*sizeof(char), MPI_BYTE, dest, WORK, MPI_COMM_WORLD,&temp_send_request);
    if(wait_on_mpi_request(&temp_send_request, MPI_STATUS_IGNORE,10000000) != MPI_SUCCESS)
    {
        fprintf(logfd,"Cancelled offset send\n");
        fflush(logfd);
        return -1;
    }

   if(st->verbose)
    {
        fprintf(logfd,"done.\n");
        fflush(logfd);
    }
    qp->count = qp->count - st->request_offsets[0];
    if(st->verbose)
    {
        fprintf(logfd,"sent %d items to %d.\n",st->request_offsets[0],dest);
        fflush(logfd);
    }
    return 0;
}
/* Checks for outstanding work requests */
int check_for_requests( work_queue * qp, state_st * st)
{
    int * requestors = (int *) calloc(st->size,sizeof(int));
    int i = 0;
    int rcount = 0;
    /* This loop is only excuted once.  It is used to initiate receives. 
     * When a received is completed, we repost it immediately to capture
     * the next request */
    if(!st->request_pending_receive)
    {
        for(i = 0; i < st->size; i++)
        {
            if(i != st->rank)
            {
                MPI_Recv_init(&st->request_recv_buf[i], 1, MPI_INT, i, WORK_REQUEST, MPI_COMM_WORLD, &st->request_request[i]);
                MPI_Start(&st->request_request[i]);
            }
        }
        st->request_pending_receive = 1;
    }

    /* Test to see if any posted receive has completed */
    for(i = 0; i < st->size; i++)
        if(i != st->rank)
        {
            st->request_flag[i] = 0;
            if(MPI_Test(&st->request_request[i], &st->request_flag[i], &st->request_status[i]) != MPI_SUCCESS)
                exit(1);
            if(st->request_flag[i])
            {
                requestors[rcount++] = i;
                st->request_flag[i] = 0;
            }
        }
    /* If we didn't received any work, no need to continue */
    if(rcount == 0)
        return 0;
    if(qp->count <= rcount+1)
    {
        for(i = 0; i < rcount; i++)
            send_no_work( requestors[i], st );
    }
    else
    {
        if(st->verbose)
        {
            fprintf(logfd,"Got work requests from %d ranks.\n",rcount);
            fflush(logfd);
        }
        send_work_to_many( qp, st, requestors, rcount);
    }
    for(i = 0; i < rcount; i++)
        MPI_Start(&st->request_request[requestors[i]]);
    free(requestors);
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
void print_offsets(unsigned int * offsets, int count)
{
    int i = 0;
    for(i = 0; i < count; i++)
        fprintf(logfd,"\t[%d] %d\n",i,offsets[i]);
}
void dumpq( work_queue * qp)
{
   int i = 0;
   char * p = qp->base;
   while(p++ != (qp->strings[qp->count-1]+strlen(qp->strings[qp->count-1])))
       if(i++ % 120 == 0) fprintf(logfd,"%c\n",*p); else fprintf(logfd,"%c",*p);

}
/* Prints a queue */
void printq( work_queue * qp )
{
    int i = 0;
    for(i = 0; i < qp->count; i++)
       fprintf(logfd,"\t[%p][%d] %s\n",qp->strings[i],i,qp->strings[i]);
    fprintf(logfd,"\n");
}

/* Pushes a path onto the work queue
 * Updates head to point to the next available empty memory
 * Updates the count 
 */
int pushq( work_queue * qp, char * str )
{
    //fprintf(logfd,"Count: %d, Start: %p, End: %p MAX_STRING_LEN: %d, Diff: %lu\n",qp->count,qp->base,qp->end,MAX_STRING_LEN,qp->end-qp->base);
    assert(strlen(str) > 0); 
    if(qp->count > 1)
        assert(qp->strings[qp->count-1] + MAX_STRING_LEN < qp->end);
    qp->strings[qp->count] = qp->head; 
    /* copy the string */
    strcpy(qp->head, str);
    assert(strlen(qp->head) < MAX_STRING_LEN);
    /* Make head point to the character after the string */
    qp->head = qp->head + strlen(qp->head) + 1;
    
    /* Make the head point to the next available memory */
    qp->count = qp->count + 1;
    fprintf(logfd,"Push: %s\tLength: %lu\tBase: %p\tString: %p\tCount :%d\tDiff: %lu\n",qp->head,strlen(qp->head),qp->base,qp->head, qp->count,qp->strings[qp->count-1] - qp->strings[qp->count-2]);
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


