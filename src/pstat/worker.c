#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include "pstat.h"

int worker( options * opts )
{
    double start_time;
    int token = WHITE;
    int token_partner;
    
    /* Holds all worker state */
    state_st s;
    state_st * sptr = &s;

    /* Holds work elements */
    work_queue queue;
    
    /* Memory for work queue */
    queue.base = (char*) malloc(sizeof(char) * MAX_STRING_LEN * INITIAL_QUEUE_SIZE);
    
    /* A pointer to each string in the queue */
    queue.strings = (char **) malloc(sizeof(char*) * INITIAL_QUEUE_SIZE);
    
    work_queue * qp = &queue;
    
    queue.head = queue.base;
    queue.end = queue.base + (MAX_STRING_LEN*INITIAL_QUEUE_SIZE);
    queue.count = 0;
    int rank = -1;
    int size = -1;
    int next_processor;
    //int cycles = 0;
    
    /* Get MPI info */
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    srand(rank);
    s.rank = rank;
    s.size = size;
    s.token = WHITE;
    next_processor = (rank+1) % size;
    token_partner = (rank-1)%size;
    s.next_processor = (rank+1) % size;
    s.token_partner = (rank-1)%size;
    if(token_partner < 0) token_partner = size-1;
   
    /* Logging */
    char logf[32];
    snprintf(logf,32,"rank%d",rank);
    logfd = fopen(logf,"w+");

    /* Initial state */
    s.have_token = 0;
    s.term_flag = 0;
    s.work_flag = 0;
    s.work_pending_request = 0;
    s.request_pending_receive = 0;
    s.term_pending_receive = 0;
    qp->num_stats = 0;
    s.incoming_token = BLACK;
    s.request_offsets = (unsigned int*) calloc(INITIAL_QUEUE_SIZE,sizeof(unsigned int));
    s.work_offsets = (unsigned int*) calloc(INITIAL_QUEUE_SIZE,sizeof(unsigned int));
    s.request_status = (MPI_Status *) malloc(sizeof(MPI_Status)*size);
    int i = 0;
    s.request_flag = (int *) calloc(size,sizeof(int));
    s.request_recv_buf = (int *) calloc(size,sizeof(int));
    s.request_request = (MPI_Request*) malloc(sizeof(MPI_Request)*size);
    for(i = 0; i < size; i++)
        s.request_request[i] = MPI_REQUEST_NULL;
    s.work_request_tries = 0;
    s.verbose = opts->verbose;
    
    /* Master rank starts out with the beginning path */
    if(rank == 0)
    {
        pushq(qp, opts->beginning_path);
        s.have_token = 1;
    }
    start_time = MPI_Wtime();
    /* Loop until done */
    while(token != DONE)
    {
        /* Check for and service work requests */
        if(opts->verbose)
        {
            fprintf(logfd,"Checking for requests...");
            fflush(logfd);
        }
        check_for_requests(qp,sptr);
        if(opts->verbose)
        {
            fprintf(logfd,"done\n");
            fflush(logfd);
        }        
        if(qp->count == 0)
        {
            if(opts->verbose)
            {
                fprintf(logfd,"Requesting work...\n");
                fflush(logfd);
            }
            //cleanup_work_messages(sptr);
            if(request_work(qp,sptr) < 0)
                token = DONE;
            if(opts->verbose)
            {      
                fprintf(logfd,"Done requesting work\n");
                fflush(logfd);
            }        
        }
        if(qp->count > 0)
        {
                process_work(qp,sptr);
        }
        else if(token != DONE)
        {
            if(opts->verbose)
            {
                fprintf(logfd,"Checking for termination...");
                fflush(logfd);    
            }
            if(check_for_term(sptr) == TERMINATE)
                token = DONE;
            if(opts->verbose)
            {           
                fprintf(logfd,"done\n");
                fflush(logfd);    
            }
        }
    }
    int j = 0;
    for(j = 0; j < sptr->size; j++)
     for(i = 0; i < sptr->size; i++)
        if(i != sptr->rank)
        {
            sptr->request_flag[i] = 0;
            if(MPI_Test(&sptr->request_request[i], &sptr->request_flag[i], &sptr->request_status[i]) != MPI_SUCCESS)
                exit(1);
            if(sptr->request_flag[i])
            {
                send_no_work(i,sptr);
                MPI_Start(&sptr->request_request[i]);
            }
        }
    printf("[Rank %d] Stats: %d\n",rank,qp->num_stats);
    fclose(logfd);
    return 0;
}
