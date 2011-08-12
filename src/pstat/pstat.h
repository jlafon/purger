#ifndef PSTAT_H
#define PSTAT_H
#include <getopt.h>
#include <string.h>
#include <stdio.h>
#include <mpi.h>
#define MAX_STRING_LEN 2048*sizeof(char)
#define INITIAL_QUEUE_SIZE 400000

enum tags { WHITE=10,BLACK=1,DONE=2,TERMINATE=3,WORK_REQUEST=4, WORK=0xBEEF, TOKEN=0 };

FILE * logfd;
typedef struct options
{
    char * beginning_path;
    int verbose;
} options;

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

typedef struct state_st
{
    int verbose;
    int rank;
    int size;
    int have_token;
    int token;
    int next_processor;
    int token_partner;
    MPI_Status term_status;
    MPI_Status work_status;
    MPI_Status request_status;
    MPI_Request term_request;
    MPI_Request work_request;
    MPI_Request request_request;
    int term_flag;
    int work_flag;
    int request_flag;
    int work_pending_request;
    int request_pending_receive;
    int term_pending_receive;
    int incoming_token;
    int * work_offsets;
    int * request_offsets;
    int work_request_tries;
} state_st;

int parse_args( int argc, char *argv[] , options * opts );

/* Worker function, executed by all ranks */
int worker( options * opts );

/* Pushes a new string onto the queue */
int pushq( work_queue * qp, char * str );

/* Pops a string from the queue */
int popq( work_queue * qp, char * str );

int check_for_requests( work_queue * qp , state_st * st);

int request_work( work_queue * qp, state_st * st);

int process_work( work_queue * qp, state_st * state );

int check_for_term( state_st * st );
#endif
