#ifndef PSTAT_H
#define PSTAT_H
#include <getopt.h>
#include <string.h>
#include <stdio.h>
#include <mpi.h>
//#include "hiredis.h"
//#include "async.h"
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
    MPI_Status work_offsets_status;
    MPI_Status work_status;
    MPI_Status * request_status;
    MPI_Request term_request;
    MPI_Request work_offsets_request;
    MPI_Request work_request;
    MPI_Request * request_request;
    int term_flag;
    int work_flag;
    int * request_flag;
    int work_pending_request;
    int request_pending_receive;
    int term_pending_receive;
    int incoming_token;
    unsigned int * work_offsets;
    unsigned int * request_offsets;
    int * request_recv_buf;
    int work_request_tries;
} state_st;

//redisAsyncContext *redis_context;
void send_work_to_many( work_queue * qp, state_st * st, int * requestors, int rcount);
void send_no_work( int dest, state_st * st );
int send_work( work_queue * qp, state_st * st, int dest, int count );
void print_offsets(unsigned int * offsets, int count);
void dumpq( work_queue * qp);
void printq( work_queue * qp );

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
