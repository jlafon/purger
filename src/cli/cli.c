//! \file cli.c
/* \author Jharrod LaFon
 * \date July 2011
 * A command line interface to Purger
 */

#include<stdio.h>
#include<unistd.h>
#include<getopt.h>
#include<stdlib.h>
#include<string.h>
#include<readline/readline.h>
#include<readline/history.h>

typedef struct 
{
    char *name;
    rl_icpfunc_t *func;
    char *doc;
} command;

void cmd_help()
{
    fprintf(stderr,"exit\n");
}

command commands[] = {
    { "help", cmd_help, "Show Help"}
};

int main(int argc, char ** argv)
{
    char * command_buf;
    static struct option long_options[] = 
    {
        {"help", no_argument, 0, 'h'}
    };
    int c = 0;
    int index = 0;
    while((c = getopt_long(argc,argv,"d:p:r:h", long_options, &index)) != -1)
    {

    }

    do
    {
        command_buf = readline("[purger] ");
        if(command_buf && *command_buf)
            add_history(command_buf);
        if(strncmp(command_buf,"exit",4) == 0)
            exit(0);
    }
    while(1);
    
    fprintf(stdout,"\n");
    return 0;
}
