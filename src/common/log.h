#ifndef LOG_H
#define LOG_H

#include <stdio.h>

typedef enum
{
    PURGER_LOG_FATAL = 1,
    PURGER_LOG_ERR   = 2,
    PURGER_LOG_WARN  = 3,
    PURGER_LOG_INFO  = 4,
    PURGER_LOG_DBG   = 5
} PURGER_loglevel;

#define LOG(level, ...) do {  \
        if (level <= PURGER_debug_level) { \
            fprintf(PURGER_debug_stream,"%d:%s:%d:", PURGER_global_rank, __FILE__, __LINE__); \
            fprintf(PURGER_debug_stream, __VA_ARGS__); \
            fprintf(PURGER_debug_stream, "\n"); \
	    fflush(PURGER_debug_stream); \
        } \
    } while (0)

extern int PURGER_global_rank;
extern FILE *PURGER_debug_stream;
extern PURGER_loglevel PURGER_debug_level;

#endif /* LOG_H */
