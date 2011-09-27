#ifndef LOG_H
#define LOG_H

#include <stdio.h>

#ifndef PURGER_LOGLEVEL
    #define PURGER_LOGLEVEL 0
#endif

#define LOG_FATAL (1)
#define LOG_ERR   (2)
#define LOG_WARN  (3)
#define LOG_INFO  (4)
#define LOG_DBG   (5)

#define LOG(level, ...) do {  \
        if (level <= PURGER_debug_level) { \
            fprintf(PURGER_dbgstream,"%d:%s:%d:", PURGER_global_rank, __FILE__, __LINE__); \
            fprintf(PURGER_dbgstream, __VA_ARGS__); \
            fprintf(PURGER_dbgstream, "\n"); \
            fflush(PURGER_dbgstream); \
        } \
    } while (0)

extern FILE *PURGER_dbgstream;
extern int  PURGER_debug_level;

#endif /* LOG_H */
