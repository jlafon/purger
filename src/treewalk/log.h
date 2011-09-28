#ifndef LOG_H
#define LOG_H

#include <stdio.h>

#ifndef TREEWALK_log_level
    #define TREEWALK_log_level 5
#endif
#define LOG_FATAL (1)
#define LOG_ERR   (2)
#define LOG_WARN  (3)
#define LOG_INFO  (4)
#define LOG_DBG   (5)

#define LOG(level, ...) do {  \
        if (level <= TREEWALK_debug_level) { \
            fprintf(TREEWALK_debug_stream,"%d:%s:%d:",CIRCLE_global_rank, __FILE__, __LINE__); \
            fprintf(TREEWALK_debug_stream, __VA_ARGS__); \
            fprintf(TREEWALK_debug_stream, "\n"); \
            fflush(TREEWALK_debug_stream); \
        } \
    } while (0)

extern FILE *TREEWALK_debug_stream;
extern int TREEWALK_debug_level;
extern int CIRCLE_global_rank;
#endif /* LOG_H */
