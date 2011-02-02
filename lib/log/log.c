#include <stdarg.h>
#include <stdio.h>

void
purger_log(const char *id, const char *format, ...)
{
  va_list ap;
    
  va_start(ap, format);
    
  /*should we be going to a log file here? */
  printf("%s: ", id);
  vprintf(format, ap);
  printf("\n");
  fflush(stdout);
    
  va_end(ap);
}

void
purger_elog(const char *id, const char *file, int line, const char *format, ...)
{
  va_list ap;
    
  va_start(ap, format);
    
  /*should we be going to a log file here? */
  printf("ERROR: %s: %s:%d: ", id, file, line);
  vprintf(format, ap);
  printf("\n");
  fflush(stdout);
    
  va_end(ap);
}
