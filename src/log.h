#ifndef PURGER_LOG_H_
#define PURGER_LOG_H_

#define PURGER_LOG(id, format, ...) \
  (purger_log(id, format, __VA_ARGS__))

#define PURGER_ELOG(id, format, ...)				\
  (purger_elog(id, __FILE__, __LINE__, format, __VA_ARGS__))

void
purger_log(const char *id, const char *format, ...);

void
purger_elog(const char *id, const char *file, int line, const char *format, ...);

#endif /* PURGER_LOG_H_ */
