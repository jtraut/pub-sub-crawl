#include "log.h"
#include <pthread.h>
#include <stdarg.h>

static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void log_printf(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    pthread_mutex_lock(&log_mutex);
    vprintf(fmt, args);
    fflush(stdout);
    pthread_mutex_unlock(&log_mutex);

    va_end(args);
}

void log_fprintf(FILE *stream, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    pthread_mutex_lock(&log_mutex);
    vfprintf(stream, fmt, args);
    fflush(stream);
    pthread_mutex_unlock(&log_mutex);

    va_end(args);
}
