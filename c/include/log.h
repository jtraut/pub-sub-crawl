#ifndef LOG_H
#define LOG_H

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// Thread-safe stand-ins for printf/fprintf: a single process-wide mutex
// serializes the format + write, so concurrent callers can't interleave
// mid-line and garble each other's output. Drop-in replacements, same
// printf-style format strings.
void log_printf(const char *fmt, ...);
void log_fprintf(FILE *stream, const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif // LOG_H
