#ifndef SHUTDOWN_H
#define SHUTDOWN_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// A single process-wide flag for the handful of blocking calls that
// can't be woken by a condition-variable broadcast: the producer's
// between-tick sleep, transport's poll()-gated accept loop, and the
// command listener's poll()-gated reads. Anything blocked in a
// msg_queue_t call instead gets a real broadcast wakeup from
// msg_queue_shutdown() -- this flag is only for waiters a queue can't
// reach.
void shutdown_request(void);
bool shutdown_requested(void);

#ifdef __cplusplus
}
#endif

#endif // SHUTDOWN_H
