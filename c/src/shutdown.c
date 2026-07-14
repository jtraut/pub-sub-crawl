#include "shutdown.h"
#include <signal.h>

// main() blocks SIGINT process-wide before spawning any thread and waits
// for it synchronously via sigwait(), so this is only ever written by
// main's own thread -- sig_atomic_t/volatile is kept anyway since it's
// read from other threads without a lock (a single word-sized flag that
// only ever goes 0 -> 1 doesn't need one).
static volatile sig_atomic_t g_shutdown_requested = 0;

void shutdown_request(void) {
    g_shutdown_requested = 1;
}

bool shutdown_requested(void) {
    return g_shutdown_requested != 0;
}
