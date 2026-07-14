#ifndef COMMAND_H
#define COMMAND_H

#include "msg_queue.h"
#include "radio_control.h"

#include <signal.h>

#ifdef __cplusplus
extern "C" {
#endif

// Heap-allocated by the caller (transport.c, one per accepted connection)
// and freed by command_listener_routine itself before it returns, since
// it's handed off across the pthread_create boundary.
typedef struct {
    int client_fd;
    msg_queue_t *ingest_queue;
    radio_control_t *control;
    // Shared with the sibling thread serving the same connection
    // (transport.c's serve_client): either side clears this the moment it
    // detects the peer is gone, so the other one's poll loop notices
    // within one timeout instead of blocking on a socket the peer already
    // walked away from.
    volatile sig_atomic_t *peer_alive;
} command_listener_args_t;

// Reads newline-terminated text commands from arg->client_fd (DESIGN.md
// sections 2/3):
//   SET_INTERVAL <ms>
//   TRIGGER_ALERT <code> <antenna>
// and applies them against arg->control / arg->ingest_queue, until the
// client disconnects, *arg->peer_alive is cleared, or shutdown_requested()
// becomes true. Matches the pthread start-routine signature so it can be
// passed directly to pthread_create; frees arg before returning.
void *command_listener_routine(void *arg);

#ifdef __cplusplus
}
#endif

#endif // COMMAND_H
