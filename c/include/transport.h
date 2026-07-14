#ifndef TRANSPORT_H
#define TRANSPORT_H

#include "pubsub.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TRANSPORT_DEFAULT_SOCKET_PATH "/tmp/radio_sim.sock"

typedef struct transport transport_t;

// Spawns one transport thread that binds/listens on an AF_UNIX stream
// socket at socket_path and serves one client connection at a time: on
// accept, it runs the HELLO/HELLO_ACK/HELLO_NACK handshake
// (handshake_perform, DESIGN.md section 3); on success it subscribes a
// fresh pair of queues to the telemetry/alerts pubsub topics on pubsub_ctx,
// then drains and writes each message out as one newline-delimited JSON
// object per line (v1 schema) until the client disconnects, at which point
// it unsubscribes and goes back to accept() for the next client. A failed
// handshake (no common version, or malformed/absent CLIENT_VERSIONS) closes
// the connection without ever subscribing it.
//
// The serializer doesn't yet branch on the negotiated version -- only v1
// exists so far (that's phase 5). pubsub_ctx is caller-owned and must
// outlive the transport. Returns NULL on socket setup failure or if the
// thread couldn't be started.
transport_t *transport_create(const char *socket_path, pubsub_context_t *pubsub_ctx);

// Joins the transport thread and frees the transport. The thread currently
// loops forever (accept -> serve -> accept...), so this blocks
// indefinitely until there's a SIGINT-driven shutdown path (not built yet).
void transport_destroy(transport_t *transport);

#ifdef __cplusplus
}
#endif

#endif // TRANSPORT_H
