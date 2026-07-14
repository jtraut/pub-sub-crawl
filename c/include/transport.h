#ifndef TRANSPORT_H
#define TRANSPORT_H

#include "pubsub.h"
#include "radio_control.h"

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
// spawns a second, sibling thread (command_listener_routine) that reads
// SET_INTERVAL/TRIGGER_ALERT text commands back over the same socket and
// applies them against control/ingest_queue, and drains+writes each
// telemetry/alert message out as one newline-delimited JSON object per
// line (v1 schema) until the client disconnects. At that point both
// threads for the connection have stopped, it unsubscribes, and goes back
// to accept() for the next client. A failed handshake (no common version,
// or malformed/absent CLIENT_VERSIONS) closes the connection without ever
// subscribing it or starting a command listener.
//
// The serializer doesn't yet branch on the negotiated version -- only v1
// exists so far (that's phase 5). Accepting a new connection is gated on
// shutdown_requested() (checked between poll() timeouts on the listening
// socket, DESIGN.md section 2's clean-shutdown piece), so transport_destroy
// actually returns once shutdown_request() has been called elsewhere.
//
// pubsub_ctx/ingest_queue/control are all caller-owned and must outlive
// the transport. Returns NULL on socket setup failure or if the thread
// couldn't be started.
transport_t *transport_create(const char *socket_path, pubsub_context_t *pubsub_ctx,
                               msg_queue_t *ingest_queue, radio_control_t *control);

// Joins the transport thread (and, transitively, whichever per-connection
// command-listener thread is still running) and frees the transport.
// Callers should call shutdown_request() before this, or it blocks until
// the thread notices on its own via the poll-timeout loop described above.
void transport_destroy(transport_t *transport);

#ifdef __cplusplus
}
#endif

#endif // TRANSPORT_H
