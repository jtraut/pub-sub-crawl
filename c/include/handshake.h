#ifndef HANDSHAKE_H
#define HANDSHAKE_H

#include "msg_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

// The server's hardcoded, ascending list of protocol versions this build
// supports (DESIGN.md section 3). Adding a new version later (phase 5) is
// the one place that decision lives.
extern const int HANDSHAKE_SERVER_VERSIONS[];
extern const size_t HANDSHAKE_SERVER_VERSION_COUNT;

// Per-connection state (DESIGN.md section 3): the client fd, the version
// negotiated with this specific client, and the subscriber queues its
// transport thread drains. negotiated_version is 0 until handshake_perform
// succeeds, so a 0 reliably means "not yet negotiated."
typedef struct {
    int client_fd;
    int negotiated_version;
    msg_queue_t *telemetry_sub;
    msg_queue_t *alerts_sub;
} client_conn_t;

// Runs the plain-text HELLO/HELLO_ACK/HELLO_NACK exchange on
// conn->client_fd (DESIGN.md section 3):
//   1. Server sends "HELLO SERVER_VERSIONS=<comma-separated list>\n".
//   2. Client sends "HELLO CLIENT_VERSIONS=<comma-separated list>\n".
//   3. Server picks the highest version present in both lists.
//      - On overlap: sends "HELLO_ACK VERSION=<n>\n", sets
//        conn->negotiated_version to <n>, and returns true.
//      - On no overlap, or any I/O/parse failure: sends
//        "HELLO_NACK REASON=no_common_version\n" on a best-effort basis
//        and returns false. conn->negotiated_version is left at 0.
// Either way, the caller owns conn->client_fd and is responsible for
// closing it -- this function never closes the socket itself.
bool handshake_perform(client_conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif // HANDSHAKE_H
