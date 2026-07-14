#include "transport.h"
#include "worker_pool.h"
#include "radio_msg.h"
#include "handshake.h"
#include "command.h"
#include "shutdown.h"
#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>

// How often accept()'s poll() wakes up to recheck shutdown_requested()
// when no client is connecting -- not a protocol detail, just how
// promptly the transport thread notices a shutdown versus how much it
// wakes up for nothing while idle.
#define TRANSPORT_ACCEPT_POLL_TIMEOUT_MS 200

struct transport {
    pthread_t thread;
    int listen_fd;
    char socket_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
    pubsub_context_t *pubsub_ctx;
    msg_queue_t *ingest_queue;
    radio_control_t *control;
};

static int format_telemetry(char *buf, size_t buf_size, const telemetry_msg_t *t) {
    return snprintf(buf, buf_size,
        "{\"schema_version\": 1, \"type\": \"telemetry\", \"seq\": %u, \"ts\": %lld, "
        "\"battery_pct\": %u, \"rssi_dbm\": %d}\n",
        t->seq, (long long)t->ts, t->battery_pct, t->rssi_dbm);
}

static int format_alert(char *buf, size_t buf_size, const alert_msg_t *a) {
    return snprintf(buf, buf_size,
        "{\"schema_version\": 1, \"type\": \"alert\", \"seq\": %u, \"ts\": %lld, "
        "\"code\": \"%s\", \"antenna\": %u}\n",
        a->seq, (long long)a->ts, a->code, a->antenna);
}

// Writes exactly len bytes to fd, retrying on partial writes and EINTR.
// Returns false if the client's gone (write error/EOF), true otherwise.
static bool write_all(int fd, const char *buf, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, buf + written, len - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        written += (size_t)n;
    }
    return true;
}

// Drains telemetry_sub/alerts_sub and writes each message to client_fd as
// one JSON line, until the client disconnects, *peer_alive is cleared by
// the sibling command-listener thread, or shutdown is requested. Two
// separate subscriber queues (matching client_conn_t in DESIGN.md section
// 3) means this can't do a single blocking dequeue, so it polls both with
// try_dequeue and sleeps briefly between empty passes rather than
// busy-spinning -- the same poll cadence doubles as this loop's chance to
// notice shutdown/peer-gone.
static void serve_client(int client_fd, msg_queue_t *telemetry_sub, msg_queue_t *alerts_sub,
                          volatile sig_atomic_t *peer_alive) {
    char line[256];
    bool client_alive = true;

    while (client_alive && *peer_alive && !shutdown_requested()) {
        msg_item_t item;
        int n = -1;

        if (msg_queue_try_dequeue(telemetry_sub, &item)) {
            n = format_telemetry(line, sizeof(line), (const telemetry_msg_t *)item.payload);
        } else if (msg_queue_try_dequeue(alerts_sub, &item)) {
            n = format_alert(line, sizeof(line), (const alert_msg_t *)item.payload);
        } else {
            struct timespec delay = { .tv_sec = 0, .tv_nsec = 10 * 1000 * 1000 }; // 10ms
            nanosleep(&delay, NULL);
            continue;
        }

        if (n > 0 && (size_t)n < sizeof(line)) {
            client_alive = write_all(client_fd, line, (size_t)n);
        }
        free(item.payload);
    }
}

static void *transport_routine(void *arg) {
    transport_t *transport = (transport_t *)arg;

    while (!shutdown_requested()) {
        // poll() with a timeout instead of blocking in accept() directly,
        // so this loop can notice shutdown_requested() even when nobody's
        // connecting -- accept() itself has no way to be told "stop
        // waiting."
        struct pollfd listen_pfd = { .fd = transport->listen_fd, .events = POLLIN };
        int ready = poll(&listen_pfd, 1, TRANSPORT_ACCEPT_POLL_TIMEOUT_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            log_fprintf(stderr, "Transport: poll() failed: %s\n", strerror(errno));
            break;
        }
        if (ready == 0) {
            continue; // timed out, recheck shutdown_requested()
        }

        log_printf("Transport: waiting for a client to connect on %s\n", transport->socket_path);
        int client_fd = accept(transport->listen_fd, NULL, NULL);
        if (client_fd < 0) {
            log_fprintf(stderr, "Transport: accept() failed: %s\n", strerror(errno));
            continue;
        }
        log_printf("Transport: client connected\n");

        client_conn_t conn = {
            .client_fd = client_fd,
            .negotiated_version = 0,
            .telemetry_sub = NULL,
            .alerts_sub = NULL,
        };

        if (!handshake_perform(&conn)) {
            log_fprintf(stderr, "Transport: handshake failed, closing connection\n");
            close(client_fd);
            continue;
        }

        conn.telemetry_sub = msg_queue_create(0);
        conn.alerts_sub = msg_queue_create(0);
        // Tracked separately so a failure on one subscribe (e.g. topic
        // registry full) can't leave the other queue subscribed while we
        // go on to destroy it -- pubsub would be left holding a dangling
        // pointer to freed memory.
        bool subscribed_telemetry = conn.telemetry_sub &&
            pubsub_subscribe(transport->pubsub_ctx, TOPIC_TELEMETRY, conn.telemetry_sub) == 0;
        bool subscribed_alerts = conn.alerts_sub &&
            pubsub_subscribe(transport->pubsub_ctx, TOPIC_ALERTS, conn.alerts_sub) == 0;

        if (subscribed_telemetry && subscribed_alerts) {
            // peer_alive is shared with the command-listener thread below:
            // either side clears it the moment it detects the client is
            // gone, so the other one's poll loop notices within one
            // timeout instead of finding out only when it next tries to
            // touch the socket itself.
            volatile sig_atomic_t peer_alive = 1;

            command_listener_args_t *cmd_args = malloc(sizeof(*cmd_args));
            pthread_t command_thread;
            bool command_thread_started = false;
            if (cmd_args) {
                cmd_args->client_fd = conn.client_fd;
                cmd_args->ingest_queue = transport->ingest_queue;
                cmd_args->control = transport->control;
                cmd_args->peer_alive = &peer_alive;
                command_thread_started =
                    pthread_create(&command_thread, NULL, command_listener_routine, cmd_args) == 0;
                if (!command_thread_started) {
                    log_fprintf(stderr, "Transport: failed to start command listener\n");
                    free(cmd_args);
                }
            } else {
                log_fprintf(stderr, "Transport: failed to allocate command listener args\n");
            }

            serve_client(conn.client_fd, conn.telemetry_sub, conn.alerts_sub, &peer_alive);

            // Either the client's gone or we're shutting down -- tell the
            // command listener (if it's not already the one that noticed
            // first) so it stops polling this fd and pthread_join below
            // doesn't wait on a thread that has no reason to exit yet.
            peer_alive = 0;
            if (command_thread_started) {
                pthread_join(command_thread, NULL);
            }
        } else {
            log_fprintf(stderr, "Transport: failed to subscribe client to topics\n");
        }

        if (subscribed_telemetry) {
            pubsub_unsubscribe(transport->pubsub_ctx, TOPIC_TELEMETRY, conn.telemetry_sub);
        }
        if (subscribed_alerts) {
            pubsub_unsubscribe(transport->pubsub_ctx, TOPIC_ALERTS, conn.alerts_sub);
        }
        msg_queue_destroy(conn.telemetry_sub);
        msg_queue_destroy(conn.alerts_sub);

        close(conn.client_fd);
        log_printf("Transport: client disconnected\n");
    }

    return NULL;
}

transport_t *transport_create(const char *socket_path, pubsub_context_t *pubsub_ctx,
                               msg_queue_t *ingest_queue, radio_control_t *control) {
    if (!socket_path || !pubsub_ctx || !ingest_queue || !control) {
        return NULL;
    }

    transport_t *transport = malloc(sizeof(transport_t));
    if (!transport) {
        return NULL;
    }
    if (strlen(socket_path) >= sizeof(transport->socket_path)) {
        log_fprintf(stderr, "Transport: socket path too long: %s\n", socket_path);
        free(transport);
        return NULL;
    }
    strncpy(transport->socket_path, socket_path, sizeof(transport->socket_path) - 1);
    transport->socket_path[sizeof(transport->socket_path) - 1] = '\0';
    transport->pubsub_ctx = pubsub_ctx;
    transport->ingest_queue = ingest_queue;
    transport->control = control;

    transport->listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (transport->listen_fd < 0) {
        log_fprintf(stderr, "Transport: socket() failed: %s\n", strerror(errno));
        free(transport);
        return NULL;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, transport->socket_path, sizeof(addr.sun_path) - 1);

    // Remove a stale socket file left behind by a previous run, if any --
    // bind() fails with EADDRINUSE otherwise.
    unlink(socket_path);

    if (bind(transport->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        log_fprintf(stderr, "Transport: bind() failed on %s: %s\n", socket_path, strerror(errno));
        close(transport->listen_fd);
        free(transport);
        return NULL;
    }

    if (listen(transport->listen_fd, 4) < 0) {
        log_fprintf(stderr, "Transport: listen() failed: %s\n", strerror(errno));
        close(transport->listen_fd);
        unlink(socket_path);
        free(transport);
        return NULL;
    }

    if (pthread_create(&transport->thread, NULL, transport_routine, transport) != 0) {
        log_fprintf(stderr, "Transport: failed to create transport thread\n");
        close(transport->listen_fd);
        unlink(socket_path);
        free(transport);
        return NULL;
    }

    log_printf("Transport: listening on %s\n", socket_path);
    return transport;
}

void transport_destroy(transport_t *transport) {
    if (!transport) {
        return;
    }

    pthread_join(transport->thread, NULL);
    close(transport->listen_fd);
    unlink(transport->socket_path);
    free(transport);
}
