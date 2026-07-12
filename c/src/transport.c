#include "transport.h"
#include "worker_pool.h"
#include "radio_msg.h"
#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>

struct transport {
    pthread_t thread;
    int listen_fd;
    char socket_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
    pubsub_context_t *pubsub_ctx;
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
// one JSON line, until the client disconnects. Two separate subscriber
// queues (matching client_conn_t in DESIGN.md section 3) means this can't
// do a single blocking dequeue, so it polls both with try_dequeue and
// sleeps briefly between empty passes rather than busy-spinning.
static void serve_client(int client_fd, msg_queue_t *telemetry_sub, msg_queue_t *alerts_sub) {
    char line[256];
    bool client_alive = true;

    while (client_alive) {
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

    while (1) {
        log_printf("Transport: waiting for a client to connect on %s\n", transport->socket_path);
        int client_fd = accept(transport->listen_fd, NULL, NULL);
        if (client_fd < 0) {
            log_fprintf(stderr, "Transport: accept() failed: %s\n", strerror(errno));
            continue;
        }
        log_printf("Transport: client connected\n");

        msg_queue_t *telemetry_sub = msg_queue_create(0);
        msg_queue_t *alerts_sub = msg_queue_create(0);
        // Tracked separately so a failure on one subscribe (e.g. topic
        // registry full) can't leave the other queue subscribed while we
        // go on to destroy it -- pubsub would be left holding a dangling
        // pointer to freed memory.
        bool subscribed_telemetry = telemetry_sub &&
            pubsub_subscribe(transport->pubsub_ctx, TOPIC_TELEMETRY, telemetry_sub) == 0;
        bool subscribed_alerts = alerts_sub &&
            pubsub_subscribe(transport->pubsub_ctx, TOPIC_ALERTS, alerts_sub) == 0;

        if (subscribed_telemetry && subscribed_alerts) {
            serve_client(client_fd, telemetry_sub, alerts_sub);
        } else {
            log_fprintf(stderr, "Transport: failed to subscribe client to topics\n");
        }

        if (subscribed_telemetry) {
            pubsub_unsubscribe(transport->pubsub_ctx, TOPIC_TELEMETRY, telemetry_sub);
        }
        if (subscribed_alerts) {
            pubsub_unsubscribe(transport->pubsub_ctx, TOPIC_ALERTS, alerts_sub);
        }
        msg_queue_destroy(telemetry_sub);
        msg_queue_destroy(alerts_sub);

        close(client_fd);
        log_printf("Transport: client disconnected\n");
    }

    return NULL;
}

transport_t *transport_create(const char *socket_path, pubsub_context_t *pubsub_ctx) {
    if (!socket_path || !pubsub_ctx) {
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
