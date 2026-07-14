#include "handshake.h"
#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

const int HANDSHAKE_SERVER_VERSIONS[] = {1};
const size_t HANDSHAKE_SERVER_VERSION_COUNT =
    sizeof(HANDSHAKE_SERVER_VERSIONS) / sizeof(HANDSHAKE_SERVER_VERSIONS[0]);

// Generous for "HELLO CLIENT_VERSIONS=1,2,3,...\n" -- the handshake is a
// handful of short control lines, not a place that needs to scale.
#define HANDSHAKE_LINE_MAX 128

// Writes exactly len bytes, retrying on EINTR/partial writes. Duplicated
// from transport.c's write_all rather than shared, so handshake.c has no
// compile-time dependency on transport internals.
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

// Reads one '\n'-terminated line into buf (newline stripped, NUL-terminated).
// One byte at a time is fine here -- this runs once per connection, not in
// a hot path. Returns false on EOF/error before a newline, or if the line
// doesn't fit in buf_size.
static bool read_line(int fd, char *buf, size_t buf_size) {
    size_t len = 0;
    while (len + 1 < buf_size) {
        char c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        if (c == '\n') {
            buf[len] = '\0';
            return true;
        }
        buf[len++] = c;
    }
    return false;
}

// Builds and sends "HELLO SERVER_VERSIONS=1,2,...\n" from the hardcoded
// server version list.
static bool send_hello(int client_fd) {
    char line[HANDSHAKE_LINE_MAX];
    int n = snprintf(line, sizeof(line), "HELLO SERVER_VERSIONS=");
    for (size_t i = 0; n > 0 && (size_t)n < sizeof(line) && i < HANDSHAKE_SERVER_VERSION_COUNT; i++) {
        n += snprintf(line + n, sizeof(line) - (size_t)n, "%s%d",
                       i == 0 ? "" : ",", HANDSHAKE_SERVER_VERSIONS[i]);
    }
    if (n <= 0 || (size_t)n >= sizeof(line) - 1) {
        return false;
    }
    line[n++] = '\n';
    return write_all(client_fd, line, (size_t)n);
}

// Parses "HELLO CLIENT_VERSIONS=1,2,3" into out[]. Returns the count
// parsed, or 0 if the line doesn't match the expected shape at all.
static size_t parse_client_versions(const char *line, int *out, size_t out_capacity) {
    static const char prefix[] = "HELLO CLIENT_VERSIONS=";
    size_t prefix_len = sizeof(prefix) - 1;
    if (strncmp(line, prefix, prefix_len) != 0) {
        return 0;
    }

    const char *p = line + prefix_len;
    if (*p == '\0') {
        return 0; // empty list
    }

    size_t count = 0;
    while (*p != '\0') {
        if (count >= out_capacity) {
            return 0; // more versions than we're willing to parse
        }
        char *end;
        long v = strtol(p, &end, 10);
        if (end == p) {
            return 0; // not a number where one was expected
        }
        out[count++] = (int)v;
        p = end;
        if (*p == ',') {
            p++;
        } else if (*p != '\0') {
            return 0; // trailing garbage after a number
        }
    }
    return count;
}

// Highest version present in both the server's hardcoded list and the
// client's advertised list, or 0 if there's no overlap.
static int highest_common_version(const int *client_versions, size_t client_count) {
    int best = 0;
    for (size_t i = 0; i < HANDSHAKE_SERVER_VERSION_COUNT; i++) {
        int server_version = HANDSHAKE_SERVER_VERSIONS[i];
        for (size_t j = 0; j < client_count; j++) {
            if (client_versions[j] == server_version && server_version > best) {
                best = server_version;
            }
        }
    }
    return best;
}

static void send_nack(int client_fd) {
    static const char nack[] = "HELLO_NACK REASON=no_common_version\n";
    // Best-effort: if the client's already gone, there's nothing more to do.
    write_all(client_fd, nack, sizeof(nack) - 1);
}

bool handshake_perform(client_conn_t *conn) {
    conn->negotiated_version = 0;

    if (!send_hello(conn->client_fd)) {
        log_fprintf(stderr, "Handshake: failed to send HELLO\n");
        return false;
    }

    char line[HANDSHAKE_LINE_MAX];
    if (!read_line(conn->client_fd, line, sizeof(line))) {
        log_fprintf(stderr, "Handshake: failed to read CLIENT_VERSIONS\n");
        return false;
    }

    int client_versions[HANDSHAKE_LINE_MAX];
    size_t client_count = parse_client_versions(line, client_versions,
        sizeof(client_versions) / sizeof(client_versions[0]));
    if (client_count == 0) {
        log_fprintf(stderr, "Handshake: malformed HELLO line: \"%s\"\n", line);
        send_nack(conn->client_fd);
        return false;
    }

    int negotiated = highest_common_version(client_versions, client_count);
    if (negotiated == 0) {
        log_printf("Handshake: no common version with client\n");
        send_nack(conn->client_fd);
        return false;
    }

    char ack[HANDSHAKE_LINE_MAX];
    int n = snprintf(ack, sizeof(ack), "HELLO_ACK VERSION=%d\n", negotiated);
    if (n <= 0 || (size_t)n >= sizeof(ack) || !write_all(conn->client_fd, ack, (size_t)n)) {
        log_fprintf(stderr, "Handshake: failed to send HELLO_ACK\n");
        return false;
    }

    conn->negotiated_version = negotiated;
    log_printf("Handshake: negotiated version %d\n", negotiated);
    return true;
}
