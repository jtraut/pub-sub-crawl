#include "command.h"
#include "radio_msg.h"
#include "shutdown.h"
#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <poll.h>

#define COMMAND_LINE_MAX 128
// How often a stalled read re-checks shutdown_requested()/*peer_alive --
// not a protocol detail, just how promptly this thread notices it should
// stop versus how long client_fd sits idle with no command coming in.
#define COMMAND_POLL_TIMEOUT_MS 200

// Reads one '\n'-terminated line into buf (newline stripped, NUL
// terminated). poll()-gates each read instead of blocking in read()
// outright, so a stalled connection (no command coming) still notices
// shutdown/peer-gone within COMMAND_POLL_TIMEOUT_MS instead of blocking
// forever. Returns false on EOF/error/shutdown/peer-gone before a
// complete line, or if the line doesn't fit in buf_size.
static bool read_command_line(int fd, volatile sig_atomic_t *peer_alive, char *buf, size_t buf_size) {
    size_t len = 0;
    while (len + 1 < buf_size) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int ready = poll(&pfd, 1, COMMAND_POLL_TIMEOUT_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (ready == 0) {
            if (shutdown_requested() || !*peer_alive) {
                return false;
            }
            continue;
        }
        if (pfd.revents & (POLLHUP | POLLERR)) {
            return false;
        }

        char c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false; // client closed its side
        }
        if (c == '\n') {
            buf[len] = '\0';
            return true;
        }
        buf[len++] = c;
    }
    return false; // line too long
}

static void apply_set_interval(radio_control_t *control, const char *args) {
    char *end;
    long ms = strtol(args, &end, 10);
    if (end == args || ms <= 0) {
        log_fprintf(stderr, "Command: malformed SET_INTERVAL argument: \"%s\"\n", args);
        return;
    }
    radio_control_set_interval_ms(control, (uint32_t)ms);
    log_printf("Command: interval set to %ldms\n", ms);
}

static void apply_trigger_alert(msg_queue_t *ingest_queue, const char *args) {
    char code[ALERT_CODE_MAX] = {0};
    unsigned int antenna = 0;
    if (sscanf(args, "%31s %u", code, &antenna) != 2) {
        log_fprintf(stderr, "Command: malformed TRIGGER_ALERT arguments: \"%s\"\n", args);
        return;
    }

    // Heap-allocated for the same reason as the producer's periodic
    // alerts (radio_msg.h/main.c): a worker thread reads it later, after
    // this stack frame is gone. The worker frees it once published.
    alert_raw_t *raw = malloc(sizeof(*raw));
    if (!raw) {
        log_fprintf(stderr, "Command: failed to allocate triggered alert\n");
        return;
    }
    strncpy(raw->code, code, ALERT_CODE_MAX - 1);
    raw->code[ALERT_CODE_MAX - 1] = '\0';
    raw->antenna = antenna;

    msg_item_t item = { .id = 0, .type = MSG_TYPE_ALERT, .payload = raw, .length = sizeof(*raw) };
    // Non-blocking: a command-triggered alert competes with the producer
    // for ingest_queue space, and this thread has no business stalling on
    // a full queue the way a slow subscriber already doesn't (DESIGN.md
    // section 2's drop-under-backpressure precedent).
    if (!msg_queue_try_enqueue(ingest_queue, &item)) {
        log_fprintf(stderr, "Command: ingest queue full, dropping triggered alert\n");
        free(raw);
        return;
    }
    log_printf("Command: triggered alert code=%s antenna=%u\n", raw->code, antenna);
}

static void apply_command(const char *line, msg_queue_t *ingest_queue, radio_control_t *control) {
    const char *space = strchr(line, ' ');
    if (!space) {
        log_fprintf(stderr, "Command: malformed command line: \"%s\"\n", line);
        return;
    }

    size_t cmd_len = (size_t)(space - line);
    const char *args = space + 1;

    if (cmd_len == strlen("SET_INTERVAL") && strncmp(line, "SET_INTERVAL", cmd_len) == 0) {
        apply_set_interval(control, args);
    } else if (cmd_len == strlen("TRIGGER_ALERT") && strncmp(line, "TRIGGER_ALERT", cmd_len) == 0) {
        apply_trigger_alert(ingest_queue, args);
    } else {
        log_fprintf(stderr, "Command: unknown command: \"%.*s\"\n", (int)cmd_len, line);
    }
}

void *command_listener_routine(void *arg) {
    command_listener_args_t *args = (command_listener_args_t *)arg;

    char line[COMMAND_LINE_MAX];
    while (read_command_line(args->client_fd, args->peer_alive, line, sizeof(line))) {
        apply_command(line, args->ingest_queue, args->control);
    }

    // The client's gone (or shutdown was requested) from this side too --
    // let the sibling transport thread's poll loop notice without waiting
    // on it to write and fail first.
    *args->peer_alive = 0;
    free(args);
    return NULL;
}
