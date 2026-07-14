#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include "msg_queue.h"
#include "pubsub.h"
#include "worker_pool.h"
#include "radio_msg.h"
#include "transport.h"
#include "radio_control.h"
#include "shutdown.h"
#include "log.h"

static const char *ALERT_CODES[] = { "ANTENNA_UNPLUGGED", "BATTERY_LOW" };
#define ALERT_CODE_COUNT (sizeof(ALERT_CODES) / sizeof(ALERT_CODES[0]))

// How finely the producer chops up its between-tick sleep. Not a protocol
// detail -- just how promptly it notices shutdown_requested() or a fresh
// SET_INTERVAL versus how often it wakes up to check.
#define PRODUCER_SLEEP_SLICE_MS 100u

typedef struct {
    msg_queue_t *ingest_queue;
    radio_control_t *control;
} producer_ctx_t;

typedef struct {
    msg_queue_t *telemetry_queue;
    msg_queue_t *alerts_queue;
} consumer_ctx_t;

// Sleeps up to total_ms, but in slices, rechecking shutdown_requested()
// between each -- a plain sleep()/nanosleep(total_ms) would otherwise sit
// blocked for the whole interval regardless of a shutdown request, since
// (unlike msg_queue_shutdown()) nothing can broadcast a sleeping thread
// awake early.
static void producer_sleep(uint32_t total_ms) {
    uint32_t slept_ms = 0;
    while (slept_ms < total_ms && !shutdown_requested()) {
        uint32_t slice_ms = (total_ms - slept_ms < PRODUCER_SLEEP_SLICE_MS)
            ? (total_ms - slept_ms) : PRODUCER_SLEEP_SLICE_MS;
        struct timespec ts = {
            .tv_sec = slice_ms / 1000,
            .tv_nsec = (long)(slice_ms % 1000) * 1000000L,
        };
        nanosleep(&ts, NULL);
        slept_ms += slice_ms;
    }
}

void* producer_routine(void* arg) {
    // Producer doesn't talk to pubsub directly: it just drops raw,
    // not-yet-sequenced telemetry/alert items on the shared ingest queue,
    // and the worker pool assigns seq/ts and routes them.
    producer_ctx_t *pctx = (producer_ctx_t *)arg;
    msg_queue_t *ingest_queue = pctx->ingest_queue;
    radio_control_t *control = pctx->control;
    uint32_t counter = 0;

    // Produces a telemetry sample every interval_ms (and, every fifth
    // tick, an alert alongside it) until told to stop.
    while (!shutdown_requested()) {
        counter++;

        // Heap-allocated because a worker thread reads it later, after
        // this stack frame has gone around the loop again. The worker
        // frees it once it's published (see worker_pool.c).
        telemetry_raw_t *telemetry = malloc(sizeof(*telemetry));
        if (!telemetry) {
            log_fprintf(stderr, "Failed to allocate telemetry payload\n");
            break;
        }
        // Fake battery drain/recharge cycle and a wandering RSSI.
        telemetry->battery_pct = 100 - (counter % 101);
        telemetry->rssi_dbm = -40 - (int32_t)(counter % 50);

        msg_item_t item = {
            .id = counter,
            .type = MSG_TYPE_TELEMETRY,
            .payload = telemetry,
            .length = sizeof(*telemetry),
        };
        if (!msg_queue_enqueue(ingest_queue, &item)) {
            // Only happens once ingest_queue has been shut down (see
            // msg_queue_shutdown) -- a normal part of clean shutdown, not
            // a real failure.
            free(telemetry);
            break;
        }

        // Simulate an occasional alert condition alongside routine telemetry.
        if (counter % 5 == 0) {
            alert_raw_t *alert = malloc(sizeof(*alert));
            if (!alert) {
                log_fprintf(stderr, "Failed to allocate alert payload\n");
                break;
            }
            const char *code = ALERT_CODES[(counter / 5) % ALERT_CODE_COUNT];
            strncpy(alert->code, code, ALERT_CODE_MAX - 1);
            alert->code[ALERT_CODE_MAX - 1] = '\0';
            alert->antenna = 1 + (counter % 2);

            msg_item_t alert_item = {
                .id = counter,
                .type = MSG_TYPE_ALERT,
                .payload = alert,
                .length = sizeof(*alert),
            };
            if (!msg_queue_try_enqueue(ingest_queue, &alert_item)) {
                free(alert);
                break;
            }
        }

        producer_sleep(radio_control_get_interval_ms(control));
    }
    return NULL;
}

// Define the consumer routine
void* consumer_routine(void* arg) {
    // A second, independent subscriber alongside the transport thread's own
    // telemetry/alerts subscriptions -- demonstrates that pubsub fan-out
    // reaches every subscriber, not just whichever one happens to be a
    // socket client. main() owns subscribing/unsubscribing and the queues
    // themselves (see main()), so it can orchestrate shutdown explicitly
    // instead of this thread privately owning state nothing else can reach.
    consumer_ctx_t *cctx = (consumer_ctx_t *)arg;

    while (1) {
        msg_item_t item;
        if (!msg_queue_dequeue(cctx->telemetry_queue, &item)) {
            // Only false once the queue's been shut down *and* drained --
            // see msg_queue_dequeue's shutdown semantics.
            break;
        }
        telemetry_msg_t *t = (telemetry_msg_t *)item.payload;
        log_printf("telemetry seq=%u battery_pct=%u rssi_dbm=%d\n", t->seq, t->battery_pct, t->rssi_dbm);
        free(item.payload);

        if (!msg_queue_try_dequeue(cctx->alerts_queue, &item)) {
            continue;
        }
        alert_msg_t *a = (alert_msg_t *)item.payload;
        log_printf("alert seq=%u code=%s antenna=%u\n", a->seq, a->code, a->antenna);
        free(item.payload);
    }
    return NULL;
}

int main(void) {
    // A client disconnecting mid-write would otherwise raise SIGPIPE, whose
    // default disposition kills the whole process -- ignore it so write()
    // just returns -1/EPIPE instead, which the transport thread already
    // handles as a normal disconnect.
    signal(SIGPIPE, SIG_IGN);

    // Block SIGINT in this thread *before* spawning any others: every
    // thread created from here on inherits the blocked mask, so Ctrl+C is
    // never delivered to (and can't randomly interrupt) the producer,
    // consumer, worker, or transport threads. It stays pending for the
    // process and is picked up synchronously by main's own sigwait() below
    // -- one dedicated place that decides how to shut down, instead of
    // racing over which thread happens to catch the signal.
    sigset_t sigint_set;
    sigemptyset(&sigint_set);
    sigaddset(&sigint_set, SIGINT);
    if (pthread_sigmask(SIG_BLOCK, &sigint_set, NULL) != 0) {
        log_fprintf(stderr, "Failed to block SIGINT\n");
        return 1;
    }

    pthread_t producer_thread, consumer_thread;

    // Pubsub will manage the queues internally, so we just need a context to pass around
    pubsub_context_t *ctx = pubsub_create();

    // Shared ingest queue: the producer drops raw items here, and the
    // worker pool is what actually routes them into pubsub.
    msg_queue_t *ingest_queue = msg_queue_create(0);

    // Shared knob between the producer (reads it every tick) and a
    // connected client's command-listener thread (writes it via
    // SET_INTERVAL, see command.c).
    radio_control_t *control = radio_control_create(RADIO_CONTROL_DEFAULT_INTERVAL_MS);

    worker_pool_t *workers = worker_pool_create(WORKER_POOL_DEFAULT_SIZE, ingest_queue, ctx);
    if (!workers) {
        log_fprintf(stderr, "Failed to create worker pool\n");
        return 1;
    }

    transport_t *transport = transport_create(TRANSPORT_DEFAULT_SOCKET_PATH, ctx, ingest_queue, control);
    if (!transport) {
        log_fprintf(stderr, "Failed to create transport\n");
        return 1;
    }

    // Consumer's own subscriber queues, created and subscribed here (not
    // inside consumer_routine) so main can unsubscribe/shut them down/
    // destroy them explicitly during teardown, the same way transport.c
    // already owns its own per-connection queues.
    msg_queue_t *consumer_telemetry_q = msg_queue_create(0);
    msg_queue_t *consumer_alerts_q = msg_queue_create(0);
    if (!consumer_telemetry_q || !consumer_alerts_q ||
        pubsub_subscribe(ctx, TOPIC_TELEMETRY, consumer_telemetry_q) != 0 ||
        pubsub_subscribe(ctx, TOPIC_ALERTS, consumer_alerts_q) != 0) {
        log_fprintf(stderr, "Failed to set up consumer subscriptions\n");
        return 1;
    }

    producer_ctx_t producer_ctx = { .ingest_queue = ingest_queue, .control = control };
    // Spawn a producer thread that enqueues raw messages every interval
    if (pthread_create(&producer_thread, NULL, producer_routine, &producer_ctx) != 0) {
        log_fprintf(stderr, "Failed to create producer thread\n");
        return 1;
    }

    consumer_ctx_t consumer_ctx = { .telemetry_queue = consumer_telemetry_q, .alerts_queue = consumer_alerts_q };
    // Spawn a consumer thread that dequeues messages from its own queues and prints them
    if (pthread_create(&consumer_thread, NULL, consumer_routine, &consumer_ctx) != 0) {
        log_fprintf(stderr, "Failed to create consumer thread\n");
        return 1;
    }

    log_printf("Running -- press Ctrl+C to shut down cleanly.\n");

    // Block here until SIGINT arrives; every other thread has it masked,
    // so this is the only place in the process it's ever observed.
    int caught_signal = 0;
    sigwait(&sigint_set, &caught_signal);
    log_printf("Caught signal %d, shutting down...\n", caught_signal);

    // Signal every thread to stop: the global flag for the handful of
    // waits a queue can't reach (producer's sleep, transport's/command
    // listener's poll loops), and a real condvar broadcast for every
    // queue a thread might be blocked inside enqueue/dequeue on.
    shutdown_request();
    msg_queue_shutdown(ingest_queue);
    msg_queue_shutdown(consumer_telemetry_q);
    msg_queue_shutdown(consumer_alerts_q);

    // Join in dependency order: the producer stops enqueueing before we
    // tear down the workers that drain ingest_queue; the workers (the
    // only publishers) stop before we touch the consumer's subscriptions;
    // transport unsubscribes/destroys its own per-connection queues
    // internally once its thread(s) return.
    pthread_join(producer_thread, NULL);
    worker_pool_destroy(workers);
    pthread_join(consumer_thread, NULL);

    pubsub_unsubscribe(ctx, TOPIC_TELEMETRY, consumer_telemetry_q);
    pubsub_unsubscribe(ctx, TOPIC_ALERTS, consumer_alerts_q);
    msg_queue_destroy(consumer_telemetry_q);
    msg_queue_destroy(consumer_alerts_q);

    transport_destroy(transport);
    msg_queue_destroy(ingest_queue);
    radio_control_destroy(control);
    pubsub_destroy(ctx);

    log_printf("Shutdown complete.\n");
    return 0;
}
