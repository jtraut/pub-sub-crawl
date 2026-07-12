#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include "msg_queue.h"
#include "pubsub.h"
#include "worker_pool.h"
#include "radio_msg.h"
#include "transport.h"
#include "log.h"

static const char *ALERT_CODES[] = { "ANTENNA_UNPLUGGED", "BATTERY_LOW" };
#define ALERT_CODE_COUNT (sizeof(ALERT_CODES) / sizeof(ALERT_CODES[0]))

void* producer_routine(void* arg) {
    // Producer doesn't talk to pubsub directly: it just drops raw,
    // not-yet-sequenced telemetry/alert items on the shared ingest queue,
    // and the worker pool assigns seq/ts and routes them.
    msg_queue_t *ingest_queue = (msg_queue_t *)arg;
    uint32_t counter = 0;

    // Infinite loop to produce a telemetry sample every second (and, every
    // fifth tick, an alert alongside it).
    while (1) {
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
            log_fprintf(stderr, "Failed to enqueue telemetry message\n");
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
            if (!msg_queue_enqueue(ingest_queue, &alert_item)) {
                log_fprintf(stderr, "Failed to enqueue alert message\n");
                free(alert);
                break;
            }
        }

        sleep(1);
    }
    return NULL;
}

// Define the consumer routine
void* consumer_routine(void* arg) {
    // A second, independent subscriber alongside the transport thread's own
    // telemetry/alerts subscriptions -- demonstrates that pubsub fan-out
    // reaches every subscriber, not just whichever one happens to be a
    // socket client.
    pubsub_context_t *ctx = (pubsub_context_t *)arg;
    msg_queue_t *telemetry_queue = msg_queue_create(0);
    if (pubsub_subscribe(ctx, TOPIC_TELEMETRY, telemetry_queue)) {
        log_fprintf(stderr, "Failed to subscribe to telemetry topic\n");
        return NULL;
    }
    msg_queue_t *alerts_queue = msg_queue_create(0);
    if (pubsub_subscribe(ctx, TOPIC_ALERTS, alerts_queue)) {
        log_fprintf(stderr, "Failed to subscribe to alerts topic\n");
        return NULL;
    }
    // Infinite loop to dequeue messages and process them
    while (1) {
        msg_item_t item;
        if (!msg_queue_dequeue(telemetry_queue, &item)) {
            log_fprintf(stderr, "Failed to dequeue telemetry message\n");
            break;
        }
        telemetry_msg_t *t = (telemetry_msg_t *)item.payload;
        log_printf("telemetry seq=%u battery_pct=%u rssi_dbm=%d\n", t->seq, t->battery_pct, t->rssi_dbm);
        free(item.payload);

        if (!msg_queue_try_dequeue(alerts_queue, &item)) {
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

    pthread_t producer_thread, consumer_thread;

    // Pubsub will manage the queues internally, so we just need a context to pass around
    pubsub_context_t *ctx = pubsub_create();

    // Shared ingest queue: the producer drops raw items here, and the
    // worker pool is what actually routes them into pubsub.
    msg_queue_t *ingest_queue = msg_queue_create(0);

    worker_pool_t *workers = worker_pool_create(WORKER_POOL_DEFAULT_SIZE, ingest_queue, ctx);
    if (!workers) {
        log_fprintf(stderr, "Failed to create worker pool\n");
        return 1;
    }

    transport_t *transport = transport_create(TRANSPORT_DEFAULT_SOCKET_PATH, ctx);
    if (!transport) {
        log_fprintf(stderr, "Failed to create transport\n");
        return 1;
    }

    // Spawn a producer thread that enqueues raw messages every second
    if (pthread_create(&producer_thread, NULL, producer_routine, ingest_queue) != 0) {
        log_fprintf(stderr, "Failed to create producer thread\n");
        return 1;
    }

    // Spawn a consumer thread that subscribes to the topic and dequeues messages
    if (pthread_create(&consumer_thread, NULL, consumer_routine, ctx) != 0) {
        log_fprintf(stderr, "Failed to create consumer thread\n");
        return 1;
    }

    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);
    worker_pool_destroy(workers);
    transport_destroy(transport);

    pubsub_destroy(ctx);
    return 0;
}
